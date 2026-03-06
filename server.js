require('dotenv').config();
const express            = require('express');
const mysql              = require('mysql2');
const multer             = require('multer');
const cors               = require('cors');
const crypto             = require('crypto');
const { Client }         = require('@elastic/elasticsearch');
const pdfParse           = require('pdf-parse');
const mammoth            = require('mammoth');
const Tesseract          = require('tesseract.js');
const { createCanvas, Image: CanvasImage } = require('canvas');
const pdfjsLib           = require('pdfjs-dist/legacy/build/pdf.js');
const fs                 = require('fs');
const path               = require('path');
const os                 = require('os');

// Polyfill Image for pdfjs-dist — it uses `new Image()` internally for JPEG rendering
if (!global.Image) global.Image = CanvasImage;

// Absolute path (no file:// prefix — require() needs a plain path, not a URL)
pdfjsLib.GlobalWorkerOptions.workerSrc =
  require.resolve('pdfjs-dist/legacy/build/pdf.worker.js');

const app      = express();
const esClient = new Client({ node: process.env.ES_NODE || 'http://localhost:9200' });
const EXTRACTS = path.join(__dirname, 'extracts');

// Ensure extracts folder exists
if (!fs.existsSync(EXTRACTS)) fs.mkdirSync(EXTRACTS);

// ── Auth helpers ──────────────────────────────────────────────
function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const hash = crypto.scryptSync(password, salt, 32).toString('hex');
  return `${salt}:${hash}`;
}

function verifyPassword(password, stored) {
  try {
    const [salt, hash] = stored.split(':');
    const derived = crypto.scryptSync(password, salt, 32);
    return crypto.timingSafeEqual(Buffer.from(hash, 'hex'), derived);
  } catch (_) { return false; }
}

// ── Auth ──────────────────────────────────────────────────────
const sessions   = new Map(); // token -> { user, expires }
const loginAttempts = new Map(); // ip -> { count, resetAt }
const MAX_ATTEMPTS  = 10;
const WINDOW_MS     = 15 * 60 * 1000; // 15 minutes

// Purge expired sessions every 30 minutes
setInterval(() => {
  const now = Date.now();
  for (const [token, s] of sessions) {
    if (s.expires < now) sessions.delete(token);
  }
  for (const [ip, a] of loginAttempts) {
    if (a.resetAt < now) loginAttempts.delete(ip);
  }
}, 30 * 60 * 1000);

function authRequired(req, res, next) {
  const header = req.headers['authorization'] || '';
  const token  = header.startsWith('Bearer ') ? header.slice(7) : null;
  const session = token ? sessions.get(token) : null;
  if (!session || session.expires < Date.now()) {
    if (token) sessions.delete(token);
    return res.status(401).json({ error: 'Unauthorized' });
  }
  req.user = session.user;
  next();
}

// ── File upload config ────────────────────────────────────────
const ALLOWED_MIME = new Set([
  'application/pdf',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  'text/plain', 'text/csv', 'text/markdown',
  'image/jpeg', 'image/png', 'image/gif', 'image/bmp', 'image/webp'
]);

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 50 * 1024 * 1024 }, // 50 MB
  fileFilter: (_req, file, cb) => {
    if (ALLOWED_MIME.has(file.mimetype)) cb(null, true);
    else cb(new Error(`File type not allowed: ${file.mimetype}`));
  }
});

app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));
app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'login.html')));

// ── Login / Logout (public) ───────────────────────────────────
app.post('/api/login', (req, res) => {
  const ip = req.ip;
  const now = Date.now();

  // Rate limit: block after MAX_ATTEMPTS in WINDOW_MS
  let attempt = loginAttempts.get(ip);
  if (!attempt || attempt.resetAt < now) {
    attempt = { count: 0, resetAt: now + WINDOW_MS };
    loginAttempts.set(ip, attempt);
  }
  if (attempt.count >= MAX_ATTEMPTS) {
    const wait = Math.ceil((attempt.resetAt - now) / 60000);
    return res.status(429).json({ error: `Too many attempts. Try again in ${wait} minute(s).` });
  }
  attempt.count++;

  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'Missing credentials' });

  db.query('SELECT password_hash FROM users WHERE username = ?', [username], (err, rows) => {
    if (err) return res.status(500).json({ error: 'Server error' });
    if (rows.length === 0 || !verifyPassword(password, rows[0].password_hash)) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }
    loginAttempts.delete(ip);
    const token = crypto.randomBytes(32).toString('hex');
    sessions.set(token, { user: username, expires: Date.now() + 8 * 60 * 60 * 1000 }); // 8h
    res.json({ token, username });
  });
});

app.post('/api/register', (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'Username and password are required' });
  if (username.length < 3)    return res.status(400).json({ error: 'Username must be at least 3 characters' });
  if (password.length < 6)    return res.status(400).json({ error: 'Password must be at least 6 characters' });

  const hash = hashPassword(password);
  db.query('INSERT INTO users (username, password_hash) VALUES (?, ?)', [username, hash], (err) => {
    if (err) {
      if (err.code === 'ER_DUP_ENTRY') return res.status(409).json({ error: 'Username already taken' });
      return res.status(500).json({ error: 'Server error' });
    }
    res.json({ success: true });
  });
});

app.post('/api/logout', (req, res) => {
  const header = req.headers['authorization'] || '';
  const token  = header.startsWith('Bearer ') ? header.slice(7) : null;
  if (token) sessions.delete(token);
  res.json({ success: true });
});

// All routes below require a valid session
app.use('/api', authRequired);

// ── Text extraction ───────────────────────────────────────────
async function extractText(buffer, filename) {
  const ext = filename.split('.').pop().toLowerCase();
  try {
    if (ext === 'pdf') {
      try {
        // native text extraction first
        const native = await pdfParse(buffer);
        const nativeText = (native.text || '').trim();

        if (nativeText.length > 100) {
          console.log(`Native PDF text (${nativeText.length} chars): ${filename}`);
          return nativeText;
        }

        console.log(`Sparse text, running OCR: ${filename}`);

        // avoids shared ArrayBuffer pool issues
        const uint8Array = new Uint8Array(buffer);

        // Factory that patches every context it creates so pdfjs internal
        // temp-canvas drawImage calls also go through the workaround
        const nodeCanvasFactory = {
          create(w, h) {
            const c   = createCanvas(w, h);
            const ctx = c.getContext('2d');
            nodeCanvasFactory._patch(ctx);
            return { canvas: c, context: ctx };
          },
          reset(cc, w, h) { cc.canvas.width = w; cc.canvas.height = h; },
          destroy(cc)     { cc.canvas.width = 0; cc.canvas.height = 0; },
          _patch(ctx) {
            const orig = ctx.drawImage.bind(ctx);
            ctx.drawImage = function (src, ...a) {
              if (src && src.data && src.width && src.height &&
                  !(src instanceof CanvasImage)) {
                const t = createCanvas(src.width, src.height);
                const tc = t.getContext('2d');
                const id = tc.createImageData(src.width, src.height);
                id.data.set(new Uint8ClampedArray(
                  ArrayBuffer.isView(src.data)
                    ? src.data.buffer.slice(src.data.byteOffset, src.data.byteOffset + src.data.byteLength)
                    : src.data
                ));
                tc.putImageData(id, 0, 0);
                return orig(t, ...a);
              }
              return orig(src, ...a);
            };
          }
        };

        const pdf = await pdfjsLib.getDocument({ data: uint8Array, canvasFactory: nodeCanvasFactory }).promise;
        let fullText = '';

        for (let pageNum = 1; pageNum <= pdf.numPages; pageNum++) {
          console.log(`OCR page ${pageNum}/${pdf.numPages}: ${filename}`);

          const page          = await pdf.getPage(pageNum);
          const viewport      = page.getViewport({ scale: 2.0 });
          const { canvas, context } = nodeCanvasFactory.create(
            Math.floor(viewport.width), Math.floor(viewport.height)
          );

          await page.render({ canvasContext: context, viewport, canvasFactory: nodeCanvasFactory }).promise;

          const tempPath = path.join(os.tmpdir(), `keepit_${Date.now()}_${pageNum}.png`);
          fs.writeFileSync(tempPath, canvas.toBuffer('image/png'));

          try {
            const { data: { text } } = await Tesseract.recognize(
              tempPath, 'eng',
              { logger: () => {}, langPath: __dirname }
            );
            fullText += text + '\n';
          } finally {
            fs.unlink(tempPath, () => {});
          }
        }

        const cleaned = fullText.trim();
        console.log(`OCR PDF text (${cleaned.length} chars): ${filename}`);
        return cleaned || '';

      } catch (err) {
        console.error(`PDF extraction failed for ${filename}:`, err.message);
        return '';
      }
    }
    if (ext === 'docx') {
      const result = await mammoth.extractRawText({ buffer });
      return result.value;
    }
    if (['txt', 'csv', 'md'].includes(ext)) {
      return buffer.toString('utf-8');
    }
    if (['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp'].includes(ext)) {
      const { data: { text } } = await Tesseract.recognize(buffer, 'eng', { logger: () => {} });
      return text;
    }
  } catch (e) {
    console.error(`Text extraction failed for ${filename}:`, e.message);
  }
  return '';
}

// ── MySQL connection pool ─────────────────────────────────────
const db = mysql.createPool({
  host:            process.env.DB_HOST,
  user:            process.env.DB_USER,
  password:        process.env.DB_PASSWORD,
  database:        process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit:    10,
  queueLimit:         0
});

// Verify connectivity and bootstrap tables
db.query('SELECT 1', err => {
  if (err) { console.error('MySQL connection failed:', err); process.exit(1); }
  console.log('Connected to MySQL');

  db.query(`
    CREATE TABLE IF NOT EXISTS documents (
      id          INT AUTO_INCREMENT PRIMARY KEY,
      name        VARCHAR(255) NOT NULL,
      size        BIGINT NOT NULL,
      type        VARCHAR(100),
      data        LONGBLOB NOT NULL,
      uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `, err => { if (err) console.error('Error creating documents table:', err); });

  db.query(`
    CREATE TABLE IF NOT EXISTS activity (
      id         INT AUTO_INCREMENT PRIMARY KEY,
      item       VARCHAR(255) NOT NULL,
      status     ENUM('Completed', 'Pending', 'Failed') NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `, err => { if (err) console.error('Error creating activity table:', err); });

  db.query(`
    CREATE TABLE IF NOT EXISTS users (
      id            INT AUTO_INCREMENT PRIMARY KEY,
      username      VARCHAR(100) NOT NULL UNIQUE,
      password_hash VARCHAR(255) NOT NULL,
      created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `, err => {
    if (err) { console.error('Error creating users table:', err); return; }
    // Seed initial user from .env if table is empty
    db.query('SELECT COUNT(*) AS count FROM users', (err, rows) => {
      if (err || rows[0].count > 0) return;
      const hash = hashPassword(process.env.AUTH_PASS || '123');
      db.query('INSERT INTO users (username, password_hash) VALUES (?, ?)',
        [process.env.AUTH_USER || 'user', hash],
        err => { if (err) console.error('Failed to seed initial user:', err); else console.log('Seeded initial user from .env'); }
      );
    });
  });

  // ── Elasticsearch index setup ───────────────────────────────
  esClient.indices.create({
    index: 'documents',
    mappings: {
      properties: {
        doc_id:  { type: 'integer' },
        name:    { type: 'text' },
        content: { type: 'text' }
      }
    }
  }).catch(e => {
    if (!e.message.includes('resource_already_exists_exception')) {
      console.error('ES index setup error:', e.message);
    }
  });
});

// ── Upload a file ─────────────────────────────────────────────
app.post('/api/upload', upload.single('file'), async (req, res) => {
  const { originalname, size, mimetype, buffer } = req.file;

  const sql = 'INSERT INTO documents (name, size, type, data) VALUES (?, ?, ?, ?)';
  db.query(sql, [originalname, size, mimetype, buffer], async (err, result) => {
    if (err) return res.status(500).json({ error: err.message });
    db.query('INSERT INTO activity (item, status) VALUES (?, ?)', [`Uploaded "${originalname}"`, 'Completed'], err2 => { if (err2) console.error('Activity log error:', err2); });

    // Extract text, save as .txt file, and index to Elasticsearch
    const text        = await extractText(buffer, originalname);
    const extractPath = path.join(EXTRACTS, `${result.insertId}.txt`);
    const header      = `Source: ${originalname}\nExtracted: ${new Date().toISOString()}\n\n`;

    fs.writeFile(extractPath, header + (text || '[No text could be extracted]'), err3 => {
      if (err3) console.error('Failed to save extract file:', err3.message);
      else console.log(`Saved extract: ${result.insertId}.txt`);
    });

    esClient.index({
      index: 'documents',
      id: String(result.insertId),
      document: { doc_id: result.insertId, name: originalname, content: text }
    }).catch(e => console.error('ES index error:', e.message));

    res.json({ id: result.insertId, name: originalname, size, type: mimetype });
  });
});

// ── Get all documents (metadata only, no blob) ────────────────
app.get('/api/documents', (req, res) => {
  db.query('SELECT id, name, size, type, uploaded_at FROM documents ORDER BY uploaded_at DESC', (err, rows) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json(rows);
  });
});

// ── Download a file ───────────────────────────────────────────
app.get('/api/documents/:id', (req, res) => {
  db.query('SELECT name, type, data FROM documents WHERE id = ?', [req.params.id], (err, rows) => {
    if (err || rows.length === 0) return res.status(404).json({ error: 'Not found' });
    const doc = rows[0];
    res.setHeader('Content-Type', doc.type);
    res.setHeader('Content-Disposition', `attachment; filename="${doc.name}"`);
    res.send(doc.data);
  });
});

// ── Delete a file ─────────────────────────────────────────────
app.delete('/api/documents/:id', (req, res) => {
  db.query('SELECT name FROM documents WHERE id = ?', [req.params.id], (err, rows) => {
    if (err || rows.length === 0) return res.status(404).json({ error: 'Not found' });
    const name = rows[0].name;
    db.query('DELETE FROM documents WHERE id = ?', [req.params.id], (err2) => {
      if (err2) return res.status(500).json({ error: err2.message });
      db.query('INSERT INTO activity (item, status) VALUES (?, ?)', [`Deleted "${name}"`, 'Completed'], err3 => { if (err3) console.error('Activity log error:', err3); });
      esClient.delete({ index: 'documents', id: String(req.params.id) })
        .catch(e => console.error('ES delete error:', e.message));
      // Delete the extract .txt file if it exists
      const extractPath = path.join(EXTRACTS, `${req.params.id}.txt`);
      fs.unlink(extractPath, () => {});
      res.json({ success: true });
    });
  });
});

// ── View a file inline (for browser preview) ─────────────────
app.get('/api/documents/:id/view', (req, res) => {
  db.query('SELECT name, type, data FROM documents WHERE id = ?', [req.params.id], (err, rows) => {
    if (err || rows.length === 0) return res.status(404).json({ error: 'Not found' });
    const doc = rows[0];
    res.setHeader('Content-Type', doc.type);
    res.setHeader('Content-Disposition', `inline; filename="${doc.name}"`);
    res.send(doc.data);
  });
});

// ── Get extracted text for a document ────────────────────────
app.get('/api/documents/:id/extract', (req, res) => {
  const extractPath = path.join(EXTRACTS, `${req.params.id}.txt`);
  if (!fs.existsSync(extractPath)) {
    return res.status(404).json({ error: 'No extract found for this document' });
  }
  res.setHeader('Content-Type', 'text/plain; charset=utf-8');
  res.sendFile(extractPath);
});

// ── Content search via Elasticsearch ─────────────────────────
app.get('/api/search/content', async (req, res) => {
  const q = (req.query.q || '').trim();
  if (!q) return res.json([]);
  try {
    const result = await esClient.search({
      index: 'documents',
      size: 50,
      query: { multi_match: { query: q, fields: ['name', 'content'] } },
      highlight: {
        fields: { content: { fragment_size: 160, number_of_fragments: 1 } }
      }
    });

    const keyword = q.toLowerCase();
    const hits = result.hits.hits.map(h => {
      const docId = h._source.doc_id;
      let matchCount = 0;
      try {
        const extractPath = path.join(EXTRACTS, `${docId}.txt`);
        if (fs.existsSync(extractPath)) {
          const text = fs.readFileSync(extractPath, 'utf-8').toLowerCase();
          let pos = 0;
          while ((pos = text.indexOf(keyword, pos)) !== -1) {
            matchCount++;
            pos += keyword.length;
          }
        }
      } catch (_) { /* ignore */ }
      return {
        id:         docId,
        name:       h._source.name,
        snippet:    h.highlight?.content?.[0] ?? '',
        matchCount
      };
    });

    hits.sort((a, b) => b.matchCount - a.matchCount);
    res.json(hits);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Get all activity ───────────────────────────────────────────
app.get('/api/activity', (_req, res) => {
  db.query('SELECT id, item, status, created_at FROM activity ORDER BY created_at DESC', (err, rows) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json(rows);
  });
});

// ── Add activity entry ─────────────────────────────────────────
app.post('/api/activity', (req, res) => {
  const { item, status } = req.body;
  if (!item || !status) return res.status(400).json({ error: 'item and status are required' });
  db.query('INSERT INTO activity (item, status) VALUES (?, ?)', [item, status], (err, result) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json({ id: result.insertId, item, status });
  });
});

// ── Delete activity entry ──────────────────────────────────────
app.delete('/api/activity/:id', (req, res) => {
  db.query('DELETE FROM activity WHERE id = ?', [req.params.id], (err) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json({ success: true });
  });
});

app.listen(3000, () => console.log('Server running at http://localhost:3000'));
