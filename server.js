require('dotenv').config();
const express            = require('express');
const mysql              = require('mysql2');
const multer             = require('multer');
const cors               = require('cors');
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

const app        = express();
const upload     = multer({ storage: multer.memoryStorage() });
const esClient   = new Client({ node: process.env.ES_NODE || 'http://localhost:9200' });
const EXTRACTS   = path.join(__dirname, 'extracts');

// Ensure extracts folder exists
if (!fs.existsSync(EXTRACTS)) fs.mkdirSync(EXTRACTS);

app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));
app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'login.html')));

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

// ── MySQL connection ──────────────────────────────────────────
const db = mysql.createConnection({
  host:     process.env.DB_HOST,
  user:     process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME
});

db.connect(err => {
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
      query: { multi_match: { query: q, fields: ['name', 'content'] } },
      highlight: {
        fields: { content: { fragment_size: 160, number_of_fragments: 1 } }
      }
    });
    const hits = result.hits.hits.map(h => ({
      id:      h._source.doc_id,
      name:    h._source.name,
      snippet: h.highlight?.content?.[0] ?? ''
    }));
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
