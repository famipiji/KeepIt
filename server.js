require('dotenv').config();
const express = require('express');
const mysql   = require('mysql2');
const multer  = require('multer');
const cors    = require('cors');

const app    = express();
const upload = multer({ storage: multer.memoryStorage() }); // store file in memory buffer

app.use(cors());
app.use(express.json());

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
});

// ── Upload a file ─────────────────────────────────────────────
app.post('/api/upload', upload.single('file'), (req, res) => {
  const { originalname, size, mimetype, buffer } = req.file;

  const sql = 'INSERT INTO documents (name, size, type, data) VALUES (?, ?, ?, ?)';
  db.query(sql, [originalname, size, mimetype, buffer], (err, result) => {
    if (err) return res.status(500).json({ error: err.message });
    db.query('INSERT INTO activity (item, status) VALUES (?, ?)', [`Uploaded "${originalname}"`, 'Completed'], err2 => { if (err2) console.error('Activity log error:', err2); });
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
      res.json({ success: true });
    });
  });
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
