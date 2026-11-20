import assert from 'node:assert/strict';

const [major,minor] = process.versions.node.split('.').map(Number);
if (!((major === 22 && minor >= 13) || major === 24 || major === 26)) {
  throw Error('Use Node 22.13+, 24.x or 26.x with built-in node:sqlite.');
}
const {DatabaseSync} = await import('node:sqlite');
const db = new DatabaseSync(':memory:');
try {
  db.exec('CREATE TABLE probe(n INTEGER) STRICT; BEGIN IMMEDIATE; INSERT INTO probe VALUES(1); ROLLBACK');
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM probe').get().n,0);
  assert.equal([...db.prepare("SELECT json_extract(?, '$.feature') AS value").iterate('{"feature":1}')][0].value,1);
  console.log('Node',process.version,'SQLite',db.prepare('SELECT sqlite_version() AS version').get().version);
} finally { db.close(); }
