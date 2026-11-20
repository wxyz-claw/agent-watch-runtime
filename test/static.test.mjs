import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { devNull, tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { checkQualityGate, checkStatic } from '../scripts/check-static.mjs';

function workspace(t) {
  const directory = mkdtempSync(join(tmpdir(), 'watch-static-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return directory;
}

function fixture(directory) {
  mkdirSync(join(directory, 'src'), { recursive: true });
  const pkg = {
    name: 'generated-watch-fixture', version: '1.0.0', private: true, license: 'MIT', type: 'module',
    files: ['src/demo.mjs'], bin: { demo: 'src/demo.mjs' }, exports: { '.': './src/demo.mjs' },
  };
  writeFileSync(join(directory, 'package.json'), JSON.stringify(pkg));
  writeFileSync(join(directory, 'src/demo.mjs'), 'export const ready = true;');
  writeFileSync(join(directory, 'README.md'), 'Generated sample');
  writeFileSync(join(directory, 'LICENSE'), 'MIT');
  return pkg;
}

test('an export is scanned without Git and the package gate requires exact inventory', t => {
  const dir = workspace(t);
  fixture(dir);
  assert.deepEqual(checkStatic(dir), { scope: 'export', files: 4, modules: 1 });
  assert.deepEqual(checkStatic(dir, 'package'), { scope: 'package', files: 4, modules: 1 });
  assert.throws(() => checkStatic(dir, 'tracked'), /checkout-required/);
  writeFileSync(join(dir, 'notes.md'), 'Extra source documentation');
  assert.equal(checkStatic(dir, 'export').files, 5);
  assert.throws(() => checkStatic(dir, 'package'), /package-inventory/);
  rmSync(join(dir, 'src/demo.mjs'));
  assert.throws(() => checkStatic(dir), /missing-modules|missing-package-file/);
});

test('privacy failures return fixed rule names and omit matched data', t => {
  const dir = workspace(t);
  fixture(dir);
  const samples = [
    [['', 'Users', 'generated', 'work'].join('/'), 'personal-path'],
    [['C:', 'Users', 'generated', 'work'].join('\\'), 'personal-path'],
    ['-----BEGIN ' + 'PRIVATE KEY-----', 'private-key'],
    ['ghp_' + 'x'.repeat(36), 'token-format'],
    ['https://' + 'sample' + ':' + 'fixture@localhost', 'credential-url'],
    ['apiKey: "' + 'y'.repeat(24) + '"', 'credential-literal'],
  ];
  for (const [value, rule] of samples) {
    writeFileSync(join(dir, 'README.md'), value);
    assert.throws(() => checkStatic(dir), error => error.message.startsWith(rule) && !error.message.includes(value));
  }
  writeFileSync(join(dir, 'README.md'), '');
  writeFileSync(join(dir, '.env.local'), 'generated fixture');
  assert.throws(() => checkStatic(dir), /sensitive-filename/);
});

test('gate checks syntax without running modules and rejects invalid manifest contents and links', t => {
  const dir = workspace(t);
  const pkg = fixture(dir);
  const script = join(dir, 'src/demo.mjs');
  writeFileSync(script, 'throw Error("the syntax check must not run this module");');
  assert.equal(checkStatic(dir).modules, 1);
  writeFileSync(script, 'export const = ;');
  assert.throws(() => checkStatic(dir), /module-syntax/);
  writeFileSync(script, 'export default true;');
  for (const [addition, expected] of [
    [{ exports: './missing.mjs' }, /manifest-entrypoint/],
    [{ dependencies: { fixture: '1' } }, /package-dependencies/],
    [{ files: ['src/*.mjs'] }, /manifest-files/],
    [{ files: ['src/missing.mjs'] }, /missing-package-file/],
  ]) {
    writeFileSync(join(dir, 'package.json'), JSON.stringify({ ...pkg, ...addition }));
    assert.throws(() => checkStatic(dir), expected);
  }
  writeFileSync(join(dir, 'package.json'), JSON.stringify(pkg));
  symlinkSync('README.md', join(dir, 'alias.md'));
  assert.throws(() => checkStatic(dir), /nonregular-entry/);
});

test('tracked mode resolves a Git metadata file, checks tracked files and refuses a broken checkout', t => {
  const directory = workspace(t);
  const work = join(directory, 'tree');
  const env = { PATH: process.env.PATH, GIT_CONFIG_GLOBAL: devNull, GIT_CONFIG_NOSYSTEM: '1' };
  const run = args => spawnSync('git', args, { env, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
  if (run(['--version']).error?.code === 'ENOENT') return t.skip('Git-specific fixture requires Git');
  assert.equal(run(['init', '--quiet', '--template=', '--separate-git-dir=' + join(directory, 'metadata'), work]).status, 0);
  assert.match(readFileSync(join(work, '.git'), 'utf8'), /^gitdir: /);
  fixture(work);
  assert.equal(run(['-C', work, 'add', '--', 'src', 'README.md', 'LICENSE', 'package.json']).status, 0);
  writeFileSync(join(work, 'local-state.key'), 'generated fixture');
  assert.deepEqual(checkStatic(work), { scope: 'tracked', files: 4, modules: 1 });
  assert.throws(() => checkStatic(work, 'export'), /export-required/);
  assert.equal(run(['-C', work, 'add', '--', 'local-state.key']).status, 0);
  assert.throws(() => checkStatic(work, 'tracked'), /sensitive-filename/);
  writeFileSync(join(work, '.git'), 'gitdir: missing\n');
  assert.throws(() => checkStatic(work), /git-inventory/);
});

test('quality gate requires the success result of every named prerequisite', () => {
  const names = ['static', 'node22', 'node24', 'node26'];
  const jobs = Object.fromEntries(names.map(name => [name, { result: 'success' }]));
  assert.deepEqual(checkQualityGate(JSON.stringify(jobs)), { gate: 'quality-gate', passed: 4 });
  for (const name of names) {
    for (const result of ['failure', 'skipped', 'cancelled', '', undefined]) {
      assert.throws(() => checkQualityGate(JSON.stringify({ ...jobs, [name]: { result } })), /required-job/);
    }
    const missing = { ...jobs };
    delete missing[name];
    assert.throws(() => checkQualityGate(JSON.stringify(missing)), /incomplete-job-results/);
  }
  const script = fileURLToPath(new URL('../scripts/check-static.mjs', import.meta.url));
  for (const [raw, status] of [[JSON.stringify(jobs), 0], ['{}', 1], ['{', 1]]) {
    const cli = spawnSync(process.execPath, [script, '--quality-gate'], { env: { WATCH_CI_RESULTS: raw } });
    assert.equal(cli.status, status);
  }
});
