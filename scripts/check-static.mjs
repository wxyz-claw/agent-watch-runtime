import { spawnSync } from 'node:child_process';
import { closeSync, constants, fstatSync, lstatSync, openSync, opendirSync, readSync, realpathSync } from 'node:fs';
import { devNull } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const requiredJobs = ['static', 'node22', 'node24', 'node26'];
const maxEntries = 1024;
const maxFileBytes = 1024 * 1024;
const maxTreeBytes = 16 * maxFileBytes;
const decoder = new TextDecoder('utf-8', { fatal: true });
const sensitiveDirectory = /^(?:\.aws|\.git|\.gnupg|\.kube|\.ssh|node_modules)$/i;
const sensitiveFile = /^(?:\.env(?:\..+)?|\.git-credentials|\.netrc|\.npmrc|\.pypirc|id_(?:rsa|dsa|ecdsa|ed25519)(?:\.pub)?|credentials(?:\.(?:csv|json))?|secrets?\.(?:json|ya?ml|toml)|(?:service[-_]account|client[-_]secret)(?:[-_][\w.-]+)?\.json)$|\.(?:key|pem|p12|pfx|sqlite3?|db)(?:-(?:journal|shm|wal))?$/i;
const contentRules = [
  ['personal-path', /(?:\/(?:Users|home)\/[A-Za-z0-9][\w.-]*|[A-Za-z]:\\+Users\\+[A-Za-z0-9][\w.-]*)/i],
  ['private-key', /-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY-----/],
  ['token-format', /\b(?:AKIA[A-Z0-9]{16}|ASIA[A-Z0-9]{16}|gh[oprsu]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,}|npm_[A-Za-z0-9]{30,}|xox[baprs]-[A-Za-z0-9-]{16,}|sk-(?:proj-)?[A-Za-z0-9_-]{24,})\b/],
  ['credential-url', /\bhttps?:\/\/[^\s/@:?]+:[^\s/@]+@/i],
  ['credential-literal', /["']?(?:access[_-]?token|api[_-]?key|client[_-]?secret|password)["']?\s*[:=]\s*["'][A-Za-z0-9+/_=-]{20,}["']/i],
];

class GateError extends Error {}
function fail(code) { throw new GateError(code); }

function checkName(name) {
  if (typeof name !== 'string' || !name || name.length > 4096 || /[\\\x00-\x1f\x7f]/.test(name)) fail('unsafe-path');
  const parts = name.split('/');
  if (parts.some(part => !part || part === '.' || part === '..' || sensitiveDirectory.test(part))) fail('unsafe-path');
  if (sensitiveFile.test(parts.at(-1))) fail('sensitive-filename');
}

function git(directory, args) {
  const command = spawnSync('git', args, {
    cwd: directory, maxBuffer: 2 * maxFileBytes, timeout: 10_000,
    env: { PATH: process.env.PATH, GIT_CONFIG_GLOBAL: devNull, GIT_CONFIG_NOSYSTEM: '1', GIT_OPTIONAL_LOCKS: '0' },
    stdio: ['ignore', 'pipe', 'ignore'],
  });
  if (command.status !== 0 || command.error) fail('git-inventory');
  return decoder.decode(command.stdout);
}

function trackedFiles(directory) {
  if (realpathSync(git(directory, ['rev-parse', '--show-toplevel']).trimEnd()) !== directory) fail('git-root');
  const records = git(directory, ['ls-files', '--cached', '--stage', '-z']).split('\0');
  if (records.pop() !== '' || !records.length || records.length > maxEntries) fail('git-inventory');
  return records.map(record => {
    const tracked = /^(?:100644|100755) [0-9a-f]{40,64} 0\t(.+)$/s.exec(record);
    if (!tracked) fail('nonregular-or-unmerged-entry');
    return tracked[1];
  });
}

function exportedFiles(directory) {
  const files = [];
  let count = 0;
  function walk(parent) {
    const dir = opendirSync(join(directory, parent));
    try {
      for (let item; (item = dir.readSync());) {
        if (++count > maxEntries) fail('inventory-limit');
        const name = parent ? parent + '/' + item.name : item.name;
        checkName(name);
        if (item.isDirectory()) walk(name);
        else if (item.isFile()) files.push(name);
        else fail('nonregular-entry');
      }
    } finally {
      dir.closeSync();
    }
  }
  walk('');
  return files;
}

function readText(directory, name, budget) {
  // Reject links in parent components as well as linked files. Tracked paths
  // can have a different filesystem type in a modified working tree.
  let path = directory;
  let info;
  for (const component of name.split('/')) {
    path = join(path, component);
    info = lstatSync(path);
    if (info.isSymbolicLink()) fail('linked-entry');
  }
  if (!info.isFile() || info.size > maxFileBytes || budget.total + info.size > maxTreeBytes) fail('file-limit-or-type');
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const opened = fstatSync(fd);
    if (!opened.isFile() || opened.size !== info.size || opened.ino !== info.ino) fail('changed-entry');
    const bytes = Buffer.alloc(info.size + 1);
    let length = 0;
    while (length < bytes.length) {
      const n = readSync(fd, bytes, length, bytes.length - length, null);
      if (!n) break;
      length += n;
    }
    if (length !== info.size) fail('changed-entry');
    const content = bytes.subarray(0, length);
    budget.total += length;
    if (content.includes(0)) fail('non-utf8');
    try { return decoder.decode(content); } catch { fail('non-utf8'); }
  } finally {
    closeSync(fd);
  }
}

function checkManifest(pkg, inventory, scope) {
  if (!pkg || typeof pkg.name !== 'string' || !pkg.name || typeof pkg.version !== 'string' || !pkg.version ||
      pkg.license !== 'MIT' || pkg.private !== true || pkg.type !== 'module' ||
      !Array.isArray(pkg.files) || !pkg.files.length || pkg.files.length > maxEntries) fail('package-manifest');
  const deps = ['dependencies', 'devDependencies', 'optionalDependencies', 'peerDependencies', 'bundleDependencies', 'bundledDependencies'];
  if (deps.some(field => Object.hasOwn(pkg, field) && (!pkg[field] || typeof pkg[field] !== 'object' || Object.keys(pkg[field]).length))) {
    fail('package-dependencies');
  }
  const identities = JSON.stringify([pkg.author, pkg.contributors, pkg.maintainers]);
  if (/[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}/.test(identities)) fail('manifest-contact');
  const allowed = new Set(['package.json', 'README.md', 'LICENSE']);
  for (const path of pkg.files) {
    checkName(path);
    if (/[!*?\[\]{}]/.test(path) || allowed.has(path)) fail('manifest-files');
    allowed.add(path);
  }
  if ([...allowed].some(path => !inventory.has(path))) fail('missing-package-file');
  if (scope === 'package' && inventory.size !== allowed.size) fail('package-inventory');
  function localTargets(target, allowBare = false) {
    if (typeof target === 'string') {
      const file = target.startsWith('./') ? target.slice(2) : allowBare ? target : null;
      if (!allowed.has(file)) fail('manifest-entrypoint');
    } else if (target && typeof target === 'object') {
      for (const value of Object.values(target)) localTargets(value, allowBare);
    } else fail('manifest-entrypoint');
  }
  localTargets(pkg.bin, true);
  localTargets(pkg.exports);
}

/** Check tracked working files, a source archive, or the exact unpacked npm payload. */
export function checkStatic(path = root, mode = 'auto') {
  const directory = realpathSync(path);
  const marker = lstatSync(join(directory, '.git'), { throwIfNoEntry: false });
  let scope = mode;
  if (scope === 'auto') scope = marker ? 'tracked' : 'export';
  if (!['tracked', 'export', 'package'].includes(scope)) fail('invalid-mode');
  if (scope === 'tracked' && (!marker || !(marker.isFile() || marker.isDirectory()))) fail('checkout-required');
  if (scope !== 'tracked' && marker) fail('export-required');
  const files = (scope === 'tracked' ? trackedFiles(directory) : exportedFiles(directory)).sort();
  const inventory = new Set(files);
  if (!files.length || inventory.size !== files.length || !inventory.has('package.json')) fail('incomplete-inventory');
  let manifest;
  let lock;
  const budget = { total: 0 };
  let modules = 0;
  files.forEach((name, index) => {
    try {
      checkName(name);
      const text = readText(directory, name, budget);
      for (const [rule, pattern] of contentRules) if (pattern.test(name) || pattern.test(text)) fail(rule);
      if (name === 'package.json') manifest = JSON.parse(text);
      if (name === 'package-lock.json') lock = JSON.parse(text);
      if (/\.[cm]?js$/.test(name)) {
        const type = name.endsWith('.cjs') ? 'commonjs' : 'module';
        const check = spawnSync(process.execPath, ['--check', '--input-type=' + type], {
          input: text, timeout: 10_000, env: {}, stdio: ['pipe', 'ignore', 'ignore'],
        });
        if (check.status !== 0 || check.error) fail('module-syntax');
        modules++;
      }
    } catch (error) {
      // Keep file contents, paths and parser diagnostics out of public CI logs.
      fail((error instanceof GateError ? error.message : 'unreadable-entry') + ' at inventory entry ' + (index + 1));
    }
  });
  if (!modules) fail('missing-modules');
  checkManifest(manifest, inventory, scope);
  if (inventory.has('package-lock.json') && (!lock || lock.name !== manifest.name ||
      lock.version !== manifest.version || !lock.packages || Object.keys(lock.packages).length !== 1 ||
      !Object.hasOwn(lock.packages, '') || Object.keys(lock.dependencies ?? {}).length)) fail('package-lock');
  return { scope, files: files.length, modules };
}

export function checkQualityGate(raw) {
  if (typeof raw !== 'string' || raw.length > 32_768) fail('missing-job-results');
  let results;
  try { results = JSON.parse(raw); } catch { fail('invalid-job-results'); }
  if (!results || typeof results !== 'object' || Array.isArray(results) ||
      Object.keys(results).length !== requiredJobs.length) fail('incomplete-job-results');
  for (const name of requiredJobs) {
    if (!Object.hasOwn(results, name) || results[name]?.result !== 'success') fail('required-job-' + name);
  }
  return { gate: 'quality-gate', passed: requiredJobs.length };
}

if (process.argv[1] && lstatSync(process.argv[1], { throwIfNoEntry: false }) &&
    realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const args = process.argv.slice(2);
    if (args.length > 1 || (args.length && !/^--(?:tracked|export|package|quality-gate)$/.test(args[0]))) fail('invalid-mode');
    const result = args[0] === '--quality-gate' ? checkQualityGate(process.env.WATCH_CI_RESULTS) :
      checkStatic(root, args.length ? args[0].slice(2) : 'auto');
    console.log(JSON.stringify(result));
  } catch (error) {
    console.error('Static gate failed (' + (error instanceof GateError ? error.message : 'unreadable-input') + ')');
    process.exitCode = 1;
  }
}
