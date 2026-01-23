const fs = require('fs');
const path = require('path');

function read(file) {
  return fs.readFileSync(file, 'utf8');
}

function write(file, content) {
  fs.writeFileSync(file, content, 'utf8');
}

function escapeRegExp(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function replaceOrThrow(content, regex, replacer, fileLabel) {
  if (!regex.test(content)) {
    throw new Error(`Не найден шаблон для обновления версии в ${fileLabel}`);
  }
  return content.replace(regex, replacer);
}

const repoRoot = path.join(__dirname, '..');
const pkgPath = path.join(repoRoot, 'package.json');
const pkg = JSON.parse(read(pkgPath));
const version = pkg.version;
if (!version) throw new Error('Не удалось прочитать version из package.json');

// README.md
const readmePath = path.join(repoRoot, 'README.md');
let readme = read(readmePath);
readme = replaceOrThrow(
  readme,
  /(\*\*Версия\*\*:\s*)(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)/,
  `$1${version}`,
  'README.md'
);
write(readmePath, readme);

// index.html
const indexHtmlPath = path.join(repoRoot, 'src', 'renderer', 'index.html');
let html = read(indexHtmlPath);
html = replaceOrThrow(
  html,
  new RegExp(`(<title>\\s*Test Data Generator\\s+v)([^<]+)(\\s*<\\/title>)`, 'i'),
  `$1${version}$3`,
  'src/renderer/index.html'
);
write(indexHtmlPath, html);

console.log(`OK: version synced to ${version}`);
