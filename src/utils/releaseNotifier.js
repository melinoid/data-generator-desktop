const https = require('https');
const fs = require('fs');
const path = require('path');
const semver = require('semver');

function readJsonSafe(filePath) {
  try {
    if (!fs.existsSync(filePath)) return null;
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return null;
  }
}

function writeJsonSafe(filePath, obj) {
  try {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, JSON.stringify(obj, null, 2), 'utf8');
  } catch {}
}

function getRepoSlugFromPackageJson(appPath) {
  try {
    const pkgPath = path.join(appPath, 'package.json');
    const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8'));
    const url = pkg?.repository?.url;
    if (!url || typeof url !== 'string') return null;
    const m = url.match(/github\.com[:/](.+?)\/(.+?)(?:\.git)?$/i);
    if (!m) return null;
    return `${m[1]}/${m[2]}`;
  } catch {
    return null;
  }
}

function httpsJson(url, { headers = {} } = {}) {
  return new Promise((resolve, reject) => {
    const req = https.get(
      url,
      {
        headers: {
          Accept: 'application/vnd.github+json',
          'User-Agent': 'data-generator',
          ...headers,
        },
      },
      (res) => {
        let data = '';
        res.on('data', (c) => (data += c));
        res.on('end', () => {
          if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
            try {
              resolve(JSON.parse(data));
            } catch (e) {
              reject(new Error(`Не удалось разобрать JSON ответа: ${e.message}`));
            }
            return;
          }
          reject(new Error(`HTTP ${res.statusCode}: ${data?.slice?.(0, 300) || ''}`));
        });
      }
    );
    req.on('error', reject);
    req.end();
  });
}

function normalizeTagToVersion(tag) {
  if (!tag || typeof tag !== 'string') return null;
  const v = tag.startsWith('v') ? tag.slice(1) : tag;
  const clean = semver.valid(semver.coerce(v));
  return clean || null;
}

async function checkLatestGithubRelease({ repo, currentVersion, allowPrerelease = false }) {
  const apiUrl = `https://api.github.com/repos/${repo}/releases/latest`;
  const json = await httpsJson(apiUrl);
  const tagName = json?.tag_name;
  const htmlUrl = json?.html_url;
  const latest = normalizeTagToVersion(tagName);
  const current = normalizeTagToVersion(currentVersion);

  if (!latest || !current) {
    return {
      ok: false,
      reason: 'bad_version',
      latestUrl: htmlUrl || null,
    };
  }

  if (!allowPrerelease && semver.prerelease(latest)) {
    return {
      ok: true,
      hasUpdate: false,
      latestVersion: latest,
      latestUrl: htmlUrl || null,
      isPrerelease: true,
    };
  }

  return {
    ok: true,
    hasUpdate: semver.gt(latest, current),
    latestVersion: latest,
    latestUrl: htmlUrl || null,
  };
}

module.exports = {
  getRepoSlugFromPackageJson,
  checkLatestGithubRelease,
};
