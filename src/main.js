const { app, BrowserWindow, dialog, ipcMain, shell } = require('electron');
const path = require('path');
const fs = require('fs').promises;
const {
  getRepoSlugFromPackageJson,
  checkLatestGithubRelease,
} = require('./utils/releaseNotifier');

function createMainWindow() {
  const win = new BrowserWindow({
    width: 1280,
    height: 720,
    show: false,
    webPreferences: {
      devTools: false,
      nodeIntegration: true,
      contextIsolation: false,
      sandbox: false,
      preload: path.join(__dirname, 'preload.js'),
    },
  });

  win.webContents.once('dom-ready', () => {
    setTimeout(() => {
      win.show();
    }, 80);
  });

  win.loadFile(path.join(__dirname, 'renderer', 'index.html'));
  win.removeMenu();

  return win;
}

app.whenReady().then(() => {
  const win = createMainWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createMainWindow();
    }
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

ipcMain.handle('show-save-dialog', async (_evt, { defaultPath, filters }) => {
  const result = await dialog.showSaveDialog({
    defaultPath,
    filters,
  });
  return result;
});

ipcMain.handle('show-open-dialog', async (_evt, options) => {
  const result = await dialog.showOpenDialog({
    ...options,
  });
  return result;
});

ipcMain.handle('write-file', async (_evt, filePath, content) => {
  await fs.writeFile(filePath, content, 'utf8');
});

ipcMain.handle('read-file', async (_evt, filePath) => {
  const content = await fs.readFile(filePath, 'utf8');
  return content;
});

ipcMain.handle('show-item-in-folder', async (_evt, filePath) => {
  if (filePath && typeof filePath === 'string') {
    shell.showItemInFolder(filePath);
  }
});

ipcMain.handle('open-external', async (_evt, url) => {
  if (url && typeof url === 'string') {
    await shell.openExternal(url);
  }
});

ipcMain.handle('check-for-updates', async () => {
  const repo = getRepoSlugFromPackageJson(app.getAppPath()) || null;

  if (!repo) {
    console.error(
      'package.json repo:',
      getRepoSlugFromPackageJson(app.getAppPath())
    );
    return { ok: false, reason: 'no_repo' };
  }

  const currentVersion = app.getVersion();

  try {
    const res = await checkLatestGithubRelease({ repo, currentVersion });

    if (!res.ok) {
      console.error('[Update Check] API check failed:', res);
      return res;
    }

    if (!res.hasUpdate) {
      console.log('[Update Check] No update available. Current:', currentVersion, 'Latest:', res.latestVersion);
      return { ok: true, hasUpdate: false, currentVersion, latestVersion: res.latestVersion };
    }

    return {
      ok: true,
      hasUpdate: true,
      latestVersion: res.latestVersion,
      latestUrl: res.latestUrl,
      currentVersion,
    };
  } catch (e) {
    console.error('[Update Check] Error:', e);
    return { ok: false, reason: 'error', error: e.message };
  }
});
