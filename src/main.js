const { app, BrowserWindow, dialog, ipcMain, shell } = require('electron');
const path = require('path');
const fs = require('fs').promises;
const {
  getRepoSlugFromPackageJson,
  checkLatestGithubRelease,
  shouldNotify,
  markNotified,
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

async function maybeNotifyAboutNewRelease({ win, silent = true } = {}) {
  const repo =
    process.env.GITHUB_REPO ||
    getRepoSlugFromPackageJson(app.getAppPath()) ||
    null;

  if (!repo) {
    if (!silent) {
      await dialog.showMessageBox(win, {
        type: 'info',
        title: 'Обновления',
        message:
          'Репозиторий GitHub не настроен.\n\nУкажите переменную окружения GITHUB_REPO=owner/repo или заполните поле "repository.url" в package.json.',
      });
    }
    return { ok: false, reason: 'no_repo' };
  }

  const statePath = path.join(app.getPath('userData'), 'update-state.json');
  const currentVersion = app.getVersion();

  try {
    const res = await checkLatestGithubRelease({ repo, currentVersion });
    if (!res.ok) return res;

    if (!res.hasUpdate) {
      if (!silent) {
        await dialog.showMessageBox(win, {
          type: 'info',
          title: 'Обновления',
          message: `У вас установлена актуальная версия (${currentVersion}).`,
        });
      }
      return { ok: true, hasUpdate: false };
    }

    if (!shouldNotify({ statePath, latestVersion: res.latestVersion }) && silent) {
      return { ok: true, hasUpdate: true, suppressed: true };
    }

    const result = await dialog.showMessageBox(win, {
      type: 'info',
      title: 'Доступно обновление',
      message: `Доступна новая версия: ${res.latestTag || res.latestVersion}\nТекущая версия: ${currentVersion}`,
      buttons: ['Открыть релиз', 'Позже'],
      defaultId: 0,
      cancelId: 1,
      noLink: true,
    });

    markNotified({ statePath, latestVersion: res.latestVersion });

    if (result.response === 0 && res.latestUrl) {
      await shell.openExternal(res.latestUrl);
    }

    return { ok: true, hasUpdate: true, latestVersion: res.latestVersion, latestUrl: res.latestUrl };
  } catch (e) {
    if (!silent) {
      await dialog.showMessageBox(win, {
        type: 'warning',
        title: 'Обновления',
        message: `Не удалось проверить обновления: ${e.message}`,
      });
    }
    return { ok: false, reason: 'error', error: e.message };
  }
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

ipcMain.handle('check-for-updates', async (evt, { silent } = {}) => {
  const repo =
    process.env.GITHUB_REPO ||
    getRepoSlugFromPackageJson(app.getAppPath()) ||
    null;

  if (!repo) {
    return { ok: false, reason: 'no_repo' };
  }

  const statePath = path.join(app.getPath('userData'), 'update-state.json');
  const currentVersion = app.getVersion();

  try {
    const res = await checkLatestGithubRelease({ repo, currentVersion });
    if (!res.ok) return res;

    if (!res.hasUpdate) {
      return { ok: true, hasUpdate: false, currentVersion };
    }

    // Проверяем, нужно ли показывать уведомление
    const shouldShow = shouldNotify({ statePath, latestVersion: res.latestVersion });

    if (shouldShow) {
      markNotified({ statePath, latestVersion: res.latestVersion });
    }

    return {
      ok: true,
      hasUpdate: true,
      latestVersion: res.latestVersion,
      latestTag: res.latestTag,
      latestUrl: res.latestUrl,
      currentVersion,
      shouldShow,
    };
  } catch (e) {
    return { ok: false, reason: 'error', error: e.message };
  }
});
