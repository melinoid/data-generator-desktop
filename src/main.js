const { app, BrowserWindow, dialog, ipcMain, shell } = require('electron');
const path = require('path');
const fs = require('fs').promises;

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
