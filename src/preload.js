const { ipcRenderer } = require('electron');
const path = require('path');

window.api = {
  showSaveDialog: (options) => ipcRenderer.invoke('show-save-dialog', options),
  showOpenDialog: (options) => ipcRenderer.invoke('show-open-dialog', options),

  writeFile: (filePath, content) => ipcRenderer.invoke('write-file', filePath, content),
  readFile: (filePath) => ipcRenderer.invoke('read-file', filePath),

  showItemInFolder: (filePath) => ipcRenderer.invoke('show-item-in-folder', filePath),

  openExternal: (url) => ipcRenderer.invoke('open-external', url),

  checkForUpdates: () => ipcRenderer.invoke('check-for-updates'),

  path: {
    join: (...args) => path.join(...args),
  },
};
