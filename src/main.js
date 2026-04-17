const { app, BrowserWindow, dialog, ipcMain, shell } = require('electron');
const path = require('path');
const fs = require('fs').promises;
const os = require('os');
const { getRepoSlugFromPackageJson, checkLatestGithubRelease } = require('./utils/releaseNotifier');

// Парсеры файлов
const { parseXLSX, parseCSV, parseJSON, parseXML, parseQVD } = require('./utils/fileParser');

// CLI SUPPORT
function parseCliArgs() {
  const args = process.argv.slice(2);
  const config = { mode: 'gui', configFile: null, attachFiles: [] };

  for (let i = 0; i < args.length; i++) {
    if ((args[i] === '-f' || args[i] === '--file') && args[i + 1]) {
      config.mode = 'cli';
      config.configFile = path.resolve(args[i + 1]);
      i++;
    } else if ((args[i] === '-a' || args[i] === '--attach') && args[i + 1]) {
      config.attachFiles.push(path.resolve(args[i + 1]));
      i++;
    }
  }

  return config;
}

function getDocumentsPath() {
  return path.join(os.homedir(), 'Documents');
}

function buildFilename(baseName, format) {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  const hours = String(now.getHours()).padStart(2, '0');
  const minutes = String(now.getMinutes()).padStart(2, '0');
  const seconds = String(now.getSeconds()).padStart(2, '0');
  const cleanBase = (baseName || 'data').trim().replace(/[<>:"/\\|?*]+/g, '_');
  return `${cleanBase}_${year}-${month}-${day}_${hours}-${minutes}-${seconds}.${format}`;
}

function loadDataSources(genType = 'advanced', language = 'ru') {
  const isLight = genType === 'simple';
  const base = path.join(__dirname, 'data');
  const light = path.join(base, 'lightData');

  const loadWithCheck = (fullPath, lightPath, key) => {
    const filePath = isLight && lightPath ? lightPath : fullPath;
    const data = require(filePath);
    if (!data?.[language]) {
      console.warn(
        `   ⚠️ В источнике "${key}" нет данных для языка "${language}". Доступные: ${
          Object.keys(data).join(', ') || 'нет'
        }`
      );
    }
    return data;
  };

  return {
    countriesData: loadWithCheck(
      path.join(base, 'countriesData.json'),
      path.join(light, 'countriesData.json'),
      'countriesData'
    ),
    genderData: require(path.join(base, 'genderTypes.json')),
    namesData: loadWithCheck(path.join(base, 'namesData.json'), path.join(light, 'namesData.json'), 'namesData'),
    regionsData: loadWithCheck(
      path.join(base, 'regionsData.json'),
      path.join(light, 'regionsData.json'),
      'regionsData'
    ),
    citiesData: loadWithCheck(path.join(base, 'citiesData.json'), path.join(light, 'citiesData.json'), 'citiesData'),
    streetsData: loadWithCheck(
      path.join(base, 'streetsData.json'),
      path.join(light, 'streetsData.json'),
      'streetsData'
    ),
  };
}

/* Загружает внешние файлы данных из конфига и парсит их в формат dataSourceCache */
async function loadExternalDataFiles(config, configDir) {
  const files = config.externalDataFiles || [];
  if (!files.length) return [];

  console.log('Загрузка внешних файлов данных...');
  const externalData = [];

  for (const fileConfig of files) {
    const filePath = path.isAbsolute(fileConfig.path) ? fileConfig.path : path.resolve(configDir, fileConfig.path);

    try {
      if (
        !(await fs
          .access(filePath)
          .then(() => true)
          .catch(() => false))
      ) {
        throw new Error(`Файл не найден: ${filePath}`);
      }

      const buffer = await fs.readFile(filePath);
      const ext = path.extname(filePath).toLowerCase();
      const baseName = path.basename(filePath);

      // Унифицируем генерацию ключа с renderer.js (сохраняем расширение, схлопываем подчёркивания)
      const safeKey = baseName.replace(/_{2,}/g, '_').replace(/^_+|_+$/g, '');

      if (ext === '.xlsx') {
        const sheets = parseXLSX(buffer);
        for (const sheet of sheets) {
          const safeSheet = sheet.sheetName.replace(/[^a-zA-Z0-9._-]/g, '_');
          const key = `${safeKey}_${safeSheet}`;
          externalData.push({
            name: `${baseName} / ${sheet.sheetName}`,
            key,
            headers: sheet.headers,
            rows: sheet.rows,
          });
        }
      } else if (ext === '.csv') {
        const parsed = parseCSV(buffer.toString('utf8'));
        externalData.push({ name: baseName, key: safeKey, headers: parsed.headers, rows: parsed.rows });
      } else if (ext === '.json') {
        const parsed = parseJSON(buffer.toString('utf8'));
        externalData.push({ name: baseName, key: safeKey, headers: parsed.headers, rows: parsed.rows });
      } else if (ext === '.xml') {
        console.warn(`⚠️ Парсинг XML в CLI режиме временно ограничен (требует DOMParser). Пропущено: ${filePath}`);
      } else if (ext === '.qvd') {
        const parsed = await parseQVD(buffer);
        externalData.push({ name: baseName, key: safeKey, headers: parsed.headers, rows: parsed.rows });
      } else {
        console.warn(`⚠️ Пропущен неподдерживаемый формат: ${filePath}`);
      }
    } catch (err) {
      console.error(`✗ Ошибка загрузки ${fileConfig.path}: ${err.message}`);
    }
  }

  console.log(`✅ Загружено ${externalData.length} внешних источников.`);
  return externalData;
}

function normalizeConfig(rawConfig) {
  const config = { ...rawConfig };

  // Поддержка rowsCount и rows
  const rawRows = config.rowsCount ?? config.rows;
  const parsedRows = Number(rawRows);
  config.rows = Number.isFinite(parsedRows) && parsedRows > 0 ? Math.floor(parsedRows) : 1000;

  // Валидация колонок
  if (!Array.isArray(config.columns) || config.columns.length === 0) {
    throw new Error('   Конфигурация должна содержать непустой массив "columns"');
  }

  config.columns = config.columns.map((col, idx) => {
    let colType = col.type === 'transformation' ? 'null' : col.type || 'id_auto';
    return {
      name: col.name?.trim() || `column_${idx + 1}`,
      type: colType,
      ...col,
    };
  });

  // 3. Дефолты
  config.format = config.format || 'csv';
  config.language = config.language || 'ru';
  config.generationType = config.generationType || 'advanced';
  config.filenameBase = config.filenameBase?.trim() || 'data';
  config.csvSeparator = config.csvSeparator || ',';

  return config;
}

/**
 * Проверяет, все ли внешние файлы, на которые ссылаются колонки, были успешно загружены.
 * @param {Array} columns - Массив конфигурации колонок
 * @param {Array} loadedExternalData - Массив загруженных внешних данных
 * @returns {Array} Массив объектов с информацией о недостающих файлах
 */
function checkMissingExternalFiles(columns, loadedExternalData) {
  const loadedKeys = new Set(loadedExternalData.map(d => d.key));
  const missing = [];

  for (const col of columns) {
    // Проверяем тип вида file_<KEY>_col_<INDEX>
    if (col.type && col.type.startsWith('file_') && col.type.includes('_col_')) {
      // Надёжное извлечение ключа между file_ и _col_
      const match = col.type.match(/^file_(.*)_col_(\d+)$/);
      if (match) {
        const expectedKey = match[1];
        if (!loadedKeys.has(expectedKey)) {
          const displayName = col.sourceName || `файл с ключом "${expectedKey}"`;
          missing.push({ column: col, expectedKey, displayName });
        }
      }
    }
  }

  return missing;
}

async function runCliGeneration(configPath, attachFiles = []) {
  try {
    console.log(`Загрузка конфигурации: ${configPath}`);
    const configDir = path.dirname(configPath);
    const rawConfig = JSON.parse(await fs.readFile(configPath, 'utf8'));
    const config = normalizeConfig(rawConfig);

    console.log(`Параметры: ${config.rows} строк | Формат: ${config.format} | Язык: ${config.language}`);

    // Загрузка внутренних данных
    const dataSources = loadDataSources(config.generationType, config.language);

    // Загрузка внешних данных из конфига
    let externalData = await loadExternalDataFiles(config, configDir);

    // Загрузка файлов, прикреплённых через флаг -a
    if (attachFiles.length > 0) {
      console.log(`Загрузка ${attachFiles.length} прикреплённых файлов...`);
      for (const filePath of attachFiles) {
        try {
          if (
            !(await fs
              .access(filePath)
              .then(() => true)
              .catch(() => false))
          ) {
            throw new Error(`Файл не найден: ${filePath}`);
          }

          const buffer = await fs.readFile(filePath);
          const ext = path.extname(filePath).toLowerCase();
          const baseName = path.basename(filePath);

          // Унифицируем генерацию ключа с renderer.js
          const safeKey = baseName.replace(/_{2,}/g, '_').replace(/^_+|_+$/g, '');

          if (ext === '.xlsx') {
            const sheets = parseXLSX(buffer);
            for (const sheet of sheets) {
              const safeSheet = sheet.sheetName.replace(/[^a-zA-Z0-9._-]/g, '_');
              const key = `${safeKey}_${safeSheet}`;
              // Не добавляем дубликаты
              if (!externalData.some(d => d.key === key)) {
                externalData.push({
                  name: `${baseName} / ${sheet.sheetName}`,
                  key,
                  headers: sheet.headers,
                  rows: sheet.rows,
                });
              }
            }
          } else if (ext === '.csv') {
            const parsed = parseCSV(buffer.toString('utf8'));
            if (!externalData.some(d => d.key === safeKey)) {
              externalData.push({ name: baseName, key: safeKey, headers: parsed.headers, rows: parsed.rows });
            }
          } else if (ext === '.json') {
            const parsed = parseJSON(buffer.toString('utf8'));
            if (!externalData.some(d => d.key === safeKey)) {
              externalData.push({ name: baseName, key: safeKey, headers: parsed.headers, rows: parsed.rows });
            }
          } else if (ext === '.xml') {
            console.warn(`⚠️ Парсинг XML в CLI режиме временно ограничен (требует DOMParser). Пропущено: ${filePath}`);
          } else if (ext === '.qvd') {
            const parsed = await parseQVD(buffer);
            if (!externalData.some(d => d.key === safeKey)) {
              externalData.push({ name: baseName, key: safeKey, headers: parsed.headers, rows: parsed.rows });
            }
          } else {
            console.warn(`⚠️ Пропущен неподдерживаемый формат: ${filePath}`);
          }
        } catch (err) {
          console.error(`✗ Ошибка загрузки прикреплённого файла ${filePath}: ${err.message}`);
        }
      }
      console.log(`✅ Всего загружено внешних источников: ${externalData.length}`);
    }

    // Проверка: поиск недостающих внешних файлов
    const missingFiles = checkMissingExternalFiles(config.columns, externalData);
    if (missingFiles.length > 0) {
      console.warn('\n⚠️  ВНИМАНИЕ: Отсутствуют необходимые внешние файлы данных:');
      missingFiles.forEach(({ column, expectedKey, displayName }) => {
        console.warn(`   • Файл ${displayName}, столбец "${column.name || 'не указан'}" (ключ: ${expectedKey})`);
      });
      console.warn('\n   Генерация продолжится, но значения в этих столбцах будут пустыми.\n');
    }

    // Проверка наличия языковых ключей
    const required = [
      { name: 'countriesData', data: dataSources.countriesData },
      { name: 'namesData', data: dataSources.namesData },
      { name: 'regionsData', data: dataSources.regionsData },
      { name: 'citiesData', data: dataSources.citiesData },
      { name: 'streetsData', data: dataSources.streetsData },
    ];

    const missing = required.filter(r => !r.data?.[config.language]);
    if (missing.length > 0) {
      console.error(
        `✗ Ошибка: отсутствуют данные для языка "${config.language}" в:`,
        missing.map(m => m.name).join(', ')
      );
      return false;
    }

    // Динамический импорт генераторов
    const {
      writeCSVStream,
      writeXMLStream,
      writeXLSXStream,
      writeJSONStream,
      writeQVDStream,
    } = require('./utils/fileGenerator');
    const writers = {
      csv: writeCSVStream,
      xlsx: writeXLSXStream,
      xml: writeXMLStream,
      json: writeJSONStream,
      qvd: writeQVDStream,
    };

    const writer = writers[config.format];
    if (!writer) throw new Error(`Неподдерживаемый формат: ${config.format}`);

    const filename = buildFilename(config.filenameBase, config.format);
    const outputPath = path.join(getDocumentsPath(), filename);
    await fs.mkdir(getDocumentsPath(), { recursive: true });

    console.log(`Вывод: ${outputPath}`);
    console.log(`Генерация...`);

    await writer(
      outputPath,
      config,
      5000,
      progress => {
        const pct = Math.round((progress / config.rows) * 100);
        if (pct % 5 === 0 || pct === 100) {
          process.stdout.write(`\r${pct}% (${progress.toLocaleString()}/${config.rows.toLocaleString()})`);
        }
      },
      {
        countriesData: dataSources.countriesData,
        genderData: dataSources.genderData,
        namesData: dataSources.namesData,
        regionsData: dataSources.regionsData,
        citiesData: dataSources.citiesData,
        streetsData: dataSources.streetsData,
        language: config.language,
        externalData,
        csvSeparator: config.csvSeparator,
      }
    );

    console.log('\n');
    const stats = await fs.stat(outputPath);
    console.log(`✓ Готово! Файл: ${outputPath}`);
    console.log(`✓ Размер: ${(stats.size / (1024 * 1024)).toFixed(2)} МБ`);
    return true;
  } catch (error) {
    console.error(`\n✗ Ошибка: ${error.message}`);
    if (process.env.NODE_ENV !== 'production') console.error(error.stack);
    return false;
  }
}

// GUI FUNCTIONS
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

  win.webContents.once('dom-ready', () => setTimeout(() => win.show(), 80));
  win.loadFile(path.join(__dirname, 'renderer', 'index.html'));
  win.removeMenu();

  return win;
}

// APP LIFECYCLE
app.whenReady().then(async () => {
  const cliConfig = parseCliArgs();

  if (cliConfig.mode === 'cli') {
    if (!cliConfig.configFile) {
      console.error('   Ошибка: не указан путь к конфигурационному файлу');
      console.error('   Использование: dataGenerator -f путь/к/конфигу.json [-a файл1 [-a файл2 ...]]');
      app.quit();
      process.exit(1);
      return;
    }

    const success = await runCliGeneration(cliConfig.configFile, cliConfig.attachFiles);
    app.quit();
    process.exit(success ? 0 : 1);
    return;
  }

  const win = createMainWindow();
  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createMainWindow();
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

// IPC HANDLERS (GUI)

ipcMain.handle('show-save-dialog', async (_evt, { defaultPath, filters }) => {
  return await dialog.showSaveDialog({ defaultPath, filters });
});

ipcMain.handle('show-open-dialog', async (_evt, options) => {
  return await dialog.showOpenDialog({ ...options });
});

ipcMain.handle('write-file', async (_evt, filePath, content) => {
  await fs.writeFile(filePath, content, 'utf8');
});

ipcMain.handle('read-file', async (_evt, filePath) => {
  return await fs.readFile(filePath, 'utf8');
});

ipcMain.handle('show-item-in-folder', async (_evt, filePath) => {
  if (filePath && typeof filePath === 'string') shell.showItemInFolder(filePath);
});

ipcMain.handle('open-external', async (_evt, url) => {
  if (url && typeof url === 'string') await shell.openExternal(url);
});

ipcMain.handle('check-for-updates', async () => {
  const repo = getRepoSlugFromPackageJson(app.getAppPath()) || null;
  if (!repo) return { ok: false, reason: 'no_repo' };

  const currentVersion = app.getVersion();
  try {
    const res = await checkLatestGithubRelease({ repo, currentVersion });
    if (!res.ok) return res;
    return res.hasUpdate
      ? { ok: true, hasUpdate: true, latestVersion: res.latestVersion, latestUrl: res.latestUrl, currentVersion }
      : { ok: true, hasUpdate: false, currentVersion, latestVersion: res.latestVersion };
  } catch (e) {
    return { ok: false, reason: 'error', error: e.message };
  }
});
