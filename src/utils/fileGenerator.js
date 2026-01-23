const ExcelJS = require('exceljs');
const { QvdDataFrame } = require('qvd4js');
const fs = require('fs');
const { buildRowObject, TYPES } = require('./dataGenerator');

/**
 * Убеждается, что у всех колонок есть корректные имена.
 * Если имя отсутствует или пустое — генерирует `column_N`.
 */
function ensureHeaders(columns) {
  return columns.map((c, idx) => (c.name && c.name.trim() ? c.name.trim() : `column_${idx + 1}`));
}

// Экранирует значение для CSV: оборачивает в кавычки при наличии специальных символов.
function escapeCsv(val) {
  const s = String(val ?? '');
  if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}

// Асинхронная запись чанка в поток с ожиданием drain при необходимости.
function writeToStream(stream, chunk) {
  return new Promise((resolve, reject) => {
    if (!stream.write(chunk)) {
      stream.once('drain', resolve);
      stream.once('error', reject);
    } else {
      resolve();
    }
  });
}

// Валидирует и возвращает источники данных для генерации строк.
function getDataSources(opts = {}) {
  const {
    countriesData,
    genderData,
    namesData,
    regionsData,
    citiesData,
    streetsData,
    language = 'ru',
    externalData = null,
  } = opts;

  if (!countriesData || !genderData || !namesData || !regionsData || !citiesData || !streetsData) {
    throw new Error(
      'Все источники данных (countriesData, genderData, namesData, regionsData, citiesData, streetsData) должны быть переданы в fileGenerator'
    );
  }

  return {
    countriesData,
    genderData,
    namesData,
    regionsData,
    citiesData,
    streetsData,
    language,
    externalData,
  };
}

/**
 * Определяет фактическое количество строк для генерации.
 * Учитывает режим `sequential_ignore_size` в колонках с кастомными списками или внешними данными.
 */
function getActualRowCount(config, externalData = null) {
  const { rows, columns } = config;
  let minIgnoreSize = Infinity;

  for (const col of columns) {
    if (col.customListMode === 'sequential_ignore_size') {
      let length = Infinity;
      if (col.type === TYPES.CUSTOM_LIST && Array.isArray(col.customList)) {
        length = col.customList.length;
      } else if (col.type.startsWith('file_') && col.type.includes('_col_') && externalData) {
        const colIndexMatch = col.type.match(/_col_(\d+)$/);
        const colIndex = colIndexMatch ? parseInt(colIndexMatch[1], 10) : -1;
        const keyMatch = col.type.match(/^file_(.+)_col_\d+$/);
        const key = keyMatch ? keyMatch[1] : null;

        if (key !== null) {
          const sourceIndex = externalData.findIndex(src => src.key === key);
          if (sourceIndex !== -1 && externalData[sourceIndex] && Array.isArray(externalData[sourceIndex].rows)) {
            length = externalData[sourceIndex].rows.length;
          }
        }
      }
      if (length < minIgnoreSize) {
        minIgnoreSize = length;
      }
    }
  }

  if (minIgnoreSize !== Infinity) {
    return minIgnoreSize;
  }

  return Math.max(0, Number(rows) || 0);
}

// Записывает данные в CSV-файл по частям (потоково).
async function writeCSVStream(
  filePath,
  config,
  chunkSize = 1000,
  progressCallback = null,
  { csvSeparator = ',', ...dataSources } = {}
) {
  const { columns, language: configLang } = config;
  const { countriesData, genderData, namesData, regionsData, citiesData, streetsData, language, externalData } =
    getDataSources({
      ...dataSources,
      language: configLang,
    });

  const headers = ensureHeaders(columns);
  const stream = fs.createWriteStream(filePath, { encoding: 'utf8' });
  await writeToStream(stream, headers.map(escapeCsv).join(csvSeparator) + '\n');

  const total = getActualRowCount(config, externalData);
  let autoId = 1;

  while (autoId <= total) {
    const end = Math.min(autoId + chunkSize - 1, total);
    let buf = '';

    for (let id = autoId; id <= end; id += 1) {
      try {
        const row = buildRowObject(
          columns,
          id,
          countriesData,
          citiesData,
          genderData,
          namesData,
          regionsData,
          streetsData,
          language,
          externalData
        );
        buf += headers.map(h => escapeCsv(row[h])).join(csvSeparator) + '\n';
      } catch (error) {
        console.error('Error generating row:', error);
      }
    }

    if (buf) {
      await writeToStream(stream, buf);
    }

    if (progressCallback) progressCallback(end);
    autoId = end + 1;
  }

  await new Promise(r => stream.end(r));
}

/**
 * Приводит строку к валидному XML-имени тега с поддержкой Unicode:
 * - Сохраняет буквы всех языков, цифры, '_', '-', '.'
 * - Заменяет пробелы и недопустимые символы на '_'
 * - Убирает повторяющиеся и крайние подчёркивания
 * - Если начинается с цифры — добавляет префикс '_'
 * - Если результат пустой — возвращает 'tag'
 */
function sanitizeXmlTagName(str) {
  let clean = String(str).replace(/\s+/g, '_');
  clean = clean.replace(/[^\p{L}\p{Nd}_.-]/gu, '_');
  clean = clean.replace(/_+/g, '_');
  clean = clean.replace(/^_+|_+$/g, '');
  if (!clean) return 'column';
  if (/^\p{Nd}/u.test(clean)) {
    clean = '_' + clean;
  }
  return clean;
}

// Записывает данные в XML-файл по частям (потоково).
async function writeXMLStream(filePath, config, chunkSize = 1000, progressCallback = null, dataSources = {}) {
  const { columns, language: configLang } = config;
  const { countriesData, genderData, namesData, regionsData, citiesData, streetsData, language, externalData } =
    getDataSources({
      ...dataSources,
      language: configLang,
    });

  const headers = ensureHeaders(columns);
  const stream = fs.createWriteStream(filePath, { encoding: 'utf8' });
  await writeToStream(stream, '<?xml version="1.0" encoding="UTF-8"?>\n<rows>\n');

  const total = getActualRowCount(config, externalData);
  let autoId = 1;

  while (autoId <= total) {
    const end = Math.min(autoId + chunkSize - 1, total);
    let buf = '';

    for (let id = autoId; id <= end; id += 1) {
      try {
        const row = buildRowObject(
          columns,
          id,
          countriesData,
          citiesData,
          genderData,
          namesData,
          regionsData,
          streetsData,
          language,
          externalData
        );
        buf += '<row>';
        for (const h of headers) {
          const tagName = sanitizeXmlTagName(h);
          const value = row[h];
          const escapedValue = String(value ?? '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '<')
            .replace(/>/g, '>')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&apos;');
          buf += `<${tagName}>${escapedValue}</${tagName}>`;
        }
        buf += '</row>\n';
      } catch (error) {
        console.error('Error generating row:', error);
      }
    }

    if (buf) {
      await writeToStream(stream, buf);
    }

    if (progressCallback) progressCallback(end);
    autoId = end + 1;
  }

  await writeToStream(stream, '</rows>\n');
  await new Promise(r => stream.end(r));
}

// Записывает данные в XLSX-файл с использованием ExcelJS (потоковый режим).
async function writeXLSXStream(filePath, config, chunkSize = 1000, progressCallback = null, dataSources = {}) {
  const { columns, language: configLang } = config;
  const { countriesData, genderData, namesData, regionsData, citiesData, streetsData, language, externalData } =
    getDataSources({
      ...dataSources,
      language: configLang,
    });

  const headers = ensureHeaders(columns);
  const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: filePath });
  const worksheet = workbook.addWorksheet('Data');
  worksheet.addRow(headers).commit();

  const total = getActualRowCount(config, externalData);
  let autoId = 1;

  while (autoId <= total) {
    const end = Math.min(autoId + chunkSize - 1, total);

    for (let id = autoId; id <= end; id += 1) {
      try {
        const row = buildRowObject(
          columns,
          id,
          countriesData,
          citiesData,
          genderData,
          namesData,
          regionsData,
          streetsData,
          language,
          externalData
        );
        const values = headers.map(h => row[h]);
        worksheet.addRow(values).commit();
      } catch (error) {
        console.error('Error generating row:', error);
      }
    }

    if (progressCallback) progressCallback(end);
    autoId = end + 1;
  }

  worksheet.commit();
  await workbook.commit();
}

//Записывает данные в JSON-файл по частям (потоково).
async function writeJSONStream(filePath, config, chunkSize = 1000, progressCallback = null, dataSources = {}) {
  const { columns, language: configLang } = config;
  const { countriesData, genderData, namesData, regionsData, citiesData, streetsData, language, externalData } =
    getDataSources({
      ...dataSources,
      language: configLang,
    });

  const stream = fs.createWriteStream(filePath, { encoding: 'utf8' });
  await writeToStream(stream, '[\n');

  const total = getActualRowCount(config, externalData);
  let autoId = 1;
  let first = true;

  while (autoId <= total) {
    const end = Math.min(autoId + chunkSize - 1, total);
    let buf = '';

    for (let id = autoId; id <= end; id += 1) {
      try {
        const obj = buildRowObject(
          columns,
          id,
          countriesData,
          citiesData,
          genderData,
          namesData,
          regionsData,
          streetsData,
          language,
          externalData
        );
        const json = JSON.stringify(obj, null, 2);
        if (!first) buf += ',\n' + json;
        else {
          buf += json;
          first = false;
        }
      } catch (error) {
        console.error('Error generating row:', error);
      }
    }

    if (buf) {
      await writeToStream(stream, buf + '\n');
    }

    if (progressCallback) progressCallback(end);
    autoId = end + 1;
  }

  await writeToStream(stream, ']');
  await new Promise(r => stream.end(r));
}

/**
 * Записывает данные в QVD-файл (весь объём в памяти из-за ограничений формата).
 * Поддерживает до 50 000 строк.
 */
async function writeQVDStream(filePath, config, chunkSize = 1000, progressCallback = null, dataSources = {}) {
  const { columns, language: configLang } = config;
  const { countriesData, genderData, namesData, regionsData, citiesData, streetsData, language, externalData } =
    getDataSources({
      ...dataSources,
      language: configLang,
    });

  const total = getActualRowCount(config, externalData);

  if (total > 50000) {
    throw new Error('QVD-формат не поддерживает более 50 000 строк из-за ограничений памяти. Выберите CSV, JSON или XLSX.');
  }

  const headers = ensureHeaders(columns);
  const data = new Array(total);

  for (let id = 1; id <= total; id++) {
    try {
      const row = buildRowObject(
        columns,
        id,
        countriesData,
        citiesData,
        genderData,
        namesData,
        regionsData,
        streetsData,
        language,
        externalData
      );
      const rowData = headers.map(h => {
        const val = row[h];
        if (val === null || val === undefined) return null;
        if (typeof val === 'boolean') return val ? 'true' : 'false';
        return val;
      });
      data[id - 1] = rowData;
    } catch (error) {
      console.error(`Error generating row ${id} for QVD:`, error);
      data[id - 1] = headers.map(() => null);
    }

    if (progressCallback && (id % chunkSize === 0 || id === total)) {
      progressCallback(id);
    }
  }

  const df = new QvdDataFrame(data, headers);
  await df.toQvd(filePath);
}

module.exports = {
  writeCSVStream,
  writeXMLStream,
  writeXLSXStream,
  writeJSONStream,
  writeQVDStream,
};
