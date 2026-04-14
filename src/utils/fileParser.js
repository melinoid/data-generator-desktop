const { QvdDataFrame } = require('qvd4js');
const xlsx = require('xlsx');

/**
 * Парсит QVD-файл в объект с заголовками и строками.
 * @param {Buffer} buffer - содержимое QVD-файла.
 * @returns {{ headers: string[], rows: string[][] }}
 */
async function parseQVD(buffer) {
  const tempPath = require('path').join(require('os').tmpdir(), `temp_${Date.now()}.qvd`);
  const fs = require('fs').promises;

  try {
    // Сохраняем буфер во временный файл (QvdFileReader работает только с файлами)
    await fs.writeFile(tempPath, buffer);

    // Читаем через QvdFileReader
    const df = await QvdDataFrame.fromQvd(tempPath);

    // Преобразуем в нужный формат
    const headers = df.columns;
    const rows = df.data.map(row => row.map(cell => String(cell ?? '')));

    if (!headers || headers.length === 0) throw new Error('QVD не содержит колонок');
    if (rows.length === 0) throw new Error('QVD пуст');

    return { headers, rows };
  } finally {
    // Удаляем временный файл
    try {
      await fs.unlink(tempPath);
    } catch (e) {
      console.warn('Не удалось удалить временный QVD-файл:', e.message);
    }
  }
}

/**
 * Парсит XLSX-файл, возвращая данные по всем листам.
 * @param {Buffer} buffer - содержимое XLSX-файла.
 * @returns {{ name: string, key: string, headers: string[], rows: string[][] }[]}
 */
function parseXLSX(buffer) {
  const workbook = xlsx.read(buffer, { type: 'buffer', cellDates: true });
  const result = [];

  for (const sheetName of workbook.SheetNames) {
    const worksheet = workbook.Sheets[sheetName];
    const json = xlsx.utils.sheet_to_json(worksheet, { header: 1, raw: false });

    if (!json || json.length === 0) {
      // Пустой лист — всё равно создаём заглушки
      result.push({
        sheetName,
        headers: ['Column 1'],
        rows: [],
      });
      continue;
    }

    // Определяем максимальное количество столбцов
    const maxCols = json.reduce((max, row) => Math.max(max, Array.isArray(row) ? row.length : 0), 0);

    // Заголовки с fallback
    const rawHeaders = new Array(maxCols).fill('');
    if (json[0]) {
      for (let i = 0; i < maxCols; i++) {
        if (i < json[0].length) {
          rawHeaders[i] = String(json[0][i] ?? '').trim();
        }
      }
    }
    const headers = rawHeaders.map((h, i) => (h === '' ? `Column ${i + 1}` : h));

    // Строки данных
    const rows = json
      .slice(1)
      .map(row => {
        const filledRow = new Array(maxCols).fill('');
        for (let i = 0; i < maxCols; i++) {
          let cell = i < row.length ? row[i] : '';
          if (cell instanceof Date) {
            if (isNaN(cell.getTime())) {
              filledRow[i] = '';
            } else {
              const pad = n => String(n).padStart(2, '0');
              filledRow[i] =
                `${pad(cell.getDate())}.${pad(cell.getMonth() + 1)}.${cell.getFullYear()} ` +
                `${pad(cell.getHours())}:${pad(cell.getMinutes())}:${pad(cell.getSeconds())}`;
            }
          } else if (typeof cell === 'number' && cell >= 0 && cell < 100000) {
            const date = new Date((cell - (cell >= 60 ? 1 : 0)) * 86400000 - 2209161600000);
            if (!isNaN(date.getTime())) {
              const pad = n => String(n).padStart(2, '0');
              filledRow[i] =
                `${pad(date.getDate())}.${pad(date.getMonth() + 1)}.${date.getFullYear()} ` +
                `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
            } else {
              filledRow[i] = String(cell);
            }
          } else {
            filledRow[i] = String(cell ?? '');
          }
        }
        return filledRow;
      })
      .filter(row => row.some(cell => cell !== ''));

    result.push({
      sheetName,
      headers,
      rows,
    });
  }

  return result;
}

/**
 * Парсит CSV-строку в объект с заголовками и строками.
 * @param {string} text - содержимое CSV-файла.
 * @returns {{ headers: string[], rows: string[][] }}
 */
function parseCSV(text) {
  // Удаляем пустые строки
  const lines = text.split(/\r\n|\n/).filter(line => line.trim() !== '');
  if (lines.length === 0) throw new Error('Файл пуст');

  // Функция для парсинга одной строки CSV с учётом кавычек
  function parseCSVLine(line) {
    const result = [];
    let inQuotes = false;
    let field = '';
    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      const nextChar = line[i + 1] || '';

      if (char === '"' && !inQuotes) {
        inQuotes = true;
      } else if (char === '"' && inQuotes && nextChar === '"') {
        // Экранированная кавычка: ""
        field += '"';
        i++; // пропускаем следующую кавычку
      } else if (char === '"' && inQuotes) {
        inQuotes = false;
      } else if (char === ',' && !inQuotes) {
        result.push(field);
        field = '';
      } else {
        field += char;
      }
    }
    result.push(field);
    return result;
  }

  // Парсим все строки
  const parsedLines = lines.map(line => parseCSVLine(line));

  // Определяем максимальное количество столбцов
  const maxCols = Math.max(...parsedLines.map(row => row.length));

  // Обрабатываем заголовки
  const rawHeaders = parsedLines[0] || [];
  while (rawHeaders.length < maxCols) rawHeaders.push('');

  const headers = rawHeaders.map((h, i) => {
    const s = String(h ?? '').trim();
    return s === '' ? `Column ${i + 1}` : s;
  });

  // Обрабатываем данные (пропускаем первую строку — заголовки)
  const rows = parsedLines
    .slice(1)
    .map(row => {
      const filled = [...row];
      while (filled.length < maxCols) filled.push('');
      return filled.map(cell => String(cell ?? ''));
    })
    .filter(row => row.some(cell => cell.trim() !== ''));

  return { headers, rows };
}

/**
 * Парсит JSON-строку в объект с заголовками и строками.
 * @param {string} text - содержимое JSON-файла.
 * @returns {{ headers: string[], rows: string[][] }}
 */
function parseJSON(text) {
  const data = JSON.parse(text);
  if (!Array.isArray(data)) throw new Error('JSON должен быть массивом объектов');
  if (data.length === 0) throw new Error('JSON пуст');

  // Собираем все ключи из всех объектов для полноты
  const allKeys = new Set();
  data.forEach(obj => {
    if (obj && typeof obj === 'object') {
      Object.keys(obj).forEach(k => allKeys.add(k));
    }
  });
  const headers = Array.from(allKeys);

  // Но если первый объект пуст или ключи — пустые строки / null — используем Column N
  if (headers.length === 0 || headers.every(h => String(h ?? '').trim() === '')) {
    // Определяем ширину по самому широкому объекту
    const maxProps = data.reduce((max, obj) => {
      return Math.max(max, obj && typeof obj === 'object' ? Object.keys(obj).length : 0);
    }, 0);
    for (let i = 0; i < maxProps; i++) {
      headers[i] = `Column ${i + 1}`;
    }
  } else {
    // Заменяем пустые/некорректные заголовки
    for (let i = 0; i < headers.length; i++) {
      const s = String(headers[i] ?? '').trim();
      if (s === '') headers[i] = `Column ${i + 1}`;
    }
  }

  const rows = data.map(obj => {
    if (!obj || typeof obj !== 'object') return headers.map(() => '');
    return headers.map(key => {
      // Для случая, когда ключ - Column N, но исходный объект не имеет такого ключа
      // (это может произойти при несовпадении структуры), возвращаем ''
      if (key.startsWith('Column ') && !Object.prototype.hasOwnProperty.call(obj, key)) {
        // Попытка сопоставить по индексу: берём значения по порядку
        const objKeys = Object.keys(obj);
        const idx = headers.indexOf(key);
        if (idx < objKeys.length) {
          return String(obj[objKeys[idx]] ?? '');
        }
        return '';
      }
      return String(obj[key] ?? '');
    });
  });

  return { headers, rows };
}

/**
 * Парсит XML-строку в объект с заголовками и строками.
 * Ожидается структура: <rows><row><col1>...</col1>...</row></rows>.
 * @param {string} text - содержимое XML-файла.
 * @returns {{ headers: string[], rows: string[][] }}
 */
function parseXML(text) {
  const parser = new DOMParser();
  const doc = parser.parseFromString(text, 'text/xml');
  const errorNode = doc.querySelector('parsererror');
  if (errorNode) throw new Error('Некорректный XML: ' + errorNode.textContent);

  const rowElements = [...doc.querySelectorAll('rows > row')];
  if (rowElements.length === 0) throw new Error('Не найдены <row> внутри <rows>');

  // Находим максимальное количество дочерних элементов
  const maxChildren = Math.max(...rowElements.map(r => r.children.length));

  // Берём теги из первой строки, но дополняем до maxChildren
  let rawHeaders = [];
  if (rowElements[0].children.length > 0) {
    rawHeaders = Array.from(rowElements[0].children).map(el => el.tagName);
  }
  while (rawHeaders.length < maxChildren) {
    rawHeaders.push('');
  }

  // Заменяем пустые/некорректные теги
  const headers = rawHeaders.map((tag, i) => {
    const s = String(tag ?? '').trim();
    return s === '' ? `Column ${i + 1}` : s;
  });

  const rows = rowElements.map(rowEl => {
    const values = [];
    for (let i = 0; i < maxChildren; i++) {
      const child = rowEl.children[i];
      if (child) {
        values.push(child.textContent || '');
      } else {
        values.push('');
      }
    }
    return values;
  });

  return { headers, rows };
}

module.exports = {
  parseCSV,
  parseJSON,
  parseXML,
  parseXLSX,
  parseQVD,
};
