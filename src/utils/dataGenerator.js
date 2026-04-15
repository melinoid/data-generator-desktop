const fs = require('fs');
const path = require('path');
const turf = require('@turf/turf');

const naughtyStrings = require('../data/naughtyStrings.json');
const emailDomainsData = require('../data/emailDomainsData.json');
const domainsData = require('../data/domainsData.json');
const phonesCountriesData = require('../data/phonesCountriesData.json');
const citiesCoordsData = require('../data/citiesCoords.json');

// Типы генерируемых данных
const TYPES = {
  ID_AUTO: 'id_auto',
  INTEGER: 'integer',
  FLOAT: 'float',
  BOOLEAN: 'boolean',
  NULL: 'null',
  EMPTY_STRING: 'empty_string',
  COUNTRY: 'country',
  COUNTRY_CODE: 'country_code',
  GENDER: 'gender',
  FIRST_NAME: 'first_name',
  MIDDLE_NAME: 'middle_name',
  LAST_NAME: 'last_name',
  GUID: 'guid',
  DATE: 'date',
  COLOR: 'color',
  REGION: 'region',
  CITY: 'city',
  STREET: 'street',
  NAUGHTY_STRINGS: 'naughty_strings',
  LATITUDE: 'latitude',
  LONGITUDE: 'longitude',
  IP: 'ip',
  MAC_ADDRESS: 'mac_address',
  COORDINATES_LAT_LNG: 'coordinates_lat_lng',
  COORDINATES_LNG_LAT: 'coordinates_lng_lat',
  LINE_BI: 'line_bi',
  LINE_GEOJSON: 'line_geojson',
  EMAIL: 'email_random',
  PHONE: 'phone',
  CUSTOM_LIST: 'custom_list',
  RANDOM_LENGTH_STRING: 'random_length_string',
  FILE_COLUMN: 'file_column',
};

// Форматы даты
const DATE_FORMATS = {
  SQL_DATETIME: 'sql_datetime',
  SQL_DATE: 'sql_date',
  SQL_TIME: 'sql_time',
  UNIX_TIMESTAMP: 'unix_timestamp',
  ISO_8601: 'iso_8601',
  EPOCH: 'epoch',
};

// Форматы цвета
const COLOR_FORMATS = {
  HEX: 'hex',
  RGB: 'rgb',
  CMYK: 'cmyk',
};

// Форматы IP-адресов
const IP_FORMATS = {
  IPV4: 'ipv4',
  IPV6: 'ipv6',
  IPV4_WITH_MASK: 'ipv4_with_mask',
  IPV6_WITH_MASK: 'ipv6_with_mask',
};

// Режимы выбора значений из пользовательского списка
const CustomListMode = {
  RANDOM: 'random',
  SEQUENTIAL: 'sequential',
  SEQUENTIAL_IGNORE_SIZE: 'sequential_ignore_size',
};

// Форматы телефонных номеров
const PHONE_FORMATS = {
  DASHED: 'dashed',
  PLAIN: 'plain',
  SPACED: 'spaced',
  PARENTHESES: 'parentheses',
};

// Округляет число до 6 знаков после запятой.
function roundTo6(value) {
  return Math.round(value * 1e6) / 1e6;
}

/**
 * Генерирует случайную точку в пределах заданного радиуса (в км) от центра.
 * @param {number} centerLat - Широта центра.
 * @param {number} centerLng - Долгота центра.
 * @param {number} radiusKm - Радиус в километрах.
 * @returns {{lat: number, lng: number}} Случайные координаты.
 */
function generateRandomPointInRadius(centerLat, centerLng, radiusKm = 4) {
  const radiusInDegrees = radiusKm / 111.32; // приблизительно 111.32 км на градус
  const angle = Math.random() * 2 * Math.PI;
  const distance = Math.sqrt(Math.random()) * radiusInDegrees;
  const deltaLat = distance * Math.cos(angle);
  const deltaLng = (distance * Math.sin(angle)) / Math.cos((centerLat * Math.PI) / 180);
  return {
    lat: roundTo6(centerLat + deltaLat),
    lng: roundTo6(centerLng + deltaLng),
  };
}

/**
 * Генерирует случайное число по распределению Парето.
 * @param {number} min - Минимальное значение.
 * @param {number} max - Максимальное значение.
 * @param {number} alpha - Параметр формы распределения (по умолчанию 1.1).
 * @returns {number} Случайное целое число в диапазоне [min, max].
 */
function paretoRandom(min, max, alpha = 1.1) {
  const u = Math.random();
  const x = min + (max - min + 1) * (1 - Math.pow(u, 1 / alpha));
  return Math.min(Math.floor(x), max);
}

/**
 * Нормализует имя заголовка колонки.
 * @param {string} name - Исходное имя.
 * @param {number} index - Индекс колонки.
 * @returns {string} Нормализованное имя.
 */
function sanitizeHeader(name, index) {
  return name && name.trim() ? name.trim() : `column_${index + 1}`;
}

/**
 * Выбирает случайный элемент из массива с использованием распределения Парето.
 * @param {Array} array - Исходный массив.
 * @returns {*} Случайный элемент.
 */
function getRandomValueFromArray(array) {
  return array[Math.floor(paretoRandom(0, array.length - 1))];
}

/**
 * Вычисляет значение по формуле преобразования.
 * Поддерживает: value, row, col(name), IF(condition, trueVal, falseVal)
 * @param {string} formula - Формула для вычисления.
 * @param {Object} row - Объект строки со значениями.
 * @param {*} currentValue - Текущее значение поля (доступно как 'value').
 * @returns {*} Результат вычисления.
 */
function evaluateTransformationFormula(formula, row, currentValue = null) {
  const expression = String(formula || '').trim();
  if (!expression) return currentValue ?? '';

  const col = name => row[String(name)] ?? '';
  const value = currentValue;

  // Excel-подобная функция IF
  const IF = (condition, trueVal, falseVal) => (condition ? trueVal : falseVal);

  try {
    // В область видимости добавлены: row, col, value, IF
    const evaluator = new Function('row', 'col', 'value', 'IF', `"use strict"; return (${expression});`);
    const result = evaluator(row, col, value, IF);
    return result ?? currentValue ?? '';
  } catch (e) {
    console.warn(`⚠️ Ошибка в формуле "${expression.slice(0, 50)}...":`, e.message);
    return currentValue ?? '';
  }
}

/**
 * Генерирует случайный GUID.
 * @returns {string} Строка в формате GUID.
 */
function generateGUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    const r = paretoRandom(0, 15) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

/**
 * Генерирует случайную дату в указанном формате.
 * @param {Date} fromDate - Начальная дата.
 * @param {Date} toDate - Конечная дата.
 * @param {string} format - Формат даты из DATE_FORMATS.
 * @returns {*} Дата в нужном формате.
 */
function generateRandomDate(fromDate, toDate, format) {
  const fromTimestamp = fromDate.getTime();
  const toTimestamp = toDate.getTime();
  const randomTimestamp = Math.floor(paretoRandom(0, toTimestamp - fromTimestamp)) + fromTimestamp;
  const date = new Date(randomTimestamp);
  switch (format) {
    case DATE_FORMATS.SQL_DATETIME:
      return date.toISOString().slice(0, 19).replace('T', ' ');
    case DATE_FORMATS.SQL_DATE:
      return date.toISOString().slice(0, 10);
    case DATE_FORMATS.SQL_TIME:
      return date.toISOString().slice(11, 19);
    case DATE_FORMATS.UNIX_TIMESTAMP:
      return Math.floor(date.getTime() / 1000);
    case DATE_FORMATS.ISO_8601:
      return date.toISOString();
    case DATE_FORMATS.EPOCH:
      return date.getTime();
    default:
      return date.toISOString();
  }
}

/**
 * Парсит строку даты в объект Date.
 * Поддерживает форматы DD.MM.YYYY и MM/DD/YYYY.
 * @param {string} dateString - Строка с датой.
 * @returns {Date} Объект даты.
 */
function parseDate(dateString) {
  const date = new Date(dateString);
  if (!isNaN(date.getTime())) {
    return date;
  }
  const parts = dateString.split('.');
  if (parts.length === 3) {
    return new Date(parts[2], parts[1] - 1, parts[0]);
  }
  const parts2 = dateString.split('/');
  if (parts2.length === 3) {
    return new Date(parts2[2], parts2[0] - 1, parts2[1]);
  }
  return new Date();
}

/**
 * Генерирует случайный цвет в заданном формате.
 * @param {string} format - Формат из COLOR_FORMATS.
 * @param {boolean} transparency - Включить альфа-канал.
 * @returns {string} Цвет в нужном формате.
 */
function generateRandomColor(format, transparency = false) {
  const r = Math.floor(paretoRandom(0, 255));
  const g = Math.floor(paretoRandom(0, 255));
  const b = Math.floor(paretoRandom(0, 255));
  const a = transparency ? Number(Math.random().toFixed(2)) : 1;

  switch (format) {
    case COLOR_FORMATS.HEX: {
      const hex = `#${r.toString(16).padStart(2, '0')}${g.toString(16).padStart(2, '0')}${b
        .toString(16)
        .padStart(2, '0')}`;
      if (transparency) {
        const alphaHex = Math.floor(a * 255)
          .toString(16)
          .padStart(2, '0');
        return hex + alphaHex;
      }
      return hex;
    }
    case COLOR_FORMATS.RGB:
      return transparency ? `rgba(${r}, ${g}, ${b}, ${a})` : `rgb(${r}, ${g}, ${b})`;
    case COLOR_FORMATS.CMYK: {
      const rNorm = r / 255;
      const gNorm = g / 255;
      const bNorm = b / 255;
      const k = 1 - Math.max(rNorm, gNorm, bNorm);
      const c = k === 1 ? 0 : (1 - rNorm - k) / (1 - k);
      const m = k === 1 ? 0 : (1 - gNorm - k) / (1 - k);
      const y = k === 1 ? 0 : (1 - bNorm - k) / (1 - k);
      const cPercent = Math.round(c * 100);
      const mPercent = Math.round(m * 100);
      const yPercent = Math.round(y * 100);
      const kPercent = Math.round(k * 100);
      return transparency
        ? `cmyk(${cPercent}%, ${mPercent}%, ${yPercent}%, ${kPercent}%, ${a})`
        : `cmyk(${cPercent}%, ${mPercent}%, ${yPercent}%, ${kPercent}%)`;
    }
    default:
      return `rgb(${r}, ${g}, ${b})`;
  }
}

// Кэши полигонов стран и регионов
const POLYGONS_CACHE = {};
const POLYGONS_DIR = path.join(__dirname, '..', 'data', 'countriesPolygonsData');
const REGION_POLYGONS_CACHE = {};

// Генерирует случайную широту (-90 до 90).
function generateRandomLatitude() {
  return paretoRandom(-90000000, 90000000) / 1000000;
}

// Генерирует случайную долготу (-180 до 180).
function generateRandomLongitude() {
  return paretoRandom(-180000000, 180000000) / 1000000;
}

// Генерирует координаты в формате [широта, долгота].
function generateRandomCoordinatesLatLng() {
  return `[${generateRandomLatitude()},${generateRandomLongitude()}]`;
}

// Генерирует координаты в формате [долгота, широта].
function generateRandomCoordinatesLngLat() {
  return `[${generateRandomLongitude()},${generateRandomLatitude()}]`;
}

/**
 * Загружает полигон страны по коду из кэша или файла.
 * @param {string} countryCode - ISO-код страны.
 * @returns {Object|null} GeoJSON-полигон или null.
 */
function loadPolygonByCountryCode(countryCode) {
  if (!countryCode) return null;
  if (POLYGONS_CACHE[countryCode]) {
    return POLYGONS_CACHE[countryCode];
  }

  const filename = `${countryCode}_PolygonsData.json`;
  const filepath = path.join(POLYGONS_DIR, filename);

  try {
    if (!fs.existsSync(filepath)) {
      return null;
    }
    const data = JSON.parse(fs.readFileSync(filepath, 'utf8'));
    const polygon = data.features?.[0]?.geometry;
    if (polygon && polygon.type === 'Polygon') {
      POLYGONS_CACHE[countryCode] = polygon;
      return polygon;
    }
    return null;
  } catch (e) {
    console.error(`Ошибка загрузки полигона для ${countryCode}:`, e.message);
    return null;
  }
}

/**
 * Загружает полигон региона по коду страны и региона.
 * @param {string} countryCode - ISO-код страны.
 * @param {string} regionCode - Код региона.
 * @returns {Object|null} GeoJSON-полигон или null.
 */
function loadPolygonByRegion(countryCode, regionCode) {
  if (!countryCode || !regionCode) return null;
  const cacheKey = `${countryCode}-${regionCode}`;
  if (REGION_POLYGONS_CACHE[cacheKey]) {
    return REGION_POLYGONS_CACHE[cacheKey];
  }

  const regionDir = path.join(POLYGONS_DIR, `${countryCode}_Region`);
  const filename = `${regionCode}_PolygonsData.json`;
  const filepath = path.join(regionDir, filename);

  try {
    if (!fs.existsSync(filepath)) {
      return null;
    }
    const data = JSON.parse(fs.readFileSync(filepath, 'utf8'));
    const polygon = data.features?.[0]?.geometry;
    if (polygon && polygon.type === 'Polygon') {
      REGION_POLYGONS_CACHE[cacheKey] = polygon;
      return polygon;
    }
    return null;
  } catch (e) {
    console.error(`Ошибка загрузки полигона региона ${regionCode} (${countryCode}):`, e.message);
    return null;
  }
}

/**
 * Генерирует случайную точку внутри заданного полигона.
 * @param {Object} polygon - GeoJSON-полигон.
 * @param {number} maxAttempts - Максимум попыток.
 * @returns {{lat: number, lng: number}} Координаты точки.
 */
function generateRandomPointInPolygon(polygon, maxAttempts = 1000) {
  const bbox = turf.bbox({ type: 'Feature', geometry: polygon });
  for (let i = 0; i < maxAttempts; i++) {
    const pt = turf.randomPoint(1, { bbox }).features[0];
    if (turf.booleanPointInPolygon(pt, { type: 'Feature', geometry: polygon })) {
      const [lng, lat] = pt.geometry.coordinates;
      return {
        lat: roundTo6(lat),
        lng: roundTo6(lng),
      };
    }
  }
  throw new Error(`Не удалось сгенерировать точку внутри полигона за ${maxAttempts} попыток`);
}

/**
 * Генерирует случайную линию (набор точек).
 * @param {number} numPoints - Количество точек.
 * @returns {string} JSON-строка с координатами.
 */
function generateRandomLine(numPoints = 2) {
  const points = [];
  for (let i = 0; i < numPoints; i++) {
    const lng = generateRandomLongitude();
    const lat = generateRandomLatitude();
    points.push([lng, lat]);
  }
  return JSON.stringify([points]);
}

/**
 * Генерирует линию внутри полигона.
 * @param {Object} polygon - GeoJSON-полигон.
 * @param {number} numPoints - Количество точек.
 * @param {number} maxAttempts - Максимум попыток на точку.
 * @returns {string} JSON-строка с координатами.
 */
function generateRandomLineInPolygon(polygon, numPoints = 2, maxAttempts = 1000) {
  const points = [];
  for (let i = 0; i < numPoints; i++) {
    let pt = null;
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const randomPt = turf.randomPoint(1, { bbox: turf.bbox({ type: 'Feature', geometry: polygon }) }).features[0];
      if (turf.booleanPointInPolygon(randomPt, { type: 'Feature', geometry: polygon })) {
        pt = randomPt;
        break;
      }
    }
    if (!pt) {
      const lng = generateRandomLongitude();
      const lat = generateRandomLatitude();
      pt = turf.point([lng, lat]);
    }
    const [lng, lat] = pt.geometry.coordinates;
    points.push([roundTo6(lng), roundTo6(lat)]);
  }
  return JSON.stringify([points]);
}

/**
 * Генерирует случайную линию в формате GeoJSON LineString.
 * @param {number} numPoints - Количество точек.
 * @returns {string} GeoJSON-строка.
 */
function generateRandomLineGeoJSON(numPoints = paretoRandom(2, 4)) {
  const coords = [];
  for (let i = 0; i < numPoints; i++) {
    const lng = generateRandomLongitude();
    const lat = generateRandomLatitude();
    coords.push([lng, lat]);
  }
  return JSON.stringify({
    type: 'LineString',
    coordinates: [coords],
  });
}

/**
 * Генерирует линию в формате GeoJSON внутри полигона.
 * @param {Object} polygon - GeoJSON-полигон.
 * @param {number} numPoints - Количество точек.
 * @param {number} maxAttempts - Максимум попыток на точку.
 * @returns {string} GeoJSON-строка.
 */
function generateRandomLineGeoJSONInPolygon(polygon, numPoints = paretoRandom(2, 4), maxAttempts = 100) {
  const coords = [];
  for (let i = 0; i < numPoints; i++) {
    let pt = null;
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const randomPt = turf.randomPoint(1, { bbox: turf.bbox({ type: 'Feature', geometry: polygon }) }).features[0];
      if (turf.booleanPointInPolygon(randomPt, { type: 'Feature', geometry: polygon })) {
        pt = randomPt;
        break;
      }
    }
    if (!pt) {
      const lng = generateRandomLongitude();
      const lat = generateRandomLatitude();
      pt = turf.point([lng, lat]);
    }
    const [lng, lat] = pt.geometry.coordinates;
    coords.push([roundTo6(lng), roundTo6(lat)]);
  }
  return JSON.stringify({
    type: 'LineString',
    coordinates: [coords],
  });
}

// Генерирует случайный IPv4-адрес.
function generateRandomIPv4() {
  return Array.from({ length: 4 }, () => Math.floor(paretoRandom(0, 255))).join('.');
}

// Генерирует случайный IPv6-адрес.
function generateRandomIPv6() {
  return Array.from({ length: 8 }, () => Math.floor(paretoRandom(0, 65535)).toString(16).padStart(4, '0')).join(':');
}

// Генерирует случайную маску для IPv4.
function generateRandomIPv4Mask() {
  const mask = Math.floor(paretoRandom(8, 30));
  return `/${mask}`;
}

// Генерирует случайную маску для IPv6.
function generateRandomIPv6Mask() {
  const mask = Math.floor(paretoRandom(64, 128));
  return `/${mask}`;
}

/**
 * Генерирует IP-адрес в заданном формате.
 * @param {string} ipFormat - Формат из IP_FORMATS.
 * @returns {string} IP-адрес.
 */
function generateIP(ipFormat) {
  switch (ipFormat) {
    case IP_FORMATS.IPV4:
      return generateRandomIPv4();
    case IP_FORMATS.IPV6:
      return generateRandomIPv6();
    case IP_FORMATS.IPV4_WITH_MASK:
      return generateRandomIPv4() + generateRandomIPv4Mask();
    case IP_FORMATS.IPV6_WITH_MASK:
      return generateRandomIPv6() + generateRandomIPv6Mask();
    default:
      return generateRandomIPv4();
  }
}

/**
 * Генерирует случайный MAC-адрес.
 * @param {string} format - Формат разделителя ('colon', 'dash', 'dot', 'none').
 * @returns {string} MAC-адрес.
 */
function generateRandomMACAddress(format = 'colon') {
  const bytes = Array.from({ length: 6 }, () => Math.floor(paretoRandom(0, 255)));
  const hexBytes = bytes.map(b => b.toString(16).padStart(2, '0'));

  switch (format) {
    case 'dash':
      return hexBytes.join('-');
    case 'dot':
      return `${hexBytes[0]}${hexBytes[1]}.${hexBytes[2]}${hexBytes[3]}.${hexBytes[4]}${hexBytes[5]}`;
    case 'none':
      return hexBytes.join('');
    case 'colon':
    default:
      return hexBytes.join(':');
  }
}

/**
 * Генерирует случайный email.
 * @returns {string} Email-адрес.
 */
function generateRandomEmail() {
  const localPartLength = paretoRandom(5, 15);
  let localPart = '';
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  for (let i = 0; i < localPartLength; i++) {
    localPart += chars.charAt(Math.floor(paretoRandom(0, chars.length - 1)));
  }
  const domain = getRandomValueFromArray(emailDomainsData.domains);
  const globalDomain = getRandomValueFromArray(domainsData.GEN);
  return `${localPart}@${domain}${globalDomain}`;
}

/**
 * Форматирует телефонный номер по заданному шаблону.
 * @param {string} rawNumber - Номер вида "+79001234567"
 * @param {string} format - Формат из PHONE_FORMATS
 * @returns {string}
 */
function formatPhoneNumber(rawNumber, format = PHONE_FORMATS.DASHED) {
  if (!rawNumber || rawNumber.length < 11) return rawNumber;
  const digits = rawNumber.replace(/\D/g, '');
  const countryCode = digits.slice(0, 1);
  const rest = digits.slice(1, 11);
  if (rest.length !== 10) return rawNumber;

  const [a, b, c, d, e, f, g, h, i, j] = rest.split('');

  switch (format) {
    case PHONE_FORMATS.PLAIN:
      return `+${countryCode}${rest}`;
    case PHONE_FORMATS.SPACED:
      return `+${countryCode} ${a}${b}${c} ${d}${e}${f} ${g}${h} ${i}${j}`;
    case PHONE_FORMATS.PARENTHESES:
      return `+${countryCode}(${a}${b}${c})${d}${e}${f}${g}${h}${i}${j}`;
    case PHONE_FORMATS.DASHED:
    default:
      return `+${countryCode}-${a}${b}${c}-${d}${e}${f}-${g}${h}-${i}${j}`;
  }
}

/**
 * Генерирует случайный телефонный номер в указанном формате.
 * @param {string} format - Формат из PHONE_FORMATS
 * @returns {string}
 */
function generateRandomPhoneNumber(format = PHONE_FORMATS.DASHED) {
  const countryCodes = Object.values(phonesCountriesData);
  if (countryCodes.length === 0) {
    return formatPhoneNumber('+79001234567', format);
  }
  let code = getRandomValueFromArray(countryCodes);
  const remainingLength = Math.max(0, 11 - code.replace(/\D/g, '').length);
  for (let i = 0; i < remainingLength; i++) {
    code += Math.floor(paretoRandom(0, 9));
  }
  return formatPhoneNumber(code, format);
}

/**
 * Находит код страны по значению источника и его типу.
 * @param {string} sourceValue - Значение из колонки-источника.
 * @param {string} sourceType - Тип колонки-источника (например, TYPES.COUNTRY).
 * @param {Object} countriesData - Данные о странах.
 * @param {string} language - Текущий язык.
 * @returns {string|null} ISO-код страны или null.
 */
function findCountryCodeBySource(sourceValue, sourceType, countriesData, language) {
  if (sourceType === TYPES.COUNTRY_CODE) return sourceValue;
  if (sourceType === TYPES.COUNTRY) {
    return Object.keys(countriesData[language]).find(code => countriesData[language][code] === sourceValue);
  }
  return null;
}

/**
 * Находит код страны и региона по значению источника (для REGION/CITY).
 * @param {string} sourceValue - Значение источника.
 * @param {string} sourceType - Тип источника.
 * @param {Object} regionsData - Данные о регионах.
 * @param {Object} citiesData - Данные о городах.
 * @param {string} language - Язык.
 * @param {Object} countriesData - Данные о странах (добавлено).
 * @returns {{countryCode: string|null, regionCode: string|null}}
 */
function findCountryAndRegionBySource(sourceValue, sourceType, regionsData, citiesData, language, countriesData) {
  if (sourceType === TYPES.COUNTRY_CODE) {
    return { countryCode: sourceValue, regionCode: null };
  }

  if (sourceType === TYPES.COUNTRY) {
    // Ищем код страны по её названию в countriesData
    const countryCode = Object.keys(countriesData[language]).find(
      code => countriesData[language][code] === sourceValue
    );
    return { countryCode: countryCode || null, regionCode: null };
  }

  if (sourceType === TYPES.REGION) {
    for (const [cCode, regions] of Object.entries(regionsData[language])) {
      for (const [rCode, rName] of Object.entries(regions)) {
        if (rName === sourceValue) return { countryCode: cCode, regionCode: rCode };
      }
    }
  }

  if (sourceType === TYPES.CITY) {
    for (const [cCode, regions] of Object.entries(citiesData[language])) {
      for (const [rCode, cityList] of Object.entries(regions)) {
        if (Array.isArray(cityList) && cityList.includes(sourceValue)) {
          return { countryCode: cCode, regionCode: rCode };
        }
      }
    }
  }

  return { countryCode: null, regionCode: null };
}

/**
 * Лениво генерирует значение для колонки-источника, если оно ещё не задано.
 * @param {number} sourceIndex - Индекс колонки-источника.
 * @param {Object} sourceCol - Описание колонки-источника.
 * @param {Array} columnValues - Массив уже сгенерированных значений.
 * @param {Object} row - Текущая строка результата.
 * @param {string} header - Заголовок текущей колонки.
 * @param {Object} countriesData - Данные о странах.
 * @param {Object} regionsData - Данные о регионах.
 * @param {Object} citiesData - Данные о городах.
 * @param {Object} streetsData - Данные об улицах.
 * @param {string} language - Язык.
 * @returns {string|undefined} Сгенерированное значение или undefined.
 */
function ensureSourceValue(
  sourceIndex,
  sourceCol,
  columnValues,
  row,
  header,
  countriesData,
  regionsData,
  citiesData,
  streetsData,
  language
) {
  if (columnValues[sourceIndex] !== undefined) return columnValues[sourceIndex];

  let sourceValue = 'Нет данных';
  const srcHeader = sanitizeHeader(sourceCol.name, sourceIndex);

  if (sourceCol.type === TYPES.COUNTRY_CODE) {
    const allCodes = Object.keys(countriesData[language]);
    sourceValue = allCodes.length ? getRandomValueFromArray(allCodes) : 'XX';
  } else if (sourceCol.type === TYPES.COUNTRY) {
    const allCodes = Object.keys(countriesData[language]);
    const code = allCodes.length ? getRandomValueFromArray(allCodes) : 'XX';
    sourceValue = countriesData[language][code] || 'Нет данных';
  } else if (sourceCol.type === TYPES.REGION) {
    const countryCodes = Object.keys(countriesData[language]);
    if (countryCodes.length > 0) {
      const countryCode = getRandomValueFromArray(countryCodes);
      const regionsInCountry = regionsData[language][countryCode];
      if (regionsInCountry && Object.keys(regionsInCountry).length > 0) {
        sourceValue = getRandomValueFromArray(Object.values(regionsInCountry));
      }
    }
  } else if (sourceCol.type === TYPES.CITY) {
    const countryCodes = Object.keys(countriesData[language]);
    if (countryCodes.length > 0) {
      const countryCode = getRandomValueFromArray(countryCodes);
      const countryCities = citiesData[language][countryCode];
      if (countryCities) {
        let allCities = [];
        Object.values(countryCities).forEach(citiesList => {
          if (Array.isArray(citiesList)) allCities.push(...citiesList);
        });
        if (allCities.length > 0) sourceValue = getRandomValueFromArray(allCities);
      }
    }
  } else if (sourceCol.type === TYPES.STREET) {
    const countryCodes = Object.keys(streetsData[language]);
    if (countryCodes.length > 0) {
      const countryCode = getRandomValueFromArray(countryCodes);
      const streets = streetsData[language][countryCode];
      if (Array.isArray(streets) && streets.length > 0) {
        sourceValue = getRandomValueFromArray(streets);
      }
    }
  }

  columnValues[sourceIndex] = sourceValue;
  row[srcHeader] = sourceValue;
  return sourceValue;
}

/**
 * Генерирует одну строку данных в соответствии с описанием колонок.
 * (Оптимизированная версия)
 */
function buildRowObject(
  columns,
  autoId,
  countriesData,
  citiesData,
  genderData,
  namesData,
  regionsData,
  streetsData,
  language,
  externalData = null
) {
  const row = {};
  const headers = columns.map((col, idx) => sanitizeHeader(col.name, idx));

  // Проверка наличия необходимых данных
  const required = [genderData, namesData, countriesData, regionsData, citiesData, streetsData];
  if (required.some(d => !d?.[language])) return row;

  const columnValues = new Array(columns.length);
  const regionInheritanceConstraints = {};

  // Сбор ограничений на страны для колонок REGION с selectedCountries
  for (let i = 0; i < columns.length; i++) {
    const col = columns[i];
    if (col.type === TYPES.REGION && col.inheritFrom != null && col.inheritFrom >= 0) {
      const sourceIndex = col.inheritFrom;
      const selected = col.selectedCountries || [];
      if (!selected.includes('all') && selected.length > 0) {
        if (!regionInheritanceConstraints[sourceIndex]) {
          regionInheritanceConstraints[sourceIndex] = new Set();
        }
        selected.forEach(code => regionInheritanceConstraints[sourceIndex].add(code));
      }
    }
  }

  // Генерация колонок
  for (let i = 0; i < columns.length; i++) {
    const col = columns[i];
    const header = headers[i];
    const type = col.type || TYPES.ID_AUTO;

    if (row[header] !== undefined) continue;

    // === Имена с наследованием пола ===
    if ([TYPES.FIRST_NAME, TYPES.MIDDLE_NAME, TYPES.LAST_NAME].includes(type)) {
      let genderValue = null;
      if (col.inheritFrom !== null && col.inheritFrom >= 0) {
        const sourceCol = columns[col.inheritFrom];
        let sourceValue = columnValues[col.inheritFrom];
        if (sourceValue === undefined) {
          if (sourceCol.type === TYPES.GENDER) {
            sourceValue = getRandomValueFromArray(Object.values(genderData[language]));
            columnValues[col.inheritFrom] = sourceValue;
            row[sanitizeHeader(sourceCol.name, col.inheritFrom)] = sourceValue;
          }
        }
        if (typeof sourceValue === 'string') genderValue = sourceValue;
      }

      let names;
      if (genderValue) {
        const genderKey = Object.keys(genderData[language]).find(k => genderData[language][k] === genderValue);
        names = genderKey && namesData[language][genderKey] ? namesData[language][genderKey] : null;
      }
      if (!names) {
        const key = getRandomValueFromArray(Object.keys(namesData[language]));
        names = namesData[language][key];
      }

      const keyMap = { [TYPES.FIRST_NAME]: 'first', [TYPES.MIDDLE_NAME]: 'middle', [TYPES.LAST_NAME]: 'last' };
      const nameList = names?.[keyMap[type]] || [];
      row[header] = nameList.length ? getRandomValueFromArray(nameList) : 'Нет данных';
      columnValues[i] = row[header];
      continue;
    }

    // === COUNTRY_CODE ===
    if (type === TYPES.COUNTRY_CODE) {
      let allowedCodes = Object.keys(countriesData[language]);
      if (regionInheritanceConstraints[i]) {
        const constrained = Array.from(regionInheritanceConstraints[i]).filter(code =>
          countriesData[language].hasOwnProperty(code)
        );
        if (constrained.length > 0) allowedCodes = constrained;
      }
      row[header] = allowedCodes.length ? getRandomValueFromArray(allowedCodes) : 'XX';
      columnValues[i] = row[header];
      continue;
    }

    // === COUNTRY ===
    if (type === TYPES.COUNTRY) {
      let countryCode;
      if (col.inheritFrom !== null && col.inheritFrom >= 0) {
        const sourceCol = columns[col.inheritFrom];
        let sourceValue = ensureSourceValue(
          col.inheritFrom,
          sourceCol,
          columnValues,
          row,
          header,
          countriesData,
          regionsData,
          citiesData,
          streetsData,
          language
        );
        countryCode = findCountryCodeBySource(sourceValue, sourceCol.type, countriesData, language);
      } else {
        let allowedCodes = Object.keys(countriesData[language]);
        if (regionInheritanceConstraints[i]) {
          const constrained = Array.from(regionInheritanceConstraints[i]).filter(code =>
            countriesData[language].hasOwnProperty(code)
          );
          if (constrained.length > 0) allowedCodes = constrained;
        }
        countryCode = allowedCodes.length ? getRandomValueFromArray(allowedCodes) : 'XX';
      }
      row[header] = countriesData[language][countryCode] || 'Нет данных';
      columnValues[i] = row[header];
      continue;
    }

    // === REGION ===
    if (type === TYPES.REGION) {
      let regions = [];
      if (col.inheritFrom !== null && col.inheritFrom >= 0) {
        const sourceCol = columns[col.inheritFrom];
        const sourceValue = ensureSourceValue(
          col.inheritFrom,
          sourceCol,
          columnValues,
          row,
          header,
          countriesData,
          regionsData,
          citiesData,
          streetsData,
          language
        );
        const { countryCode } = findCountryAndRegionBySource(
          sourceValue,
          sourceCol.type,
          regionsData,
          citiesData,
          language,
          countriesData
        );
        if (countryCode && regionsData[language][countryCode]) {
          regions = Object.values(regionsData[language][countryCode]);
        }
      } else {
        const selected = col.selectedCountries || [];
        if (selected.includes('all') || selected.length === 0) {
          Object.values(regionsData[language]).forEach(country => {
            Object.values(country).forEach(r => regions.push(r));
          });
        } else {
          const valid = selected.filter(code => regionsData[language][code]);
          valid.forEach(code => {
            Object.values(regionsData[language][code]).forEach(r => regions.push(r));
          });
        }
      }
      row[header] = regions.length ? getRandomValueFromArray(regions) : 'Нет данных';
      columnValues[i] = row[header];
      continue;
    }

    // === CITY ===
    if (type === TYPES.CITY) {
      let allCities = [];
      if (col.inheritFrom !== null && col.inheritFrom >= 0) {
        const sourceCol = columns[col.inheritFrom];
        const sourceValue = ensureSourceValue(
          col.inheritFrom,
          sourceCol,
          columnValues,
          row,
          header,
          countriesData,
          regionsData,
          citiesData,
          streetsData,
          language
        );
        const { countryCode, regionCode } = findCountryAndRegionBySource(
          sourceValue,
          sourceCol.type,
          regionsData,
          citiesData,
          language,
          countriesData
        );
        if (countryCode && citiesData[language][countryCode]) {
          const countryCities = citiesData[language][countryCode];
          if (regionCode && countryCities[regionCode]) {
            allCities = countryCities[regionCode];
          } else {
            Object.values(countryCities).forEach(citiesList => {
              if (Array.isArray(citiesList)) allCities.push(...citiesList);
            });
          }
        }
      } else {
        Object.values(citiesData[language]).forEach(country => {
          Object.values(country).forEach(citiesList => {
            if (Array.isArray(citiesList)) allCities.push(...citiesList);
          });
        });
      }
      row[header] = allCities.length ? getRandomValueFromArray(allCities) : 'Нет данных';
      columnValues[i] = row[header];
      continue;
    }

    // === STREET ===
    if (type === TYPES.STREET) {
      let streets = [];
      if (col.inheritFrom !== null && col.inheritFrom >= 0) {
        const sourceCol = columns[col.inheritFrom];
        const sourceValue = ensureSourceValue(
          col.inheritFrom,
          sourceCol,
          columnValues,
          row,
          header,
          countriesData,
          regionsData,
          citiesData,
          streetsData,
          language
        );
        const { countryCode } = findCountryAndRegionBySource(
          sourceValue,
          sourceCol.type,
          regionsData,
          citiesData,
          language,
          countriesData
        );
        if (countryCode && streetsData[language][countryCode]) {
          streets = streetsData[language][countryCode];
        }
      } else {
        const selected = col.selectedCountries || [];
        if (selected.includes('all') || selected.length === 0) {
          Object.values(streetsData[language]).forEach(country => {
            if (Array.isArray(country)) streets.push(...country);
          });
        } else {
          const valid = selected.filter(code => streetsData[language][code]);
          valid.forEach(code => {
            if (Array.isArray(streetsData[language][code])) {
              streets.push(...streetsData[language][code]);
            }
          });
        }
      }
      row[header] = streets.length ? getRandomValueFromArray(streets) : 'Нет данных';
      columnValues[i] = row[header];
      continue;
    }

    // === PHONE ===
    if (type === TYPES.PHONE) {
      const format = col.phoneFormat || PHONE_FORMATS.DASHED;
      let countryCode = null;

      if (col.inheritFrom !== null && col.inheritFrom >= 0) {
        const sourceCol = columns[col.inheritFrom];
        const sourceValue = ensureSourceValue(
          col.inheritFrom,
          sourceCol,
          columnValues,
          row,
          header,
          countriesData,
          regionsData,
          citiesData,
          streetsData,
          language
        );
        countryCode = findCountryCodeBySource(sourceValue, sourceCol.type, countriesData, language);
        if (!countryCode) {
          const result = findCountryAndRegionBySource(sourceValue, sourceCol.type, regionsData, citiesData, language);
          countryCode = result.countryCode;
        }
      }

      if (countryCode && phonesCountriesData[countryCode]) {
        const code = phonesCountriesData[countryCode];
        const remainingLength = Math.max(0, 11 - code.replace(/\D/g, '').length);
        let fullNumber = code;
        for (let j = 0; j < remainingLength; j++) {
          fullNumber += Math.floor(paretoRandom(0, 9));
        }
        row[header] = formatPhoneNumber(fullNumber, format);
      } else {
        row[header] = generateRandomPhoneNumber(format);
      }
      columnValues[i] = row[header];
      continue;
    }

    // === ГЕО-ТИПЫ с наследованием ===
    const isGeoType = [
      TYPES.LATITUDE,
      TYPES.LONGITUDE,
      TYPES.COORDINATES_LAT_LNG,
      TYPES.COORDINATES_LNG_LAT,
      TYPES.LINE_BI,
      TYPES.LINE_GEOJSON,
    ].includes(type);

    if (isGeoType && col.inheritFrom !== null && col.inheritFrom >= 0) {
      const sourceCol = columns[col.inheritFrom];
      const sourceValue = ensureSourceValue(
        col.inheritFrom,
        sourceCol,
        columnValues,
        row,
        header,
        countriesData,
        regionsData,
        citiesData,
        streetsData,
        language
      );

      let generatedPoint = null;
      let polygonUsed = null;

      // 1. Город → точные координаты
      if (sourceCol.type === TYPES.CITY) {
        outerCity: for (const [countryCode, regions] of Object.entries(citiesData[language])) {
          for (const [regionCode, cityList] of Object.entries(regions)) {
            const idx = cityList.indexOf(sourceValue);
            if (idx !== -1) {
              const countryCoords = citiesCoordsData[countryCode];
              if (countryCoords?.[regionCode]?.[idx]) {
                const [lng, lat] = countryCoords[regionCode][idx];
                if (typeof lng === 'number' && typeof lat === 'number') {
                  generatedPoint = generateRandomPointInRadius(lat, lng, 4);
                  break outerCity;
                }
              }
            }
          }
        }
      }

      // 2. Полигон (регион → страна)
      if (!generatedPoint) {
        const { countryCode, regionCode } = findCountryAndRegionBySource(
          sourceValue,
          sourceCol.type,
          regionsData,
          citiesData,
          language,
          countriesData
        );

        if (regionCode && countryCode) {
          const regionPolygon = loadPolygonByRegion(countryCode, regionCode);
          if (regionPolygon) {
            try {
              generatedPoint = generateRandomPointInPolygon(regionPolygon);
              polygonUsed = regionPolygon;
            } catch (e) {
              console.warn(
                `Не удалось сгенерировать точку в полигоне региона ${regionCode} (${countryCode}):`,
                e.message
              );
            }
          }
        }

        if (!generatedPoint && countryCode) {
          const countryPolygon = loadPolygonByCountryCode(countryCode);
          if (countryPolygon) {
            try {
              generatedPoint = generateRandomPointInPolygon(countryPolygon);
              polygonUsed = countryPolygon;
            } catch (e) {
              console.warn(`Не удалось сгенерировать точку в полигоне страны ${countryCode}:`, e.message);
            }
          }
        }
      }

      // 3. Формирование результата
      if (generatedPoint) {
        const { lat, lng } = generatedPoint;
        switch (type) {
          case TYPES.LATITUDE:
            row[header] = lat;
            break;
          case TYPES.LONGITUDE:
            row[header] = lng;
            break;
          case TYPES.COORDINATES_LAT_LNG:
            row[header] = `[${lat},${lng}]`;
            break;
          case TYPES.COORDINATES_LNG_LAT:
            row[header] = `[${lng},${lat}]`;
            break;
          case TYPES.LINE_BI:
            row[header] = polygonUsed
              ? generateRandomLineInPolygon(polygonUsed)
              : JSON.stringify(
                  Array.from({ length: paretoRandom(2, 4) }, () => {
                    const pt = generateRandomPointInRadius(lat, lng, 4);
                    return [pt.lng, pt.lat];
                  })
                );
            break;
          case TYPES.LINE_GEOJSON:
            row[header] = polygonUsed
              ? generateRandomLineGeoJSONInPolygon(polygonUsed)
              : JSON.stringify({
                  type: 'LineString',
                  coordinates: Array.from({ length: paretoRandom(2, 4) }, () => {
                    const pt = generateRandomPointInRadius(lat, lng, 4);
                    return [pt.lng, pt.lat];
                  }),
                });
            break;
        }
      } else {
        row[header] = 'Нет данных';
      }
      columnValues[i] = row[header];
      continue;
    }

    // === CUSTOM_LIST и file_* ===
    if (type === TYPES.CUSTOM_LIST) {
      const list = Array.isArray(col.customList) ? col.customList : [];
      if (list.length === 0) {
        row[header] = '';
      } else {
        const mode = col.customListMode || CustomListMode.RANDOM;
        if (mode === CustomListMode.RANDOM) {
          row[header] = getRandomValueFromArray(list);
        } else {
          const index = (autoId - 1) % list.length;
          row[header] = list[index];
        }
      }
      columnValues[i] = row[header];
      continue;
    }

    if (type.startsWith('file_') && type.includes('_col_')) {
      const colIndexMatch = type.match(/_col_(\d+)$/);
      const keyMatch = type.match(/^file_(.+)_col_\d+$/);
      const colIndex = colIndexMatch ? parseInt(colIndexMatch[1], 10) : -1;
      const key = keyMatch ? keyMatch[1] : null;

      if (key !== null && colIndex >= 0 && externalData) {
        const source = externalData.find(src => src.key === key);
        if (source?.rows?.length > 0) {
          const rows = source.rows;
          const mode = col.customListMode || 'random';
          const selectedRow =
            mode === 'sequential' || mode === 'sequential_ignore_size'
              ? rows[(autoId - 1) % rows.length]
              : getRandomValueFromArray(rows);
          row[header] = colIndex < selectedRow.length ? selectedRow[colIndex] : '';
        } else {
          row[header] = '';
        }
      } else {
        row[header] = '';
      }
      columnValues[i] = row[header];
      continue;
    }

    // === Остальные типы ===
    switch (type) {
      case TYPES.ID_AUTO:
        row[header] = autoId;
        break;
      case TYPES.INTEGER: {
        const min = Math.min(col.min ?? 1, col.max ?? 1000);
        const max = Math.max(col.min ?? 1, col.max ?? 1000);
        row[header] = Math.floor(paretoRandom(min, max));
        break;
      }
      case TYPES.FLOAT: {
        const min = col.min ?? 0;
        const max = col.max ?? 1000;
        const precision = col.precision ?? 5;
        const range = max - min;
        const randomValue = paretoRandom(0, range * Math.pow(10, precision)) / Math.pow(10, precision) + min;
        row[header] = parseFloat(randomValue.toFixed(precision));
        break;
      }
      case TYPES.BOOLEAN:
        row[header] = paretoRandom(0, 1) >= 0.5;
        break;
      case TYPES.NULL:
        row[header] = null;
        break;
      case TYPES.EMPTY_STRING:
        row[header] = '';
        break;
      case TYPES.GUID:
        row[header] = generateGUID();
        break;
      case TYPES.GENDER:
        row[header] = getRandomValueFromArray(Object.values(genderData[language]));
        break;
      case TYPES.DATE: {
        const from = col.fromDate ? parseDate(col.fromDate) : new Date(2000, 0, 1);
        const to = col.toDate ? parseDate(col.toDate) : new Date();
        row[header] = generateRandomDate(from, to, col.dateFormat || DATE_FORMATS.ISO_8601);
        break;
      }
      case TYPES.COLOR:
        row[header] = generateRandomColor(col.colorFormat || COLOR_FORMATS.HEX, col.transparency || false);
        break;
      case TYPES.LATITUDE:
        row[header] = generateRandomLatitude();
        break;
      case TYPES.LONGITUDE:
        row[header] = generateRandomLongitude();
        break;
      case TYPES.COORDINATES_LAT_LNG:
        row[header] = generateRandomCoordinatesLatLng();
        break;
      case TYPES.COORDINATES_LNG_LAT:
        row[header] = generateRandomCoordinatesLngLat();
        break;
      case TYPES.LINE_BI:
        row[header] = generateRandomLine();
        break;
      case TYPES.LINE_GEOJSON:
        row[header] = generateRandomLineGeoJSON();
        break;
      case TYPES.IP:
        row[header] = generateIP(col.ipFormat || IP_FORMATS.IPV4);
        break;
      case TYPES.MAC_ADDRESS:
        row[header] = generateRandomMACAddress(col.macFormat || 'colon');
        break;
      case TYPES.NAUGHTY_STRINGS:
        row[header] = getRandomValueFromArray(naughtyStrings);
        break;
      case TYPES.EMAIL:
        row[header] = generateRandomEmail();
        break;
      case TYPES.PHONE:
        row[header] = generateRandomPhoneNumber();
        break;
      case TYPES.RANDOM_LENGTH_STRING: {
        const baseText = col.baseText || 'текст';
        const maxLength = Math.max(0, parseInt(col.maxLength, 10) || 10);
        const randomLength = paretoRandom(0, maxLength);
        row[header] = baseText.substring(0, randomLength);
        break;
      }
      default:
        row[header] = '';
    }
    columnValues[i] = row[header];
  }

  // Универсальное применение формул преобразования
  for (let i = 0; i < columns.length; i++) {
    const col = columns[i];
    const header = headers[i];

    if (!col.transformFormula) continue;
    if (row[header] === undefined) continue;

    const originalValue = row[header];
    row[header] = evaluateTransformationFormula(col.transformFormula, row, originalValue);
    columnValues[i] = row[header];
  }

  // Применение вероятности null
  for (let i = 0; i < columns.length; i++) {
    const col = columns[i];
    const header = headers[i];
    const prob = parseFloat(col.nullProbability) || 0;
    if (prob > 0 && Math.random() * 100 < prob) {
      row[header] = null;
    }
  }

  return row;
}

module.exports = {
  TYPES,
  DATE_FORMATS,
  COLOR_FORMATS,
  IP_FORMATS,
  PHONE_FORMATS,
  buildRowObject,
  generateGUID,
  generateRandomDate,
  generateRandomColor,
};
