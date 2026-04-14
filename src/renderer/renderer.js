const { marked } = require('marked');
const {
  writeCSVStream,
  writeXMLStream,
  writeXLSXStream,
  writeJSONStream,
  writeQVDStream,
} = require('../utils/fileGenerator');
const { TYPES, DATE_FORMATS, COLOR_FORMATS, IP_FORMATS, buildRowObject } = require('../utils/dataGenerator');
const { parseXLSX, parseQVD, parseCSV, parseJSON, parseXML } = require('../utils/fileParser');

// Загрузка данных (полные и облегчённые версии)
const countriesDataFull = require('../data/countriesData.json');
const genderDataFull = require('../data/genderTypes.json');
const namesDataFull = require('../data/namesData.json');
const regionsDataFull = require('../data/regionsData.json');
const citiesDataFull = require('../data/citiesData.json');
const streetsDataFull = require('../data/streetsData.json');

const countriesDataLight = require('../data/lightData/countriesData.json');
const namesDataLight = require('../data/lightData/namesData.json');
const regionsDataLight = require('../data/lightData/regionsData.json');
const citiesDataLight = require('../data/lightData/citiesData.json');
const streetsDataLight = require('../data/lightData/streetsData.json');

// Глобальные переменные
let history = [];
let historyIndex = -1;
const MAX_HISTORY = 40;

const LOCAL_STORAGE_CONFIG_KEY = 'test-data-generator-config-v1';

let currentTheme = localStorage.getItem('theme') || 'system';
let generationTypeSelect = null;
let autoScrollInterval = null;

// Ссылки на DOM-элементы
const filenameBaseInput = document.getElementById('filenameBase');
const columnsList = document.getElementById('columnsList');
const addColumnBtn = document.getElementById('addColumn');
const generateBtn = document.getElementById('generateBtn');
const rowsCountInput = document.getElementById('rowsCount');
const formatSelect = document.getElementById('format');
const languageSelect = document.getElementById('language');
const refreshPreviewBtn = document.getElementById('refreshPreview');
const previewTable = document.getElementById('previewTable');
const saveSettingsBtn = document.getElementById('saveSettingsBtn');
const loadSettingsBtn = document.getElementById('loadSettingsBtn');
const dataSourceFileInput = document.getElementById('dataSourceFileInput');
const dataSourceFileBtn = document.getElementById('dataSourceFileBtn');

// Кэш внешних источников данных (файлы, загруженные пользователем)
let dataSourceCache = [];
const originalTitle = document.title;

// Обработчик выбора файла-источника данных
dataSourceFileBtn?.addEventListener('click', () => {
  dataSourceFileInput?.click();
});

dataSourceFileInput?.addEventListener('change', async e => {
  const fileList = e.target.files;
  if (!fileList || fileList.length === 0) return;
  const newFiles = Array.from(fileList);
  const newCacheEntries = [];

  // Парсинг каждого загруженного файла
  for (const file of newFiles) {
    try {
      const lowerName = file.name.toLowerCase();
      let result = null;
      const safeKey = file.name.replace(/_{2,}/g, '_').replace(/^_+|_+$/g, '');

      if (lowerName.endsWith('.csv')) {
        const text = await file.text();
        result = parseCSV(text);
      } else if (lowerName.endsWith('.json')) {
        const text = await file.text();
        result = parseJSON(text);
      } else if (lowerName.endsWith('.xml')) {
        const text = await file.text();
        result = parseXML(text);
      } else if (lowerName.endsWith('.xlsx')) {
        const arrayBuffer = await file.arrayBuffer();
        const sheets = parseXLSX(Buffer.from(arrayBuffer));
        result = sheets.map(sheet => ({
          name: `${file.name} / ${sheet.sheetName}`,
          key: `${safeKey}_${sheet.sheetName.replace(/[^a-zA-Z0-9._-]/g, '_')}`,
          headers: sheet.headers,
          rows: sheet.rows,
        }));
        newCacheEntries.push(...result);
        continue;
      } else if (lowerName.endsWith('.qvd')) {
        const arrayBuffer = await file.arrayBuffer();
        result = await parseQVD(Buffer.from(arrayBuffer));
      } else {
        throw new Error('Неподдерживаемый формат');
      }

      newCacheEntries.push({
        name: file.name,
        key: safeKey || 'file',
        headers: result.headers,
        rows: result.rows,
      });
    } catch (err) {
      console.error(`Ошибка парсинга ${file.name}:`, err);
      alert(`Не удалось загрузить ${file.name}: ${err.message}`);
    }
  }

  // Добавление уникальных файлов в кэш
  const existingNames = new Set(dataSourceCache.map(f => f.name));
  const uniqueNewEntries = newCacheEntries.filter(entry => !existingNames.has(entry.name));
  dataSourceCache.push(...uniqueNewEntries);

  renderDataSourceFileList();
  refreshAllTypeSelects();
  updatePreview();
  e.target.value = '';
});

// Отображение списка загруженных файлов
function renderDataSourceFileList() {
  const container = document.getElementById('dataSourceFileList');
  if (!container) return;
  container.innerHTML = '';

  if (dataSourceCache.length === 0) {
    container.style.display = 'none';
    if (dataSourceFileInput) dataSourceFileInput.value = '';
    return;
  }

  container.style.display = 'flex';
  dataSourceCache.forEach((source, index) => {
    const item = document.createElement('div');
    item.className = 'data-source-file-item';
    item.textContent = source.name;
    item.title = 'Добавить все столбцы файла';
    item.style.cursor = 'pointer';

    // Клик — добавить все столбцы из файла
    item.addEventListener('click', e => {
      if (e.target.classList.contains('data-source-file-remove')) return;
      const headers = source.headers || [];
      if (headers.length === 0) {
        alert(`Файл "${source.name}" не содержит столбцов.`);
        return;
      }
      headers.forEach((header, idx) => {
        const columnName = header || `Столбец ${idx + 1}`;
        const newColumnConfig = {
          name: columnName,
          type: `file_${source.key}_col_${idx}`,
          customListMode: 'sequential_ignore_size',
          sourceName: source.name,
          columnName: columnName,
        };
        const newRow = createColumnRow(newColumnConfig);
        columnsList?.appendChild(newRow);
      });
      updateInheritSelectors();
      updatePreview();
    });

    // Кнопка удаления файла из кэша
    const removeBtn = document.createElement('span');
    removeBtn.className = 'data-source-file-remove';
    removeBtn.textContent = '✕';
    removeBtn.title = 'Удалить файл из источников';
    removeBtn.addEventListener('click', e => {
      e.stopPropagation();
      dataSourceCache.splice(index, 1);
      renderDataSourceFileList();
      refreshAllTypeSelects();
      updatePreview();
    });

    item.appendChild(removeBtn);
    container.appendChild(item);
  });
}

// Обновление заголовка окна с прогрессом генерации
function updateTitle(percentage) {
  const p = Math.min(100, Math.max(0, percentage));
  document.title =
    p === 0
      ? originalTitle
      : p === 100
      ? `Готово! - ${originalTitle}`
      : `Генерация: ${Math.round(p)}% - ${originalTitle}`;
}

// Вспомогательные функции
function formatDateForInput(date) {
  return date.toISOString().split('T')[0];
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

function getCurrentDataSources() {
  const genType = generationTypeSelect?.value || 'advanced';
  const isLight = genType === 'simple';
  return {
    countriesData: isLight ? countriesDataLight : countriesDataFull,
    genderData: genderDataFull,
    namesData: isLight ? namesDataLight : namesDataFull,
    regionsData: isLight ? regionsDataLight : regionsDataFull,
    citiesData: isLight ? citiesDataLight : citiesDataFull,
    streetsData: isLight ? streetsDataLight : streetsDataFull,
  };
}

// Глобальная переменная для отслеживания открытого селекта
let currentlyOpenCustomSelect = null;

function createCustomTypeSelect(initialValue = TYPES.ID_AUTO, onValueChange = null) {
  const wrapper = document.createElement('div');
  wrapper.className = 'custom-type-select-wrapper';
  const isDark = document.body.classList.contains('dark-theme');
  const displayInput = document.createElement('input');
  displayInput.type = 'text';
  displayInput.readOnly = true;
  displayInput.className = 'custom-type-select-input';
  if (isDark) displayInput.classList.add('dark');

  let dropdown = null;
  let list = null;

  const groupedOptions = {
    Идентификаторы: [
      { value: TYPES.ID_AUTO, text: 'ID (Автоинкремент)' },
      { value: TYPES.GUID, text: 'GUID (UUID)' },
    ],
    'Числовые данные': [
      { value: TYPES.INTEGER, text: 'Целые числа' },
      { value: TYPES.FLOAT, text: 'Числа с плавающей точкой' },
    ],
    'Логические данные': [{ value: TYPES.BOOLEAN, text: 'Булевые значения' }],
    'Текстовые данные': [
      { value: TYPES.RANDOM_LENGTH_STRING, text: 'Строка случайной длины' },
      { value: TYPES.CUSTOM_LIST, text: 'Кастомный список' },
      { value: TYPES.TRANSFORMATION, text: 'Преобразования' },
      { value: TYPES.EMPTY_STRING, text: 'Всегда пустая строка' },
      { value: TYPES.NULL, text: 'Всегда null' },
      { value: TYPES.NAUGHTY_STRINGS, text: 'Капризные строки' },
    ],
    'Персональные данные': [
      { value: TYPES.GENDER, text: 'Пол' },
      { value: TYPES.LAST_NAME, text: 'Фамилии' },
      { value: TYPES.FIRST_NAME, text: 'Имена' },
      { value: TYPES.MIDDLE_NAME, text: 'Отчества' },
      { value: TYPES.EMAIL, text: 'Email' },
      { value: TYPES.PHONE, text: 'Номера телефонов' },
    ],
    'Дата и время': [{ value: TYPES.DATE, text: 'Даты' }],
    Цвета: [{ value: TYPES.COLOR, text: 'Цвета' }],
    'Географические данные': [
      { value: TYPES.COUNTRY_CODE, text: 'Коды стран' },
      { value: TYPES.COUNTRY, text: 'Страны' },
      { value: TYPES.REGION, text: 'Регионы стран' },
      { value: TYPES.CITY, text: 'Города' },
      { value: TYPES.STREET, text: 'Улицы' },
      { value: TYPES.LATITUDE, text: 'Широта' },
      { value: TYPES.LONGITUDE, text: 'Долгота' },
      { value: TYPES.COORDINATES_LAT_LNG, text: 'Координаты [широта, долгота] (Для PIX BI <1.32)' },
      { value: TYPES.COORDINATES_LNG_LAT, text: 'Координаты [долгота, широта]' },
      { value: TYPES.LINE_BI, text: 'Линии (Полигоны для PIX BI)' },
      { value: TYPES.LINE_GEOJSON, text: 'Линии (Полигоны GeoJSON)' },
    ],
    'Сетевые данные': [
      { value: TYPES.IP, text: 'IP-адреса' },
      { value: TYPES.MAC_ADDRESS, text: 'MAC-адреса' },
    ],
  };

  // Группы файлов
  const fileGroups = [];
  if (dataSourceCache.length > 0) {
    dataSourceCache.forEach(source => {
      const groupItems = [];
      source.headers.forEach((header, idx) => {
        const displayName = header || `Столбец ${idx + 1}`;
        groupItems.push({
          value: `file_${source.key}_col_${idx}`,
          text: displayName,
        });
      });
      if (groupItems.length > 0) {
        fileGroups.push({
          groupName: `Данные из ${source.name}`,
          items: groupItems,
        });
      }
    });
  }

  // Сбор всех значений для отображения
  const valueToTextMap = {};
  for (const [group, opts] of Object.entries(groupedOptions)) {
    opts.forEach(opt => {
      valueToTextMap[opt.value] = opt.text;
    });
  }
  fileGroups.forEach(group => {
    group.items.forEach(opt => {
      valueToTextMap[opt.value] = `${opt.text} — ${group.groupName.replace('Данные из ', '')}`;
    });
  });

  function updateDisplayText(value) {
    const text = valueToTextMap[value] || value;
    displayInput.value = text;
    displayInput.title = text;
    // Сброс стилей фантома при валидном значении
    if (valueToTextMap[value]) {
      displayInput.style.color = '';
    }
  }

  let currentValue = initialValue;
  updateDisplayText(currentValue);

  function closeDropdown() {
    if (!dropdown) return;
    document.body.removeChild(dropdown);
    dropdown = null;
    list = null;
    currentlyOpenCustomSelect = null;
    document.removeEventListener('click', closeOnClickOutside);
  }

  function closeOnClickOutside(e) {
    if (wrapper.contains(e.target) || (dropdown && dropdown.contains(e.target))) return;
    closeDropdown();
  }

  function openDropdown() {
    if (dropdown) return;
    if (currentlyOpenCustomSelect && currentlyOpenCustomSelect !== wrapper) {
      currentlyOpenCustomSelect.__close();
    }

    dropdown = document.createElement('div');
    dropdown.className = 'custom-type-select-dropdown';
    if (isDark) dropdown.classList.add('dark');
    list = document.createElement('div');
    list.className = 'custom-type-select-list';

    // Рендер стандартных групп
    for (const [groupName, groupOpts] of Object.entries(groupedOptions)) {
      const groupEl = document.createElement('div');
      groupEl.className = 'custom-type-select-group';
      const groupLabel = document.createElement('div');
      groupLabel.className = 'custom-type-select-group-label';
      groupLabel.textContent = groupName;
      if (isDark) groupLabel.classList.add('dark');
      groupEl.appendChild(groupLabel);
      groupOpts.forEach(opt => {
        const item = document.createElement('div');
        item.className = 'custom-type-select-item';
        if (isDark) item.classList.add('dark');
        item.textContent = opt.text;
        item.dataset.value = opt.value;
        if (opt.value === currentValue) {
          item.classList.add('selected');
        }
        item.addEventListener('click', () => {
          currentValue = opt.value;
          updateDisplayText(currentValue);
          closeDropdown();
          if (onValueChange) onValueChange(currentValue);
        });
        groupEl.appendChild(item);
      });
      list.appendChild(groupEl);
    }

    // Рендер групп по файлам
    fileGroups.forEach(fileGroup => {
      const groupEl = document.createElement('div');
      groupEl.className = 'custom-type-select-group';
      const groupLabel = document.createElement('div');
      groupLabel.className = 'custom-type-select-group-label';
      groupLabel.textContent = fileGroup.groupName;
      if (isDark) groupLabel.classList.add('dark');
      groupEl.appendChild(groupLabel);
      fileGroup.items.forEach(opt => {
        const item = document.createElement('div');
        item.className = 'custom-type-select-item';
        if (isDark) item.classList.add('dark');
        item.textContent = opt.text;
        item.dataset.value = opt.value;
        if (opt.value === currentValue) {
          item.classList.add('selected');
        }
        item.addEventListener('click', () => {
          currentValue = opt.value;
          updateDisplayText(currentValue);
          closeDropdown();
          if (onValueChange) onValueChange(currentValue);
        });
        groupEl.appendChild(item);
      });
      list.appendChild(groupEl);
    });

    dropdown.appendChild(list);
    document.body.appendChild(dropdown);

    // Позиционирование
    const rect = displayInput.getBoundingClientRect();
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;
    const listHeight = Math.min(list.scrollHeight, 400);
    const listWidth = rect.width;
    let top, left;
    const spaceBelow = viewportHeight - rect.bottom;
    const spaceAbove = rect.top;
    if (spaceBelow >= listHeight || spaceBelow >= spaceAbove) {
      top = rect.bottom;
    } else {
      top = rect.top - listHeight;
    }
    left = rect.left;
    if (left + listWidth > viewportWidth) {
      left = Math.max(0, viewportWidth - listWidth - 10);
    }
    dropdown.style.left = left + 'px';
    dropdown.style.top = top + 'px';
    dropdown.style.width = listWidth + 'px';

    // Прокрутка к выбранному элементу
    setTimeout(() => {
      const selectedItem = list.querySelector('.custom-type-select-item.selected');
      if (selectedItem) {
        const containerRect = list.getBoundingClientRect();
        const itemRect = selectedItem.getBoundingClientRect();
        const isVisible = itemRect.top >= containerRect.top && itemRect.bottom <= containerRect.bottom;
        if (!isVisible) {
          selectedItem.scrollIntoView({ block: 'nearest', behavior: 'instant' });
        }
      }
    }, 0);

    currentlyOpenCustomSelect = wrapper;
    wrapper.__close = closeDropdown;
    setTimeout(() => {
      document.addEventListener('click', closeOnClickOutside);
    }, 0);
  }

  // Клик всегда открывает dropdown, даже для фантомных значений
  displayInput.addEventListener('click', e => {
    e.stopPropagation();
    if (dropdown) {
      closeDropdown();
    } else {
      openDropdown();
    }
  });

  wrapper.appendChild(displayInput);
  return {
    element: wrapper,
    getValue: () => currentValue,
    setValue: newValue => {
      currentValue = newValue;
      updateDisplayText(currentValue);
    },
  };
}

// Обновление опций в выпадающих списках
function refreshAllTypeSelects() {
  const rows = [...columnsList.querySelectorAll('.col-row')];
  rows.forEach(row => {
    const oldWrapper = row.querySelector('.custom-type-select-wrapper');
    if (!oldWrapper) return;
    const oldValue = row.__typeSelect?.getValue() || TYPES.ID_AUTO;
    const isPhantom =
      oldValue.startsWith('file_') && !dataSourceCache.some(src => oldValue.startsWith(`file_${src.key}_`));
    const newWrapper = createCustomTypeSelect(oldValue, () => {
      updateFieldVisibility(row);
      updateInheritSelectors();
      updatePreview();
      saveToHistory();
    });
    if (isPhantom) {
      const inputEl = newWrapper.element.querySelector('.custom-type-select-input');
      const displayName = row.dataset.columnName || 'Неизвестный столбец';
      const sourceName = row.dataset.sourceName || 'неизвестный файл';
      inputEl.value = `⚠️ Удалён: "${displayName}" из "${sourceName}"`;
      inputEl.style.color = '#e74c3c';
      inputEl.title = inputEl.value;
    }
    oldWrapper.replaceWith(newWrapper.element);
    row.__typeSelect = newWrapper;
  });
}

// Мультивыбор стран для типа "Регион"
function createCountriesMultiSelect(selectedCountries = ['all'], countriesData, onSelectionChange = null) {
  const container = document.createElement('div');
  container.className = 'countries-multiselect-container';

  const {
    element: dropdown,
    getValue,
    setValue,
  } = createSearchableMultiSelect({
    options: [
      { value: 'all', label: 'Все страны' },
      ...Object.entries(countriesData.ru).map(([code, name]) => ({ value: code, label: name })),
    ],
    selectedValues: selectedCountries,
    placeholder: 'Выберите страны...',
    onChange: onSelectionChange,
  });

  container.appendChild(dropdown);
  return {
    container,
    getValue,
    setValue,
  };
}

function updateFieldVisibility(row) {
  const t = row.__typeSelect.getValue();
  const params = row.querySelectorAll('.param-group');
  const isFileColumn = t.startsWith('file_') && t.includes('_col_');

  const show = (idx, cond) => {
    if (params[idx]) params[idx].style.display = cond ? 'block' : 'none';
  };

  show(0, t === TYPES.INTEGER || t === TYPES.FLOAT); // min
  show(1, t === TYPES.INTEGER || t === TYPES.FLOAT); // max
  show(2, t === TYPES.FLOAT); // precision
  show(3, t === TYPES.DATE); // date from
  show(4, t === TYPES.DATE); // date to
  show(5, t === TYPES.DATE); // date format
  show(6, t === TYPES.COLOR); // color format
  show(7, t === TYPES.COLOR); // transparency
  row.querySelector('.countries-multiselect-container').style.display = t === TYPES.REGION ? 'block' : 'none';
  show(8, t === TYPES.IP); // ip format
  show(9, t === TYPES.MAC_ADDRESS); // mac format
  show(10, t === TYPES.PHONE); // phone format
  show(11, t === TYPES.CUSTOM_LIST); // custom list
  show(12, t === TYPES.CUSTOM_LIST); // custom separator
  show(13, t === TYPES.CUSTOM_LIST || isFileColumn); // custom mode
  show(14, t === TYPES.RANDOM_LENGTH_STRING); // base text
  show(15, t === TYPES.RANDOM_LENGTH_STRING); // max length
  show(16, t === TYPES.TRANSFORMATION); // transformation formula
  if (params[17]) params[17].style.display = 'block'; // null prob
}

// Обновление мультивыбора стран при смене режима генерации
function refreshRegionCountrySelectors() {
  const dataSources = getCurrentDataSources();
  const rows = [...columnsList.querySelectorAll('.col-row')];
  rows.forEach(row => {
    const typeSelect = row.querySelector('select');
    if (row.__typeSelect.getValue() !== TYPES.REGION) return;
    const countriesMultiSelectContainer = row.querySelector('.countries-multiselect-container');
    if (!countriesMultiSelectContainer) return;

    // Находим или создаём .param-group с лейблом "Страны"
    let paramGroup = countriesMultiSelectContainer.querySelector('.param-group');
    if (!paramGroup) {
      const label = document.createElement('label');
      label.textContent = 'Страны';
      label.style.display = 'block';
      label.style.fontSize = '10px';
      label.style.marginBottom = '4px';
      paramGroup = document.createElement('div');
      paramGroup.className = 'param-group';
      paramGroup.appendChild(label);
      countriesMultiSelectContainer.appendChild(paramGroup);
    }

    // Сохраняем текущий выбор
    let selected = ['all'];
    const oldInstance = countriesMultiSelectContainer.__searchableInstance;
    if (oldInstance) {
      selected = oldInstance.getValue();
    }

    while (paramGroup.children.length > 1) {
      paramGroup.removeChild(paramGroup.lastChild);
    }

    const newInstance = createCountriesMultiSelect(selected, dataSources.countriesData);

    paramGroup.appendChild(newInstance.container);
    countriesMultiSelectContainer.__searchableInstance = newInstance;
  });
}

function createSearchableMultiSelect({ options, selectedValues = [], placeholder, onChange }) {
  const wrapper = document.createElement('div');
  wrapper.className = 'searchable-multiselect-wrapper';

  const input = document.createElement('input');
  input.type = 'text';
  input.placeholder = placeholder;
  input.readOnly = true;
  input.className = 'searchable-multiselect-input';
  const isDark = document.body.classList.contains('dark-theme');
  if (isDark) {
    input.classList.add('dark');
  }

  let dropdown = null;
  let searchInput = null;
  let list = null;
  const itemElements = new Map();

  function renderList(filter = '') {
    if (!list) return;
    list.innerHTML = '';
    itemElements.clear();

    const filtered = options.filter(
      opt =>
        opt.label.toLowerCase().includes(filter.toLowerCase()) || opt.value.toLowerCase().includes(filter.toLowerCase())
    );

    filtered.forEach(opt => {
      const item = document.createElement('div');
      item.className = 'searchable-multiselect-item';
      if (isDark) item.classList.add('dark');
      if (selectedValues.includes(opt.value)) {
        item.classList.add('selected');
      }
      item.textContent = opt.label;
      item.dataset.value = opt.value;

      list.appendChild(item);
      itemElements.set(opt.value, item);

      item.addEventListener('click', () => {
        toggleSelection(opt.value);
      });
    });
  }

  function toggleSelection(value) {
    const hasAll = selectedValues.includes('all');
    const hasOthers = selectedValues.some(v => v !== 'all');

    if (value === 'all') {
      selectedValues = ['all'];
    } else {
      if (hasAll) {
        // Снимаем "Все", начинаем с текущей страны
        selectedValues = [value];
      } else {
        if (selectedValues.includes(value)) {
          selectedValues = selectedValues.filter(v => v !== value);
          if (selectedValues.length === 0) {
            selectedValues = ['all'];
          }
        } else {
          selectedValues.push(value);
        }
      }
    }

    // Обновляем классы
    itemElements.forEach((el, val) => {
      if (selectedValues.includes(val)) {
        el.classList.add('selected');
      } else {
        el.classList.remove('selected');
      }
    });

    updateInputText();
    if (onChange) onChange();
  }

  function updateInputText() {
    if (selectedValues.includes('all')) {
      input.value = 'Все страны';
    } else if (selectedValues.length === 0) {
      input.value = '';
      input.placeholder = placeholder;
    } else {
      const labels = selectedValues.map(code => options.find(opt => opt.value === code)?.label || code).slice(0, 3);
      input.value = labels.join(', ') + (selectedValues.length > 3 ? ` (+${selectedValues.length - 3})` : '');
    }
  }

  function positionDropdown() {
    if (!dropdown) return;
    const rect = input.getBoundingClientRect();
    const scrollTop = window.scrollY || document.documentElement.scrollTop;
    const scrollLeft = window.scrollX || document.documentElement.scrollLeft;
    dropdown.style.left = rect.left + scrollLeft + 'px';
    dropdown.style.top = rect.bottom + scrollTop + 'px';
    dropdown.style.width = rect.width + 'px';
  }

  function openDropdown() {
    if (dropdown) return;

    dropdown = document.createElement('div');
    dropdown.className = 'searchable-multiselect-dropdown';
    if (isDark) dropdown.classList.add('dark');

    searchInput = document.createElement('input');
    searchInput.type = 'text';
    searchInput.placeholder = 'Поиск...';
    searchInput.className = 'searchable-multiselect-search';
    if (isDark) searchInput.classList.add('dark');

    list = document.createElement('div');
    list.className = 'searchable-multiselect-list';

    dropdown.appendChild(searchInput);
    dropdown.appendChild(list);
    document.body.appendChild(dropdown);

    renderList('');
    positionDropdown();

    searchInput.addEventListener('input', e => renderList(e.target.value));
    searchInput.focus();

    setTimeout(() => {
      document.addEventListener('click', closeOnClickOutside);
    }, 0);
  }

  function closeDropdown() {
    if (!dropdown) return;
    document.body.removeChild(dropdown);
    dropdown = null;
    searchInput = null;
    list = null;
    document.removeEventListener('click', closeOnClickOutside);
  }

  function closeOnClickOutside(e) {
    if (wrapper.contains(e.target) || (dropdown && dropdown.contains(e.target))) return;
    closeDropdown();
  }

  input.addEventListener('click', e => {
    e.stopPropagation();
    if (dropdown) {
      closeDropdown();
    } else {
      openDropdown();
    }
  });

  wrapper.appendChild(input);
  updateInputText();

  return {
    element: wrapper,
    getValue: () => [...selectedValues],
    setValue: newValues => {
      let cleanValues = [...newValues];
      const hasAll = cleanValues.includes('all');
      const hasOthers = cleanValues.some(v => v !== 'all');
      if (hasAll && hasOthers) {
        cleanValues = cleanValues.filter(v => v !== 'all');
      } else if (!hasAll && !hasOthers) {
        cleanValues = ['all'];
      }
      selectedValues = cleanValues;

      itemElements.forEach((el, val) => {
        if (selectedValues.includes(val)) {
          el.classList.add('selected');
        } else {
          el.classList.remove('selected');
        }
      });

      updateInputText();
    },
  };
}

// Создание строки конфигурации столбца
function createColumnRow(initial = {}) {
  const row = document.createElement('div');
  row.className = 'col-row';
  row.addEventListener('input', () => {
    updatePreview();
    saveToHistory();
  });
  row.addEventListener('change', () => {
    updatePreview();
    saveToHistory();
  });

  // Handle для перетаскивания
  const handle = document.createElement('div');
  handle.className = 'handle';
  handle.textContent = '⇅';
  row.appendChild(handle);
  handle.addEventListener('mousedown', e => {
    if (e.button !== 0) return;
    row.draggable = true;
    row.setAttribute('data-drag-source', 'handle');
  });

  row.addEventListener('dragend', () => {
    row.draggable = false;
    row.removeAttribute('data-drag-source');
    row.classList.remove('dragging');
    if (autoScrollInterval) {
      clearInterval(autoScrollInterval);
      autoScrollInterval = null;
    }
    updatePreview();
  });

  // Вспомогательная функция для создания блоков параметров
  function createParamGroup(labelText, element) {
    const group = document.createElement('div');
    group.className = 'param-group';
    group.style.display = 'block';
    const label = document.createElement('label');
    label.textContent = labelText;
    label.style.display = 'block';
    label.style.fontSize = '10px';
    label.style.marginBottom = '4px';
    group.appendChild(label);
    group.appendChild(element);
    return group;
  }

  // Название столбца
  const nameInput = document.createElement('input');
  nameInput.type = 'text';
  nameInput.placeholder = 'Название столбца';
  nameInput.value = initial.name || '';

  nameInput.addEventListener('input', () => {
    const rowIndex = Array.from(columnsList.children).indexOf(row);
    if (rowIndex === -1) return;

    const newName = nameInput.value.trim() || `Столбец ${rowIndex + 1}`;
    const sourceType = row.__typeSelect.getValue();

    document.querySelectorAll('.inherit-select').forEach(select => {
      const option = select.querySelector(`option[value="${rowIndex}"]`);
      if (option) {
        option.textContent = `${newName} (${sourceType})`;
      }
    });
  });

  const nameGroup = createParamGroup('Название столбца', nameInput);
  row.appendChild(nameGroup);

  // Тип данных
  const typeSelectWrapper = createCustomTypeSelect(initial.type || TYPES.ID_AUTO, () => {
    updateFieldVisibility();
    updateInheritSelectors();
    updatePreview();
    saveToHistory();
  });

  // Обработка "фантомных" удалённых файлов
  if (initial.type?.startsWith('file_') && initial.type.includes('_col_') && initial._phantom) {
    typeSelectWrapper.setValue(initial.type);
    const inputEl = typeSelectWrapper.element.querySelector('.custom-type-select-input');
    const displayName = initial.columnName || 'Неизвестный столбец';
    const sourceName = initial.sourceName || 'неизвестный файл';
    inputEl.value = `⚠️ Удалён: "${displayName}" из "${sourceName}"`;
    inputEl.style.color = '#e74c3c';
    inputEl.title = inputEl.value;
  }

  if (initial.type?.startsWith('file_') && initial.type.includes('_col_')) {
    row.dataset.sourceName = initial.sourceName || '';
    row.dataset.columnName = initial.columnName || '';
  }

  const typeGroup = createParamGroup('Тип данных', typeSelectWrapper.element);
  row.appendChild(typeGroup);

  // Сохраняем ссылку для доступа извне
  row.__typeSelect = typeSelectWrapper;

  // Селектор наследования
  const inheritContainer = document.createElement('div');
  inheritContainer.className = 'inherit-container';
  inheritContainer.style.display = 'none';

  const inheritSelect = document.createElement('select');
  inheritSelect.className = 'inherit-select';

  // Опция "Случайно"
  const randomOption = document.createElement('option');
  randomOption.value = '';
  randomOption.textContent = 'Случайно';
  inheritSelect.appendChild(randomOption);

  // Оборачиваем select в param-group с лейблом
  const inheritGroup = createParamGroup('Наследовать от', inheritSelect);
  inheritContainer.appendChild(inheritGroup);

  row.appendChild(inheritContainer);

  // Мультивыбор стран (для типа "Регион")
  const countriesMultiSelectContainer = document.createElement('div');
  countriesMultiSelectContainer.className = 'countries-multiselect-container';
  countriesMultiSelectContainer.style.display = 'none';

  let countriesDataForSelect = countriesDataFull;
  try {
    const dataSources = getCurrentDataSources();
    countriesDataForSelect = dataSources.countriesData;
  } catch (e) {
    console.warn('Не удалось определить тип генерации, используем полные данные', e);
  }

  const {
    container: countriesSelectContainer,
    getValue,
    setValue,
  } = createCountriesMultiSelect(initial.selectedCountries || ['all'], countriesDataForSelect, updatePreview);
  countriesMultiSelectContainer.__searchableInstance = { getValue, setValue };

  const countriesGroup = createParamGroup('Страны', countriesSelectContainer);
  countriesMultiSelectContainer.appendChild(countriesGroup);
  row.appendChild(countriesMultiSelectContainer);

  // Контейнер параметров
  const paramsContainer = document.createElement('div');
  paramsContainer.className = 'params-container';

  // Параметры
  const minInput = document.createElement('input');
  minInput.type = 'number';
  minInput.step = 'any';
  minInput.placeholder = 'Минимум';
  minInput.className = 'range-input';
  minInput.value = initial.min ?? 0;
  const minGroup = createParamGroup('Минимум', minInput);

  const maxInput = document.createElement('input');
  maxInput.type = 'number';
  maxInput.step = 'any';
  maxInput.placeholder = 'Максимум';
  maxInput.className = 'range-input';
  maxInput.value = initial.max ?? 1000;
  const maxGroup = createParamGroup('Максимум', maxInput);

  const precisionInput = document.createElement('input');
  precisionInput.type = 'number';
  precisionInput.placeholder = 'Знаков после точки';
  precisionInput.className = 'precision-input';
  precisionInput.value = initial.precision ?? 5;
  const precisionGroup = createParamGroup('Точность', precisionInput);

  // Параметры даты
  const dateFromInput = document.createElement('input');
  dateFromInput.type = 'date';
  dateFromInput.className = 'date-input';
  dateFromInput.value = initial.fromDate
    ? formatDateForInput(new Date(initial.fromDate))
    : formatDateForInput(new Date(2000, 0, 1));
  const dateFromGroup = createParamGroup('Дата от', dateFromInput);

  const dateToInput = document.createElement('input');
  dateToInput.type = 'date';
  dateToInput.className = 'date-input';
  dateToInput.value = initial.toDate ? formatDateForInput(new Date(initial.toDate)) : formatDateForInput(new Date());
  const dateToGroup = createParamGroup('Дата до', dateToInput);

  const dateFormatSelect = document.createElement('select');
  dateFormatSelect.className = 'date-format-select';
  Object.entries({
    'SQL DATETIME': DATE_FORMATS.SQL_DATETIME,
    'SQL DATE': DATE_FORMATS.SQL_DATE,
    'SQL TIME': DATE_FORMATS.SQL_TIME,
    'UNIX TIMESTAMP': DATE_FORMATS.UNIX_TIMESTAMP,
    'ISO 8601': DATE_FORMATS.ISO_8601,
    EPOCH: DATE_FORMATS.EPOCH,
  }).forEach(([text, value]) => {
    const option = document.createElement('option');
    option.value = value;
    option.textContent = text;
    dateFormatSelect.appendChild(option);
  });
  dateFormatSelect.value = initial.dateFormat || DATE_FORMATS.ISO_8601;
  const dateFormatGroup = createParamGroup('Формат даты', dateFormatSelect);

  // Параметры цвета
  const colorFormatSelect = document.createElement('select');
  colorFormatSelect.className = 'color-format-select';
  Object.entries(COLOR_FORMATS).forEach(([text, value]) => {
    const option = document.createElement('option');
    option.value = value;
    option.textContent = text;
    colorFormatSelect.appendChild(option);
  });
  colorFormatSelect.value = initial.colorFormat || COLOR_FORMATS.HEX;
  const colorFormatGroup = createParamGroup('Формат цвета', colorFormatSelect);

  const transparencyCheckbox = document.createElement('input');
  transparencyCheckbox.type = 'checkbox';
  transparencyCheckbox.className = 'transparency-checkbox';
  transparencyCheckbox.checked = initial.transparency || false;
  const transparencyLabel = document.createElement('label');
  transparencyLabel.textContent = 'С прозрачностью';
  const transparencyContainer = document.createElement('div');
  transparencyContainer.style.display = 'flex';
  transparencyContainer.style.alignItems = 'center';
  transparencyContainer.style.gap = '6px';
  transparencyContainer.append(transparencyCheckbox, transparencyLabel);
  const transparencyGroup = createParamGroup('Прозрачность', transparencyContainer);

  // Параметры IP
  const ipFormatSelect = document.createElement('select');
  ipFormatSelect.className = 'ip-format-select';
  Object.entries({
    IPv4: IP_FORMATS.IPV4,
    IPv6: IP_FORMATS.IPV6,
    'IPv4 с маской': IP_FORMATS.IPV4_WITH_MASK,
    'IPv6 с маской': IP_FORMATS.IPV6_WITH_MASK,
  }).forEach(([text, value]) => {
    const option = document.createElement('option');
    option.value = value;
    option.textContent = text;
    ipFormatSelect.appendChild(option);
  });
  ipFormatSelect.value = initial.ipFormat || IP_FORMATS.IPV4;
  const ipFormatGroup = createParamGroup('Формат IP-адреса', ipFormatSelect);

  // Параметры MAC
  const macFormatSelect = document.createElement('select');
  macFormatSelect.className = 'mac-format-select';
  Object.entries({
    'aa:bb:cc:dd:ee:ff': 'colon',
    'aa-bb-cc-dd-ee-ff': 'dash',
    'aabb.ccdd.eeff': 'dot',
    aabbccddeeff: 'none',
  }).forEach(([text, value]) => {
    const option = document.createElement('option');
    option.value = value;
    option.textContent = text;
    macFormatSelect.appendChild(option);
  });
  macFormatSelect.value = initial.macFormat || 'colon';
  const macFormatGroup = createParamGroup('Формат MAC-адреса', macFormatSelect);

  // Параметры телефона
  const phoneFormatSelect = document.createElement('select');
  phoneFormatSelect.className = 'phone-format-select';
  Object.entries({
    '+7-900-123-45-67': 'dashed',
    '+79001234567': 'plain',
    '+7 900 123 45 67': 'spaced',
    '+7(900)1234567': 'parentheses',
  }).forEach(([text, value]) => {
    const opt = document.createElement('option');
    opt.value = value;
    opt.textContent = text;
    phoneFormatSelect.appendChild(opt);
  });
  phoneFormatSelect.value = initial.phoneFormat || 'dashed';
  const phoneFormatGroup = createParamGroup('Формат телефона', phoneFormatSelect);
  phoneFormatGroup.classList.add('phone-format-group');

  // Параметры кастомного списка
  const customListTextarea = document.createElement('textarea');
  customListTextarea.placeholder = 'Введите значения (по одному на строку)';
  customListTextarea.className = 'custom-list-textarea';
  customListTextarea.rows = 3;
  customListTextarea.cols = 20;
  customListTextarea.value = (initial.customList || []).join('\n');
  const customListGroup = createParamGroup('Список значений', customListTextarea);

  const customListSeparatorInput = document.createElement('input');
  customListSeparatorInput.type = 'text';
  customListSeparatorInput.placeholder = 'Разделитель (по умолчанию — перенос строки)';
  customListSeparatorInput.className = 'custom-list-separator';
  customListSeparatorInput.value = initial.customSeparator || '';
  const customSeparatorGroup = createParamGroup('Разделитель', customListSeparatorInput);

  const customListModeSelect = document.createElement('select');
  [
    { value: 'random', text: 'Случайно' },
    { value: 'sequential', text: 'Последовательно' },
    { value: 'sequential_ignore_size', text: 'Последовательно (игнорируя размер)' },
  ].forEach(({ value, text }) => {
    const opt = document.createElement('option');
    opt.value = value;
    opt.textContent = text;
    customListModeSelect.appendChild(opt);
  });
  customListModeSelect.className = 'custom-list-mode';
  customListModeSelect.value = initial.customListMode || 'random';
  const customModeGroup = createParamGroup('Режим выбора', customListModeSelect);

  // Параметры случайной строки
  const randomStringBaseInput = document.createElement('input');
  randomStringBaseInput.type = 'text';
  randomStringBaseInput.placeholder = 'Базовый текст';
  randomStringBaseInput.className = 'random-string-base';
  randomStringBaseInput.value =
    initial.baseText ||
    'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.';
  const baseTextGroup = createParamGroup('Базовый текст', randomStringBaseInput);

  const randomStringLengthInput = document.createElement('input');
  randomStringLengthInput.type = 'number';
  randomStringLengthInput.placeholder = 'Макс. длина';
  randomStringLengthInput.className = 'random-string-length';
  randomStringLengthInput.value = initial.maxLength || 10;
  randomStringLengthInput.min = 0;
  const maxLengthGroup = createParamGroup('Макс. длина', randomStringLengthInput);

  // Параметры преобразования
  const transformationFormulaTextarea = document.createElement('textarea');
  transformationFormulaTextarea.placeholder = 'Пример: row["столбец1"] + row["столбец2"] или Number(col("сумма")) * 10';
  transformationFormulaTextarea.className = 'transform-formula-textarea';
  transformationFormulaTextarea.rows = 3;
  transformationFormulaTextarea.cols = 24;
  transformationFormulaTextarea.value = initial.transformFormula || '';
  const transformationFormulaGroup = createParamGroup('Формула', transformationFormulaTextarea);

  // --- Вероятность null ---
  const nullProbContainer = document.createElement('div');
  nullProbContainer.style.display = 'flex';
  nullProbContainer.style.flexDirection = 'column';
  nullProbContainer.style.gap = '2px';
  const nullProbLabel = document.createElement('label');
  nullProbLabel.textContent = 'Пропуски (%)';
  nullProbLabel.style.fontSize = '10px';
  const nullProbabilityInput = document.createElement('input');
  nullProbabilityInput.type = 'number';
  nullProbabilityInput.placeholder = '0–100';
  nullProbabilityInput.className = 'null-probability';
  nullProbabilityInput.min = 0;
  nullProbabilityInput.max = 100;
  nullProbabilityInput.value = initial.nullProbability ?? 0;
  nullProbContainer.appendChild(nullProbLabel);
  nullProbContainer.appendChild(nullProbabilityInput);
  const nullProbGroup = document.createElement('div');
  nullProbGroup.className = 'param-group';
  nullProbGroup.appendChild(nullProbContainer);

  // Добавление всех параметров
  paramsContainer.append(
    minGroup,
    maxGroup,
    precisionGroup,
    dateFromGroup,
    dateToGroup,
    dateFormatGroup,
    colorFormatGroup,
    transparencyGroup,
    ipFormatGroup,
    macFormatGroup,
    phoneFormatGroup,
    customListGroup,
    customSeparatorGroup,
    customModeGroup,
    baseTextGroup,
    maxLengthGroup,
    transformationFormulaGroup,
    nullProbGroup
  );
  row.appendChild(paramsContainer);

  // Обновление видимости параметров при смене типа
  function updateFieldVisibility() {
    const t = typeSelectWrapper.getValue();
    const showGroup = (group, condition) => {
      group.style.display = condition ? 'block' : 'none';
    };
    showGroup(minGroup, t === TYPES.INTEGER || t === TYPES.FLOAT);
    showGroup(maxGroup, t === TYPES.INTEGER || t === TYPES.FLOAT);
    showGroup(precisionGroup, t === TYPES.FLOAT);
    showGroup(dateFromGroup, t === TYPES.DATE);
    showGroup(dateToGroup, t === TYPES.DATE);
    showGroup(dateFormatGroup, t === TYPES.DATE);
    showGroup(colorFormatGroup, t === TYPES.COLOR);
    showGroup(transparencyGroup, t === TYPES.COLOR);
    showGroup(countriesMultiSelectContainer, t === TYPES.REGION);
    showGroup(ipFormatGroup, t === TYPES.IP);
    showGroup(macFormatGroup, t === TYPES.MAC_ADDRESS);
    showGroup(phoneFormatGroup, t === TYPES.PHONE);
    showGroup(customListGroup, t === TYPES.CUSTOM_LIST);
    showGroup(customSeparatorGroup, t === TYPES.CUSTOM_LIST);
    const isFileColumn = t.startsWith('file_') && t.includes('_col_');
    showGroup(customModeGroup, t === TYPES.CUSTOM_LIST || isFileColumn);
    showGroup(baseTextGroup, t === TYPES.RANDOM_LENGTH_STRING);
    showGroup(maxLengthGroup, t === TYPES.RANDOM_LENGTH_STRING);
    showGroup(transformationFormulaGroup, t === TYPES.TRANSFORMATION);
    nullProbGroup.style.display = 'block';
  }

  // Инициализация видимости
  updateFieldVisibility();

  // Кнопка удаления
  const removeBtn = document.createElement('button');
  removeBtn.type = 'button';
  removeBtn.className = 'remove-column-btn';
  removeBtn.title = 'Удалить столбец';
  removeBtn.setAttribute('aria-label', 'Удалить столбец');
  const trashIcon = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  trashIcon.setAttribute('width', '16');
  trashIcon.setAttribute('height', '16');
  trashIcon.setAttribute('viewBox', '0 0 24 24');
  trashIcon.setAttribute('fill', 'none');
  trashIcon.setAttribute('stroke', 'currentColor');
  trashIcon.setAttribute('stroke-width', '2');
  trashIcon.setAttribute('stroke-linecap', 'round');
  trashIcon.setAttribute('stroke-linejoin', 'round');
  const path1 = document.createElementNS('http://www.w3.org/2000/svg', 'path');
  path1.setAttribute('d', 'M3 6h18');
  const path2 = document.createElementNS('http://www.w3.org/2000/svg', 'path');
  path2.setAttribute('d', 'M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2');
  trashIcon.appendChild(path1);
  trashIcon.appendChild(path2);
  removeBtn.appendChild(trashIcon);
  removeBtn.addEventListener('click', () => {
    row.remove();
    updateInheritSelectors();
    updatePreview();
    saveToHistory();
  });
  row.appendChild(removeBtn);

  setupDragAndDrop(row);

  // Принудительный вызов обновления чекбоксов
  setTimeout(() => {
    updateInheritSelectors();
  }, 0);

  return row;
}

// Поддержка drag-and-drop для строк столбцов
function setupDragAndDrop(row) {
  row.addEventListener('dragstart', e => {
    row.classList.add('dragging');
    e.dataTransfer.effectAllowed = 'move';
  });
  row.addEventListener('dragend', () => {
    row.classList.remove('dragging');
    if (autoScrollInterval) {
      clearInterval(autoScrollInterval);
      autoScrollInterval = null;
    }

    // Сохраняем текущие выборы по ссылкам на DOM
    const preserved = preserveInheritSelections();

    // Обновляем структуру (перетаскивание уже завершено)
    updateInheritSelectors();
    restoreInheritSelections(preserved);

    updatePreview();
    saveToHistory();
  });
}

function preserveInheritSelections() {
  const selections = new Map();
  document.querySelectorAll('.inherit-select').forEach(select => {
    const option = select.selectedOptions[0];
    if (option && option.value !== '') {
      selections.set(select, option.__sourceRow);
    }
  });
  return selections;
}

function restoreInheritSelections(selections) {
  selections.forEach((sourceRow, select) => {
    if (!sourceRow || !select) return;
    const newRows = [...columnsList.querySelectorAll('.col-row')];
    const newIndex = newRows.indexOf(sourceRow);
    if (newIndex !== -1) {
      const newOption = [...select.options].find(opt => opt.__sourceRow === sourceRow);
      if (newOption) {
        select.value = newOption.value;
      } else {
        select.value = '';
      }
    } else {
      select.value = '';
    }
  });
}

function getDragAfterElement(container, y) {
  const elements = [...container.querySelectorAll('.col-row:not(.dragging)')];
  return elements.reduce(
    (closest, child) => {
      const box = child.getBoundingClientRect();
      const offset = y - box.top - box.height / 2;
      return offset < 0 && offset > closest.offset ? { offset, element: child } : closest;
    },
    { offset: -Infinity }
  ).element;
}

// Обновление чекбоксов наследования
function updateInheritSelectors() {
  const rows = [...columnsList.querySelectorAll('.col-row')];

  // Собираем типы всех столбцов
  const columnTypes = rows.map(row => row.__typeSelect?.getValue());

  // Карта: тип → список индексов
  const typeToIndices = {};
  columnTypes.forEach((type, idx) => {
    if (!typeToIndices[type]) typeToIndices[type] = [];
    typeToIndices[type].push(idx);
  });

  // Определяем, какие типы могут быть источниками для наследования
  const inheritMap = {
    [TYPES.LAST_NAME]: [TYPES.GENDER],
    [TYPES.FIRST_NAME]: [TYPES.GENDER],
    [TYPES.MIDDLE_NAME]: [TYPES.GENDER],
    [TYPES.COUNTRY]: [TYPES.COUNTRY_CODE],
    [TYPES.REGION]: [TYPES.COUNTRY_CODE, TYPES.COUNTRY],
    [TYPES.CITY]: [TYPES.COUNTRY_CODE, TYPES.COUNTRY, TYPES.REGION],
    [TYPES.STREET]: [TYPES.COUNTRY_CODE, TYPES.COUNTRY, TYPES.REGION, TYPES.CITY],
    [TYPES.PHONE]: [TYPES.COUNTRY_CODE, TYPES.COUNTRY, TYPES.REGION],
    [TYPES.LATITUDE]: [TYPES.COUNTRY_CODE, TYPES.COUNTRY, TYPES.REGION, TYPES.CITY],
    [TYPES.LONGITUDE]: [TYPES.COUNTRY_CODE, TYPES.COUNTRY, TYPES.REGION, TYPES.CITY],
    [TYPES.COORDINATES_LAT_LNG]: [TYPES.COUNTRY_CODE, TYPES.COUNTRY, TYPES.REGION, TYPES.CITY],
    [TYPES.COORDINATES_LNG_LAT]: [TYPES.COUNTRY_CODE, TYPES.COUNTRY, TYPES.REGION, TYPES.CITY],
    [TYPES.LINE_BI]: [TYPES.COUNTRY_CODE, TYPES.COUNTRY, TYPES.REGION, TYPES.CITY],
    [TYPES.LINE_GEOJSON]: [TYPES.COUNTRY_CODE, TYPES.COUNTRY, TYPES.REGION, TYPES.CITY],
  };

  rows.forEach((row, targetIndex) => {
    const targetType = columnTypes[targetIndex];
    const inheritContainer = row.querySelector('.inherit-container');
    const inheritSelect = row.querySelector('.inherit-select');

    if (!inheritContainer || !inheritSelect) return;

    const allowedSourceTypes = inheritMap[targetType] || [];

    // Проверяем, есть ли хотя бы один подходящий источник
    const hasValidSources = allowedSourceTypes.some(t => typeToIndices[t]?.length > 0);

    if (allowedSourceTypes.length === 0 || !hasValidSources) {
      inheritContainer.style.display = 'none';
      inheritSelect.value = '';
      return;
    }

    // Показываем селектор
    inheritContainer.style.display = 'block';

    // Сохраняем текущее значение
    const currentValue = inheritSelect.value;

    // Очищаем все опции, кроме "Случайно"
    while (inheritSelect.options.length > 1) {
      inheritSelect.remove(1);
    }

    // Добавляем подходящие столбцы как опции
    allowedSourceTypes.forEach(sourceType => {
      const indices = typeToIndices[sourceType] || [];
      indices.forEach(sourceIndex => {
        if (sourceIndex === targetIndex) return;
        const sourceRow = rows[sourceIndex];
        const nameInput = sourceRow.querySelector('input[placeholder="Название столбца"]');
        const sourceName = nameInput?.value.trim() || `Столбец ${sourceIndex + 1}`;
        const option = document.createElement('option');
        option.value = sourceIndex;
        option.textContent = `${sourceName} (${sourceType})`;
        option.__sourceRow = sourceRow; // ← ключевая строка
        inheritSelect.appendChild(option);
      });
    });

    // Восстанавливаем выбор, если он ещё валиден
    if (inheritSelect.querySelector(`option[value="${currentValue}"]`)) {
      inheritSelect.value = currentValue;
    } else {
      inheritSelect.value = '';
    }
  });
}

// Чтение конфигурации из UI
function readConfigFromUI() {
  const cols = [...columnsList.querySelectorAll('.col-row')].map(row => {
    const nameInput = row.querySelector('input[placeholder="Название столбца"]');
    const inheritSelect = row.querySelector('.inherit-select');
    const inheritFrom = inheritSelect?.value === '' ? null : parseInt(inheritSelect.value, 10);
    const name = nameInput?.value.trim() || '';
    const type = row.__typeSelect.getValue();
    const config = { name, type, inheritFrom };

    const params = row.querySelector('.params-container');

    if (type === TYPES.REGION) {
      const inst = row.querySelector('.countries-multiselect-container')?.__searchableInstance;
      config.selectedCountries = inst ? inst.getValue() : ['all'];
    } else if ([TYPES.INTEGER, TYPES.FLOAT].includes(type)) {
      const [minEl, maxEl] = params.querySelectorAll('.range-input');
      config.min = minEl?.value !== '' ? Number(minEl.value) : type === TYPES.INTEGER ? 0 : 0;
      config.max = maxEl?.value !== '' ? Number(maxEl.value) : type === TYPES.INTEGER ? 1000 : 1000;
      if (type === TYPES.FLOAT) config.precision = Number(params.querySelector('.precision-input')?.value) || 5;
    } else if (type === TYPES.DATE) {
      const [from, to] = params.querySelectorAll('.date-input');
      config.fromDate = from?.value || '';
      config.toDate = to?.value || '';
      config.dateFormat = params.querySelector('.date-format-select')?.value || DATE_FORMATS.ISO_8601;
    } else if (type === TYPES.COLOR) {
      config.colorFormat = params.querySelector('.color-format-select')?.value || COLOR_FORMATS.HEX;
      config.transparency = params.querySelector('.transparency-checkbox')?.checked || false;
    } else if (type === TYPES.IP) {
      config.ipFormat = params.querySelector('.ip-format-select')?.value || IP_FORMATS.IPV4;
    } else if (type === TYPES.MAC_ADDRESS) {
      config.macFormat = params.querySelector('.mac-format-select')?.value || 'colon';
    } else if (type === TYPES.PHONE) {
      config.phoneFormat = params.querySelector('.phone-format-select')?.value || 'dashed';
    } else if (type === TYPES.CUSTOM_LIST) {
      const textarea = params.querySelector('.custom-list-textarea');
      const sepInput = params.querySelector('.custom-list-separator');
      const raw = textarea?.value || '';
      const sep = sepInput?.value || '\n';
      config.customList = raw
        .split(sep)
        .map(s => s.trim())
        .filter(Boolean);
      config.customSeparator = sepInput?.value || '';
      config.customListMode = params.querySelector('.custom-list-mode')?.value || 'random';
    } else if (type.startsWith('file_') && type.includes('_col_')) {
      config.customListMode = params.querySelector('.custom-list-mode')?.value || 'random';
      const parts = type.split('_');
      const fileKey = parts.slice(1, -2).join('_');
      const colIdx = parseInt(parts[parts.length - 1], 10);
      const source = dataSourceCache.find(s => s.key === fileKey);
      if (source) {
        config.sourceName = source.name;
        config.columnName = source.headers?.[colIdx] || `Столбец ${colIdx + 1}`;
      }
    } else if (type === TYPES.RANDOM_LENGTH_STRING) {
      config.baseText = params.querySelector('.random-string-base')?.value || '';
      config.maxLength = Number(params.querySelector('.random-string-length')?.value) || 10;
    } else if (type === TYPES.TRANSFORMATION) {
      config.transformFormula = params.querySelector('.transform-formula-textarea')?.value || '';
    }

    const nullProb = params.querySelector('.null-probability');
    config.nullProbability = Math.max(0, Math.min(100, Number(nullProb?.value) || 0));
    return config;
  });

  return {
    rows: Math.max(1, Number(rowsCountInput.value) || 1),
    format: formatSelect.value,
    language: languageSelect.value,
    csvSeparator: document.getElementById('csvSeparator')?.value || ',',
    columns: cols,
  };
}

// Уникализация имён столбцов
function makeColumnNamesUnique(columns) {
  const seen = new Map();
  return columns.map(col => {
    const baseName = col.name && col.name.trim() ? col.name.trim() : 'column';
    const count = seen.get(baseName) || 0;
    seen.set(baseName, count + 1);
    return {
      ...col,
      __uniqueName: count === 0 ? baseName : `${baseName}_${count}`,
    };
  });
}

// Генерация данных для предпросмотра
function generatePreviewData() {
  const config = readConfigFromUI();
  if (!config.columns.length) {
    return { headers: [], data: [] };
  }

  const uniqueColumns = makeColumnNamesUnique(config.columns);
  const headers = uniqueColumns.map((col, idx) =>
    col.name && col.name.trim() ? col.name.trim() : `column_${idx + 1}`
  );

  let minIgnoreSize = Infinity;
  for (const col of uniqueColumns) {
    if (col.customListMode === 'sequential_ignore_size') {
      let length = Infinity;
      if (col.type === TYPES.CUSTOM_LIST && Array.isArray(col.customList)) {
        length = col.customList.length;
      } else if (col.type.startsWith('file_') && col.type.includes('_col_') && dataSourceCache) {
        const parts = col.type.split('_');
        const fileKey = parts.slice(1, -2).join('_');
        const colIdx = parseInt(parts[parts.length - 1], 10);
        const source = dataSourceCache.find(s => s.key === fileKey);
        if (source && Array.isArray(source.rows)) {
          length = source.rows.length;
        }
      }
      if (length < minIgnoreSize) {
        minIgnoreSize = length;
      }
    }
  }

  let previewRows = 10;
  if (minIgnoreSize !== Infinity) {
    previewRows = Math.min(minIgnoreSize, 10);
  }

  const dataSources = getCurrentDataSources();
  const data = [];
  for (let i = 1; i <= previewRows; i++) {
    const tempColumns = uniqueColumns.map(col => ({
      ...col,
      name: col.__uniqueName,
    }));
    const row = buildRowObject(
      tempColumns,
      i,
      dataSources.countriesData,
      dataSources.citiesData,
      dataSources.genderData,
      dataSources.namesData,
      dataSources.regionsData,
      dataSources.streetsData,
      config.language,
      dataSourceCache
    );
    const rowData = uniqueColumns.map(col => {
      const value = row[col.__uniqueName];
      if (value === null) return 'null';
      if (value === '') return '""';
      if (typeof value === 'boolean') return value ? 'true' : 'false';
      return String(value);
    });
    data.push(rowData);
  }

  return { headers, data };
}

// Обновление таблицы предпросмотра
function updatePreview() {
  if (!previewTable) return;
  try {
    const { headers, data } = generatePreviewData();
    previewTable.innerHTML = '';

    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    headers.forEach(header => {
      const th = document.createElement('th');
      th.textContent = header;
      headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    previewTable.appendChild(thead);

    const tbody = document.createElement('tbody');
    if (data.length > 0) {
      data.forEach(rowData => {
        const tr = document.createElement('tr');
        rowData.forEach(cellData => {
          const td = document.createElement('td');
          td.textContent = cellData;
          td.title = cellData;
          tr.appendChild(td);
        });
        tbody.appendChild(tr);
      });
    } else {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.colSpan = headers.length || 1;
      td.textContent = 'Нет данных для отображения';
      td.style.textAlign = 'center';
      td.style.color = '#999';
      tr.appendChild(td);
      tbody.appendChild(tr);
    }
    previewTable.appendChild(tbody);
  } catch (error) {
    console.error('Ошибка при обновлении предпросмотра:', error);
    previewTable.innerHTML = '';
    const tbody = document.createElement('tbody');
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.textContent = 'Ошибка при генерации предпросмотра: ' + error.message;
    td.style.color = 'red';
    tr.appendChild(td);
    tbody.appendChild(tr);
    previewTable.appendChild(tbody);
  }
}

// Генерация и сохранение файла
async function handleGenerate() {
  const config = readConfigFromUI();
  if (!config.columns.length) return alert('Добавьте хотя бы один столбец');

  const uniqueColumns = makeColumnNamesUnique(config.columns);
  const tempConfig = {
    ...config,
    columns: uniqueColumns.map(col => ({
      ...col,
      name: col.__uniqueName,
    })),
  };

  const dataSources = getCurrentDataSources();

  const extMap = {
    csv: [{ name: 'CSV', extensions: ['csv'] }],
    xlsx: [{ name: 'Excel Workbook', extensions: ['xlsx'] }],
    xml: [{ name: 'XML', extensions: ['xml'] }],
    json: [{ name: 'JSON', extensions: ['json'] }],
    qvd: [{ name: 'QVD', extensions: ['qvd'] }],
  };

  const baseName = filenameBaseInput?.value || 'data';
  const defaultFilename = buildFilename(baseName, config.format);

  const { canceled, filePath } = await window.api.showSaveDialog({
    defaultPath: defaultFilename,
    filters: extMap[config.format],
  });

  if (canceled || !filePath) return;

  generateBtn.disabled = true;
  updateTitle(0);

  try {
    const CHUNK = 1000;
    const total = config.rows;
    const progress = cur => updateTitle((cur / total) * 100);

    const writers = {
      csv: writeCSVStream,
      xlsx: writeXLSXStream,
      xml: writeXMLStream,
      json: writeJSONStream,
      qvd: writeQVDStream,
    };

    await writers[config.format](filePath, tempConfig, CHUNK, progress, {
      countriesData: dataSources.countriesData,
      genderData: dataSources.genderData,
      namesData: dataSources.namesData,
      regionsData: dataSources.regionsData,
      citiesData: dataSources.citiesData,
      streetsData: dataSources.streetsData,
      language: config.language,
      externalData: dataSourceCache,
      csvSeparator: config.csvSeparator,
    });

    updateTitle(100);
    setTimeout(async () => {
      updateTitle(0);
      alert('Файл сохранён: ' + filePath);
      try {
        await window.api.showItemInFolder(filePath);
      } catch (err) {
        console.warn('Не удалось открыть проводник:', err);
      }
    }, 1000);
  } catch (err) {
    console.error(err);
    updateTitle(0);
    alert('Ошибка генерации: ' + err.message);
  } finally {
    generateBtn.disabled = false;
  }
}

// Обработка drag-and-drop для сортировки столбцов
columnsList.addEventListener('dragover', e => {
  e.preventDefault();
  if (autoScrollInterval) {
    clearInterval(autoScrollInterval);
    autoScrollInterval = null;
  }

  const dragging = document.querySelector('.col-row[data-drag-source="handle"]');
  if (!dragging) return;

  const containerRect = columnsList.getBoundingClientRect();
  const y = e.clientY;
  const scrollZone = 200;
  const scrollSpeed = 5;

  if (y - containerRect.top < scrollZone && columnsList.scrollTop > 0) {
    autoScrollInterval = setInterval(() => {
      columnsList.scrollTop -= scrollSpeed;
    }, 16);
  } else if (
    containerRect.bottom - y < scrollZone &&
    columnsList.scrollTop < columnsList.scrollHeight - columnsList.clientHeight
  ) {
    autoScrollInterval = setInterval(() => {
      columnsList.scrollTop += scrollSpeed;
    }, 16);
  }

  const after = getDragAfterElement(columnsList, e.clientY);
  columnsList.insertBefore(dragging, after || null);
});

// Обработчики кнопок
addColumnBtn?.addEventListener('click', () => {
  const newRow = createColumnRow();
  columnsList?.appendChild(newRow);
  updateInheritSelectors();
  updatePreview();
  saveToHistory();
});

generateBtn.addEventListener('click', () =>
  handleGenerate().catch(err => {
    console.error(err);
    alert('Ошибка генерации: ' + err.message);
  })
);

refreshPreviewBtn?.addEventListener('click', updatePreview);
rowsCountInput?.addEventListener('input', saveToHistory);
filenameBaseInput?.addEventListener('input', saveToHistory);
formatSelect?.addEventListener('change', saveToHistory);
languageSelect?.addEventListener('change', () => {
  updatePreview();
  saveToHistory();
});
generationTypeSelect?.addEventListener('change', () => {
  refreshRegionCountrySelectors();
  refreshAllTypeSelects();
  updatePreview();
  saveToHistory();
});

formatSelect?.addEventListener('change', () => {
  const separatorRow = document.getElementById('separatorRow');
  if (formatSelect.value === 'csv') {
    separatorRow.style.display = 'flex';
  } else {
    separatorRow.style.display = 'none';
  }
  updatePreview();
});

// Работа с настройками (сохранение/загрузка)
function getFullConfig() {
  const base = readConfigFromUI();
  return {
    filenameBase: filenameBaseInput.value.trim() || 'data',
    rowsCount: base.rows,
    format: base.format,
    language: base.language,
    generationType: generationTypeSelect?.value || 'advanced',
    columns: base.columns,
  };
}

function saveConfigToLocalStorage() {
  try {
    const config = getFullConfigSnapshot();
    localStorage.setItem(LOCAL_STORAGE_CONFIG_KEY, JSON.stringify(config));
  } catch (err) {
    console.warn('Не удалось сохранить настройки в localStorage:', err);
  }
}

function loadConfigFromLocalStorage() {
  try {
    const raw = localStorage.getItem(LOCAL_STORAGE_CONFIG_KEY);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch (err) {
    console.warn('Не удалось загрузить настройки из localStorage:', err);
    return null;
  }
}

function buildSettingsFilename(config) {
  const base = (config.filenameBase || 'data').replace(/[<>:"/\\|?*]+/g, '_');
  const rows = config.rowsCount || '0';
  const fmt = config.format || 'csv';
  const lang = config.language || 'ru';
  const gen = config.generationType || 'advanced';
  const now = new Date();
  const timestamp = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(
    now.getDate()
  ).padStart(2, '0')}_${String(now.getHours()).padStart(2, '0')}-${String(now.getMinutes()).padStart(2, '0')}`;
  return `Config – ${base} ${rows} ${fmt} ${lang} ${gen} ${timestamp}.json`;
}

function applyConfig(config) {
  if (!config) return;
  filenameBaseInput.value = config.filenameBase || 'data';
  rowsCountInput.value = config.rowsCount || 10;
  formatSelect.value = config.format || 'csv';
  languageSelect.value = config.language || 'ru';
  generationTypeSelect.value = config.generationType || 'advanced';

  columnsList.innerHTML = '';
  const rowsToCreate = [];
  const inheritFromMap = new Map();

  if (Array.isArray(config.columns)) {
    config.columns.forEach((col, idx) => {
      if (col.type.startsWith('file_') && col.type.includes('_col_')) {
        const matchedSource = dataSourceCache.find(src => src.name === col.sourceName);
        if (matchedSource) {
          const parts = col.type.split('_');
          const expectedKey = matchedSource.key;
          const colIdx = parseInt(parts[parts.length - 1], 10);
          if (matchedSource.headers && colIdx < matchedSource.headers.length) {
            col.type = `file_${expectedKey}_col_${colIdx}`;
            col._phantom = false;
          } else {
            col._phantom = true;
          }
        } else {
          col._phantom = true;
        }
      }

      const newRow = createColumnRow(col);
      columnsList.appendChild(newRow);
      rowsToCreate.push(newRow);

      // Сохраняем inheritFrom для последующего применения
      if (col.inheritFrom !== undefined && col.inheritFrom !== null) {
        inheritFromMap.set(idx, col.inheritFrom);
      }
    });
  }

  // Обновляем селекторы наследования после создания всех строк
  updateInheritSelectors();

  // Теперь безопасно устанавливаем значения селекторов
  inheritFromMap.forEach((targetIndex, sourceRowIndex) => {
    const row = rowsToCreate[sourceRowIndex];
    if (!row) return;
    const inheritSelect = row.querySelector('.inherit-select');
    if (inheritSelect) {
      // Проверяем, существует ли опция с таким значением
      if (inheritSelect.querySelector(`option[value="${targetIndex}"]`)) {
        inheritSelect.value = String(targetIndex);
      } else {
        inheritSelect.value = '';
      }
    }
  });

  refreshRegionCountrySelectors();
  updatePreview();
}

saveSettingsBtn?.addEventListener('click', async () => {
  const config = getFullConfig();
  const filename = buildSettingsFilename(config);
  const { canceled, filePath } = await window.api.showSaveDialog({
    defaultPath: filename,
    filters: [{ name: 'JSON Settings', extensions: ['json'] }],
  });
  if (canceled || !filePath) return;
  try {
    await window.api.writeFile(filePath, JSON.stringify(config, null, 2));
    try {
      localStorage.setItem(LOCAL_STORAGE_CONFIG_KEY, JSON.stringify(config));
    } catch (err) {
      console.warn('Не удалось сохранить настройки в localStorage после экспорта:', err);
    }
    history = [config];
    historyIndex = 0;
    alert('Настройки сохранены: ' + filePath);
  } catch (err) {
    console.error(err);
    alert('Ошибка сохранения: ' + err.message);
  }
});

loadSettingsBtn?.addEventListener('click', async () => {
  const { canceled, filePaths } = await window.api.showOpenDialog({
    filters: [{ name: 'JSON Settings', extensions: ['json'] }],
    properties: ['openFile'],
  });
  if (canceled || !filePaths || filePaths.length === 0) return;
  try {
    const content = await window.api.readFile(filePaths[0]);
    const config = JSON.parse(content);
    applyConfig(config);
    try {
      localStorage.setItem(LOCAL_STORAGE_CONFIG_KEY, JSON.stringify(config));
    } catch (err) {
      console.warn('Не удалось сохранить загруженные настройки в localStorage:', err);
    }
    history = [config];
    historyIndex = 0;
    alert('Настройки загружены из: ' + filePaths[0]);
  } catch (err) {
    console.error(err);
    alert('Ошибка загрузки: ' + err.message);
  }
});

// Инициализация приложения
document.addEventListener('DOMContentLoaded', () => {
  applyTheme(currentTheme);

  // Слушатель на изменение системной темы (только если выбрана "системная")
  const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
  const handleSystemThemeChange = e => {
    if (currentTheme === 'system') {
      applyTheme('system');
    }
  };
  mediaQuery.addEventListener('change', handleSystemThemeChange);

  // Инициализация остального UI
  generationTypeSelect = document.getElementById('generationType');

  const savedConfig = loadConfigFromLocalStorage();
  if (savedConfig) {
    applyConfig(savedConfig);
    history = [savedConfig];
    historyIndex = 0;
  } else if (columnsList && columnsList.children.length === 0) {
    columnsList.appendChild(createColumnRow({ name: 'id', type: TYPES.ID_AUTO }));
  }

  if (generationTypeSelect) {
    generationTypeSelect.addEventListener('change', () => {
      refreshRegionCountrySelectors();
      refreshAllTypeSelects();
      updatePreview();
    });
  }
  setTimeout(updatePreview, 100);
  setTimeout(() => {
    saveToHistory();
  }, 200);
  renderDataSourceFileList();

  // Автоматическая проверка обновлений после загрузки страницы
  setTimeout(() => {
    checkForUpdatesOnLoad();
  }, 1500);

  // Обработчик копирования из таблицы предпросмотра с уведомлением
  document.getElementById('previewTable')?.addEventListener('click', async e => {
    const cell = e.target.closest('td');
    if (!cell) return;

    const text = cell.textContent.trim();
    if (!text) return;

    // Копирование в буфер обмена
    let copied = false;
    try {
      await navigator.clipboard.writeText(text);
      copied = true;
    } catch (err) {
      const textarea = document.createElement('textarea');
      textarea.value = text;
      textarea.style.position = 'fixed';
      textarea.style.opacity = '0';
      document.body.appendChild(textarea);
      textarea.select();
      try {
        if (document.execCommand('copy')) {
          copied = true;
        }
      } catch (ex) {
        console.error('Не удалось скопировать данные', ex);
      } finally {
        document.body.removeChild(textarea);
      }
    }

    if (!copied) return;

    // Создание уведомления
    const tooltip = document.createElement('div');
    tooltip.className = 'copy-tooltip';
    tooltip.textContent = 'Скопировано';

    const rect = cell.getBoundingClientRect();

    const tooltipWidth = 100;
    const tooltipHeight = 20;

    tooltip.style.left = rect.left + (rect.width - tooltipWidth) / 2 + 'px';
    tooltip.style.top = rect.top - tooltipHeight - 6 + 'px';

    document.body.appendChild(tooltip);

    // Анимация появления
    requestAnimationFrame(() => {
      tooltip.classList.add('show');
    });

    setTimeout(() => {
      tooltip.classList.remove('show');
      setTimeout(() => {
        if (tooltip.parentNode) {
          tooltip.parentNode.removeChild(tooltip);
        }
      }, 150);
    }, 1000);
  });
});

// Хранение оригинального заголовка окна
let originalWindowTitle = document.title;
let hasUpdateInTitle = false;

function markTitleAsHasUpdate() {
  if (hasUpdateInTitle) return;
  originalWindowTitle = document.title || originalWindowTitle || 'Test Data Generator';
  document.title = `${originalWindowTitle} - Доступно обновление!`;
  hasUpdateInTitle = true;
}

// Автоматическая проверка обновлений при загрузке страницы
async function checkForUpdatesOnLoad() {
  try {
    if (!window.api?.checkForUpdates) {
      return;
    }

    const result = await window.api.checkForUpdates();

    if (result?.ok && result?.hasUpdate) {
      showUpdateModal(result);
      markTitleAsHasUpdate();
    }
  } catch (e) {
  }
}

// Показ модального окна обновления
function showUpdateModal(updateInfo) {
  const modal = document.getElementById('updateModal');
  const messageEl = document.getElementById('updateMessage');
  const downloadBtn = document.getElementById('downloadUpdateBtn');
  const closeModalBtn = document.getElementById('closeUpdateModal');

  if (!modal || !messageEl || !downloadBtn) return;

  const latestVersion = updateInfo.latestVersion || 'неизвестна';

  messageEl.textContent = `Доступна новая версия: ${latestVersion}\n`;

  // Обработчик скачивания
  downloadBtn.onclick = async () => {
    if (updateInfo.latestUrl && window.api?.openExternal) {
      await window.api.openExternal(updateInfo.latestUrl);
    }
    modal.style.display = 'none';
  };

  // Обработчики закрытия
  const closeModal = () => {
    modal.style.display = 'none';
  };

  closeModalBtn.onclick = closeModal;

  // Закрытие по клику вне модального окна
  modal.onclick = (e) => {
    if (e.target === modal) {
      closeModal();
    }
  };

  modal.style.display = 'flex';
}

document.addEventListener('keydown', e => {
  const isMac = navigator.platform.toUpperCase().includes('MAC');
  const ctrlOrCmd = isMac ? e.metaKey : e.ctrlKey;

  if (ctrlOrCmd && e.key.toLowerCase() === 'z' && !e.shiftKey) {
    e.preventDefault();
    undo();
  } else if (ctrlOrCmd && (e.key.toLowerCase() === 'y' || (e.shiftKey && e.key.toLowerCase() === 'z'))) {
    e.preventDefault();
    redo();
  }
});

function applyTheme(theme) {
  currentTheme = theme;
  localStorage.setItem('theme', theme);

  const html = document.documentElement;
  html.classList.remove('dark-theme', 'light-theme');

  const isDark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches);

  if (isDark) {
    html.classList.add('dark-theme');
  } else {
    html.classList.add('light-theme');
  }

  // Обновляем текст кнопки
  const themeButton = document.getElementById('themeToggleBtn');
  if (themeButton) {
    if (theme === 'system') {
      themeButton.textContent = '🌓 Системная';
    } else if (theme === 'dark') {
      themeButton.textContent = '🌙 Тёмная';
    } else {
      themeButton.textContent = '☀️ Светлая';
    }
  }
}

// Обработчик переключения темы
document.getElementById('themeToggleBtn')?.addEventListener('click', () => {
  if (currentTheme === 'system') {
    applyTheme('dark');
  } else if (currentTheme === 'dark') {
    applyTheme('light');
  } else {
    applyTheme('system');
  }
});

// Справка (из README.md)
async function loadHelpMarkdown() {
  try {
    const helpPath = window.api.path.join(__dirname, '../../README.md');
    const content = await window.api.readFile(helpPath);
    const html = marked.parse(content);
    document.getElementById('helpContent').innerHTML = html;
  } catch (err) {
    console.error('Не удалось загрузить справку:', err);
    document.getElementById('helpContent').innerHTML = `
      <p>Ошибка загрузки справки: ${err.message}</p>
      <p>Убедитесь, что файл <code>README.md</code> существует в папке <code>help/</code>.</p>
    `;
  }
}

// Обработчики модального окна
document.getElementById('helpBtn')?.addEventListener('click', () => {
  document.getElementById('helpModal').style.display = 'flex';
  loadHelpMarkdown();
});

document.getElementById('closeHelpModal')?.addEventListener('click', () => {
  document.getElementById('helpModal').style.display = 'none';
});

// Закрытие по клику вне окна
document.getElementById('helpModal')?.addEventListener('click', e => {
  if (e.target === document.getElementById('helpModal')) {
    document.getElementById('helpModal').style.display = 'none';
  }
});

function getFullConfigSnapshot() {
  return getFullConfig();
}

function saveToHistory() {
  const config = getFullConfigSnapshot();
  // Удаляем "будущее", если были отмены
  if (historyIndex < history.length - 1) {
    history = history.slice(0, historyIndex + 1);
  }
  history.push(config);
  if (history.length > MAX_HISTORY) {
    history.shift();
  } else {
    historyIndex = history.length - 1;
  }
  saveConfigToLocalStorage();
}

function undo() {
  if (historyIndex <= 0) return;
  historyIndex--;
  applyConfig(history[historyIndex]);
}

function redo() {
  if (historyIndex >= history.length - 1) return;
  historyIndex++;
  applyConfig(history[historyIndex]);
}
