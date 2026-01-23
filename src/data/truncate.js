const fs = require('fs');

function truncateCoordinates(geojson) {
  function truncateNumber(num) {
    const parts = num.toString().split('.');
    if (parts.length === 1) {
      return num;
    }
    return parseFloat(parts[0] + '.' + parts[1].substring(0, 5));
  }

  function processCoordinates(coords) {
    if (Array.isArray(coords)) {
      if (typeof coords[0] === 'number') {
        return [truncateNumber(coords[0]), truncateNumber(coords[1])];
      } else {
        return coords.map(processCoordinates);
      }
    }
    return coords;
  }

  if (geojson.features && Array.isArray(geojson.features)) {
    geojson.features.forEach(feature => {
      if (feature.geometry && feature.geometry.coordinates) {
        feature.geometry.coordinates = processCoordinates(feature.geometry.coordinates);
      }
    });
  }

  return geojson;
}

const path = 'countriesPolygonsData/PL_PolygonsData.json'

// Читаем исходный файл
const inputData = JSON.parse(fs.readFileSync(path, 'utf8'));

// Обрабатываем координаты
const result = truncateCoordinates(inputData);

// Записываем результат в новый файл
fs.writeFileSync(path, JSON.stringify(result, null, 2));

console.log('Готово! Результат записан в '+path);
