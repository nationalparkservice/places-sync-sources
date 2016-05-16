var reverseMappings = function (mappings) {
  if (!mappings) return mappings;
  var newMappings = {};
  mappings = JSON.parse(JSON.stringify(mappings)) || {};
  for (var field in mappings) {
    if (typeof mappings[field] === 'string') {
      newMappings[mappings[field]] = field;
    }
  }
  return newMappings;
};

var addNonArrayAttributes = function (originalArray, newArray) {
  // Sometimes you can throw extra attribues onto an array, and they get chopped off when you do a map
  // ex. a = [0,2,1]; a.sum = 3; a.map(function(b){return b}) will not have a "a.sum" attribute
  // This adds those back!
  for (var i in originalArray) {
    if (parseInt(i, 10).toString() !== i.toString()) {
      newArray[i] = originalArray[i];
    }
  }
  return newArray;
};

var mapColumns = function (columns, mappings) {
  if (!mappings) return columns;
  columns = JSON.parse(JSON.stringify(columns));
  var newColumns = columns.map(function (column) {
    if (typeof mappings[column.name] === 'string') {
      column.name = mappings[column.name];
    }
    return column;
  }).filter(function (column) {
    // Map to null if you want to column to disappear
    return mappings[column.name] !== null;
  });
  return addNonArrayAttributes(columns, newColumns);
};

var mapData = function (data, mappings) {
  if (!mappings) return data;
  var newData = data.map(function (row) {
    var newRow = {};
    for (var i in row) {
      if (typeof mappings[i] === 'string') {
        newRow[mappings[i]] = row[i];
      } else if (mappings[i] !== null) {
        newRow[i] = row[i];
      }
    }
    return newRow;
  });
  return addNonArrayAttributes(data, newData);
};

module.exports = {
  'data': {
    // mappings = direction === 'from' ? reverseMappings(mappings) : mappings;
    'to': mapData,
    'from': function (data, mappings) {
      return mapData(data, reverseMappings(mappings));
    }
  },
  'columns': {
    'to': mapColumns,
    'from': function (columns, mappings) {
      return mapColumns(columns, reverseMappings(mappings));
    }
  }
};
