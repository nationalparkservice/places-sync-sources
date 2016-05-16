var tools = require('jm-tools');
var normalizeToType = tools.normalizeToType;
var getDataType = tools.getDataType;

var parse = function (value, sqliteType) {
  return sqliteType === 'text' ? value : parseFloat(value, 10);
};

module.exports = function (tableData) {
  // Cast Variables to Integers, Floats, or Strings after guessing the type for the entire column
  // This is useful for CSV files, or other sources that cast everything to string by default
  //
  // Set the maxType (most restrictive) value type to integer for each column
  var maxTypes = tableData[0].map(function () {
    return 'integer';
  });

  // Go through each row and determine the actual datatype
  tableData.forEach(function (row) {
    row.forEach(function (column, index) {
      maxTypes[index] = getDataType(column, maxTypes[index]);
    });
  });

  // Now that we know that the sqlite types are, we should cast our values to those types
  return tableData.map(function (row) {
    return row.map(function (column, index) {
      return parse(normalizeToType(column, maxTypes[index] === 'text' ? 'string' : 'number'), maxTypes[index]);
    });
  });
};
