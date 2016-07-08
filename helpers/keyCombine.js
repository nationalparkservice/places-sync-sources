var normalizeToType = require('jm-tools').normalizeToType;
var stringify = require('./stringify');

var parse = function (v) {
  var returnValue = v;
  try {
    returnValue = JSON.parse(v);
  } catch (e) {
    returnValue = v;
  }
  return returnValue;
};

var combine = function (columns, record) {
  return columns.map(function (column) {
    return stringify(normalizeToType(record[column]));
  }).join(',');
};
combine.split = function (columns, record) {
  // Converts back into an object
  var returnObj = {};
  record = record.toString();
  var splitRecord = record.split(',');
  columns.forEach(function (column, i) {
    returnObj[column] = parse(splitRecord[i]);
  });
  return returnObj;
};

module.exports = combine;
