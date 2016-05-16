var normalizeToType = require('jm-tools').normalizeToType;

var stringify = function (v) {
  var returnValue = v;
  try {
    JSON.parse(v);
    if (typeof v === 'string') {
      returnValue = v;
    } else {
      returnValue = JSON.stringify(v);
    }
  } catch (e) {
    returnValue = JSON.stringify(v);
  }
  return returnValue;
};
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
