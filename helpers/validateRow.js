var columnsToKeys = require('./columnsToKeys');
var arrayify = require('jm-tools').arrayify;

var allTrue = function (a, b) {
  return a && b;
};

module.exports = function (row, columns) {
  var keys = columnsToKeys(columns);
  var verified = false;

  // Goes through the fields in each of the columns and verifies that they are not undefined
  var notNullFields = ['primaryKeys', 'notNullFields', 'lastUpdatedField'];
  verified = notNullFields.map(function (fields) {
    return arrayify(keys[fields]).map(function (field) {
      if (row[field] === undefined) {
        console.log('failed on field', field);
      }
      return row[field] !== undefined;
    }).reduce(allTrue, true);
  }).reduce(allTrue, true);

  return verified;
};
