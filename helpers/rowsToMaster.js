var columnsToKeys = require('./columnsToKeys');
var keyCombine = require('./keyCombine');
var md5 = require('jm-tools').md5;
var simplifyArray = require('jm-tools').simplifyArray;

var rowsToMaster = function (rows, columns, sourceName, processName, foreignKeys, removeRow) {
  var keys = columnsToKeys(columns);
  return rows.map(function (row) {
    var primaryKey = keyCombine(keys.primaryKeys, row);
    var foreignKey = (foreignKeys && foreignKeys[primaryKey]) || primaryKey;
    return {
      'key': primaryKey,
      'foreign_key': foreignKey,
      'process': processName || 'sync',
      'source': sourceName,
      'hash': md5(keyCombine(simplifyArray(columns), row)),
      'last_updated': parseFloat(row[keys.lastUpdatedField], 10), // Make sure we only add a number here
      'is_removed': removeRow ? 1 : 0
    };
  });
};

module.exports = rowsToMaster;
