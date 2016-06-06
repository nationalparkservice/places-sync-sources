var guid = require('jm-tools').guid;
var Promise = require('bluebird');
var fandlebars = require('fandlebars');
var simplifyArray = require('jm-tools').simplifyArray;
var md5 = require('jm-tools').md5;
var jsonSource = require('../');
var keyCombine = require('./keyCombine');
var sql = require('fs').readFileSync(require('path').resolve(__dirname, './getUpdates.sql'), 'UTF8');

var recordsToCompare = function (records, sourceName, keys, ignoreKeys, emptyHash) {
  var returnValue = [];
  records.forEach(function (record) {
    var newKey = keyCombine(keys.primaryKeys, record);
    var newForeignKey = keyCombine(keys.foreignKeys, record);

    if (!ignoreKeys || ignoreKeys.indexOf(newKey) === -1) {
      var newHash = emptyHash ? null : (keys.hashField ? record[keys.hashField] : md5(keyCombine(keys.all, record)));

      // TODO, this may zip the data, but right now it's just a copy of the hash
      var newData = emptyHash ? null : (keys.dataField ? record[keys.dataField] : keyCombine(keys.all, record));

      returnValue.push({
        'source': sourceName,
        'lastUpdated': record[keys.lastUpdatedField],
        'key': newKey,
        'foreignKey': newForeignKey,
        'hash': newHash,
        'data': newData
      });
    }
  });
  return returnValue;
};

module.exports = function (lastSyncTime, updatedSinceTime, allKeys, allMasterKeys, sourceColumns) {
  var jsonUpdatedRecords = recordsToCompare(updatedSinceTime, 'user', sourceColumns);
  var jsonAllKeys = recordsToCompare(allKeys, 'user', sourceColumns, simplifyArray(jsonUpdatedRecords, 'key'), true);
  var jsonMasterKeys = recordsToCompare(allMasterKeys, 'master', {
    'primaryKeys': ['key'],
    'foreignKeys': ['foreign_key'],
    'hashField': 'hash',
    'dataField': 'data',
    'lastUpdatedField': 'last_updated'
  });
  var data = [].concat(jsonUpdatedRecords, jsonAllKeys, jsonMasterKeys);
  if (data.length === 0) {
    // No updates, no need to run this
    return new Promise(function (fulfill) {
      fulfill({});
    });
  }

  var tempSource = {
    'name': guid(),
    'connection': {
      'data': data,
      'type': 'json',
      'returnDatabase': true
    },
    'fields': {
      'primaryKey': ['key', 'source'],
      'lastUpdated': 'lastUpdated'
    }
  };
  return jsonSource(tempSource).then(function (source) {
    var query = fandlebars(sql, {
      'tableName': 'cached',
      'lastUpdated': parseFloat(lastSyncTime, 10)
    });
    return source.get._database().query(query).then(function (changedData) {
      return source.close().then(function () {
        return new Promise(function (fulfill) {
          var returnObject = {
            'metadata': changedData
          };
          changedData.forEach(function (record) {
            if (!returnObject[record.action]) {
              returnObject[record.action] = [];
            }
            var matchedRecord = updatedSinceTime[simplifyArray(jsonUpdatedRecords, 'key').indexOf(record.key.toString())];
            returnObject[record.action].push(matchedRecord || keyCombine.split(sourceColumns.primaryKeys, record.key));
          });
          fulfill(returnObject);
        });
      });
    });
  });
};
