// var sources = tools.requireDirectory(__dirname, [__filename], sourceType === 'json' ? ['json.js'] : undefined);
/* OpenStreetMap Connections:
 */

var Promise = require('bluebird');
var Immutable = require('immutable');
var tools = require('jm-tools');
var columnsFromConfig = require('../helpers/columnsFromConfig');
var columnsToKeys = require('../helpers/columnsToKeys');
var databases = require('places-sync-databases');

var WriteFn = function (connection, options, columns) {
  // var keys = columnsToKeys(columns);

  return function (updated, removed, metadata) {
    console.log(metadata);
    process.exit(0);
    var tasks = [];
    var removeTasks = [];
    var keys = columnsToKeys(columns);
    if (keys.removedFieldValue !== undefined) {
      removed.forEach(function (row) {
        row[keys.removedField] = keys.removedFieldValue;
        updated.push(row);
      });
      removed = [];
    }
    updated.forEach(function (updatedRow, i) {
      var masterKey = keys.primaryKeys.map(function (key) {
        return updatedRow[key];
      }).join('');
      var matchedMetadata = tools.arrayify(metadata).filter(function (record) {
        return record.key === masterKey;
      });
      tasks.push({
        'name': 'Remove / Write Update Row ' + i + JSON.stringify(updatedRow),
        'task': tools.iterateTasks,
        'params': [
          [{
            'name': 'Remove',
            'task': connection.query,
            'params': ['remove', updatedRow, keys.primaryKeys, matchedMetadata]
          }, {
            'name': 'Write',
            'task': connection.query,
            'params': ['insert', updatedRow, columns, matchedMetadata]
          }],
          'update cartodb db', true
        ]
      });
    });
    removed.forEach(function (removedRow, i) {
      removeTasks.push({
        'name': 'Remove Removed Row ' + i,
        'task': connection.query // ,
      // 'params': createQueries('remove', removedRow, keys.primaryKeys, tableName)
      });
    });

    return Promise.all(tasks.map(function (task) {
      return task.task.apply(this, task.params);
    })).then(function () {
      return Promise.all(removeTasks.map(function (removeTask) {
        return removeTask.task.apply(this, removeTask.params);
      }));
    });
  };
};

module.exports = function (sourceConfig, options) {
  return new Promise(function (fulfill, reject) {
    // Clean up the connectionConfig, and set the defaults
    var connectionConfig = new Immutable.Map(sourceConfig.connection);
    var requiredConnectionFields = ['address', 'consumer_key', 'consumer_secret', 'access_key', 'access_secret'];
    requiredConnectionFields.forEach(function (field) {
      if (typeof connectionConfig.get(field) !== 'string') {
        reject(new Error(field + ' must be defined for an OpenStreetMap Connection'));
      }
    });
    if (tools.getJsType(sourceConfig.columns) !== 'array') {
      reject(new Error('Source Columns must be defined for an OpenStreetMap Connection'));
    }

    // Define the taskList
    var tasks = [{
      'name': 'createConnection',
      'description': 'Connects to the OpenStreetMap API and verifies user credentials',
      'task': databases,
      'params': [
        connectionConfig.toObject()
      ]
    }];
    tools.iterateTasks(tasks, 'osm').then(function (r) {
      var columns = columnsFromConfig(sourceConfig.columns, sourceConfig.fields);
      fulfill({
        'data': [], // TODO: Support queries from OpenStreetMap
        'columns': columns,
        'writeFn': new WriteFn(r[0], options, columns),
        'querySource': undefined // TODO: Support queries from OpenStreetMap
      });
    }).catch(reject);
  });
};
