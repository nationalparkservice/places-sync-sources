/* SQLite File:
 *   *connection.filePath: (path to the sqlite file)
 *   *  using a different primary key than what is defined in sqlite may
 *   * cause unexpected issues (with null and unique contraints)
 *   *  so pay attention to this if you're using custom primary keys
 */

var Promise = require('bluebird');
var Immutable = require('immutable');
var tools = require('jm-tools');
var databases = require('places-sync-databases');
var CreateQueries = require('../helpers/createQueries');
var columnsFromConfig = require('../helpers/columnsFromConfig');
var columnsToKeys = require('../helpers/columnsToKeys');

var QuerySource = function (connection, options, tableName, columns) {
  return function (type, whereObj, returnColumns) {
    returnColumns = returnColumns || columns;
    var keys = columnsToKeys(returnColumns);
    var createQueries = new CreateQueries(columns, keys.primaryKeys, keys.lastUpdatedField, keys.removedField);
    return connection.query.apply(this, createQueries(type, whereObj, returnColumns, tableName));
  };
};

var WriteFn = function (connection, options, tableName, columns) {
  var keys = columnsToKeys(columns);
  var createQueries = new CreateQueries(columns, keys.primaryKeys);

  return function (updated, removed) {
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
      tasks.push({
        'name': 'Remove / Write Update Row ', //+ i + JSON.stringify(updatedRow),
        'task': tools.iterateTasks,
        'params': [
          [{
            'name': 'Remove',
            'task': connection.query,
            'params': createQueries('remove', updatedRow, keys.primaryKeys, tableName)
          }, {
            'name': 'Write',
            'task': connection.query,
            'params': [createQueries('insert', undefined, columns, tableName)[0], updatedRow]
          }],
          'update sqlite db', true
        ]
      });
    });
    removed.forEach(function (removedRow, i) {
      removeTasks.push({
        'name': 'Remove Removed Row ' + i,
        'task': connection.query,
        'params': createQueries('remove', removedRow, keys.primaryKeys, tableName)
      });
    });

    return Promise.all(tasks.map(function (task) {
      return task.task.apply(this, task.params);
    })).then(function () {
      return Promise.all(removeTasks.map(function (removeTask) {
        return removeTask.task.apply(this, removeTask.params).then(function () {
          return tools.dummyPromise({
            'updated': updated,
            'removed': removed
          });
        });
      }));
    });
  };
};

var readSqlite = function (connection, options, tableName) {
  return new Promise(function (fulfill, reject) {
    connection.query("PRAGMA table_info('" + tableName + "');").then(function (rawColumns) {
      // TODO: allow an option to pull the data into the source, like in csvs
      var data;
      // Convert the SQLite column format into ours
      var columns = rawColumns.map(function (rawColumn) {
        return {
          'name': rawColumn.name,
          'type': rawColumn.type,
          'sqliteType': rawColumn.type,
          'primaryKey': rawColumn.pk !== 0,
          'defaultValue': typeof rawColumn.dflt_value === 'object' ? undefined : rawColumn.dflt_value,
          'notNull': rawColumn.notnull === 1,
          'sqliteColumnId': rawColumn.cid
        };
      });
      fulfill({
        'data': data,
        'columns': columns
      });
    }).catch(reject);
  });
};

module.exports = function (sourceConfig, options) {
  return new Promise(function (fulfill, reject) {
    // Clean up the connectionConfig, and set the defaults
    var connectionConfig = new Immutable.Map(sourceConfig.connection);
    if (typeof connectionConfig.get('filePath') !== 'string') {
      throw new Error('filePath must be defined for a SQLite file');
    }
    if (typeof (connectionConfig.get('table')) !== 'string') {
      throw new Error('A table name must be set in the connection for a SQLite file');
    }

    // Define the taskList
    var tasks = [{
      'name': 'createDbConnection',
      'description': 'Connects to the SQLite Database',
      'task': databases,
      'params': [{
        'type': 'sqlite',
        'connection': connectionConfig.get('filePath')
      }]
    }, {
      'name': 'convertFromTable',
      'description': 'Takes the source data and creates an update/remove sqlite table in memory for it',
      'task': readSqlite,
      'params': ['{{createDbConnection}}', options, connectionConfig.get('table')]
    }];
    tools.iterateTasks(tasks, 'sqlite').then(function (r) {
      var columns = columnsFromConfig(r.convertFromTable.columns, sourceConfig.fields);
      fulfill({
        'data': r[1].data,
        'columns': columns,
        'writeFn': new WriteFn(r[0], options, connectionConfig.get('table'), columns),
        'querySource': new QuerySource(r[0], options, connectionConfig.get('table'), columns)
      });
    }).catch(reject);
  });
};
