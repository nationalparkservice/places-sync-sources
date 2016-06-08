/* JSON File:
 *   *filePath: (path the the json file)
 *   encoding: (defaults to 'UTF8')
 */

var Promise = require('bluebird');
var Immutable = require('immutable');
var tools = require('jm-tools');
var fs = Promise.promisifyAll(require('fs'));
var stringify = require('json-stringify-pretty-compact');
var columnsFromConfig = require('../helpers/columnsFromConfig');

var WriteFn = function (data, columns, filePath, fileEncoding) {
  // This is in here to prevent a memory leak
  var updateJsonSource = filePath ? require('../helpers/updateJsonSource') : undefined;

  return function (updated, removed) {
    return new Promise(function (fulfill, reject) {
      if (!updateJsonSource) {
        reject(new Error('Cannot write to a JSON source without a filePath specified in its config'));
      } else {
        updateJsonSource(data, updated, removed, columns)
          .then(function (newData) {
            return writeJson(newData, columns, filePath, fileEncoding).then(function (result) {
              var returnValue = {
                'updated': updated,
                'removed': removed
              };
              if (filePath === ':memory:') {
                returnValue.data = result;
              }
              fulfill(returnValue);
            }).catch(reject);
          })
          .catch(reject);
      }
    });
  };
};

var writeJson = function (data, columns, filePath, fileEncoding) {
  data = data.map(function (row) {
    var newRow = {};
    columns.forEach(function (column) {
      newRow[column.name] = tools.isUndefined(row[column.name], tools.isUndefined(column.defaultValue, ''));
    });
    return newRow;
  });
  return filePath === ':memory:' ? tools.dummyPromise(stringify(data)) : fs.writeFileAsync(filePath, stringify(data), fileEncoding);
};

var readJson = function (data, predefinedColumns) {
  // Attempt to parse it if it's a string
  if (typeof data === 'string') {
    try {
      data = JSON.parse(data);
    } catch (e) {
      if (data.length < 2) {
        data = [];
      } else {
        throw new Error('Data is not valid JSON')
      }
    }
  }
  var columns = predefinedColumns || [];
  if (!predefinedColumns) {
    data.forEach(function (row) {
      var thisColumn;
      for (var column in row) {
        thisColumn = columns.filter(function (value) {
          return value.name === column;
        })[0];
        if (!thisColumn) {
          var newColumnIdx = columns.push({
            'name': column
          }) - 1;
          thisColumn = columns[newColumnIdx];
        }
        thisColumn.type = tools.getDataType(row[column], thisColumn.type || 'integer', typeof row[column]);
      }
    });
  }
  return {
    'data': data,
    'columns': columns
  };
};

module.exports = function (sourceConfig) {
  return new Promise(function (fulfill, reject) {
    // Clean up the connectionConfig, and set the defaults
    var connectionConfig = new Immutable.Map(sourceConfig.connection);
    if (typeof connectionConfig.get('filePath') !== 'string' && connectionConfig.get('data') === undefined) {
      throw new Error('data or filePath must be defined for a JSON file');
    }
    connectionConfig = connectionConfig.set('encoding', connectionConfig.get('encoding') || 'UTF8');

    var returnData = function () {
      return connectionConfig.get('data');
    };

    // Define the taskList
    var tasks = [{
      'name': 'openFile',
      'description': 'Does a few checks on opens the file if it can',
      'task': connectionConfig.get('data') === undefined ? fs.readFileAsync : returnData,
      'params': [connectionConfig.get('filePath'), connectionConfig.get('encoding')]
    }, {
      'name': 'createDatabaseFromJson',
      'description': 'Creates a sqlite database from Json data',
      'task': readJson,
      'params': ['{{openFile}}', sourceConfig.columns]
    }];
    tools.iterateTasks(tasks).then(function (results) {
      var columns = columnsFromConfig(results.createDatabaseFromJson.columns, sourceConfig.fields);
      fulfill({
        'data': results.createDatabaseFromJson.data,
        'columns': columns,
        'writeFn': new WriteFn(results.createDatabaseFromJson.data, columns, connectionConfig.get('filePath'), connectionConfig.get('encoding')),
        'querySource': false
      });
    }).catch(reject);
  });
};
