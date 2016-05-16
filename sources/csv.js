/* CSV File:
 *   The CSV file MUST have headers
 *   *filePath: (path the the csv file)
 *   encoding: (defaults to 'UTF8')
 */

var Promise = require('bluebird');
var csv = require('csv');
var Immutable = require('immutable');
var tools = require('jm-tools');
var castToSqliteType = require('../helpers/castToSqliteType');
var updateJsonSource = require('../helpers/updateJsonSource');
var columnsFromConfig = require('../helpers/columnsFromConfig');
var fs = Promise.promisifyAll(require('fs'));

var WriteFn = function (data, columns, filePath, fileEncoding) {
  // TODO: instead of passing data back in, we could just read it from the file again
  // That way we could take it out of memory
  return function (updated, removed) {
    return new Promise(function (fulfill, reject) {
      updateJsonSource(data, updated, removed, columns)
        .then(function (newData) {
          return writeCsv(newData, columns, filePath, fileEncoding).then(function () {
            fulfill({
              'updated': updated,
              'removed': removed
            });
          }).catch(reject);
        })
        .catch(reject);
    });
  };
};

var writeCsv = function (data, columns, filePath, fileEncoding) {
  return new Promise(function (fulfill, reject) {
    var headers = tools.simplifyArray(columns);
    var csvRows = data.map(function (row) {
      return columns.map(function (column) {
        return tools.isUndefined(row[column.name], tools.isUndefined(column.defaultValue, ''));
      });
    });
    csvRows.unshift(headers);
    csv.stringify(csvRows, function (e, r) {
      if (e) {
        reject(e);
      } else {
        fs.writeFileAsync(filePath, r, fileEncoding).then(fulfill).catch(reject);
      }
    });
  });
};

var readCsv = function (data, predefinedColumns) {
  var jsonData;
  var columns;
  return new Promise(function (fulfill, reject) {
    csv.parse(data, function (e, r) {
      if (e) {
        reject(e);
      } else if (r.length < 2 && !predefinedColumns) {
        reject(new Error('CSV Must have at least one row, or have its columns defined'));
      } else if (predefinedColumns && r.length < 2) {
        fulfill({
          'data': [],
          'columns': predefinedColumns
        });
      } else {
        // // Remove the first row, which should be headers
        jsonData = castToSqliteType(r.slice(1) || []);

        columns = r[0].map(function (name, index) {
          return {
            'name': name,
            'type': 'text'
          };
        });

        fulfill({
          'data': jsonData.map(function (row) {
            return tools.nameArrayValues(row, r[0]);
          }),
          'columns': columns
        });
      }
    });
  });
};

module.exports = function (sourceConfig) {
  return new Promise(function (fulfill, reject) {
    // Clean up the connectionConfig, and set the defaults
    var connectionConfig = new Immutable.Map(sourceConfig.connection);
    var fieldsConfig = new Immutable.Map(sourceConfig.fields);
    if (typeof connectionConfig.get('filePath') !== 'string') {
      throw new Error('filePath must be defined for a CSV file');
    }
    connectionConfig = connectionConfig.set('encoding', connectionConfig.get('encoding') || 'UTF8');

    // Define the taskList
    var tasks = [{
      'name': 'openFile',
      'description': 'Does a few checks on opens the file if it can',
      'task': fs.readFileAsync,
      'params': [connectionConfig.get('filePath'), connectionConfig.get('encoding')]
    }, {
      'name': 'convertFromCsv',
      'description': 'Takes the source data and converts it to Json',
      'task': readCsv,
      'params': ['{{openFile}}', sourceConfig.columns]
    }];
    tools.iterateTasks(tasks, 'csv').then(function (r) {
      var columns = columnsFromConfig(r.convertFromCsv.columns, sourceConfig.fields);
      fulfill({
        'data': r[1].data,
        'columns': columns,
        'writeFn': new WriteFn(r[1].data, columns, connectionConfig.get('filePath'), connectionConfig.get('encoding')),
        'querySource': false
      });
    }).catch(reject);
  });
};
