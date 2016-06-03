var tools = require('jm-tools');
var Promise = require('bluebird');
var database = require('places-sync-databases');

var getColumnInfo = function (data, existingColumns) {
  // Figure out what the columns are
  // Fill in any values that we didn't have before
  // If there are defined columns, use only columns that arent in the defined list
  var newColumns = [];
  var defaultValues = {};
  // Try to read column info from the data, if data exists, otherwise use the existing columns

  if (data) {
    data.forEach(function (row) {
      for (var column in row) {
        var columnIndex = tools.simplifyArray(newColumns).indexOf(column) > -1 ? tools.simplifyArray(newColumns).indexOf(column) : newColumns.push({
          'name': column,
          'type': tools.getJsType(row[column])
        }) - 1;
        // Define the type we're going to use for SQLite
        newColumns[columnIndex].sqliteType = tools.getDataType(row[column], newColumns[columnIndex].sqliteType || 'integer', tools.getJsType(row[column]));
        // If every value in the table for a single column is the same, we'll assume that to be the default
        // A table will need more than one row for this to work, so we limit it with a (data > 1) if
        if (defaultValues[column]) {
          defaultValues[column].hasDefault = defaultValues[column].hasDefault && defaultValues[column].firstValue === row[column] && data.length > 1;
        } else {
          defaultValues[column] = {
            'hasDefault': true,
            'firstValue': row[column]
          };
        }
      }
    });
    // Add in the default values if there are any
    newColumns = newColumns.map(function (c) {
      if (defaultValues[c.name].hasDefault === true) {
        c.defaultValue = defaultValues[c.name].firstValue;
      }
      return c;
    });
  } else {
    newColumns = JSON.parse(JSON.stringify(existingColumns));
  }

  // If there defined columns, use only columns that aren't in the defined list
  if (existingColumns) {
    newColumns = existingColumns.map(function (c) {
      var matchedColumn = newColumns.filter(function (nc) {
        return nc.name === c.name;
      })[0] || {};
      for (var k in matchedColumn) {
        c[k] = c[k] === 'undefined' ? matchedColumn[k] : c[k];
      }
      return tools.denullify(c, true, [undefined]);
    });
  }
  return newColumns;
};

module.exports = function (data, existingColumns) {
  return new Promise(function (resolve, reject) {
    console.log('a', data, existingColumns);
    var columns = getColumnInfo(data, existingColumns);

    var tempDbConfig = {
      'type': 'sqlite',
      'connection': ':memory:'
    };

    var createColumns = function (includeColumns) {
      var primaryKeys = [];
      var columnArray = includeColumns.map(function (column) {
        var createColumn = tools.surroundValues(column.name, '"') + ' ' + column.sqliteType;
        if (column.defaultValue !== undefined) {
          createColumn += ' DEFAULT ' + tools.surroundValues(tools.normalizeToType(column.defaultValue), typeof tools.normalizeToType(column.defaultValue) === 'string' ? "'" : '') + ' ';
        } else if (column.notNull === true) {
          createColumn += ' NOT NULL ';
        }
        if (column.primaryKey === true || typeof column.primaryKey === 'number') {
          primaryKeys.push(column.name);
        }
        return createColumn;
      });
      if (primaryKeys.length > 0) {
        columnArray.push('PRIMARY KEY (' + tools.surroundValues(primaryKeys, '"').join(', ') + ')');
      }
      return '(' + columnArray.join(', ') + ');';
    };

    // Create a 'CREATE TABLE' statement for the sqlite database
    var createStatementCache = 'CREATE TABLE "cached" ' + createColumns(columns);
    var createStatementUpdate = 'CREATE TABLE "updated" ' + createColumns(columns);
    var createStatementRemove = 'CREATE TABLE "removed" ' + createColumns(columns);
    var createStatementMetadata = 'CREATE TABLE "metadata" ("key" TEXT, "foreignKey" TEXT, "lastUpdated" NUMERIC, "hash" text, "data" BLOB, "action" TEXT, PRIMARY KEY ("key"));';

    // console.log(createStatementCache);
    // console.log(createStatementUpdate);
    // console.log(createStatementRemove);
    // Create an 'INSERT INTO' statement for the sqlite database
    // TODO, use the createQueries tool
    var insertStatement = 'INSERT INTO "cached" (' + tools.simplifyArray(columns).map(function (c) {
      return '"' + c + '"';
    }).join(', ') + ') VALUES (' + tools.simplifyArray(columns).map(function (c) {
      return '{{' + c + '}}';
    }).join(', ') + ')';

    var taskList = [{
      'name': 'Create Database',
      'description': 'Create a sqlite database in memory',
      'task': database,
      'params': [tempDbConfig]
    }, {
      'name': 'Create Data Table',
      'task': '{{Create Database.query}}',
      'params': [createStatementCache]
    }, {
      'name': 'Create Updates Table',
      'task': '{{Create Database.query}}',
      'params': [createStatementUpdate]
    }, {
      'name': 'Create Removes Table',
      'task': '{{Create Database.query}}',
      'params': [createStatementRemove]
    }, {
      'name': 'Create Metadata Table',
      'task': '{{Create Database.query}}',
      'params': [createStatementMetadata]
    }, {
      'name': 'Insert Data',
      'task': '{{Create Database.queryList}}',
      'params': [insertStatement, tools.arrayify(data)]
    }];
    tools.iterateTasks(taskList, 'jsonToSqlite', false).then(function (a) {
      resolve(tools.arrayGetLast(a, true));
    }).catch(reject);
  });
};
