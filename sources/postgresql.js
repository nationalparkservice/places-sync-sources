/* Postgresql File:
 */

var Promise = require('bluebird');
var Immutable = require('immutable');
var tools = require('jm-tools');
var databases = require('places-sync-databases');
var CreateQueries = require('../helpers/createQueries');
var columnsFromConfig = require('../helpers/columnsFromConfig');
var getSqliteType = require('../helpers/getSqliteTypeFromPg');
var columnsToKeys = require('../helpers/columnsToKeys');
var mapFields = require('../helpers/mapFields');
var postgresDateTransform = require('../helpers/postgresDateTransform');
var addTransforms = require('../helpers/addTransforms');

var QuerySource = function (connection, options, tableName, columns, fields, baseWhereClause) {
  return function (type, whereObj, returnColumns) {
    var newWhereObj = mapFields.data.from([tools.mergeObjects(baseWhereClause || {}, whereObj)], fields.mapped)[0];
    returnColumns = mapFields.columns.from(returnColumns || columns, fields.mapped);

    var keys = columnsToKeys(returnColumns);

    var newColumns = mapFields.columns.from(columns, fields.mapped);
    var createQueries = new CreateQueries(newColumns, keys.primaryKeys, keys.lastUpdatedField, keys.removedField, options);

    // Casts!
    var preQuery = createQueries(type, newWhereObj, returnColumns, tableName);

    return connection.query(preQuery[0], preQuery[1]).then(function (result) {
      return mapFields.data.to(result, fields.mapped);
    });
  };
};

var WriteFn = function (connection, options, tableName, columns, fields) {
  // Parse out the original columns from the  columns so we can translate to the source Schema
  var newColumns = mapFields.columns.from(columns, fields.mapped);

  // Determine the primary keys
  var keys = columnsToKeys(newColumns);

  // Add the transformations
  newColumns = addTransforms(options, newColumns);

  // Create the query object that we use to generate the queries
  var createQueries = new CreateQueries(newColumns, keys.primaryKeys, keys.lastUpdatedField, keys.removedField, options);

  return function (updated, removed) {
    var tasks = [];
    var removeTasks = [];

    // Remove the updates that have really been removed
    // TODO move this elsewhere
    if (keys.removedFieldValue !== undefined) {
      removed.forEach(function (row) {
        row[keys.removedField] = keys.removedFieldValue;
        updated.push(row);
      });
      removed = [];
    }

    // We need to map the columns back over
    updated = mapFields.data.from(updated, fields.mapped);
    removed = mapFields.data.from(removed, fields.mapped);

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
            'params': [createQueries('insert', undefined, newColumns, tableName)[0], updatedRow]
          }],
          'update postgresql db', true
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

var readPostgresql = function (connection, options, tableName, tableSchema, sourceFields) {
  tableSchema = tableSchema || 'public';
  var queryObj = {
    'tableSchema': tableSchema,
    'tableName': tableName
  };
  return new Promise(function (fulfill, reject) {
    Promise.all([
      connection.query('SELECT pg_attribute.attname AS "key" FROM pg_index, pg_class, pg_attribute, pg_namespace WHERE   pg_class.oid = {{tableName}}::regclass AND   indrelid = pg_class.oid AND   nspname = {{tableSchema}} AND   pg_class.relnamespace = pg_namespace.oid AND   pg_attribute.attrelid = pg_class.oid AND   pg_attribute.attnum = any(pg_index.indkey) AND indisprimary;', queryObj),
      connection.query('SELECT * FROM information_schema.columns WHERE table_schema = {{tableSchema}} AND table_name = {{tableName}};', queryObj)
    ]).then(function (result) {
      var rawColumns = result[1];
      var pkeys = tools.simplifyArray(result[0].key);
      var columns = rawColumns.map(function (rawColumn) {
        return {
          'name': rawColumn.column_name,
          'type': rawColumn.data_type,
          'sqliteType': getSqliteType(rawColumn.data_type),
          'primaryKey': pkeys.indexOf(rawColumn.name) > -1, // http://stackoverflow.com/questions/1214576/how-do-i-get-the-primary-keys-of-a-table-from-postgres-via-plpgsql
          // 'defaultValue': typeof rawColumn.column_default === 'object' ? undefined : rawColumn.column_default, // TODO: Maybe get these working later? not sure if we need them
          'notNull': rawColumn.is_nullable === 'NO',
          'columnId': rawColumn.ordinal_position
        };
      });
      fulfill({
        'data': undefined, // mapFields.data.to(data, sourceFields.mapped),
        'columns': mapFields.columns.to(columns, sourceFields.mapped)
      });
    }).catch(reject);
  });
};

module.exports = function (sourceConfig, options) {
  return new Promise(function (fulfill, reject) {
    // Clean up the connectionConfig, and set the defaults
    var connectionConfig = new Immutable.Map(sourceConfig.connection);
    if (typeof (connectionConfig.get('table')) !== 'string') {
      throw new Error('A table name must be set in the connection for a PostgreSQL connection');
    }

    // Define the taskList
    var tasks = [{
      'name': 'createDbConnection',
      'description': 'Connects to the PostgreSQL Database',
      'task': databases,
      'params': [{
        'type': 'postgresql',
        'connection': sourceConfig.connection
      }]
    }, {
      'name': 'convertFromTable',
      'description': 'Takes the source data and creates an update/remove postgresql table in memory for it',
      'task': readPostgresql,
      'params': ['{{createDbConnection}}', options, connectionConfig.get('table'), connectionConfig.get('schema'), sourceConfig.fields]
    }];
    tools.iterateTasks(tasks, 'postgresql').then(function (result) {
      var columns = columnsFromConfig(result.convertFromTable.columns, sourceConfig.fields);

      // Add transforms for the timestamp with time zone field
      sourceConfig.fields.transforms = sourceConfig.fields.transforms || {};
      columns.forEach(function (column) {
        if (column.type === 'timestamp with time zone' || column.type === 'timestamp without time zone') {
          sourceConfig.fields.transforms[column.name] = sourceConfig.fields.transforms[column.name] || postgresDateTransform;
        }
      });

      // Copy these transforms into the options (might need to rethink this)
      options = options || {};
      options.transforms = sourceConfig.fields.transforms;

      fulfill({
        'data': result[1].data,
        'columns': columns,
        'writeFn': new WriteFn(result[0], options, connectionConfig.get('table'), columns, sourceConfig.fields),
        'querySource': new QuerySource(result[0], options, connectionConfig.get('table'), columns, sourceConfig.fields, sourceConfig.where)
      });
    }).catch(reject);
  });
};
