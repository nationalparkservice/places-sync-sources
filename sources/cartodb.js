/* CartoDB File:
 */

var Promise = require('bluebird');
var Immutable = require('immutable');
var tools = require('jm-tools');
var CreateQueries = require('../helpers/createQueries');
var columnsFromConfig = require('../helpers/columnsFromConfig');
var columnsToKeys = require('../helpers/columnsToKeys');
var databases = require('places-sync-databases');
var getSqliteType = require('../helpers/getSqliteTypeFromPg');
var mapFields = require('../helpers/mapFields');
var postgresDateTransform = require('../helpers/postgresDateTransform');
var addTransforms = require('../helpers/addTransforms');

var QuerySource = function (connection, options, account, tableName, columns, fields, baseFilter) {
  return function (type, whereObj, returnColumns) {
    // Allow the calling function to specify what columns get returned
    var newWhereObj = mapFields.data.from([tools.mergeObjects(baseFilter || {}, whereObj)], fields.mapped)[0];
    returnColumns = mapFields.columns.from(returnColumns || columns, fields.mapped);

    // Add the transformations
    returnColumns = addTransforms(options, returnColumns);

    // Determine the primary Keys
    var keys = columnsToKeys(returnColumns);

    // Create the query object that we use to generate the queries
    var newColumns = mapFields.columns.from(columns, fields.mapped);
    var createQueries = new CreateQueries(newColumns, keys.primaryKeys, keys.lastUpdatedField, keys.removedField, options);

    // Create the query that we will run on the server
    var cartoDbQuery = createQueries(type, newWhereObj, returnColumns, tableName);

    // Run the query
    return connection.query(cartoDbQuery[0], cartoDbQuery[1], false, columns)
      .then(function (result) {
        return mapFields.data.to(result, fields.mapped);
      });
  };
};

var WriteFn = function (connection, options, account, tableName, columns, fields) {
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
        'name': 'Remove / Write Update Row ', // + i + JSON.stringify(updatedRow),
        'task': tools.iterateTasks,
        'params': [
          [{
            'name': 'Remove',
            'task': connection.query,
            'params': createQueries('remove', updatedRow, keys.primaryKeys, tableName).concat([false, newColumns])
          }, {
            'name': 'Write',
            'task': connection.query,
            'params': [createQueries('insert', undefined, newColumns, tableName)[0], updatedRow, false, newColumns]
          }],
          'update cartodb db', true
        ]
      });
    });
    removed.forEach(function (removedRow, i) {
      removeTasks.push({
        'name': 'Remove Removed Row ' + i,
        'task': connection.query,
        'params': createQueries('remove', removedRow, keys.primaryKeys, tableName).concat([false, newColumns])
      });
    });

    return tools.iterateRateLimit(tasks, 'remove / update tasks', false, false, 250).then(function () {
      return tools.iterateRateLimit(removeTasks, 'remove tasks', false, false, 250).then(function () {
        return tools.dummyPromise({
          'updated': updated,
          'removed': removed
        });
      });
    });
  };
};

var readCartoDB = function (connection, options, config, sourceFields) {
  var queryObj = {
    'tableSchema': config.account,
    'tableName': config.table
  };
  var queries = [];

  // This will get us most of the info we need on the columns, but some types are (user defined)
  var query = '';
  query += 'SELECT';
  query += '  cdb_columnnames AS "name",';
  query += "  cdb_columntype('" + queryObj.tableSchema + '.' + queryObj.tableName + "',";
  query += '  cdb_columnnames) AS "pgtype"';
  query += 'FROM';
  query += "  cdb_columnnames('";
  query += queryObj.tableSchema + '.' + queryObj.tableName + "');";
  queries.push(query);

  // Since the user defined fields aren't available in columnNames, we run this query to get them
  queries.push('SELECT * FROM "' + queryObj.tableSchema + '"."' + queryObj.tableName + '" LIMIT 0;');

  // To get if fields are nullable, and other info, we load this too
  query = '';
  query += 'SELECT ';
  query += '"column_name", "column_default", "is_nullable", "ordinal_position", "data_type" ';
  query += 'FROM "information_schema"."columns" ';
  query += 'WHERE "table_schema" = \'' + queryObj.tableSchema + '\' AND "table_name" = \'' + queryObj.tableName + "' ";
  query += 'ORDER BY "ordinal_position";';
  queries.push(query);

  // We need to run this to get the primary keys, since nothing else has them
  query = '';
  query += 'SELECT distinct ';
  query += 'unnest("index_keys") AS "name" ';
  query += "FROM CDB_TableIndexes('" + queryObj.tableSchema + '.' + queryObj.tableName + "') ";
  query += 'WHERE index_primary = true;';
  queries.push(query);

  return new Promise(function (fulfill, reject) {
    Promise.all(queries.map(function (q) {
      return connection.query(q, queryObj);
    })).then(function (result) {
      var columnNames = result[0] || [];
      var resultFields = result[1].fields || {};
      var columnInfoSchema = result[2] || [];
      var pkeys = tools.simplifyArray(result[3]);

      // Convert the SQLite column format into ours
      var columns = columnNames.map(function (column) {
        // Get types for when cartodb says "USER-DEFINED"
        var type = column.pgtype;
        if (type === 'USER-DEFINED' && resultFields[column.name] && resultFields[column.name].type) {
          type = resultFields[column.name].type;
        }

        // Get the row that matches from the infoSchema
        var matchedInfoSchema = columnInfoSchema[tools.simplifyArray(columnInfoSchema, 'column_name').indexOf(column.name)] || {};
        return {
          'name': column.name,
          'type': type,
          'sqliteType': getSqliteType(type),
          'primaryKey': pkeys.indexOf(column.name) > -1,
          'defaultValue': undefined, // typeof matchedInfoSchema.column_default === 'object' ? undefined : matchedInfoSchema.column_default,
          'notNull': matchedInfoSchema.is_nullable === 'NO',
          'columnId': matchedInfoSchema.ordinal_position
        };
      });

      fulfill({
        'data': undefined,
        'columns': mapFields.columns.to(columns, sourceFields.mapped)
      });
    }).catch(reject);
  });
};

module.exports = function (sourceConfig, options) {
  return new Promise(function (fulfill, reject) {
    // Clean up the connectionConfig, and set the defaults
    var connectionConfig = new Immutable.Map(sourceConfig.connection);
    var requirements = ['account', 'apiKey', 'table'];
    requirements.forEach(function (requirement) {
      if (typeof connectionConfig.get(requirement) !== 'string') {
        throw new Error(requirement + ' must be defined for a CartoDB connection');
      }
    });
    // Define the taskList
    var tasks = [{
      'name': 'createDbConnection',
      'description': 'Connects to the SQLite Database',
      'task': databases,
      'params': [{
        'type': 'cartodb',
        'connection': connectionConfig.toObject()
      }]
    }, {
      'name': 'convertFromTable',
      'description': 'Takes the source data and creates an update/remove cartodb table in memory for it',
      'task': readCartoDB,
      'params': ['{{createDbConnection}}', options, connectionConfig.toObject(), sourceConfig.fields]
    }];
    tools.iterateTasks(tasks, 'cartodb').then(function (result) {
      var columns = columnsFromConfig(result.convertFromTable.columns, sourceConfig.fields);

      // Add transforms for the timestamp with time zone field
      sourceConfig.fields.transforms = sourceConfig.fields.transforms || {};
      columns.forEach(function (column) {
        if (column.type === 'timestamp with time zone') {
          sourceConfig.fields.transforms[column.name] = sourceConfig.fields.transforms[column.name] || postgresDateTransform;
        }
      });

      // Copy these transforms into the options (might need to rethink this)
      options = options || {};
      options.transforms = sourceConfig.fields.transforms;

      fulfill({
        'data': result[1].data,
        'columns': columns,
        'writeFn': new WriteFn(result[0], options, connectionConfig.get('account'), connectionConfig.get('table'), columns, sourceConfig.fields),
        'querySource': new QuerySource(result[0], options, connectionConfig.get('account'), connectionConfig.get('table'), columns, sourceConfig.fields, sourceConfig.filter)
      });
    }).catch(reject);
  });
};
