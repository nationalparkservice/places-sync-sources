var Immutable = require('immutable');
var Promise = require('bluebird');
var tools = require('jm-tools');
var columnsFromConfig = require('../helpers/columnsFromConfig');
var jsonToSqlite = require('../helpers/jsonToSqlite');

// ///////////////////////////////////////////////////
// ///////////////////////////////////////////////////
// ///////////////////////////////////////////////////
// This is a copy of the new index, all sources will slowly be moved over to use this template
// ///////////////////////////////////////////////////
// ///////////////////////////////////////////////////
// ///////////////////////////////////////////////////
var addDefaultFields = function (immutableConfig, defaultFields) {
  // Even though these fields are immutable, this is the one place that we modify them to add defaults
  // Usually this is still like UTF8 for file encoding
  // and PostgreSQL databases usually have the schema public
  // things like that, that make the configs a lot cleaner

  var sourceConfigType;
  for (var configType in defaultFields) {
    sourceConfigType = immutableConfig[configType] || {};
    for (var field in defaultFields[configType]) {
      if (sourceConfigType.get(field) === undefined) {
        immutableConfig[configType] = sourceConfigType.set(field, defaultFields[configType][field]);
      }
    }
  }

  return immutableConfig;
};

var checkRequiredFields = function (immutableConfig, requireFields) {
  // Check if there are any required fields in the sourceConfig
  requireFields = requireFields || {};
  var validFields = [];
  var sourceConfigType;
  var sourceConfigTypeField;

  var checkType = function (field, sourceConfigTypeField, requiredField) {
    // It's valid if the source config has the config type, and the field is not === undefined, and the field type === the specified type
    // (you CAN specify undefined as a type, although I can't think of a case why you'd want that)
    return {
      'field': field,
      'valid': (
        sourceConfigTypeField &&
        (
          (typeof sourceConfigTypeField === requiredField) ||
          (sourceConfigTypeField !== undefined && requiredField === 'any') ||
          (requiredField === 'objectData' && typeof sourceConfigTypeField === 'object' && Object.keys(sourceConfigTypeField).length > 0)
        )
        ) || false
    };
  };

  for (var configType in requireFields) {
    sourceConfigType = immutableConfig[configType];
    if (typeof requireFields[configType] === 'object') {
      for (var field in requireFields[configType]) {
        sourceConfigTypeField = sourceConfigType && sourceConfigType.toJS() && sourceConfigType.toJS()[field];
        validFields.push(checkType(configType + '.' + field, sourceConfigTypeField, requireFields[configType][field]));
      }
    } else {
      validFields.push(checkType(configType, sourceConfigType && sourceConfigType.toJS(), requireFields[configType]));
    }
  }
  return validFields;
};

var createSource = function (sourceConfig, returnObj) {
  // Extract the values we need and make them Immutable
  var immutableConfig = {
    'connection': new Immutable.Map(sourceConfig.connection),
    'fields': new Immutable.Map(sourceConfig.fields),
    'columns': new Immutable.Set(sourceConfig.columns),
    'filter': new Immutable.Map(sourceConfig.filter),
    'transforms': new Immutable.Map(sourceConfig.transforms)
  };
  var sourceName = sourceConfig.name;

  // Load the source
  var source = returnObj;

  // Add the defaults from the source
  immutableConfig = addDefaultFields(immutableConfig, source.defaultFields);

  // Check the required fields for this type of source
  var validFields = checkRequiredFields(immutableConfig, source.requiredFields);
  var invalidFields = validFields.filter(function (field) {
    return field.valid === false;
  });
  if (invalidFields.length > 0) {
    // We have invalid or missing fields!
    return tools.dummyPromise(null, immutableConfig.connection.get('type') + ' required field' + (invalidFields.length === 1 ? '' : 's') + ' missing: \n\t' + tools.simplifyArray(invalidFields, 'field').join('\n\t'));
  }

  // Check if the name field is populated
  if (typeof sourceName !== 'string') {
    return tools.dummyPromise(null, 'All sources must be named with a string');
  }

  return new Promise(function (resolve, reject) {
    var taskList = [{
      'name': 'sourceTasks',
      'description': 'Load the tasks from the source',
      'task': source.load,
      'params': [immutableConfig.connection.toJS(), immutableConfig.columns.toJS()]
    }, {
      'name': 'dataToJson',
      'description': 'Converts the source into a JSON format',
      'task': tools.iterateTasks,
      'params': ['{{sourceTasks}}', 'Loading source ' + sourceName]
    }, {
      'name': 'columns',
      'description': 'Runs the columnsFromConfig function on the columns',
      'task': columnsFromConfig,
      'params': ['{{dataToJson.columns}}', immutableConfig.fields.toJS()]
    }, {
      'name': 'database',
      'description': 'Creates a database from the JSON representation of the data and the columns',
      'task': jsonToSqlite,
      'params': ['{{dataToJson.data}}', '{{columns}}']
    }];
    tools.iterateTasks(taskList, 'create source ' + sourceConfig.name, false).then(function (result) {
      var columns = result.columns; // TODO, should we use the columns from the db (result.database) instead?
      var data = result.database;
      var databaseConnection = result.dataToJson.databaseConnection || result.dataToJson.data;
      var writeToSource = source.WriteFn && new source.WriteFn(databaseConnection, immutableConfig.connection.toJS(), columns);
      var querySource = source.QuerySource && new source.QuerySource(databaseConnection, immutableConfig.connection.toJS(), columns);

      resolve({
        'data': data,
        'columns': columns,
        'writeFn': writeToSource,
        'querySource': querySource
      });
    }).catch(reject);
  });
};

module.exports = createSource;
