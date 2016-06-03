// A very basic way to run queries in various source formats
// Connections are mostly defined by the source types
// connection.type IS required by this step

var Promise = require('bluebird');
var jsonToSqlite = require('./helpers/jsonToSqlite');
var tools = require('jm-tools');
var CreateActions = require('./helpers/createActions');

module.exports = function (sourceConfig, masterCache) {
  var sourceType = sourceConfig.connection && sourceConfig.connection.type;
  var sources = tools.requireDirectory(__dirname + '/sources', [__filename], sourceType === 'json' ? ['json.js'] : undefined);
  var source = sources[sourceType];

  // TODO: Compare source permissions
  if (!source) {
    return tools.dummyPromise(undefined, 'Invalid Source type specified in connection: ' + (sourceConfig.connection && sourceConfig.connection.type));
  } else if (!sourceConfig.name) {
    return tools.dummyPromise(undefined, 'All sources must have a name\n\t' + JSON.stringify(sourceConfig, null, 2));
  } else {
    return new Promise(function (fulfill, reject) {
      var taskList = [{
        'name': 'dataToJson',
        'description': 'Converts the source into a JSON format',
        'task': sources[sourceConfig.connection.type],
        'params': [sourceConfig]
      }, {
        'name': 'Create Database',
        'description': 'Creates a database from the JSON representation of the data and the columns',
        'task': function (d, c) {
          if (d && d.query) {
            return d;
          } else {
            return jsonToSqlite(d, c);
          }
        },
        'params': ['{{dataToJson.data}}', '{{dataToJson.columns}}']
      }];

      tools.iterateTasks(taskList, 'create source ' + sourceConfig.name, false).then(function (r) {
        var database = tools.arrayGetLast(r);
        var columns = r[0].columns; // TODO, should we use the columns from the db (r[1]) instead?
        var writeToSource = r[0].writeFn;
        var querySource = r[0].querySource;
        fulfill(new CreateActions(database, columns, writeToSource, querySource, masterCache, sourceConfig));
      }).catch(reject);
    });
  }
};
