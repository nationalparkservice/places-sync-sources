var Promise = require('bluebird');
var template = require('./_template.js');
var tools = require('jm-tools');
var sources = tools.requireDirectory(__dirname + '/', [__filename, '_template.js', 'index.js']);
var superagent = require('superagent');
var OAuth = require('oauth').OAuth;
var columnsFromConfig = require('../helpers/columnsFromConfig');
require('superagent-oauth')(superagent);

var directReturn = function (_) {
  return _;
};

var parseColumns = function (inColumns) {
  console.log('INCOLUMZ', inColumns);
  return columnsFromConfig(inColumns);
  /*
  return columnsFromConfig([{
    'name': 'key',
    'type': 'text',
    'sqliteType': 'text',
    'defaultValue': '',
    'notNull': true,
    'sqliteColumnId': 0,
    'primaryKey': 1
  }, {
    'name': 'members',
    'type': 'text',
    'sqliteType': 'text',
    'defaultValue': '',
    'notNull': true,
    'sqliteColumnId': 2,
    'primaryKey': 0
  }, {
    'name': 'foreign_key',
    'type': 'text',
    'sqliteType': 'text',
    'defaultValue': '',
    'notNull': true,
    'sqliteColumnId': 3,
    'primaryKey': 0
  }, {
    'name': 'source_tags',
    'type': 'text',
    'sqliteType': 'text',
    'defaultValue': '',
    'notNull': true,
    'sqliteColumnId': 4,
    'primaryKey': 0
  }, {
    'name': 'last_update',
    'type': 'NUMERIC',
    'sqliteType': 'NUMERIC',
    'defaultValue': 0,
    'notNull': true,
    'sqliteColumnId': 5,
    'primaryKey': 0
  }]);
  */
};

var createOauth = function (connection) {
  console.log('***', connection, '***');
  return new OAuth(
    'http://' + connection.address + 'oauth/request_token',
    'http://' + connection.address + 'oauth/access_token',
    connection.consumer_key,
    connection.consumer_secret,
    '1.0',
    null,
    'HMAC-SHA1');
};

var validateUser = function (connection, oauth) {
  return new Promise(function (resolve, reject) {
    superagent.get(connection.address + '0.6/user/details.json')
      .sign(oauth, connection.access_key, connection.access_secret)
      .end(function (err, res) {
        if (!err && res.status === 200) {
          resolve(res.body);
        } else {
          reject(new Error(err));
        }
      });
  });
};

var createCache = function (cacheConfig) {
  return sources[cacheConfig.type](cacheConfig).then(function (source) {});
};

var loadCache = function (cacheConnectionConfig) {
  return sources[cacheConnectionConfig];
};

var load = function (connection, columns) {
  tasks = [{
    'name': 'OAuth',
    'description': 'Creates the oauth object',
    'task': createOauth,
    'params': [connection]
  }, {
    'name': 'userInfo',
    'description': 'Tests the OAuth by getting the user info',
    'task': validateUser,
    'params': [connection, '{{OAuth}}']
  }, {
    'name': 'columns',
    'description': 'columns',
    'task': parseColumns,
    'params': [columns]
  }, {
    'name': 'data',
    'description': 'data',
    'task': directReturn,
    'params': [
      []
    ]
  }];
  return tasks;
};

var QuerySource = function (databaseConnection) {
  return function (type, whereObj, returnColumns) {
    returnColumns = returnColumns || columns;
    var keys = columnsToKeys(returnColumns);
    var createQueries = new CreateQueries(columns, keys.primaryKeys, keys.lastUpdatedField, keys.removedField);
  // / TODO: ?
  };
};

var WriteFn = function (databaseConnection, connectionConfig, columns) {};

// Prepare for the next release of places-sync-sources
var osmSource = {
  'QuerySource': QuerySource,
  'WriteFn': WriteFn,
  'defaultFields': {},
  'requiredFields': {
    'connection': {
      'address': 'string',
      'consumer_key': 'string',
      'consumer_secret': 'string',
      'access_key': 'string',
      'access_secret': 'string'
    },
    'columns': 'objectData',
    'fields': 'objectNoData'
  },
  'load': load
};

module.exports = function (config) {
  return template(config, osmSource);
};
