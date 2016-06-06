var Promise = require('bluebird');
var template = require('./_template.js');
// var tools = require('jm-tools')
var superagent = require('superagent');
var OAuth = require('oauth').OAuth;
var columnsFromConfig = require('../helpers/columnsFromConfig');
require('superagent-oauth')(superagent);

var directReturn = function (_) {
  return _;
};

var createOauth = function (connection) {
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

var load = function (connection, columns) {
  var tasks = [{
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
    'task': columnsFromConfig,
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

var WriteFn = function (databaseConnection, connectionConfig, columns) {
  return function () {
    return new Promise(function (resolve, reject) {
      // TODO, write stuff
      resolve();
    });
  };
};

// Prepare for the next release of places-sync-sources
var osmSource = {
  'QuerySource': undefined,
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
    'translation': 'objectData',
    'fields': 'objectNoData'
  },
  'load': load
};

module.exports = function (config) {
  return template(config, osmSource);
};
