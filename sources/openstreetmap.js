var Promise = require('bluebird');
var template = require('./_template.js');
var tools = require('jm-tools');
var translator;
var columnsToKeys = require('../helpers/columnsToKeys');
var superagent = require('superagent');
var OAuth = require('oauth').OAuth;
var columnsFromConfig = require('../helpers/columnsFromConfig');
var osmSubmit = require('openstreetmap-submit');
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

var createSource = function (data, translationType, columns) {
  var sourceTemplate = {
    'name': tools.guid(),
    'connection': {
      'data': [],
      'type': 'json',
      'translationType': 'generic'
    },
    'columns': []
  };
  var returnValue = JSON.parse(JSON.stringify(sourceTemplate));
  returnValue.connection.data = data;
  returnValue.connection.translationType = translationType;
  returnValue.columns = columns;
  return returnValue;
};

var matchKeys = function (updated, removed, metadata, keys) {
  // Create the template that this will return
  var changes = {
    'create': [],
    'modify': [],
    'remove': [],
    'possible remove': []
  };

  var cache = {};

  // Add all the updates to the cache as "create"
  updated.forEach(function (record) {
    // Create a key for this updated event
    var key = keys.primaryKeys.map(function (pk) {
      return record[pk];
    }).join(',');
    cache[key] = {
      'type': 'create',
      'data': record
    };
  });

  // If there's a remove and a create for a record, that's really a replace
  // So we only remove what we don't have a create for
  // We mark it as a possible remove, because if it's not in OSM, we don't remove it at all
  removed.forEach(function (record) {
    // Create a key for this updated event
    var key = keys.primaryKeys.map(function (pk) {
      return record[pk];
    }).join(',');
    if (!cache[key]) {
      cache[key] = {
        'type': 'possible remove',
        'data': record
      };
    }
  });

  // Go through the metadata
  // If a create already exists in OSM, it's really a modify
  // If a possible remove exists in OSM, it's really a remove
  metadata.forEach(function (record) {
    var key = keys.primaryKeys.map(function (pk) {
      return record[pk];
    }).join(',');
    if (cache[key]) {
      cache[key].osmId = record.foreignKey;
      cache[key].primaryKey = key;
      if (cache[key].type === 'create') {
        cache[key].type = 'modify';
      } else if (cache[key].type === 'possible remove') {
        cache[key].type = 'remove';
      }
    }
  });

  // Loop through the cache and add the osmid to the records
  Object.keys(cache).forEach(function (key) {
    changes[cache[key].type].push(cache[key].data);
  });

  // We don't need the possible removes, so lets take them out of memory
  delete changes['possible remove'];

  return {
    'changes': changes,
    'cache': cache
  };
};

var WriteFn = function (databaseConnection, connectionConfig, columns, immutableConfig) {
  // Extract the translation type
  var translationType = immutableConfig.translation.toJS().join('');
  var keys = columnsToKeys(columns);

  return function (updated, removed, metadata) {
    // Define the translator only when it's called to prevent circular requires
    translator = translator || require('places-sync-translator');

    // Determine the created/modified/deleted data
    var changesObj = matchKeys(updated, removed, metadata, keys);
    var changes = changesObj.changes;

    // Create the translation tasks for created/modified/deleted data
    var changesKeys = [];
    var tasks = [];
    Object.keys(changes).forEach(function (key) {
      if (changes[key].length) {
        changesKeys.push(key);
        // console.log(createSource(changes[key], translationType, columns))
        tasks.push(translator(createSource(changes[key], translationType, columns)));
      }
    });

    // also throw in a task to create the osm-submit object at the end
    var osmSubmitOptions = {
      'limit': 15
    };

    tasks.push(osmSubmit({
      'connection': connectionConfig
    },
      osmSubmitOptions
    ));

    return Promise.all(tasks).then(function (results) {
      // Map the result items back out to an object to make them easier to deal with
      var translatedObj = {};
      changesKeys.map(function (key, i) {
        // Add the OSM id and version if we have it
        results[i].map(function (result) {
          if (changes.cache[result.foreignKey]) {
            result.osmId = cache[result.foreignKey].osmId;
          }
          return result;
        });
        translatedObj[key] = results[i];
      });
      var submit = results[results.length - 1];
      var dummyGeoJson = {
        'features': []
      };

      console.log('create');
      console.log(translatedObj.create);
      console.log('modify');
      console.log(translatedObj.modify);
      console.log('remove');
      console.log(translatedObj.remove);
      process.exit(0);

      // Run the submit!
      return submit(translatedObj.create || dummyGeoJson, translatedObj.modify || dummyGeoJson, translatedObj.remove || dummyGeoJson).then(function (submitResult) {
        console.log('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^');
        console.log('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^');
        console.log('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^');
        console.log(JSON.stringify(submitResult, null, 2));
        console.log('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^');
        console.log('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^');
        console.log('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^');
        return {
          'updated': updated,
          'removed': removed
        };
      });
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
