/* GeoJSON File:
 *   Basically just a JSON file, but in a geo format
 *   *filePath: (path the the json file)
 *   encoding: (defaults to 'UTF8')
 */

var Promise = require('bluebird');
var Immutable = require('immutable');
var jsonSource = require('./json');
var tools = require('jm-tools');
var fs = Promise.promisifyAll(require('fs'));
var stringify = require('json-stringify-pretty-compact');

var WriteFn = function (jsonWriteFn, filePath, fileEncoding) {
  return function () {
    return jsonWriteFn.apply(this, arguments).then(function (results) {
      return writeGeojson(results.data, filePath, fileEncoding).then(function () {
        return new Promise(function (fulfill) {
          var returnValue = {
            'updated': results.updated,
            'removed': results.removed
          };
          fulfill(returnValue);
        });
      });
    });
  };
};

var writeGeojson = function (data, filePath, fileEncoding) {
  return fs.writeFileAsync(filePath, stringify(jsonToGeojson(data)), fileEncoding);
};

var readJson = function (data) {
  // Attempt to parse it if it's a string
  if (typeof data === 'string') {
    try {
      data = JSON.parse(data);
    } catch (e) {
      if (data.length < 2) {
        data = [];
      } else {
        console.log(data, data.length);
        throw new Error('Data is not valid GeoJSON');
      }
    }
  }
  return data;
};

var readGeojson = function (data) {
  return geojsonToJson(readJson(data));
};

var geojsonToJson = function (geojson) {
  // Only supports a single feature collection
  var features = geojson && geojson.features && geojson.features.map(function (feature) {
    var newRow = feature.properties;
    newRow.geometry = JSON.stringify(feature.geometry);
    return newRow;
  });
  return features || [];
};

var jsonToGeojson = function (json) {
  // TODO, projection information?
  json = readJson(json);
  var geojsonObj = {
    'type': 'FeatureCollection',
    'crs': {
      'type': 'name',
      'properties': {
        'name': 'EPSG:4326'
      }
    },
    'features': []
  };
  geojsonObj.features = json.map(function (row) {
    var geoColumnName = 'geometry';
    var feature = {
      'type': 'Feature',
      'geometry': JSON.parse(row[geoColumnName]),
      'properties': JSON.parse(JSON.stringify(row))
    };
    delete feature.properties[geoColumnName];
    return feature;
  });
  return geojsonObj;
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

    var jsonConnectionConfig = connectionConfig.set('data', undefined);
    jsonConnectionConfig = jsonConnectionConfig.set('filePath', ':memory:');
    jsonConnectionConfig = jsonConnectionConfig.toJS();
    var jsonSourceConfig = new Immutable.Map(sourceConfig);
    jsonSourceConfig = jsonSourceConfig.set('connection', undefined);
    jsonSourceConfig = jsonSourceConfig.toJS();

    // Define the taskList
    var tasks = [{
      'name': 'openFile',
      'description': 'Does a few checks on opens the file if it can',
      'task': connectionConfig.get('data') === undefined ? fs.readFileAsync : returnData,
      'params': [connectionConfig.get('filePath'), connectionConfig.get('encoding')]
    }, {
      'name': 'convertedFile',
      'description': 'Tries to convert the file from geojson to json',
      'task': readGeojson,
      'params': ['{{openFile}}']
    }, {
      'name': 'jsonConnectionConfig',
      'description': 'Add the json data to the new config',
      'task': tools.setProperty,
      'params': ['data', '{{convertedFile}}', jsonConnectionConfig]
    }, {
      'name': 'jsonSourceConfig',
      'description': 'Adds the connection config to the source config',
      'task': tools.setProperty,
      'params': ['connection', '{{jsonConnectionConfig}}', jsonSourceConfig]
    }, {
      'name': 'jsonSource',
      'description': 'Loads the source as JSON',
      'task': jsonSource,
      'params': ['{{jsonSourceConfig}}']
    }];
    tools.iterateTasks(tasks).then(function (results) {
      var jsonSourceObj = results.jsonSource;
      fulfill({
        'data': jsonSourceObj.data,
        'columns': jsonSourceObj.columns,
        'writeFn': new WriteFn(jsonSourceObj.writeFn, connectionConfig.get('filePath'), connectionConfig.get('fileEncoding')),
        'querySource': jsonSourceObj.querySource
      });
    }).catch(reject);
  });
};
