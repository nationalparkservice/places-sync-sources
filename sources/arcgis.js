/* ArcGIS Source:
 */

var Immutable = require('immutable');
var Promise = require('bluebird');
var columnsFromConfig = require('../helpers/columnsFromConfig');
var superagent = require('superagent');
var terraformer = require('terraformer-arcgis-parser');
var tools = require('jm-tools');
var fandlebars = require('fandlebars');
var columnsToKeys = require('../helpers/columnsToKeys');
var CreateQueries = require('../helpers/createQueries');

var geojsonToRows = function (geojson) {
  var features = geojson.features;
  var property;
  var rows = features.map(function (feature) {
    var properties = {};
    for (property in feature.properties) {
      properties[property] = feature.properties[property];
    }
    properties['geometry'] = JSON.stringify(feature.geometry);
    return properties;
  });
  return rows;
};

var esriToGeoJson = function (esriJson, options) {
  // This accepts a full esriJson object from an esri endpoint
  // the options are defined here: http://terraformer.io/arcgis-parser/#arcgisparse

  // Create a skeleton geojson obj
  var geojson = {
    'type': 'FeatureCollection'
  };

  // If the sr is specified, add it in
  if (options.sr) {
    geojson.crs = {
      'type': 'name',
      'properties': {
        'name': 'EPSG:' + options.sr
      }
    };
  }

  var createFeature = function (esriFeature, esriOptions) {
    return {
      'type': 'Feature',
      'id': esriFeature[esriOptions.idAttribute || 'OBJECTID'],
      'geometry': terraformer.parse(esriFeature.geometry, esriOptions),
      'properties': esriFeature.attributes
    };
  };

  geojson.features = esriJson.features.map(function (feature) {
    return createFeature(feature, options);
  });

  return geojson;
};

var arcgisWhereObj = function (whereObj, dateColumns) {
  // esri dates come in as a unix timestamp in milliseconds, but go out as date strings
  var timestampToDate = function (ts) {
    var dt = new Date(ts);
    return (dt.getUTCMonth() + 1) + '/' + dt.getUTCDate() + '/' + dt.getUTCFullYear() + ' ' + dt.getUTCHours() + ':' + dt.getUTCHours() + ':' + dt.getUTCMinutes() + '.' + dt.getUTCMilliseconds();
  };
  var newWhereObj = {};
  for (var field in whereObj) {
    if (dateColumns.indexOf(field) > -1 && typeof whereObj[field] !== 'object') {
      newWhereObj[field] = "'" + timestampToDate(whereObj[field]) + "'";
    } else {
      newWhereObj[field] = "'" + whereObj[field] + "'";
    }
  }
  return newWhereObj;
};

var runQuery = function (sourceUrl, queryObj, primaryKeys) {
  var getCount = {
    'where': queryObj.where,
    'returnCountOnly': 'true',
    'f': 'json'
  };

  return postAsync(sourceUrl + 'query', getCount).then(function (countResult) {
    if (!countResult.error) {
      var requests = [];
      var count = countResult.count;
      for (var i = 0; i < count; i += queryObj.resultRecordCount) {
        if (queryObj.resultOffset !== undefined) {
          queryObj.resultOffset = i;
        }
        requests.push([sourceUrl + 'query', queryObj]);
      }
      return tools.iterateTasks(requests.map(function (req) {
        return {
          'name': 'Query ' + req,
          'task': postAsync,
          'params': req
        };
      })).then(function (baseResults) {
        baseResults = Array.isArray(baseResults) ? baseResults : [];
        return new Promise(function (resolve, reject) {
          var esriJson = JSON.parse(JSON.stringify(baseResults[0] || {}));
          var outputJson = [];
          var geoJson;
          var hasGeometries = false;
          var sr = (esriJson.spatialReference && (esriJson.spatialReference.latestWkid || esriJson.spatialReference.wkid)) || queryObj.outSR;

          esriJson.features = [];
          baseResults.forEach(function (baseResult) {
            baseResult.features.forEach(function (row) {
              if (row.geometry !== undefined) {
                hasGeometries = true;
              }
              esriJson.features.push(row);
            });
          });

          if (hasGeometries) {
            geoJson = esriToGeoJson(esriJson, {
              'sr': sr,
              'idAttribute': primaryKeys[0]
            });

            // Really make sure we put the right spatial reference here
            geoJson.crs = geoJson.crs || {
              'type': 'name',
              'properties': {
                'name': 'EPSG:' + sr
              }
            };
            geoJson.crs.properties = geoJson.crs.properties || {
              'name': 'EPSG:' + sr
            };
            // TODO, code to reproject this?
            // https://github.com/nationalparkservice/places-sync/blob/898499f44c14f4b7a34d88f5333d2026f1a64361/src/transformers/geojson.js
            outputJson = geojsonToRows(geoJson);
          } else {
            outputJson = esriJson.features.map(function (v) {
              return v.attributes;
            });
          }

          console.log('outputJson', outputJson);
          resolve(outputJson);
        });
      });
    } else {
      return new Promise(function (resolve, reject) {
        reject(new Error(countResult.error.code + ' ' + countResult.error.message + ' ' + countResult.error.details));
      });
    }
  });
};

var QuerySource = function (connectionString, sourceInfo, baseFilter, columns, fields) {
  return function (type, whereObj, returnColumns) {
    // If there's a where object already defined in the source, we need to merge them
    // TODO: mapped fields
    // var newWhereObj = mapFields.data.from([tools.mergeObjects(baseFilter || {}, whereObj)],fields.mapped)[0]
    var newWhereObj = tools.mergeObjects(baseFilter || {}, whereObj);

    // Define the columns we're going to return to the user
    // TODO: mapped fields
    // returnColumns = mapFields.columns.from(returnColumns || columns, fields.mapped)
    returnColumns = returnColumns || columns;

    // Add the columns we're querying, if they exist
    for (var k in newWhereObj) {
      if (returnColumns.indexOf(k) < 0 && tools.simplifyArray(columns).indexOf(k) >= 0) {
        returnColumns.push(k);
      }
    }

    // Filter out the columns that don't have a geometry datatype
    var columnsNoGeometry = returnColumns.filter(function (c) {
      return c.name !== 'geometry';
    });

    // Determine the primaryKeys
    var keys = columnsToKeys(columns);

    // Determine the columns that are dates
    var dateColumns = tools.simplifyArray(columns.filter(function (c) {
      return c.type === 'esriFieldTypeDate';
    }));

    // Update the newWhereObj with some special stuff to deal with dates in AGOL
    newWhereObj = arcgisWhereObj(newWhereObj, dateColumns);

    // Create the query making object
    var createQueries = new CreateQueries(columns, keys.primaryKeys, keys.lastUpdatedField, keys.removedField);

    // Create a query (we will parse it later)
    var preQuery = createQueries(type, newWhereObj, columnsNoGeometry, 'ESRI');

    // TODO: ArcGIS returns its dates with millisecond precision, but queries them with more precision

    var query = {
      'where': preQuery[0].split('WHERE')[1] || '1=1',
      'outFields': tools.simplifyArray(columnsNoGeometry).join(','),
      'returnGeometry': tools.simplifyArray(returnColumns).indexOf('geometry') > -1,
      'outSR': '4326',
      'f': 'json'
    };

    // Make sure we can do pagination for arcgis queries, if so, then set the offsets
    if (sourceInfo.advancedQueryCapabilities && sourceInfo.advancedQueryCapabilities.supportsPagination === true) {
      query.resultOffset = '0';
      query.resultRecordCount = sourceInfo.maxRecordCount;
    }

    // Replaces the values in the where clause with the fields since arcgis won't do a parameterized query
    query.where = fandlebars(query.where, preQuery[1]);

    return runQuery(connectionString.url, query, keys.primaryKeys).then(function (result) {
      // TODO Map Fields
      return result;
    // return mapFields.data.to(result, fields.mapped)
    });
  };
};

var copyValues = function (values, baseObj) {
  var returnValue = {};
  values.forEach(function (value) {
    if (baseObj[value] !== undefined) {
      returnValue[value] = JSON.parse(JSON.stringify(baseObj[value]));
    }
  });
  return returnValue;
};
var postAsync = function (url, query) {
  return new Promise(function (resolve, reject) {
    superagent.post(url)
      .set('Accept', 'application/json')
      .send(superagent.serialize['application/x-www-form-urlencoded'](query))
      .end(function (err, res) {
        var body;
        try {
          body = JSON.parse(res.text);
        } catch (e) {
          err = err || e;
        }
        if (err) {
          reject(err);
        } else {
          resolve(body);
        }
      });
  });
};

var getSqliteType = function (esriType) {
  var returnValue = 'BLOB';
  var types = {
    'INTEGER': ['esriFieldTypeOID', 'esriFieldTypeSmallInteger', 'esriFieldTypeDate', 'esriFieldTypeInteger'],
    'REAL': ['esriFieldTypeDouble', 'esriFieldTypeSingle'],
    'TEXT': ['esriFieldTypeString', 'esriFieldTypeGlobalID', 'esriFieldTypeBlob', 'esriFieldTypeRaster', 'esriFieldTypeGUID', 'esriFieldTypeXML']
  };
  for (var type in types) {
    if (types[type].indexOf(esriType) > -1) {
      returnValue = type;
      break;
    }
  }
  return returnValue;
};

module.exports = function (sourceConfig) {
  var connectionConfig = Immutable.Map(sourceConfig.connection);

  if (typeof connectionConfig.get('url') !== 'string') {
    throw new Error('url must be defined for a ArcGIS file');
  }

  // The regexp adds a trailing slash if there isn't already one
  connectionConfig = connectionConfig.set('url', connectionConfig.get('url').replace(/^(.+?)\/?$/g, '$1/'));

  return postAsync(connectionConfig.get('url'), {
    'f': 'json'
  }).then(function (source) {
    return new Promise(function (resolve, reject) {
      // From this source, we just need to get the column info
      var esriColumns = source.fields.map(function (column, i) {
        return {
          'name': column.name,
          'type': column.type,
          'sqliteType': getSqliteType(column.type),
          // 'primaryKey':' // These will need to be added in the config
          'defaultValue': column.defaultValue,
          'notNull': column.nullable === false,
          'sqliteColumnId': i
        };
      });

      // There is also a geometry, which ESRI treats differently than a normal column
      // Bit we treat it as a column
      esriColumns.unshift({
        'name': 'geometry',
        'type': 'geometry',
        'sqliteType': 'BLOB',
        'defaultValue': '',
        'notNull': true,
        'sqliteColumnId': -1
      });

      var sourceInfo = copyValues(['id', 'objectIdField', 'name', 'editFieldsInfo', 'editingInfo', 'maxRecordCount', 'advancedQueryCapabilities'], source);

      // Get defaults from ArcGIS
      var sourceInfoFields = {
        'primaryKey': sourceInfo.objectIdField,
        'lastUpdated': sourceInfo.editFieldsInfo && sourceInfo.editDateField
      };

      // If there is anything in the config, use those fields instead
      for (var field in sourceConfig.fields) {
        sourceInfoFields[field] = sourceConfig.fields[field];
      }

      var columns = columnsFromConfig(esriColumns, sourceInfoFields, true);

      resolve({
        'data': undefined,
        'columns': columns,
        'writeFn': undefined, // TODO allow writing
        'sourceInfo': sourceInfo,
        'querySource': new QuerySource(connectionConfig.toJS(), sourceInfo, sourceConfig.filter, columns, sourceConfig.fields)
      });
    });
  });
};