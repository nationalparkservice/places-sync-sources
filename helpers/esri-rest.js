/* ArcGIS Source:
 */

var Promise = require('bluebird');
var superagent = require('superagent');
var terraformer = require('terraformer-arcgis-parser');
var iterateTasksLight = require('jm-tools').iterateTasksLight;
var runList = require('./recursive-tasklist');

var startQuery = function (sourceUrl, origQueryObj, primaryKeys, sourceInfo, options) {
  if (sourceInfo) {
    return runQuery(sourceUrl, origQueryObj, primaryKeys, sourceInfo, options);
  } else {
    return postAsync(sourceUrl, {
      'f': 'json'
    }).then(function (source) {
      var fields = source.fields.map(function (field) {
        return field.name;
      });
      return runQuery(sourceUrl, origQueryObj, primaryKeys || fields, source, options);
    }).catch(function (e) {
      return new Promise(function (f, r) {
        r(e);
      });
    });
  }
};

var runQuery = function (sourceUrl, origQueryObj, primaryKeys, sourceInfo, options) {
  // Returns the ESRI Output JSON
  var taskList = [];
  var results = [];

  var reduceQuery = function (sourceUrl, queryObj, primaryKeys, extent) {
    var newExtents = splitBbox(extent);
    // console.log('-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-');
    // console.log('Splitting: ', extent);
    // console.log('To: ', newExtents);
    // console.log('-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-');
    newExtents.forEach(function (newExtent) {
      taskList.push({
        'name': 'Query ' + JSON.stringify(newExtent),
        'description': 'Partial Extent Query',
        'task': queryServer,
        'params': [sourceUrl, queryObj, primaryKeys, newExtent]
      });
    });
  };

  var splitBbox = function (bbox) {
    // From: https://github.com/openaddresses/esri-dump/blob/master/lib/geometry.js
    var halfWidth = (bbox.xmax - bbox.xmin) / 2.0,
      halfHeight = (bbox.ymax - bbox.ymin) / 2.0;
    return [{
      xmin: bbox.xmin,
      ymin: bbox.ymin,
      ymax: bbox.ymin + halfHeight,
      xmax: bbox.xmin + halfWidth
    },
    {
      xmin: bbox.xmin + halfWidth,
      ymin: bbox.ymin,
      ymax: bbox.ymin + halfHeight,
      xmax: bbox.xmax
    },
    {
      xmin: bbox.xmin,
      ymin: bbox.ymin + halfHeight,
      xmax: bbox.xmin + halfWidth,
      ymax: bbox.ymax
    },
    {
      xmin: bbox.xmin + halfWidth,
      ymin: bbox.ymin + halfHeight,
      xmax: bbox.xmax,
      ymax: bbox.ymax
    }
    ];
  };

  var queryServer = function (sourceUrl, queryObj, primaryKeys, extent) {
    var newQueryObj = JSON.parse(JSON.stringify(queryObj));
    // Add bounding box query
    newQueryObj.inSR = (extent && extent.spatialReference && extent.spatialReference.wkid) || newQueryObj.inSR || null;
    newQueryObj.geometry = extent ? [extent.xmin, extent.ymin, extent.xmax, extent.ymax].join(',') : null;
    newQueryObj.geometryType = 'esriGeometryEnvelope';
    newQueryObj.spatialRel = 'esriSpatialRelIntersects';
    newQueryObj.f = 'json';

    // Check to see if we're going to go over the max limit
    newQueryObj.outFields = primaryKeys ? primaryKeys[0] : '';
    newQueryObj.returnGeometry = false;

    // Make it a query URL if it isn' already one
    sourceUrl = sourceUrl.replace(/query\??$/ig, '');
    sourceUrl = sourceUrl + (sourceUrl.substr(sourceUrl.length - 1) === '/' ? '' : '/') + 'query';

    // Get URL
    // console.log('getting url', sourceUrl, newQueryObj);
    return postAsync(sourceUrl, newQueryObj).then(function (data) {
      return new Promise(function (resolve, reject) {
        // console.log('prequery finished');
        if (data.exceededTransferLimit) {
          // console.log('EXCEEDED', extent);
          reduceQuery(sourceUrl, newQueryObj, primaryKeys, extent);
          resolve(null);
        } else if (data.features && data.features.length === 0) {
          // console.log('NO FEATURES LEFT');
          // No features in this query
          resolve(null);
        } else if (data.features) {
          // console.log('Found Features', data && data.features && data.features.length);
          newQueryObj.outFields = origQueryObj.outFields || '*';
          newQueryObj.returnGeometry = origQueryObj.returnGeometry;
          // TODO: Add a parameter to set a max features that is less than the limit
          // if (data.features.length > 5000) {
          //   reduceQuery(sourceUrl, newQueryObj, primaryKeys, extent);
          //   resolve(null);
          // }
          return postAsync(sourceUrl, newQueryObj).then(function (data) {
            // We got the data!
            if (data && data.error) {
              // console.log('* ERROR getting features **********************************');
              // console.log(data.error);
              // console.log('* ERROR ************************************');
              reduceQuery(sourceUrl, newQueryObj, primaryKeys, extent);
              resolve(null);
            } else {
              resolve(data); // TODO: transform it?
            }
          }).catch(function (e) {
            // There was some kind of error, check it, and maybe split bbox?
            reduceQuery(sourceUrl, newQueryObj, primaryKeys, extent);
            resolve(null);
          });
        } else if (data.error) {
          // console.log('* ERROR with prequery  **********************************');
          // console.log(data.error);
          // console.log('* ERROR ************************************');
          newQueryObj.outFields = origQueryObj.outFields || '*';
          newQueryObj.returnGeometry = origQueryObj.returnGeometry;

          reduceQuery(sourceUrl, newQueryObj, primaryKeys, extent);
          resolve(null);
        } else {
          // Not much else we can do? error? null?
          console.log('???????????????????????????');
          process.exit();
          resolve(null);
        }
      });
    });
  };

  taskList.push({
    'name': 'First Query',
    'description': 'Starts off the query',
    'task': queryServer,
    'params': [sourceUrl, origQueryObj, primaryKeys, sourceInfo.extent]
  });

  return runList(taskList, results).then(function (data) {
    // return iterateTasksLight(taskList, 'Load Tasks').then(function (data) {
    var records = [];
    data.forEach(function (result) {
      var rows = [];
      var esriOptions = {
        'sr': (result && (result.spatialReference.latestWkid || result.spatialReference.wkid)) || null,
        'stringifyGeometry': options.geojsonAsString !== undefined ? options.geojsonAsString : true,
        'asGeoJSON': options.asGeoJSON !== undefined ? options.asGeoJSON : true
      };
      if (result && result.features) {
        rows = result.features.map(function (feature) {
          var geometry = null;
          try {
            geometry = terraformer.parse(esriOptions.asGeoJSON ? feature : feature.geometry, esriOptions);
          } catch (e) {
            console.log('error with geometry', e);
          }
          if (esriOptions.asGeoJSON) {
            return geometry;
          } else {
            feature.attributes = feature.attributes || {};
            feature.attributes.geometry = esriOptions.stringifyGeometry ? JSON.stringify(geometry) : geometry;
            return feature.attributes;
          }
        });
        records = records.concat(rows);
      }
    });
    return records;
  });
};

var postAsync = function (url, query) {
  return new Promise(function (resolve, reject) {
    superagent.post(url)
      .set('Accept', 'application/json')
      .send(superagent.serialize['application/x-www-form-urlencoded'](query))
      .end(function (err, res) {
        var body;
        console.log('2323232323');
        console.log('url', url);
        console.log('query',query);
        console.log(res.text);
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

module.exports = function (url, whereObj, primaryKeys, sourceInfo, options) {
  options = options || {};
  return startQuery(url, whereObj, primaryKeys, sourceInfo, options);
};
