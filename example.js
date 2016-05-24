var sources = require('./');

var example = function (source) {
  sources(source).then(function (connection) {
    return connection.get.all();
  }).then(function (arcGisData) {
    console.log(arcGisData);
  }).catch(function (err) {
    throw err;
  });
};

var testSource = {
  'name': 'Generic ArcGIS',
  'connection': {
    'type': 'arcgis',
    'url': 'http://services1.arcgis.com/fBc8EJBxQRMcHlei/ArcGIS/rest/services/BLCACURE/FeatureServer/0'
  }
};

example(testSource);
