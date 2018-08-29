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
    'type': 'esri-rest',
    // 'type': 'arcgis',
    'url': 'https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/BLCACURE_Overlooks/FeatureServer/0'
  }
};

example(testSource);
