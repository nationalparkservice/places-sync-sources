var translator= require('places-sync-translator');

var testSource = {
  'name': 'Generic ArcGIS',
  'connection': {
    'type': 'arcgis',
    'url': 'http://services1.arcgis.com/fBc8EJBxQRMcHlei/ArcGIS/rest/services/BLCACURE/FeatureServer/0'
  },
  'fields': {
    'primaryKey': 'GlobalID'
  }
};

var example = function (source, type) {
  translator(source).then(function (translatedGeoJson) {
    console.log(translatedGeoJson);
  }).catch(function (err) {
    throw err;
  });
};


example(testSource, 'poi');
