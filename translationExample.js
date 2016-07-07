var translator = require('places-sync-translator');
var example = module.exports = function (source, type) {
  source.connection.translationType = source.connection.translationType || type;
  return translator(source, type).then(function (translatedGeoJson) {
    return translatedGeoJson;
  }).catch(function (err) {
    throw err;
  });
};

var testSource = {
  'name': 'Generic ArcGIS',
  'connection': {
    'type': 'arcgis',
    'url': 'http://services1.arcgis.com/fBc8EJBxQRMcHlei/ArcGIS/rest/services/BLCACURE/FeatureServer/13'
  },
  'fields': {
    'primaryKey': 'GlobalID'
  }
};

example(testSource, 'parkinglots').then(function (r) {
  console.log('done');
  console.log(JSON.stringify(JSON.parse(r), null, 2));
});
