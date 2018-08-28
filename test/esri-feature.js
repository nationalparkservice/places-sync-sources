var sources = require('../');
var arcGisSource = {
  'name': 'ArcGIS Test Source',
  'connection': {
    'type': 'esri-feature',
    'url': 'https://mapservices.nps.gov/arcgis/rest/services/NPS_Public_POIs/FeatureServer/',
    'layer_id': '0',
    'layer_name': 'NPS_Public_POIs'
  }
};
var filters = [{
  'SEASONAL': 'Yes'
}, {
  'SEASONAL': {
    '$eq': 'No'
  }
}, {
  'SEASONAL': {
    '$ne': 'No'
  }
}];

var filterTests = ['>=', '<=', '>='];

var mainTask = [{
  'name': 'loadArcGisSource',
  'task': sources,
  'params': [arcGisSource],
  'operator': 'structureEqual',
  'expected': {
    'cache': {},
    'get': {
      'name': arcGisSource.name
    },
    modify: {}
  }
}, {
  'name': 'getDataA',
  'task': '{{loadArcGisSource.get.all}}',
  'params': [],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'getCount',
  'task': function (_) {
    return _;
  },
  'params': ['{{getDataA.length}}'],
  'operator': 'jstype',
  'expected': 'number'
}];

filters.forEach(function (filter, i) {
  var tasks = [{
    'name': 'filteredConnection' + i,
    'description': 'Add the filter to the connection',
    'task': function (s, f) {
      var newSource = JSON.parse(JSON.stringify(s));
      newSource.filter = f;
      return newSource;
    },
    'params': [arcGisSource, filters[i]],
    'operator': 'structureEqual',
    'expected': arcGisSource
  }, {
    'name': 'loadTest' + i,
    'description': 'Loads the source with the filter',
    'task': sources,
    'params': ['{{filteredConnection' + i + '}}'],
    'operator': 'structureEqual',
    'expected': {
      'cache': {},
      'get': {
        'name': arcGisSource.name
      },
      modify: {}
    }
  }, {
    'name': 'getDataTest' + i,
    'task': '{{loadTest' + i + '.get.all}}',
    'params': [],
    'operator': 'jstype',
    'expected': 'array'
  }, {
    'name': 'getCount' + i,
    'task': function (_) {
      return _;
    },
    'params': ['{{getDataTest' + i + '.length}}'],
    'operator': 'jstype',
    'expected': 'number'
  }, {
    'name': 'compareCount',
    'task': function (a, b, operator) {
      var tests = {
        '>=': function (a, b) {
          return a >= b;
        },
        '<=': function (a, b) {
          return a <= b;
        },
        '!=': function (a, b) {
          return a !== b;
        },
        '>': function (a, b) {
          return a > b;
        },
        '<': function (a, b) {
          return a < b;
        },
        '=': function (a, b) {
          return a === b;
        }
      };
      return tests[operator](a, b);
    },
    'params': ['{{getCount' + i + '}}', '{{getCount}}', filterTests[i]],
    'operator': 'equal',
    'expected': true
  }];
  tasks.forEach(function (task) {
    mainTask.push(task);
  });
});

module.exports = mainTask;
