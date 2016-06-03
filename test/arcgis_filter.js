var sources = require('../');
var arcGisSource = {
  'name': 'ArcGIS Test Source',
  'connection': {
    'type': 'arcgis',
    'url': 'http://services1.arcgis.com/fBc8EJBxQRMcHlei/ArcGIS/rest/services/BLCACURE/FeatureServer/0'
  }
};
var filters = [{
  'INPLACES': 'Yes'
}, {
  'INPLACES': {
    '$eq': 'No'
  }
}, {
  'INPLACES': {
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
  'name': 'getColumns',
  'task': '{{loadArcGisSource.get.columns}}',
  'params': [],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'getDataA',
  'task': '{{loadArcGisSource.get.all}}',
  'params': [],
  'operator': 'jstype',
  'expected': 'array'
}, {
  'name': 'getCount',
  'task': function(_) {
    return _;
  },
  'params': ['{{getDataA.length}}'],
  'operator': 'jstype',
  'expected': 'number'
}];

filters.forEach(function(filter, i) {
  var tasks = [{
    'name': 'filteredConnection' + i,
    'description': 'Add the filter to the connection',
    'task': function(s, f) {
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
    'task': function(_) {
      return _;
    },
    'params': ['{{getDataTest' + i + '.length}}'],
    'operator': 'jstype',
    'expected': 'number'
  }, {
    'name': 'compareCount',
    'task': function(a, b, operator) {
      var tests = {
        '>=': function(a, b) {
          return a >= b;
        },
        '<=': function(a, b) {
          return a <= b;
        },
        '!=': function(a, b) {
          return a !== b;
        },
        '>': function(a, b) {
          return a > b;
        },
        '<': function(a, b) {
          return a < b;
        },
        '=': function(a, b) {
          return a === b;
        }
      };
      return tests[operator](a, b);
    },
    'params': ['{{getCount' + i + '}}', '{{getCount}}', filterTests[i]],
    'operator': 'equal',
    'expected': true
  }];
  tasks.forEach(function(task) {
    mainTask.push(task);
  });
});

module.exports = mainTask;
