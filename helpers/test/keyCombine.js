var keyCombine = require('../keyCombine');
var iterateTapeTasks = require('../../../tools/iterateTapeTasks');

var data = [{
  'a': 1,
  'b': 2,
  'c': null
}, {
  'a': 'something',
  'b': null,
  'c': 0.013
}, {
  'a': '#$T#Q$FAWAF%T#FYE^UWTDG&GRU%EU',
  'b': 'g',
  'c': 1
}];

for (var i = 0; i < 1000; i++) {
  data.push({
    'a': Math.round(Math.random() * 100000000).toString(36),
    'b': Math.round(Math.random() * 100000000).toString(36),
    'c': Math.round(Math.random() * 100000000).toString(36)
  });
}

var columns = ['a', 'b', 'c'];

createTests = function (columns, data) {
  var tests = [];

  data.forEach(function (record, index) {
    tests.push({
      'name': 'Convert Row ' + index,
      'description': 'Combine the rows into a single value',
      'task': keyCombine,
      'params': [columns, record],
      'operator': 'jstype',
      'expected': 'string'
    });
    tests.push({
      'name': 'Split Row ' + index,
      'description': 'Split the row back into an object',
      'task': keyCombine.split,
      'params': [columns, '{{Convert Row ' + index + '}}'],
      'operator': 'deepEqual',
      'expected': record
    });
  });

  return tests;
};

var tests = createTests(columns, data);

iterateTapeTasks(tests, true, true, true).then(function (results) {
  console.log('done');
}).catch(function (e) {
  console.log('error');
  throw e;
});
