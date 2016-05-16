// Since we only get the updates and removes sent back to us
// And sometimes we can't just run an update or delete on a database
// And we really just need to rewrite the thing from scratch
// So this loads the JSON into a sqlite database, and runs the updates and deletes
// And returns the full data object
var Promise = require('bluebird');
var guid = require('jm-tools').guid;
var jsonSource = require('../');

module.exports = function (fullData, updated, removed, columns) {
  var tempSource = {
    'name': guid(),
    'connection': {
      'data': fullData,
      'type': 'json'
    },
    'columns': columns
  };

  return jsonSource(tempSource).then(function (source) {
    var tasks = [];
    removed.forEach(function (row) {
      tasks.push(source.modify.remove(row));
    });
    updated.forEach(function (row) {
      tasks.push(source.modify.update(row));
    });
    return Promise.all(tasks).then(function () {
      return source.cache.selectAll().then(function (allData) {
        return source.close().then(function () {
          return allData;
        });
      });
    });
  });
};
