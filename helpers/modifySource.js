var Promise = require('bluebird');
var columnsToKeys = require('../helpers/columnsToKeys');
var CreateQueries = require('../helpers/createQueries');
var addColumnDefaultValues = require('../helpers/addColumnDefaultValues');
var tools = require('jm-tools');

module.exports = function (database, columns) {
  var keys = columnsToKeys(columns);

  // Create the object we will use to create SQLite Queries
  var createQueries = new CreateQueries(columns, keys.primaryKeys, keys.lastUpdatedField);

  return {
    'update': function (row, remove) {
      // Runs updates and deletes on the internal database
      // Automatically adds in default values if they were not specified
      var query = createQueries('run' + (remove ? 'Remove' : 'Update'))[0];
      var completeRow = addColumnDefaultValues(row, columns);
      return database.query.apply(this, createQueries('cleanUpdate', row, keys.primaryKeys)).then(function () {
        return database.query.apply(this, createQueries('cleanRemove', row, keys.primaryKeys));
      }).then(function () {
        return database.query(query, completeRow).then(function () {
          return tools.dummyPromise(tools.setProperty(remove ? 'remove' : 'update', completeRow));
        });
      });
    },
    'refresh': function (updates, removes) {
      // Updates the cache object to match the updates on save
      return Promise.all(updates.concat(removes).map(function (row) {
        // Remove from ALL tables
        return Promise.all([
          database.query.apply(this, createQueries('remove', row, keys.primaryKeys, 'cached')),
          database.query.apply(this, createQueries('remove', row, keys.primaryKeys, 'updated')),
          database.query.apply(this, createQueries('remove', row, keys.primaryKeys, 'removed'))
        ]);
      })).then(function () {
        // Add data to the cached table
        return Promise.all(updates.map(function (row) {
          return database.query(createQueries('insert', undefined, columns, 'cached')[0], row);
        }));
      });
    }
  };
};
