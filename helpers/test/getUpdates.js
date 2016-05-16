var tools = require('../../../tools');
var getUpdates = require('../getUpdates');

var lastSyncTime = 100; // 1458000000000 + Math.round(Math.random() * 1000000000);
var sourceColumns = {
  'all': ['key', 'last_updated'],
  'primaryKeys': ['key'],
  'lastUpdatedField': 'last_updated',
  'removed': 'removed'
};

var updatedSinceTime = [{
  'key': 11,
  'last_updated': 100
}, {
  'key': 12,
  'last_updated': 101
}, {
  'key': 13,
  'last_updated': 102
}, {
  'key': 14,
  'last_updated': 103
}];
var allKeys = [{
  'key': 1,
  'last_updated': 10
}, {
  'key': 2,
  'last_updated': 11
}, {
  'key': 3,
  'last_updated': 12
}, {
  'key': 4,
  'last_updated': 13
}, {
  'key': 11,
  'last_updated': 100
}, {
  'key': 12,
  'last_updated': 101
}, {
  'key': 13,
  'last_updated': 102
}, {
  'key': 14,
  'last_updated': 103
}];

var allMasterKeys = [{
  'key': 1,
  'last_updated': 10,
  'hash': '1,10'
}, {
  'key': 3,
  'last_updated': 12,
  'hash': '3,12'

}, {
  'key': 4,
  'last_updated': 13,
  'hash': '4,13'
}];

getUpdates(lastSyncTime, updatedSinceTime, allKeys, allMasterKeys, sourceColumns).then(function (r) {
  console.log(r);
});
