module.exports = function (primaryKeys, row) {
  var rowPrimaryKeys = {};
  row = row || {};
  primaryKeys.forEach(function (pk) {
    rowPrimaryKeys[pk] = row[pk];
  });
  return rowPrimaryKeys;
};
