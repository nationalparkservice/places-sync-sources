var simplifyArray = require('jm-tools').simplifyArray;

module.exports = function (options, columns) {
  if (options && options.transforms) {
    for (var transform in options.transforms) {
      var columnIdx = simplifyArray(columns).indexOf(transform);
      if (columnIdx > -1) {
        columns[columnIdx].transformed = true;
      }
    }
  }
  return columns;
};
