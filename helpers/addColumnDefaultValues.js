module.exports = function (row, columns) {
  var newRow = {};
  columns.forEach(function (column) {
    if (row[column.name] !== undefined) {
      newRow[column.name] = row[column.name];
    } else if (column.defaultValue === undefined) {
      newRow[column.name] = null;
    } else {
      newRow[column.name] = column.defaultValue;
    }
  });
  return newRow;
};
