var notFalsey = function (v, name, ignoreCase) {
  v = Array.isArray(v) ? v : [v];

  // If we're ignoring case, make an array of everything in lower case
  var lowerV = ignoreCase ? v.map(function (a) {
    return a.toLowerCase ? a.toLowerCase() : a;
  }) : v;
  var idx = lowerV.indexOf(ignoreCase ? name.toLowerCase() : name);
  return idx > -1 ? idx : false;
};

var fieldMap = {
  'primaryKey': {
    'name': 'primaryKey',
    'process': notFalsey
  },
  'foreignKey': {
    'name': 'foreignKey',
    'process': notFalsey
  },
  'lastUpdated': {
    'name': 'lastUpdated',
    'process': notFalsey
  },
  'removed': {
    'name': 'removed',
    'process': notFalsey
  },
  'removedValue': {
    'name': 'removedValue',
    'process': function () {
      return undefined;
    }
  },
  'hash': {
    'name': 'hash',
    'process': notFalsey
  },
  'data': {
    'name': 'data',
    'process': notFalsey
  },
  'forced': {
    'name': 'forced',
    'process': notFalsey
  }
};

module.exports = function (columns, fields, ignoreCase) {
  // Copy the objects
  columns = JSON.parse(JSON.stringify(columns));
  fields = fields ? JSON.parse(JSON.stringify(fields)) : {};

  // Create a new object for the columns
  var newColumns = columns.map(function (column) {
    for (var option in fields) {
      if (column[option] === undefined && fieldMap[option]) {
        // If we don't already have this option, and we have a converter for it
        // we can use it
        column[option] = fieldMap[option].process(fields[fieldMap[option].name], column.name, ignoreCase);
      }
      if (option === 'primaryKey' && (!column[option] || column[option] === 0) && fieldMap[option] !== undefined) {
        // Primary keys are a special case here, because we allow more to be added, but don't allow any to be removed
        // Basically, if it's primary to someone, then it should be primary to everyone
        // Otherwise you could get data constancy errors
        column[option] = fieldMap[option].process(fields[fieldMap[option].name], column.name, ignoreCase);
      }
      if (column.removedField === null || column.removedField === undefined) {
        column.removedValue = fields.removedValue;
      }
    }
    return column;
  });
  return newColumns;
};
