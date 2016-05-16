var tools = require('jm-tools');
var valueExists = function (v) {
  // We use index of 0 to show that things exist, so we can't use falsey
  return !(v === undefined || v === null || v === false);
};

var mapFields = function (fields, mappings) {
  // This isn't used anywhere
  fields = JSON.parse(JSON.stringify(fields));
  var isArray, newField;
  for (var field in fields) {
    isArray = Array.isArray(fields[field]);
    newField = tools.arrayify(fields[field]).map(function (c) {
      return mappings && mappings[c] !== false ? mappings[c] : c;
    }).filter(function (c) {
      valueExists(c);
    });
    if (!isArray) {
      fields[field] = newField[0];
    } else {
      fields[field] = newField;
    }
  }
  return fields;
};

module.exports = function (columns, mapKeyFields) {
  var returnValue = {
    'all': tools.simplifyArray(columns),
    'primaryKeys': tools.simplifyArray(columns.filter(function (c) {
      return valueExists(c.primaryKey);
    })),
    'foreignKeys': tools.simplifyArray(columns.filter(function (c) {
      return valueExists(c.foreignKey);
    })),
    'notNullFields': tools.simplifyArray(columns.filter(function (c) {
      return valueExists(c.notNull);
    })),
    'lastUpdatedField': tools.simplifyArray(columns.filter(function (c) {
      return valueExists(c.lastUpdated);
    }))[0],
    'removedField': tools.simplifyArray(columns.filter(function (c) {
      return valueExists(c.removed);
    }))[0],
    'forcedField': tools.simplifyArray(columns.filter(function (c) {
      return valueExists(c.forced);
    }))[0],
    'hashField': tools.simplifyArray(columns.filter(function (c) {
      return valueExists(c.hash);
    }))[0],
    'dataField': tools.simplifyArray(columns.filter(function (c) {
      return valueExists(c.data);
    }))[0],
    'removedValue': ((columns.filter(function (c) {
      return valueExists(c.removed);
    })[0]) || {}).removedValue || 'true'
  };
  if (mapKeyFields) {
    returnValue = mapFields(returnValue, returnValue.mapFields);
  }
  return returnValue;
};
