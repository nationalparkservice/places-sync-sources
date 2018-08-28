var tools = require('jm-tools');

module.exports = function (columns, primaryKey, lastUpdatedField, removedField, options) {
  var queryKey = tools.arrayify(primaryKey || tools.simplifyArray(columns));

  var arrayToColumns = function (columns, tableName, quotes, toFrom) {
    quotes = quotes || ['"', '"'];
    quotes[0] = tableName ? quotes[0] + tableName + quotes[1] + '.' + quotes[0] : quotes[0];
    var newColumns = tools.simplifyArray(columns).map(function (column) {
      var newColumn = tools.surroundValues(column, quotes[0], quotes[1]);

      if (options && options.transforms && options.transforms[column] && options.transforms[column][toFrom]) {
        return tools.surroundValues.apply(this, [newColumn].concat(options.transforms[column][toFrom])) + (toFrom === 'from' ? (' AS "' + column + '"') : '');
      } else {
        return newColumn;
      }
    });
    return newColumns.join(', ');
  };

  var arraysToObj = function (keys, values) {
    // Takes two arrays ['a','b','c'], [1,2,3]
    // And makes an object {'a':1,'b':2,'c':3}
    var returnObject = {};
    if (Array.isArray(values)) {
      keys.forEach(function (key, i) {
        returnObject[key] = values[i];
      });
    } else if (typeof values === 'object') {
      returnObject = values;
    }
    return returnObject;
  };

  var createWhereObj = function (keys, values, options) {
    // console.log('keys', keys, 'values', values);
    var valuesObj = arraysToObj(keys, values);
    // console.log(valuesObj)
    var whereObj = {};

    // If nothing is specified for a value, the default is (null or not null)
    var defaultWhere = (options && options.defaultWhere !== undefined) ?
      options.defaultWhere : {
      '$or': [{
        '$eq': null
      }, {
        '$ne': null
      }]
    };

    // Add the default value where nothing else is
    keys.forEach(function (pk) {
      whereObj[pk] = valuesObj[pk] || defaultWhere;
    });

    // Add any other requests
    // This is only here for the where clause stuff, which is poorly supported
    // for (var idx in valuesObj) {
      // whereObj[idx] = whereObj[idx] || valuesObj[idx] || defaultWhere;
    // }

    if (removedField) {
      whereObj[removedField] = {
        '$ne': 1 // TODO Use removedValue
      };
    }

    var cleanWhereObj = {};
    for (var pk in whereObj) {
      if (whereObj[pk]) {
        cleanWhereObj[pk] = whereObj[pk];
      }
    }
    var whereClause = tools.createWhereClause(cleanWhereObj, tools.simplifyArray(columns), options);
    if (whereClause[0] === undefined) {
      whereClause[0] = options.emptyWhereClause;
    }
    return whereClause;
  };

  var queries = {
    'selectAllInCache': function (tableName, queryColumns) {
      tableName = tableName || 'all_data';
      queryColumns = queryColumns || columns;
      var selectAllQuery = 'SELECT ' + arrayToColumns(queryColumns, 'all_data') + ' FROM (';
      selectAllQuery += ' SELECT ' + arrayToColumns(queryColumns, 'cached');
      selectAllQuery += ' FROM "cached"';
      selectAllQuery += ' LEFT JOIN "removed" ON ' + queryKey.map(function (pk) {
        return '"removed"."' + pk + '" = "cached"."' + pk + '"';
      }).join(' AND ');
      selectAllQuery += ' LEFT JOIN  "updated" ON ' + queryKey.map(function (pk) {
        return '"updated"."' + pk + '" = "cached"."' + pk + '"';
      }).join(' AND ');
      selectAllQuery += ' WHERE';
      selectAllQuery += queryKey.map(function (pk) {
        return '"removed"."' + pk + '" IS NULL';
      }).join(' AND ');
      selectAllQuery += ' AND ';
      selectAllQuery += queryKey.map(function (pk) {
        return '"updated"."' + pk + '" IS NULL';
      }).join(' AND ');
      selectAllQuery += ' UNION';
      selectAllQuery += ' SELECT ' + arrayToColumns(queryColumns, 'updated');
      selectAllQuery += ' FROM "updated") AS "all_data"';
      return selectAllQuery;
    },
    'selectLastUpdate': function (tableName) {
      var lastUpdateColumn = arrayToColumns([lastUpdatedField], tableName, undefined, 'from');
      return 'SELECT COALESCE(MAX(' + lastUpdateColumn + '), -1) AS "lastUpdate" FROM "' + tableName + '" ';
    },
    'cleanUpdate': function () {
      return queries.remove('updated');
    },
    'runUpdate': function () {
      return queries.insert('updated');
    },
    'cleanRemove': function () {
      return queries.remove('removed');
    },
    'runRemove': function () {
      return queries.insert('removed');
    },
    'getUpdated': function () {
      return queries.select('updated');
    },
    'getCached': function () {
      return queries.select('cached');
    },
    'getRemoved': function () {
      return queries.select('removed');
    },
    'selectSince': function (tableName, queryColumns) {
      return queries.select(tableName, queryColumns);
    },
    'insert': function (tableName, queryColumns) {
      queryColumns = queryColumns || columns;
      var returnValue = 'INSERT INTO "' + tableName + '" (' + arrayToColumns(queryColumns) + ') VALUES (' + arrayToColumns(queryColumns, undefined, ['{{', '}}'], 'to') + ')';
      return returnValue;
    },
    'select': function (tableName, queryColumns) {
      queryColumns = queryColumns || columns;
      return 'SELECT ' + arrayToColumns(queryColumns, tableName, undefined, 'from') + ' FROM "' + tableName + '"';
    },
    'remove': function (tableName) {
      return 'DELETE FROM "' + tableName + '"';
    }
  };
  return function (queryName, origWhereObj, requestedKeys, tableName) {
    var where;
    if (origWhereObj) {
      if (queryName === 'selectSince') {
        // Special case for the last updated which requires a greater than
        var lastUpdatedObject = tools.arrayify(origWhereObj[lastUpdatedField]).map(function (value, i) {
          var operator = '$eq';
          if (i === 0) {
            operator = '$gt';
          }
          return tools.setProperty(lastUpdatedField, tools.setProperty(operator, value, {}), {});
        });
        delete origWhereObj[lastUpdatedField];
        var newWhereObj = tools.setProperty('$or', lastUpdatedObject, origWhereObj);
        where = tools.createWhereClause(newWhereObj, tools.simplifyArray(columns), options);
      } else {
        where = createWhereObj(tools.simplifyArray(requestedKeys || queryKey), origWhereObj, options);
      }
    }

    var query = queries[queryName](tableName, tools.simplifyArray(requestedKeys || queryKey)) + (where ? ' WHERE ' + where[0] : ';');

    return [query, where && where[1]];
  };
};
