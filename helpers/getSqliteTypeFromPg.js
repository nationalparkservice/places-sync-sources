module.exports = function (pgType) {
  // TODO Map to SQLite types
  // http://www.postgresql.org/docs/9.4/static/datatype.html
  var translations = {
    'bigint': 'INTEGER',
    'int8': 'INTEGER',
    'boolean': 'INTEGER',
    'integer': 'INTEGER',
    'int': 'INTEGER',
    'int4': 'INTEGER',
    'smallint': 'INTEGER',
    'date': 'REAL',
    'double precision': 'REAL',
    'float8': 'REAL',
    'text': 'TEXT'
  };
  return translations[pgType] || 'BLOB';
};
