// Converts objects into a string if they aren't already a string

module.exports = function (v) {
  var returnValue = v;
  try {
    if (typeof v === 'string') {
      returnValue = v;
    } else {
      returnValue = JSON.stringify(v);
    }
  } catch (e) {
    returnValue = JSON.stringify(v);
  }
  return returnValue;
};
