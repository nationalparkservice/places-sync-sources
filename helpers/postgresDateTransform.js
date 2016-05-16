module.exports = {
  'from': ['(EXTRACT(EPOCH FROM ', ')) * 1000'],
  'to': ["(TIMESTAMP 'epoch' + ", " * INTERVAL '1 millisecond') AT TIME ZONE 'GMT'"]
};
