'use strict';

var bson = require('bson');

var BSON = new bson.BSONPure.BSON();

function StreamingReader(buffer, start, end) {
  this.buffer = buffer;
  this.offset = start;
  this.end = end;
}

StreamingReader.prototype.isEof = function() {
  return this.offset >= this.end;
};

StreamingReader.prototype.readInt8 = function() {
  var out = this.buffer.readInt8(this.offset);
  this.offset += 1;
  return out;
};

StreamingReader.prototype.readInt32 = function() {
  var out = this.buffer.readInt16LE(this.offset);
  this.offset += 2;
  return out;
};

StreamingReader.prototype.readInt32 = function() {
  var out = this.buffer.readInt32LE(this.offset);
  this.offset += 4;
  return out;
};

StreamingReader.prototype.readCString = function() {
  var stringEnd = this.buffer.indexOf(0, this.offset);
  if (stringEnd === -1) {
    throw new Error('Unexpected missing string terminator...');
  }

  var out = this.buffer.toString('utf8', this.offset, stringEnd);
  this.offset = stringEnd + 1;
  return out;
};

StreamingReader.prototype.readBsonDoc = function() {
  var docLen = this.buffer.readInt32LE(this.offset);
  var out = BSON.deserialize(this.buffer.slice(this.offset, this.offset + docLen));
  this.offset += docLen;
  return out;
};

module.exports = StreamingReader;
