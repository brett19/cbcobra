'use strict';

var bson = require('bson');
var BSON = new bson.BSONPure.BSON();

function updateSetApplier(source, data) {
  for (var i in data) {
    var pathParts = i.split('.');
    var entry = source;
    var lastPart = pathParts.pop();
    while (pathParts.length > 0) {
      var nextPart = pathParts.pop();
      if (entry[nextPart] === undefined) {
        entry[nextPart] = {};
      }
      entry = entry[nextPart];
    }
    entry[lastPart] = data[i];
  }
}
function updateSetOnInsertApplier(source, data, isInsert) {
  if (!isInsert) {
    return;
  } else {
    return updateSetApplier(source, data);
  }
}
function updateUnsetApplier(source, data) {
  for (var i in data) {
    var pathParts = i.split('.');
    var entry = source;
    var lastPart = pathParts.pop();
    while (pathParts.length > 0) {
      var nextPart = pathParts.pop();
      if (entry[nextPart] === undefined) {
        return;
      }
      entry = entry[nextPart];
    }
    delete entry[lastPart];
  }
}
function updateRenameApplier(source, data) {
  for (var i in data) {
    var pathParts = i.split('.');
    var entry = source;
    var lastPart = pathParts.pop();
    while (pathParts.length > 0) {
      var nextPart = pathParts.pop();
      if (entry[nextPart] === undefined) {
        return;
      }
      entry = entry[nextPart];
    }
    if (!entry.hasOwnProperty(lastPart)) {
      return;
    }
    var originalValue = entry[lastPart];
    var unsetter = {};
    unsetter[i] = '';
    updateUnsetApplier(source, unsetter);
    var setter = {};
    setter[data[i]] = originalValue;
    updateSetApplier(source, setter);
  }
}
var UPDATE_APPLIERS = {
  '$set': updateSetApplier,
  '$setOnInsert': updateSetOnInsertApplier,
  '$unset': updateUnsetApplier,
  '$rename': updateRenameApplier
};

function UpdateApplier(updates) {
  this.updates = updates;

  if (!this.isValid()) {
    throw new Error('Invalid updater specified');
  }
}

UpdateApplier.prototype.isValid = function() {
  // Cant be both...
  if (this.isDocument()) {
    return !this.isUpdater();
  } else {
    return this.isUpdater();
  }
};

UpdateApplier.prototype.isDocument = function() {
  var isDoc = true;
  for (var i in this.updates) {
    if (this.updates.hasOwnProperty(i)) {
      if (UPDATE_APPLIERS[i]) {
        isDoc = false;
      }
    }
  }
  return isDoc;
};
UpdateApplier.prototype.isUpdater = function() {
  var isUpd = true;
  for (var i in this.updates) {
    if (this.updates.hasOwnProperty(i)) {
      if (!UPDATE_APPLIERS[i]) {
        isUpd = false;
      }
    }
  }
  return isUpd;
};

UpdateApplier.prototype.needsSource = function() {
  return !this.isDocument();
};

UpdateApplier.prototype.apply = function(sourceDoc) {
  if (this.isDocument()) {
    return this.updates;
  } else {
    var isInsert = true;
    if (sourceDoc) {
      sourceDoc = BSON.deserialize(BSON.serialize(sourceDoc));
      isInsert = false;
    } else {
      sourceDoc = {};
    }
    for (var i in this.updates) {
      if (this.updates.hasOwnProperty(i)) {
        UPDATE_APPLIERS[i](sourceDoc, this.updates[i], isInsert);
      }
    }
    return sourceDoc;
  }
};

module.exports = UpdateApplier;
