'use strict';

var util = require('util');
var couchbase = require('couchbase');
var bson = require('bson');

var TmpMultiWait = require('./tmpmultiwait');

var BSON = new bson.BSONPure.BSON();
var ObjectID = bson.BSONPure.ObjectID;
var Long = bson.BSONPure.Long;

var DEFAULT_DB_CONFIG = {
  primaryKey: '_id',
  consistency: {
    enabled: true,
    cache: {
      enabled: false,
      timeout: 5000,
      size: 100000
    }
  }
};

function PDatabase(name, config) {
  var fullConfig = util._extend(DEFAULT_DB_CONFIG, config);

  this.name = name;
  this.config = fullConfig;
  var cluster = new couchbase.Cluster(fullConfig.connstr);
  this.bucket = cluster.openBucket(fullConfig.bucket);
}

function _buildN1qlCompare(left, op, right) {
  if (Array.isArray(right)) {
    if (right.length <= 0) {
      return left + ' ' + op + '[]';
    }

    var values = [];
    for (var i = 0; i < right.length; ++i) {
      if (right[i] instanceof Date) {
        values.push('STR_TO_MILLIS(' + JSON.stringify(right[i]) + ')')
      } else if (right[i] instanceof ObjectID) {
        values.push(JSON.stringify(right[i].toString()))
      } else {
        values.push(JSON.stringify(right[i]));
      }
    }

    return left + ' ' + op + ' [' + values.join(', ') + ']';
  } else {
    if (right instanceof Date) {
      return 'STR_TO_MILLIS(' + left + ') ' + op +
          ' STR_TO_MILLIS(' + JSON.stringify(right) + ')';
    } else if (right instanceof ObjectID) {
      return left + ' ' + op + ' ' + JSON.stringify(right.toString());
    } else {
      if (right) {
        return left + ' ' + op + ' ' + JSON.stringify(right);
      } else {
        return left + ' ' + op;
      }
    }
  }
}

function _buildFilterExprs(conds, filter, root) {
  for (var i in filter) {
    if (i[0] === '$') {
      if (i === '$or') {
        var subexprs = [];
        for (var j = 0; j < filter[i].length; ++j) {
          var subconds = [];
          _buildFilterExprs(subconds, filter[i][j], root);
          subexprs.push(subconds.join(' AND '));
        }
        conds.push('(' + subexprs.join(' OR ') + ')');
      } else if (i === '$exists') {
        if (filter[i]) {
          conds.push(_buildN1qlCompare(root, 'IS VALUED'));
        } else {
          conds.push(_buildN1qlCompare(root, 'IS NOT VALUED'));
        }
      } else if (i === '$gt') {
        conds.push(_buildN1qlCompare(root, '>', filter[i]));
      } else if (i === '$in') {
        conds.push(_buildN1qlCompare(root, 'IN', filter[i]));
      } else {
        throw new Error('Unexpected filter operator ' + i);
      }
    } else {
      var filterKey = root;
      if (filterKey !== '') {
        filterKey += '.';
      }
      filterKey += '`' + i + '`';

      if (filter[i] instanceof Object &&
          !(filter[i] instanceof Date || filter[i] instanceof ObjectID)) {
        _buildFilterExprs(conds, filter[i], filterKey);
      } else {
        conds.push(_buildN1qlCompare(filterKey, '=', filter[i]));
      }
    }
  }
};

PDatabase.prototype._buildWhere = function(collection, filter) {
  var conds = [];
  conds.push('SPLIT(META().id,"$~")[0]="' + collection + '"');
  _buildFilterExprs(conds, filter, '');

  if (conds.length > 0) {
    return ' WHERE ' + conds.join(' AND ') + ' ';
  } else {
    return ' ';
  }
};

PDatabase.prototype._findSome = function(collection, filter, limit, callback) {
  var qs = '';
  qs += 'SELECT ';
  if (limit !== -1) {
    qs += ' ARRAY_AGG(META().id) AS ids, '
  }
  qs += 'COUNT(*) AS cnt FROM default d ';
  qs += this._buildWhere(collection, filter);
  if (limit !== -1 && limit > 0) {
    qs += ' LIMIT ' + limit + ' ';
  }

  console.log('Executing Query', qs);

  var q = couchbase.N1qlQuery.fromString(qs);
  if (this.config.consistency.enabled) {
    q.consistency(couchbase.N1qlQuery.Consistency.REQUEST_PLUS);
  }
  this.bucket.query(q, function(err, rows) {
    if (err) {
      callback(err, null, null);
      return;
    }

    if (rows.length != 1) {
      callback(new Error('wat?'), null, null);
      return;
    }

    var matchedCount = rows[0].cnt;
    var ids = rows[0].ids;

    callback(null, ids ? ids : [], matchedCount);
  });
};

PDatabase.prototype._applyInsert = function(collection, doc, callback) {
  if (doc._id === undefined) {
    doc._id = new ObjectID();
  }

  var key = collection + '$~' + doc._id;
  this.bucket.insert(key, doc, callback);
};



PDatabase.prototype._applyInsertFromUpdate = function(collection, update, callback) {
  var updater = new UpdateApplier(update);
  var newDoc = updater.apply();
  this._applyInsert(collection, newDoc, function(err, res) {
    callback(err, newDoc);
  });
};

PDatabase.prototype._applyUpdate = function(id, update, callback) {
  var self = this;

  var updater = new UpdateApplier(update);

  self.bucket.get(id, function(err, res) {
    if (err && err.code === couchbase.errors.keyNotFound) {
      callback(err, res);
      return;
    }

    var originalDoc = res.value;
    var newDoc = updater.apply(originalDoc);
    self.bucket.replace(id, newDoc, {cas: res.cas}, function(err, rres) {
      if (err && err.code === couchbase.errors.keyAlreadyExists) {
        // If we get a cas error, just try again.
        self._applyUpdate(id, update, callback);
        return;
      } else if (err) {
        callback(err, null);
        return;
      }

      callback(null, newDoc, originalDoc);
    });
  });
};

PDatabase.prototype.handleInsert = function(info, callback) {
  console.log('handleInsert', info);

  var waiter = new TmpMultiWait(info.documents.length);
  for (var i = 0; i < info.documents.length; ++i) {
    var doc = info.documents[i];

    this._applyInsert(info.collection, doc, function(err, res) {
      waiter.tick();
    });
  }

  waiter.wait(function() {
    callback(null, {
      n: info.documents.length
    });
  });
};

PDatabase.prototype.handleUpdate = function(info, callback) {
  console.log('handleUpdate', info);

  if (info.updates.length != 1) {
    callback(new Error('Cannot handle multiple update groups.'));
    return;
  }
  var updInfo = info.updates[0];

  var self = this;
  this._findSome(info.collection, updInfo.q, updInfo.multi ? 0 : 1, function(err, ids, matched) {
    if (err) {
      callback(err, null);
      return;
    }

    if (ids.length === 0 && updInfo.upsert) {
      self._applyInsertFromUpdate(info.collection, updInfo.u, function(err, res) {
        callback(null, {
          n: 0,
          nModified: 1
        });
      });
    } else {
      var waiter = new TmpMultiWait(ids.length);
      for (var i = 0; i < ids.length; ++i) {
        self._applyUpdate(ids[i], updInfo.u, function(err, res) {
          waiter.tick();
        });
      }

      waiter.wait(function() {
        callback(null, {
          n: matched,
          nModified: ids.length
        });
      });
    }
  });
};

PDatabase.prototype.handleDelete = function(info, callback) {
  console.log('handleDelete', info);

  if (info.deletes.length != 1) {
    callback(new Error('Cannot handle multiple delete groups.'));
    return;
  }
  var delInfo = info.deletes[0];

  var self = this;
  this._findSome(info.collection, delInfo.q, delInfo.limit, function(err, ids, matched) {
    if (err) {
      callback(err, null);
      return;
    }

    var waiter = new TmpMultiWait(ids.length);
    for (var i = 0; i < ids.length; ++i) {
      self.bucket.remove(ids[i], function(err, res) {
        waiter.tick();
      });
    }

    waiter.wait(function() {
      callback(null, {
        n: ids.length
      });
    });
  });
};

PDatabase.prototype.handleFind = function(info, callback) {
  console.log('handleFind', info);

  var self = this;
  this._findSome(info.collection, info.filter, 0, function(err, ids) {
    if (err) {
      callback(err, null);
      return;
    }

    var results = [];
    var waiter = new TmpMultiWait(ids.length);
    for (var i = 0; i < ids.length; ++i) {
      (function(i) {
        self.bucket.get(ids[i], function(err, res) {
          results[i] = res.value;
          waiter.tick();
        });
      })(i);
    }

    waiter.wait(function() {
      callback(null, {
        waitedMS: Long.fromNumber(0),
        cursor: {
          id: Long.fromNumber(0),
          ns: self.name + '.' + info.collection,
          firstBatch: results
        }
      });
    });
  });
};

/*
 collection: data.info.findandmodify,
 query: data.info.query,
 sort: data.info.sort,
 remove: data.info.remove,
 update: data.info.update,
 new: data.info.new,
 fields: data.info.fields,
 upsert: data.info.upsert,
 */

PDatabase.prototype.handleFindAndModify = function(info, callback) {
  console.log('handleFindAndModify', info);

  var self = this;
  this._findSome(info.collection, info.query, 1, function(err, ids, matched) {
    if (err) {
      callback(err, null);
      return;
    }

    var originalDoc = null;
    var newDoc = null;
    function dispatchCallback(updatedExisting) {
      callback(null, {
        lastErrorObject: {
          updatedExisting: updatedExisting,
          n: matched
        },
        value: info.new ? newDoc : originalDoc
      });
    }

    if (info.update) {
      if (ids.length === 0 && info.upsert) {
        this._applyInsertFromUpdate(info.collection, info.update, function (err, res, newDocX) {
          newDoc = newDocX;

          if (err) {
            callback(err);
            return;
          }

          dispatchCallback(false);
        });
      } else if (ids.length === 0) {
        dispatchCallback(false);
      } else {
        self._applyUpdate(ids[0], info.update, function(err, newDocX, originalDocX) {
          originalDoc = originalDocX;
          newDoc = newDocX;
          dispatchCallback(true);
        });
      }
    } else if (info.remove) {
      if (ids.length > 0) {
        if (!info.new) {
          // If the user doesnt want the new document, we need to fetch it first.
          self.bucket.get(ids[0], function(err, res) {
            originalDoc = res.value;

            self.bucket.remove(ids[0], function(err, res) {
              dispatchCallback(false);
            });
          });
        } else {
          self.bucket.remove(ids[0], function(err, res) {
            dispatchCallback(false);
          });
        }
      } else {
        dispatchCallback(false);
      }
    } else {
      console.log('Unexpected findAndModify', info);
      callback(mongoError('Unexpected findAndModify', 0));
    }
  });
}

PDatabase.prototype.handleCount = function(info, callback) {
  console.log('handleCount', info);

  var self = this;
  this._findSome(info.collection, info.query, -1, function(err, ids, matched) {
    if (err) {
      callback(err, null);
      return;
    }

    callback(null, {
      waitedMS: Long.fromNumber(0),
      n: matched
    });
  });
};

PDatabase.prototype.handleListIndexes = function(info, callback) {
  console.log('handleListIndexes', info);

  var self = this;

  var results = [];
  results.push({
    v: 1,
    key: { _id: 1 },
    name: '_id_',
    ns: self.name + '.' + info.collection
  });

  callback(null, {
    waitedMS: Long.fromNumber(0),
    cursor: {
      id: Long.fromNumber(0),
      ns: self.name + '.$cmd.listIndexes.' + info.collection,
      firstBatch: results
    }
  });
};

PDatabase.prototype.handleCreateIndexes = function(info, callback) {
  console.log('handleCreateIndexes', info);

  callback(null, {
    createdCollectionAutomatically: true,
    numIndexesBefore: 1,
    numIndexesAfter: 2
  });
};

module.exports = PDatabase;
