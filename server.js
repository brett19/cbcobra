'use strict';

var util = require('util');
var net = require('net');
var couchbase = require('couchbase');
var bson = require('bson');

var BSON = new bson.BSONPure.BSON();

var StreamingReader = require('./streamingreader');
var PDatabase = require('./pdatabase');

require('buffer').INSPECT_MAX_BYTES = 2048 * 16;

var config = {
  databases: {
    'drywall': {
      connstr: 'couchbase://localhost',
      bucket: 'default'
    }
  }
};

/*
 var x = new Buffer('68000000036c6173744572726f724f626a656374001e00000008757064617465644578697374696e670001106e0001000000000376616c75650021000000075f696400570fe69707e0d33b150be21c016100000000000000374000016f6b00000000000000f03f000500000000'.replace(/ /g,''), 'hex');
 console.log(util.inspect(BSON.deserialize(x), {depth: 4}));
 return;
 //*/

var databases = {};
for (var i in config.databases) {
  databases[i] = new PDatabase(i, config.databases[i]);
  console.log('Initialized database', i, config.databases[i]);
}


function writeReply(socket, responseTo, docs) {
  console.log('Writing Reply', util.inspect(docs, {depth:8}));

  var totalDocsLen = 0;
  var docsBytes = [];
  for (var i = 0; i < docs.length; ++i) {
    var docBytes = BSON.serialize(docs[i]);
    docsBytes.push(docBytes);
    totalDocsLen += docBytes.length;
  }

  var header = new Buffer(36);
  header.writeInt32LE(16 + 20 + totalDocsLen, 0); // length
  header.writeInt32LE(0, 4); // command id
  header.writeInt32LE(responseTo, 8);
  header.writeInt32LE(1, 12); // cmd
  header.writeInt32LE(8, 16); // flags (AwaitCapable)
  header.writeInt32LE(0, 20); // cursorID
  header.writeInt32LE(0, 24); // cursorID (64 bit upper)
  header.writeInt32LE(0, 28); // startingFrom
  header.writeInt32LE(docs.length, 32); // numReturned
  socket.write(header);
  for (var i = 0; i < docsBytes.length; ++i) {
    socket.write(docsBytes[i]);
  }
};

function write2011Reply(socket, responseTo, doc, otherDoc) {
  console.log('Writing 2011 Reply', util.inspect(doc, {depth:8}));

  var totalDocsLen = 0;
  var docBytes = null;
  var otherDocBytes = null;
  if (doc) {
    docBytes = BSON.serialize(doc);
    totalDocsLen += docBytes.length;
  }
  if (otherDoc) {
    otherDocBytes = BSON.serialize(otherDoc);
    totalDocsLen += otherDocBytes.length;
  }

  var header = new Buffer(16);
  header.writeInt32LE(16 + totalDocsLen, 0); // length
  header.writeInt32LE(0, 4); // command id
  header.writeInt32LE(responseTo, 8);
  header.writeInt32LE(2011, 12); // cmd
  socket.write(header);
  if (docBytes) {
    socket.write(docBytes);
  }
  if (otherDocBytes) {
    socket.write(otherDocBytes);
  }
};

var server = net.createServer(function(socket) {
  console.log('New Connection...');

  var buffer = null;
  socket.on('data', function(data) {
    if (buffer) {
      buffer = Buffer.concat([buffer, data]);
    } else {
      buffer = data;
    }

    var offset = 0;
    while (true) {
      if (buffer.length < offset + 4) {
        // Not even the length yet...
        break;
      }

      var msgLen = buffer.readInt32LE(offset + 0);

      if (buffer.length < offset + msgLen) {
        // Don't have a full packet yet.
        break;
      }

      var msgId = buffer.readInt32LE(offset + 4);
      var respTo = buffer.readInt32LE(offset + 8);
      var opCode = buffer.readInt32LE(offset + 12);

      var rdr = new StreamingReader(buffer, offset + 16, offset + msgLen);
      socket.emit('op_packet', msgId, respTo, opCode, rdr);

      offset += msgLen;
      if (offset >= buffer.length) {
        buffer = null;
        break;
      } else {
        buffer = buffer.slice(offset);
      }
    }
  });

  function mongoError(str, code) {
    var err = new Error(str);
    err.code = code;
    return err;
  }

  /*
  info:
    database
    info
    fields
   */
  function processCommand(socket, data, callback)
  {
    var infoX = {};
    for (var i in data.info) {
      infoX[i.toLowerCase()] = data.info[i];
    }
    data.info = infoX;

    if (data.info.whatsmyuri) {
      var whatsMyUriDoc = {
        you: socket.remoteAddress + ':' + socket.remotePort,
        ok: 1
      };
      callback(null, whatsMyUriDoc);
    } else if (data.info.replsetgetstatus) {
      callback(mongoError('not running with --replSet', 76));
    } else if (data.info.ismaster) {
      var isMasterDoc = {
        ismaster: true,
        maxBsonObjectSize: 16777216,
        maxMessageSizeBytes: 48000000,
        maxWriteBatchSize: 1000,
        localTime: new Date(),
        maxWireVersion: 4,
        minWireVersion: 1,
        ok: 1
      };
      callback(null, isMasterDoc);
    } else if (data.info.getlog) {
      if (data.info.getlog === 'startupWarnings') {
        var getLogStartupWarningsDoc = {
          totalLinesWritten: 1,
          log: [
            'FAKELOG TEST MESSAGE'
          ],
          ok: 1
        };
        callback(null, getLogStartupWarningsDoc);
      } else {
        console.log('Unknown op_admquery getLog type', data.info.getlog);
        socket.end();
        return;
      }
    } else if (data.info.buildinfo) {
      var buildInfoDoc = {
        version: '3.2.4',
        gitVersion: 'e2ee9ffcf9f5a94fad76802e28cc978718bb7a30',
        modules: [],
        allocator: 'system',
        javascriptEngine: 'v8',
        sysInfo: 'deprecated',
        versionArray: [ 3, 2, 4, 0 ],
        openssl: { running: 'disabled', compiled: 'disabled' },
        buildEnvironment: {},
        bits: 64,
        debug: false,
        maxBsonObjectSize: 16777216,
        storageEngines: [ 'devnull', 'ephemeralForTest', 'mmapv1', 'wiredTiger' ],
        ok: 1 };
      callback(null, buildInfoDoc);
    } else {
      var database = databases[data.database];
      if (!database) {
        console.log('Unknown database', data.database);
        socket.end();
        return;
      }
      if (data.info.findandmodify) {
        database.handleFindAndModify({
          collection: data.info.findandmodify,
          query: data.info.query,
          sort: data.info.sort,
          remove: data.info.remove,
          update: data.info.update,
          new: data.info.new,
          fields: data.info.fields,
          upsert: data.info.upsert,
        }, callback);
      } else if (data.info.insert) {
        database.handleInsert({
          collection: data.info.insert,
          documents: data.info.documents,
          ordered: data.info.ordered
        }, callback);
      } else if (data.info.delete) {
        database.handleDelete({
          collection: data.info.delete,
          deletes: data.info.deletes,
          ordered: data.info.ordered
        }, callback);
      } else if (data.info.update) {
        database.handleUpdate({
          collection: data.info.update,
          updates: data.info.updates,
          ordered: data.info.ordered
        }, callback);
      } else if (data.info.find) {
        database.handleFind({
          collection: data.info.find,
          filter: data.info.filter
        }, callback);
      } else if (data.info.count) {
        database.handleCount({
          collection: data.info.count,
          query: data.info.query
        }, callback);
      } else if (data.info.listindexes) {
        database.handleListIndexes({
          collection: data.info.listindexes,
          cursor: data.info.cursor
        }, callback);
      } else if (data.info.createindexes) {
        database.handleCreateIndexes({
          collection: data.info.createindexes,
          indexes: data.info.indexes
        }, callback);
      } else {
        console.log('Unknown command type', data.info);
        socket.end();
        return;
      }
    }
  }

  socket.on('op_packet', function(msgId, respTo, opCode, rdr) {
    console.log('Decoded Header:', {
      msgId: msgId,
      respTo: respTo,
      opCode: opCode
    });

    if (opCode === 2002) {
      var insertFlags = rdr.readInt32();
      var collectionName = rdr.readCString();
      var documents = [];
      while (!rdr.isEof()) {
        documents.push(rdr.readBsonDoc());
      }

      var insertData = {
        msgId: msgId,
        respTo: respTo,
        flags: insertFlags,
        collection: collectionName,
        documents: documents
      };
      socket.emit('op_insert', insertData);
    } else if (opCode === 2004) {
      var queryFlags = rdr.readInt32();
      var collectionName = rdr.readCString();
      var numToSkip = rdr.readInt32();
      var numToReturn = rdr.readInt32();
      var queryInfo = rdr.readBsonDoc();
      var queryFields = null;
      if (!rdr.isEof()) {
        queryFields = rdr.readBsonDoc();
      }

      var queryData = {
        msgId: msgId,
        respTo: respTo,
        flags: queryFlags,
        collection: collectionName,
        numToSkip: numToSkip,
        numToReturn: numToReturn,
        queryInfo: queryInfo,
        queryFields: queryFields
      };
      socket.emit('op_query', queryData);
    } else if (opCode === 2010) {
      var db = rdr.readCString();
      var cmd = rdr.readCString();
      var data = rdr.readBsonDoc();

      var queryData = {
        msgId: msgId,
        respTo: respTo,
        database: db,
        command: cmd,
        data: data
      };
      socket.emit('op_admquery', queryData);
    } else {
      // Need to write a failure reply...
      console.log('Unknown wire opcode', opCode);
      socket.end();
      return;
    }
  });

  socket.on('op_admquery', function(info) {
    console.log('Decoded Admin Query', util.inspect(info, {depth: 8}));

    processCommand(socket, {
      database: info.database,
      info: info.data,
    }, function(err, respData) {
      if (err) {
        var errData = {
          ok: 0,
          errmsg: err.message,
          code: err.code
        };
        write2011Reply(socket, info.msgId, errData, {});
      } else {
        respData.ok = 1;
        write2011Reply(socket, info.msgId, respData, {});
      }
    });
  });

  socket.on('op_insert', function(info) {
    console.log('Decoded Insert', util.inspect(info, {depth: 8}));

    console.log('Unexpected OP_INSERT');
    socket.end();
  });

  socket.on('op_query', function(info) {
    console.log('Decoded Query', util.inspect(info, {depth: 8}));

    var colSplit = info.collection.split('.');

    var databaseName = '';
    var collectionName = '';
    if (colSplit.length > 0) {
      collectionName = colSplit.pop();
    }
    if (colSplit.length > 0) {
      databaseName = colSplit.pop();
    }

    if (collectionName == '$cmd') {

      processCommand(socket, {
        database: databaseName,
        info: info.queryInfo
      }, function(err, respData) {
        if (err) {
          var errData = {
            ok: 0,
            errmsg: err.message,
            code: err.code
          };
          writeReply(socket, info.msgId, [errData]);
        } else {
          respData.ok = 1;
          writeReply(socket, info.msgId, [respData]);
        }
      });

    } else {
      console.log('We cannot handle non-$cmd collections.');
      socket.end();
      return;
    }
  });
});

server.listen(27018, '127.0.0.1');
