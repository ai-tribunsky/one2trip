#!/usr/bin/env node
'use strict';

const redis = require('redis');
const uuid = require('node-uuid');

const redisOptions = {
  host: '127.0.0.1',
  port: 6379,
  db: 1,
  prefix: 'one2trip:'
};

// Node for distributed message processing
var Node = function (redisOptions) {
  const EVENTS_QUEUE = 'nodes:queue';
  const NODES_LIST = 'nodes:ids:list';
  const NODES_TABLE = 'nodes:table';
  const NODES_EMITTER = 'nodes:emitter';
  const EVENT_SENDING_INTERVAL = 500; // ms
  const NODE_ALIVE_INTERVAL = 200; // ms

  var redisClient = redis.createClient(redisOptions);
  redisClient.on('error', function (error) {
    console.error('Redis error: ' + error);
  });

  var id = uuid.v1();

  function run() {
    var node = {
      id: id,
      lastActivity: (new Date()).getTime()
    };

    _registerNode(node);

    _log('Seek for emitter');
    redisClient.get(NODES_EMITTER, function (error, emitterId) {
      if (error) {
        _log(error);
        return;
      }

      if (emitterId === null || emitterId === '') {
        _log('Emitter doesn\'t exist. Assign new one');
        _assignEmitter(function (emitter) {
          _log('New emitter: ' + emitter);
          if (id === emitter) {
            _emitEvents();
          } else {
            _receiveEvents();
          }
        });
      } else {
        _log('Emitter is exists');
        if (id === emitterId) {
          _emitEvents();
        } else {
          _receiveEvents();
        }
      }
    });
  }

  function _registerNode(node) {
    _log('Register node');
    _log(node);

    var row = {};
    row[node.id] = JSON.stringify(node);
    redisClient.multi()
      .hmset(NODES_TABLE, row, redis.print)
      .rpush(NODES_LIST, node.id, redis.print)
      .exec(redis.print);
  }

  function _emitEvents() {
    var sendingInterval = setInterval(function () {
      var message = _getMessage();
      redisClient.rpush(EVENTS_QUEUE, message, function (error, reply) {
        if (!error && reply) {
          console.log('Event "' + message + '" was emitted');
        } else {
          console.log('Error occurred during event emitting');
          console.log(error);
          console.log(reply);
          clearInterval(sendingInterval);
        }
      });
    }, EVENT_SENDING_INTERVAL);
  }

  var cnt = 0;
  function _getMessage() {
    return cnt++;
  }

  function _receiveEvents() {
    setInterval(function () {
      redisClient.lpop(EVENTS_QUEUE, function (error, reply) {
        _eventHandler(reply, function (error, event) {
          if (reply === null) {
            return;
          }
          if (error) {
            _log('Event "' + event + '" was processed with error');
          } else {
            _log('Event "' + event + '" was processed successfully');
          }
        });
      });
    }, 0, this);
  }

  function _eventHandler(event, callback){
    setTimeout(
      _onEventProcessingComplete,
      Math.floor(Math.random()*1000),
      event,
      callback
    );
  }

  function _onEventProcessingComplete(msg, callback) {
    var error = Math.random() > 0.85;
    callback(error, msg);
  }

  function _assignEmitter(onComplete) {
    redisClient.lpop(NODES_LIST, function (error, id) {
      if (error) {
        _log('Assign new emitter failed');
        _log(error);
        return;
      }

      if (id) {
        redisClient.set(NODES_EMITTER, id, function (error) {
          if (error) {
            _log('Assign new emitter failed');
            _log(error);
            return;
          }
          onComplete(id);
        });
      } else {
        onComplete(null);
      }
    });
  }

  function _log(message) {
    if (message instanceof Object) {
      message = JSON.stringify(message, null, 4);
    }
    console.log(message);
  }

  return {
    init: run
  };
};

Node(redisOptions).init();
