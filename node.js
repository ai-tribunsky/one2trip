'use strict';

var argv = require('minimist')(process.argv.slice(2));
var redis = require('redis');

// Node for distributed messages processing
exports.node = function (argv) {
  var EVENTS_QUEUE = 'nodes:queue';
  var EVENTS_ERRORS_LIST = 'nodes:errors';
  var EVENTS_ERRORS_LIST_TTL = 86400; // sec
  var NODES_IDS_LIST = 'nodes:ids';
  var NODES_LIST = 'nodes:list';
  var NODES_EMITTER = 'nodes:emitter';
  var NODES_KEY_TTL = 10; // sec
  var EVENT_EMITTING_INTERVAL = 500; // ms
  var NODE_HEALTH_CHECK_INTERVAL = 200; // ms
  var NODE_ACTIVITY_THRESHOLD = 1000; // ms

  var id = 'node-' + Date.now() + '-' + Math.round(Math.random() * 1000);
  var isEmitter = false;
  var emitEventInterval;
  var receiveEventInterval;

  var redisClient = redis.createClient({
    host: argv.hasOwnProperty('rhost') ? argv.rhost : '127.0.0.1',
    port: argv.hasOwnProperty('rport') ? argv.rport : 6379,
    db: argv.hasOwnProperty('rdb') ? argv.rdb : 0,
    prefix: 'one2trip:'
  });
  redisClient.on('error', function (error) {
    _log('[ERROR] Redis:', error);
    clearInterval(emitEventInterval);
    clearInterval(receiveEventInterval);
  });

  function run() {
    if (argv.hasOwnProperty('getErrors')) {
      _getErrors(function () {
        process.exit(-1);
      });
      return;
    }
    _registerNode();
    _checkEmitter();

    emitEventInterval = setInterval(
      function _emitEventLoop() {
        if (isEmitter) {
          _emitEvent();
        }
      },
      EVENT_EMITTING_INTERVAL
    );
    receiveEventInterval = setInterval(
      function _receiveEventLoop() {
        if (!isEmitter) {
          _receiveEvent();
        }
      },
      0
    );

    _nodesHealthCheck();
  }

  function _getErrors(onComplete) {
    redisClient.rpop(EVENTS_ERRORS_LIST, function (error, event) {
      if (error) {
        _log('[ERROR] Getting event with error failed:', error);
        return;
      }
      if (null === event) {
        _log('Errors list is empty');
        onComplete();
        return;
      }
      _log(event);
      _getErrors(onComplete);
    });
  }

  function _registerNode() {
    _log('Register node: ' + id);

    var lastActivity = Date.now();
    redisClient.multi()
      .hset(NODES_LIST, id, lastActivity, function (error) {
        if (error) {
          _log('[ERROR] Adding node to nodes list failed:', error);
        }
      })
      .rpush(NODES_IDS_LIST, id, function (error) {
        if (error) {
          _log('[ERROR] Adding node id to nodes ids list failed:', error);
        }
      })
      .exec(function (error) {
        if (error) {
          _log('[ERROR] Node registration failed:', error);
        }
      });
  }

  function _checkEmitter() {
    redisClient.get(NODES_EMITTER, function (error, emitterId) {
      if (error) {
        _log('[ERROR] Checking emitter failed:', error);
      } else {
        isEmitter = emitterId === id;
        _checkEmitter();
      }
    });
  }

  function _emitEvent() {
    var message = _getMessage();
    var lastActivity = Date.now();
    redisClient.batch()
      .rpush(EVENTS_QUEUE, message, function (error, reply) {
        if (!error && reply) {
          _log('Event "' + message + '" was emitted');
        } else {
          _log('[ERROR] Event emitting failed:', error, 'Reply: ' + reply);
        }
      })
      .hset(NODES_LIST, id, lastActivity, function (error) {
        if (error) {
          _log('[ERROR] Updating node last activity failed:', error);
        }
      })
      .exec();
  }

  var cnt = 0;
  function _getMessage() {
    return cnt++;
  }

  function _eventHandler(event, callback){
    setTimeout(
      _onEventProcessingComplete,
      Math.floor(Math.random() * 1000),
      event,
      callback
    );
  }

  function _onEventProcessingComplete(msg, callback) {
    var error = Math.random() > 0.85;
    callback(error, msg);
  }

  function _receiveEvent() {
    var eventHandlerCallback = function (error, event) {
      if (error) {
        _log('Event "' + event + '" was processed with an error');
        _saveEventWithError(event);
      } else {
        _log('Event "' + event + '" was processed successfully');
      }
    };

    var lastActivity = Date.now();
    redisClient.batch()
      .lpop(EVENTS_QUEUE, function (error, reply) {
        if (null === reply) {
          return;
        }
        if (error) {
          _log('[ERROR] Event dequeueing failed:', error);
        } else {
          _eventHandler(reply, eventHandlerCallback);
        }
      })
      .hset(NODES_LIST, id, lastActivity, function (error) {
        if (error) {
          _log('[ERROR] Updating node last activity failed:', error);
        }
      })
      .exec();
  }

  function _saveEventWithError(event) {
    var message = '[' + (new Date()).toLocaleString() + '] Event: ' + event + ' (Node: ' + id + ')';
    redisClient.batch()
      .rpush(EVENTS_ERRORS_LIST, message, function (error) {
        if (error) {
          _log('[ERROR] Saving event with error failed:', error);
        }
      })
      .expire(EVENTS_ERRORS_LIST, EVENTS_ERRORS_LIST_TTL)
      .exec();
  }

  function _nodesHealthCheck() {
    var healthCheck = function _healthCheck() {
      redisClient.batch()
        .get(NODES_EMITTER, function (error, emitterId) {
          if (error) {
            _log('[ERROR] Getting emitter failed:', error);
            return;
          }

          if (null === emitterId || '' === emitterId) {
            _log('Emitter is not found');
            _assignEmitter();
          }

          redisClient.hgetall(NODES_LIST, function (error, nodesList) {
            if (error) {
              _log('[ERROR] Getting node list failed:', error);
              return;
            }

            var nodeId;
            var lastActivity;
            var now;
            var removeNodeCallback = function (error) {
              if (error) {
                _log('[ERROR] Removing node from node list failed:', error);
              }
            };
            var removeNodeIdCallback = function (error) {
              if (error) {
                _log('[ERROR] Removing node id from node ids list failed:', error);
              }
            };
            var unsettingEmitterCallback = function (error) {
              if (error) {
                _log('[ERROR] Unsetting emitter failed:', error);
              }
            };
            for (nodeId in nodesList) {
              now = Date.now();
              lastActivity = parseInt(nodesList[nodeId], 10);
              if (now - lastActivity < NODE_ACTIVITY_THRESHOLD) {
                continue;
              }

              var multiExec = redisClient.multi()
                .hdel(NODES_LIST, nodeId, removeNodeCallback)
                .lrem(NODES_IDS_LIST, 0, nodeId, removeNodeIdCallback);
              if (emitterId === nodeId) {
                multiExec.set(NODES_EMITTER, '', unsettingEmitterCallback);
              }
              multiExec.exec(function (error) {
                if (error) {
                  _log('[ERROR] Removing inactive node failed:', error);
                  return;
                }

                if (emitterId === nodeId) {
                  _log('Emitter was removed');
                  _assignEmitter();
                }
              });
            }
          });
        })
        .expire(NODES_EMITTER, NODES_KEY_TTL)
        .expire(NODES_IDS_LIST, NODES_KEY_TTL)
        .expire(NODES_LIST, NODES_KEY_TTL)
        .expire(EVENTS_QUEUE, NODES_KEY_TTL)
        .exec(function (error) {
          if (error) {
            _log('[ERROR] Error occurred during health checking:', error);
          }
        });
    };
    setInterval(healthCheck, NODE_HEALTH_CHECK_INTERVAL);
  }

  function _assignEmitter() {
    redisClient.lindex(NODES_IDS_LIST, 0, function (error, id) {
      if (error) {
        _log('[ERROR] Getting potential node-emitter failed:', error);
        return;
      }

      if (id) {
        var lastActivity = Date.now();
        redisClient.batch()
          .set(NODES_EMITTER, id, function (error) {
            if (error) {
              _log('[ERROR] Assigning new emitter failed:', error);
            }
          })
          .hset(NODES_LIST, id, lastActivity, function (error) {
            if (error) {
              _log('[ERROR] Updating emitter node activity failed:', error);
            }
          })
          .exec(function (error) {
            if (!error) {
              _log('New emitter: ' + id);
            }
          });
      }
    });
  }

  function _log() {
    var i;
    var message;
    for (i in arguments) {
      message = arguments[i];
      if (message instanceof Object) {
        message = JSON.stringify(message, null, 4);
      }
      console.log(message);
    }
  }

  return {
    run: run
  };
};

exports.node(argv).run();
