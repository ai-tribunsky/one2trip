#!/usr/bin/env node
'use strict';

const redis = require('redis');
const redisOptions = {
  host: '127.0.0.1',
  port: 6379,
  db: 1,
  prefix: 'one2trip:'
};

// Node for distributed messages processing
var Node = function (redisOptions) {
  const EVENTS_QUEUE = 'nodes:queue';
  const NODES_IDS_LIST = 'nodes:ids';
  const NODES_LIST = 'nodes:list';
  const NODES_EMITTER = 'nodes:emitter';
  const NODES_KEY_TTL = 10; // sec
  const EVENT_EMITTING_INTERVAL = 500; // ms
  const NODE_HEALTH_CHECK_INTERVAL = 200; // ms
  const NODE_ACTIVITY_THRESHOLD = 1000; // ms

  var id = 'node-' + (new Date()).getTime() + '-' + Math.round(Math.random() * 1000);
  var isEmitter = false;
  var emitEventInterval;
  var receiveEventInterval;

  const redisClient = redis.createClient(redisOptions);
  redisClient.on('error', function (error) {
    _log('[ERROR] Redis:', error);
    clearInterval(emitEventInterval);
    clearInterval(receiveEventInterval);
  });

  function run() {
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

  function _registerNode() {
    _log('Register node: ' + id);

    var lastActivity = (new Date()).getTime();
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
    var lastActivity = (new Date()).getTime();
    redisClient.batch()
      .rpush(EVENTS_QUEUE, message, function (error, reply) {
        if (!error && reply) {
          _log('Event "' + message + '" was emitted');
        } else {
          _log('[ERROR] Event emitting failed:', error, 'Reply:' + reply);
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
      } else {
        _log('Event "' + event + '" was processed successfully');
      }
    };

    var lastActivity = (new Date()).getTime();
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

            var nodeId, lastActivity, now;
            for (nodeId in nodesList) {
              now = (new Date()).getTime();
              lastActivity = parseInt(nodesList[nodeId], 10);
              if (now - lastActivity < NODE_ACTIVITY_THRESHOLD) {
                continue;
              }

              var multiExec = redisClient.multi()
                .hdel(NODES_LIST, nodeId, function (error) {
                  if (error) {
                    _log('[ERROR] Removing node from node list failed:', error);
                  }
                })
                .lrem(NODES_IDS_LIST, 0, nodeId, function (error) {
                  if (error) {
                    _log('[ERROR] Removing node id from node ids list failed:', error);
                  }
                });
              if (emitterId === nodeId) {
                multiExec.set(NODES_EMITTER, '', function (error) {
                  if (error) {
                    _log('[ERROR] Unsetting emitter failed:', error);
                  }
                });
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
        var lastActivity = (new Date()).getTime();
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
    var i, message;
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

Node(redisOptions).run();
