#!/usr/bin/env node
'use strict';

const redis = require('redis');
const redisClient = redis.createClient({
  host: '127.0.0.1',
  port: 6379,
  db: 1,
  prefix: 'one2trip:'
});

// Node for distributed message processing
var Node = function (redisClient) {
  const EVENTS_QUEUE = 'nodes:queue';
  const NODES_LIST = 'nodes:ids:list';
  const NODES_TABLE = 'nodes:table';
  const NODES_EMITTER = 'nodes:emitter';
  const NODES_KEY_TTL = 10; // sec
  const EVENT_SENDING_INTERVAL = 500; // ms
  const NODE_HEALTH_CHECK_INTERVAL = 200; // ms
  const NODE_ACTIVITY_THRESHOLD = 1000; // ms

  var workLoopInterval;

  redisClient.on('error', function (error) {
    _log('Redis error: ' + error);
    clearInterval(workLoopInterval);
  });

  var id = 'node-' + (new Date()).getTime() + '-' + Math.round(Math.random() * 1000);

  function run() {
    _registerNode();

    var workLoop = function _workLoop () {
      redisClient.get(NODES_EMITTER, function (error, emitterId) {
        if (error) {
          _log(error);
          return;
        }

        if (emitterId === id) {
          _emitEvent();
        } else {
          _receiveEvent();
        }
      });
    };
    workLoopInterval = setInterval(workLoop, 0);

    _nodesHealthCheck();
  }

  function _registerNode() {
    _log('Register node: ' + id);

    var lastActivity = (new Date()).getTime();
    redisClient.multi()
      .hset(NODES_TABLE, id, lastActivity, function (error) {
        if (error) {
          _log('Add node to nodes list failed: ' + error);
        }
      })
      .rpush(NODES_LIST, id, function (error) {
        if (error) {
          _log('Add node id to nodes ids list failed: ' + error);
        }
      })
      .exec(function (error) {
        if (error) {
          _log('Node registration failed: ' + error);
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
          _log('Error occurred during event emitting: ' + error + ' Reply: ' + reply);
        }
      })
      .hset(NODES_TABLE, id, lastActivity)
      .exec(function (error) {
        if (error) {
          _log('Error occurred during event enqueue or node last activity updating');
          _log(error);
        }
      });
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
        _log('Event "' + event + '" was processed with error');
      } else {
        _log('Event "' + event + '" was processed successfully');
      }
    };

    var lastActivity = (new Date()).getTime();
    redisClient.batch()
      .hset(NODES_TABLE, id, lastActivity)
      .lpop(EVENTS_QUEUE, function (error, reply) {
        if (null === reply) {
          return;
        }
        if (error) {
          _log('Error occurred during events dequeue: ' + error);
          return;
        }
        _eventHandler(reply, eventHandlerCallback);
      })
      .exec(function (error) {
        if (error) {
          _log('Error occurred during events dequeue or node last activity updating: ' + error);
        }
      });
  }

  function _nodesHealthCheck() {
    var healthCheck = function _healthCheck() {
      redisClient.batch()
        .get(NODES_EMITTER, function (error, emitterId) {
          if (error) {
            _log(error);
            return;
          }

          if (null === emitterId || '' === emitterId) {
            _log('Emitter not found');
            _assignEmitter();
          }

          redisClient.hgetall(NODES_TABLE, function (error, nodesList) {
            if (error) {
              _log('Getting node list failed: ' + error);
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
                .hdel(NODES_TABLE, nodeId, function (error) {
                  if (error) {
                    _log('Removing node from node list failed: ' + error);
                  }
                })
                .lrem(NODES_LIST, 0, nodeId, function (error) {
                  if (error) {
                    _log('Removing node id from node ids list failed: ' + error);
                  }
                });
              if (emitterId === nodeId) {
                multiExec.set(NODES_EMITTER, '', function (error) {
                  if (error) {
                    _log('Clean up emitter failed: ' + error);
                  }
                });
              }
              multiExec.exec(function (error) {
                if (error) {
                  _log('Remove inactive node failed: ' + error);
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
        .expire(NODES_LIST, NODES_KEY_TTL)
        .expire(NODES_TABLE, NODES_KEY_TTL)
        .expire(EVENTS_QUEUE, NODES_KEY_TTL)
        .exec(function (error) {
          if (error) {
            _log('Error occurred during health check: ' + error);
          }
        });
    };
    setInterval(healthCheck, NODE_HEALTH_CHECK_INTERVAL);
  }

  function _assignEmitter() {
    redisClient.lindex(NODES_LIST, 0, function (error, id) {
      if (error) {
        _log('Assign new emitter failed');
        _log(error);
        return;
      }

      if (id) {
        var lastActivity = (new Date()).getTime();
        redisClient.batch()
          .set(NODES_EMITTER, id, function (error) {
            if (error) {
              _log('Assign new emitter failed: ' + error);
            }
          })
          .hset(NODES_TABLE, id, lastActivity)
          .exec();
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
    run: run
  };
};

Node(redisClient).run();
