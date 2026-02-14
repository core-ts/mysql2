"use strict";
var __assign = (this && this.__assign) || function () {
  __assign = Object.assign || function(t) {
    for (var s, i = 1, n = arguments.length; i < n; i++) {
      s = arguments[i];
      for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
        t[p] = s[p];
    }
    return t;
  };
  return __assign.apply(this, arguments);
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
  function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
  return new (P || (P = Promise))(function (resolve, reject) {
    function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
    function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
    function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
    step((generator = generator.apply(thisArg, _arguments || [])).next());
  });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
  var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
  return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
  function verb(n) { return function (v) { return step([n, v]); }; }
  function step(op) {
    if (f) throw new TypeError("Generator is already executing.");
    while (_) try {
      if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
      if (y = 0, t) op = [op[0] & 2, t.value];
      switch (op[0]) {
        case 0: case 1: t = op; break;
        case 4: _.label++; return { value: op[1], done: false };
        case 5: _.label++; y = op[1]; op = [0]; continue;
        case 7: op = _.ops.pop(); _.trys.pop(); continue;
        default:
          if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
          if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
          if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
          if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
          if (t[2]) _.ops.pop();
          _.trys.pop(); continue;
      }
      op = body.call(thisArg, _);
    } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
    if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
  }
};
function __export(m) {
  for (var p in m) if (!exports.hasOwnProperty(p)) exports[p] = m[p];
}
Object.defineProperty(exports, "__esModule", { value: true });
var mysql2_1 = require("mysql2");
var build_1 = require("./build");
__export(require("./build"));
var resource = (function () {
  function resource() {
  }
  return resource;
}());
exports.resource = resource;
function createPool(conf) {
  if (conf.max && conf.max > 0 && !conf.connectionLimit) {
    conf.connectionLimit = conf.max;
  }
  var pool = mysql2_1.createPool(__assign(__assign({}, conf), { rowsAsArray: true }));
  return pool;
}
exports.createPool = createPool;
function getConnection(pool) {
  return new Promise(function (resolve, reject) {
    pool.getConnection(function (err, connection) {
      if (err) {
        return reject(err);
      }
      return resolve(connection);
    });
  });
}
exports.getConnection = getConnection;
function beginTransaction(connection, rollbackIfError) {
  return new Promise(function (resolve, reject) {
    connection.beginTransaction(function (err) {
      if (err) {
        if (!rollbackIfError) {
          return reject(err);
        }
        connection.rollback(function () {
          return reject(err);
        });
      }
      return resolve();
    });
  });
}
exports.beginTransaction = beginTransaction;
function commit(connection, rollbackIfError) {
  return new Promise(function (resolve, reject) {
    connection.commit(function (err) {
      if (err) {
        if (!rollbackIfError) {
          return reject(err);
        }
        connection.rollback(function () {
          return reject(err);
        });
      }
      resolve();
    });
  });
}
exports.commit = commit;
function rollback(connection) {
  return new Promise(function (resolve, reject) {
    connection.rollback(function (err) {
      if (err) {
        return reject(err);
      }
      resolve();
    });
  });
}
exports.rollback = rollback;
var PoolManager = (function () {
  function PoolManager(pool) {
    this.pool = pool;
    this.driver = "mysql";
    this.param = this.param.bind(this);
    this.execute = this.execute.bind(this);
    this.executeBatch = this.executeBatch.bind(this);
    this.query = this.query.bind(this);
    this.queryOne = this.queryOne.bind(this);
    this.executeScalar = this.executeScalar.bind(this);
    this.count = this.count.bind(this);
  }
  PoolManager.prototype.param = function (i) {
    return "?";
  };
  PoolManager.prototype.beginTransaction = function () {
    var _this = this;
    return new Promise(function (resolve, reject) {
      _this.pool.getConnection(function (er0, connection) {
        if (er0) {
          return reject(er0);
        }
        connection.beginTransaction(function (er1) {
          if (er1) {
            connection.rollback(function () {
              return reject(er1);
            });
          }
          var tx = new PoolConnectionManager(connection);
          return resolve(tx);
        });
      });
    });
  };
  PoolManager.prototype.execute = function (sql, args, ctx) {
    var p = ctx ? ctx : this.pool;
    return execute(p, sql, args);
  };
  PoolManager.prototype.executeBatch = function (statements, firstSuccess, ctx) {
    var p = ctx ? ctx : this.pool;
    return executeBatch(p, statements, firstSuccess);
  };
  PoolManager.prototype.query = function (sql, args, m, bools, ctx) {
    var p = ctx ? ctx : this.pool;
    return query(p, sql, args, m, bools);
  };
  PoolManager.prototype.queryOne = function (sql, args, m, bools, ctx) {
    var p = ctx ? ctx : this.pool;
    return queryOne(p, sql, args, m, bools);
  };
  PoolManager.prototype.executeScalar = function (sql, args, ctx) {
    var p = ctx ? ctx : this.pool;
    return executeScalar(p, sql, args);
  };
  PoolManager.prototype.count = function (sql, args, ctx) {
    var p = ctx ? ctx : this.pool;
    return count(p, sql, args);
  };
  return PoolManager;
}());
exports.PoolManager = PoolManager;
var PoolConnectionManager = (function () {
  function PoolConnectionManager(connection) {
    this.connection = connection;
    this.driver = "mysql";
    this.param = this.param.bind(this);
    this.execute = this.execute.bind(this);
    this.executeBatch = this.executeBatch.bind(this);
    this.query = this.query.bind(this);
    this.queryOne = this.queryOne.bind(this);
    this.executeScalar = this.executeScalar.bind(this);
    this.count = this.count.bind(this);
  }
  PoolConnectionManager.prototype.param = function (i) {
    return "?";
  };
  PoolConnectionManager.prototype.commit = function () {
    var _this = this;
    return new Promise(function (resolve, reject) {
      _this.connection.commit(function (er3) {
        if (er3) {
          return reject(er3);
        }
        return resolve();
      });
    });
  };
  PoolConnectionManager.prototype.rollback = function () {
    var _this = this;
    return new Promise(function (resolve, reject) {
      _this.connection.rollback(function (er3) {
        if (er3) {
          return reject(er3);
        }
        return resolve();
      });
    });
  };
  PoolConnectionManager.prototype.execute = function (sql, args, ctx) {
    var p = ctx ? ctx : this.connection;
    return execute(p, sql, args);
  };
  PoolConnectionManager.prototype.executeBatch = function (statements, firstSuccess, ctx) {
    var p = ctx ? ctx : this.connection;
    return executeBatchConnection(p, statements, firstSuccess);
  };
  PoolConnectionManager.prototype.query = function (sql, args, m, bools, ctx) {
    var p = ctx ? ctx : this.connection;
    return query(p, sql, args, m, bools);
  };
  PoolConnectionManager.prototype.queryOne = function (sql, args, m, bools, ctx) {
    var p = ctx ? ctx : this.connection;
    return queryOne(p, sql, args, m, bools);
  };
  PoolConnectionManager.prototype.executeScalar = function (sql, args, ctx) {
    var p = ctx ? ctx : this.connection;
    return executeScalar(p, sql, args);
  };
  PoolConnectionManager.prototype.count = function (sql, args, ctx) {
    var p = ctx ? ctx : this.connection;
    return count(p, sql, args);
  };
  return PoolConnectionManager;
}());
exports.PoolConnectionManager = PoolConnectionManager;
function executeBatch(pool, statements, firstSuccess) {
  return __awaiter(this, void 0, void 0, function () {
    var connection;
    return __generator(this, function (_a) {
      switch (_a.label) {
        case 0:
          if (!statements || statements.length === 0) {
            return [2, Promise.resolve(0)];
          }
          else if (statements.length === 1) {
            return [2, execute(pool, statements[0].query, statements[0].params)];
          }
          if (!resource.multipleStatements) return [3, 1];
          if (firstSuccess) {
            return [2, new Promise(function (resolve, reject) {
                pool.getConnection(function (er0, connection) {
                  if (er0) {
                    return reject(er0);
                  }
                  connection.beginTransaction(function (er1) {
                    if (er1) {
                      connection.rollback(function () {
                        return reject(er1);
                      });
                    }
                    else {
                      connection.execute(statements[0].query, toArray(statements[0].params), function (er2a, results0) {
                        if (er2a) {
                          connection.rollback(function () {
                            return reject(er2a);
                          });
                        }
                        else {
                          if (results0 && results0.affectedRows === 0) {
                            connection.commit(function (er3) {
                              if (er3) {
                                connection.rollback(function () {
                                  return reject(er3);
                                });
                              }
                            });
                            return 0;
                          }
                          else {
                            var queries = [];
                            var params = [];
                            var l = statements.length;
                            for (var j = 1; j < l; j++) {
                              var item = statements[j];
                              if (item.query.endsWith(";")) {
                                queries.push(item.query);
                              }
                              else {
                                queries.push(item.query + ";");
                              }
                              if (item.params && item.params.length > 0) {
                                for (var _i = 0, _a = item.params; _i < _a.length; _i++) {
                                  var p = _a[_i];
                                  params.push(p);
                                }
                              }
                            }
                            connection.query(queries.join(""), toArray(params), function (er2, results) {
                              if (er2) {
                                connection.rollback(function () {
                                  return reject(er2);
                                });
                              }
                              else {
                                connection.commit(function (er3) {
                                  if (er3) {
                                    connection.rollback(function () {
                                      return reject(er3);
                                    });
                                  }
                                });
                                var c = 0;
                                c += results0.affectedRows + results.affectedRows;
                                return resolve(c);
                              }
                            });
                          }
                        }
                      });
                    }
                  });
                });
              })];
          }
          else {
            return [2, new Promise(function (resolve, reject) {
                pool.getConnection(function (er0, connection) {
                  if (er0) {
                    return reject(er0);
                  }
                  connection.beginTransaction(function (er1) {
                    if (er1) {
                      connection.rollback(function () {
                        return reject(er1);
                      });
                    }
                    else {
                      var queries_1 = [];
                      var params_1 = [];
                      statements.forEach(function (item) {
                        if (item.query.endsWith(";")) {
                          queries_1.push(item.query);
                        }
                        else {
                          queries_1.push(item.query + ";");
                        }
                        if (item.params && item.params.length > 0) {
                          for (var _i = 0, _a = item.params; _i < _a.length; _i++) {
                            var p = _a[_i];
                            params_1.push(p);
                          }
                        }
                      });
                      connection.query(queries_1.join(""), toArray(params_1), function (er2, results) {
                        if (er2) {
                          connection.rollback(function () {
                            buildError(er2);
                            return reject(er2);
                          });
                        }
                        else {
                          connection.commit(function (er3) {
                            if (er3) {
                              connection.rollback(function () {
                                return reject(er3);
                              });
                            }
                          });
                          return resolve(results.affectedRows);
                        }
                      });
                    }
                  });
                });
              })];
          }
          return [3, 3];
        case 1: return [4, getConnection(pool)];
        case 2:
          connection = _a.sent();
          return [2, executeBatchConnectionTx(connection, statements, firstSuccess)];
        case 3: return [2];
      }
    });
  });
}
exports.executeBatch = executeBatch;
function executeBatchConnectionTx(connection, statements, firstSuccess) {
  return __awaiter(this, void 0, void 0, function () {
    var count_1, l, j, item, c, err_1, count_2, l, j, item, c, err_2;
    return __generator(this, function (_a) {
      switch (_a.label) {
        case 0:
          if (!statements || statements.length === 0) {
            return [2, Promise.resolve(0)];
          }
          else if (statements.length === 1) {
            return [2, execute(connection, statements[0].query, statements[0].params)];
          }
          if (!resource.multipleStatements) return [3, 1];
          if (firstSuccess) {
            return [2, new Promise(function (resolve, reject) {
                connection.beginTransaction(function (er1) {
                  if (er1) {
                    connection.rollback(function () {
                      return reject(er1);
                    });
                  }
                  else {
                    connection.execute(statements[0].query, toArray(statements[0].params), function (er2a, results0) {
                      if (er2a) {
                        connection.rollback(function () {
                          return reject(er2a);
                        });
                      }
                      else {
                        if (results0 && results0.affectedRows === 0) {
                          connection.commit(function (er3) {
                            if (er3) {
                              connection.rollback(function () {
                                return reject(er3);
                              });
                            }
                          });
                          return 0;
                        }
                        else {
                          var queries = [];
                          var params = [];
                          var l = statements.length;
                          for (var j = 1; j < l; j++) {
                            var item = statements[j];
                            if (item.query.endsWith(";")) {
                              queries.push(item.query);
                            }
                            else {
                              queries.push(item.query + ";");
                            }
                            if (item.params && item.params.length > 0) {
                              for (var _i = 0, _a = item.params; _i < _a.length; _i++) {
                                var p = _a[_i];
                                params.push(p);
                              }
                            }
                          }
                          connection.query(queries.join(""), toArray(params), function (er2, results) {
                            if (er2) {
                              connection.rollback(function () {
                                return reject(er2);
                              });
                            }
                            else {
                              connection.commit(function (er3) {
                                if (er3) {
                                  connection.rollback(function () {
                                    return reject(er3);
                                  });
                                }
                              });
                              var c = 0;
                              c += results0.affectedRows + results.affectedRows;
                              return resolve(c);
                            }
                          });
                        }
                      }
                    });
                  }
                });
              })];
          }
          else {
            return [2, new Promise(function (resolve, reject) {
                connection.beginTransaction(function (er1) {
                  if (er1) {
                    connection.rollback(function () {
                      return reject(er1);
                    });
                  }
                  else {
                    var queries_2 = [];
                    var params_2 = [];
                    statements.forEach(function (item) {
                      if (item.query.endsWith(";")) {
                        queries_2.push(item.query);
                      }
                      else {
                        queries_2.push(item.query + ";");
                      }
                      if (item.params && item.params.length > 0) {
                        for (var _i = 0, _a = item.params; _i < _a.length; _i++) {
                          var p = _a[_i];
                          params_2.push(p);
                        }
                      }
                    });
                    connection.query(queries_2.join(""), toArray(params_2), function (er2, results) {
                      if (er2) {
                        connection.rollback(function () {
                          buildError(er2);
                          return reject(er2);
                        });
                      }
                      else {
                        connection.commit(function (er3) {
                          if (er3) {
                            connection.rollback(function () {
                              return reject(er3);
                            });
                          }
                        });
                        return resolve(results.affectedRows);
                      }
                    });
                  }
                });
              })];
          }
          return [3, 18];
        case 1:
          if (!firstSuccess) return [3, 11];
          _a.label = 2;
        case 2:
          _a.trys.push([2, 9, , 10]);
          return [4, beginTransaction(connection, true)];
        case 3:
          _a.sent();
          return [4, execute(connection, statements[0].query, toArray(statements[0].params))];
        case 4:
          count_1 = _a.sent();
          if (count_1 === 0) {
            commit(connection, true);
            return [2, 0];
          }
          l = statements.length;
          j = 1;
          _a.label = 5;
        case 5:
          if (!(j < l)) return [3, 8];
          item = statements[j];
          return [4, execute(connection, item.query, toArray(item.params))];
        case 6:
          c = _a.sent();
          count_1 = count_1 + c;
          _a.label = 7;
        case 7:
          j++;
          return [3, 5];
        case 8:
          commit(connection, true);
          return [2, count_1];
        case 9:
          err_1 = _a.sent();
          rollback(connection);
          throw err_1;
        case 10: return [3, 18];
        case 11:
          _a.trys.push([11, 17, , 18]);
          return [4, beginTransaction(connection, true)];
        case 12:
          _a.sent();
          count_2 = 0;
          l = statements.length;
          j = 0;
          _a.label = 13;
        case 13:
          if (!(j < l)) return [3, 16];
          item = statements[j];
          return [4, execute(connection, item.query, toArray(item.params))];
        case 14:
          c = _a.sent();
          count_2 = count_2 + c;
          _a.label = 15;
        case 15:
          j++;
          return [3, 13];
        case 16:
          commit(connection, true);
          return [2, count_2];
        case 17:
          err_2 = _a.sent();
          rollback(connection);
          throw err_2;
        case 18: return [2];
      }
    });
  });
}
exports.executeBatchConnectionTx = executeBatchConnectionTx;
function executeBatchConnection(connection, statements, firstSuccess) {
  return __awaiter(this, void 0, void 0, function () {
    var count_3, l, j, item, c, er1_1, er0_1, count_4, l, j, item, c, err_3;
    return __generator(this, function (_a) {
      switch (_a.label) {
        case 0:
          if (!statements || statements.length === 0) {
            return [2, Promise.resolve(0)];
          }
          else if (statements.length === 1) {
            return [2, execute(connection, statements[0].query, statements[0].params)];
          }
          if (!resource.multipleStatements) return [3, 1];
          if (firstSuccess) {
            return [2, new Promise(function (resolve, reject) {
                connection.execute(statements[0].query, toArray(statements[0].params), function (er2a, results0) {
                  if (er2a) {
                    return reject(er2a);
                  }
                  else {
                    if (results0 && results0.affectedRows === 0) {
                      return 0;
                    }
                    else {
                      var queries = [];
                      var params = [];
                      var l = statements.length;
                      for (var j = 1; j < l; j++) {
                        var item = statements[j];
                        if (item.query.endsWith(";")) {
                          queries.push(item.query);
                        }
                        else {
                          queries.push(item.query + ";");
                        }
                        if (item.params && item.params.length > 0) {
                          for (var _i = 0, _a = item.params; _i < _a.length; _i++) {
                            var p = _a[_i];
                            params.push(p);
                          }
                        }
                      }
                      connection.query(queries.join(""), toArray(params), function (er2, results) {
                        if (er2) {
                          return reject(er2);
                        }
                        else {
                          var c = 0;
                          c += results0.affectedRows + results.affectedRows;
                          return resolve(c);
                        }
                      });
                    }
                  }
                });
              })];
          }
          else {
            return [2, new Promise(function (resolve, reject) {
                var queries = [];
                var params = [];
                statements.forEach(function (item) {
                  if (item.query.endsWith(";")) {
                    queries.push(item.query);
                  }
                  else {
                    queries.push(item.query + ";");
                  }
                  if (item.params && item.params.length > 0) {
                    for (var _i = 0, _a = item.params; _i < _a.length; _i++) {
                      var p = _a[_i];
                      params.push(p);
                    }
                  }
                });
                connection.query(queries.join(""), toArray(params), function (er2, results) {
                  if (er2) {
                    connection.rollback(function () {
                      buildError(er2);
                      return reject(er2);
                    });
                  }
                  else {
                    connection.commit(function (er3) {
                      if (er3) {
                        connection.rollback(function () {
                          return reject(er3);
                        });
                      }
                    });
                    return resolve(results.affectedRows);
                  }
                });
              })];
          }
          return [3, 20];
        case 1:
          if (!firstSuccess) return [3, 13];
          _a.label = 2;
        case 2:
          _a.trys.push([2, 11, , 12]);
          return [4, execute(connection, statements[0].query, toArray(statements[0].params))];
        case 3:
          count_3 = _a.sent();
          if (count_3 === 0) {
            return [2, 0];
          }
          l = statements.length;
          _a.label = 4;
        case 4:
          _a.trys.push([4, 9, , 10]);
          j = 1;
          _a.label = 5;
        case 5:
          if (!(j < l)) return [3, 8];
          item = statements[j];
          return [4, execute(connection, item.query, toArray(item.params))];
        case 6:
          c = _a.sent();
          count_3 = count_3 + c;
          _a.label = 7;
        case 7:
          j++;
          return [3, 5];
        case 8: return [2, count_3];
        case 9:
          er1_1 = _a.sent();
          throw er1_1;
        case 10: return [3, 12];
        case 11:
          er0_1 = _a.sent();
          throw er0_1;
        case 12: return [3, 20];
        case 13:
          count_4 = 0;
          l = statements.length;
          _a.label = 14;
        case 14:
          _a.trys.push([14, 19, , 20]);
          j = 0;
          _a.label = 15;
        case 15:
          if (!(j < l)) return [3, 18];
          item = statements[j];
          return [4, execute(connection, item.query, toArray(item.params))];
        case 16:
          c = _a.sent();
          count_4 = count_4 + c;
          _a.label = 17;
        case 17:
          j++;
          return [3, 15];
        case 18: return [2, count_4];
        case 19:
          err_3 = _a.sent();
          throw err_3;
        case 20: return [2];
      }
    });
  });
}
exports.executeBatchConnection = executeBatchConnection;
function buildError(err) {
  if (err.errno === 1062 && err.code === "ER_DUP_ENTRY") {
    err.error = "duplicate";
  }
  return err;
}
function execute(pool, sql, args) {
  return __awaiter(this, void 0, void 0, function () {
    var p;
    return __generator(this, function (_a) {
      p = toArray(args);
      return [2, new Promise(function (resolve, reject) {
          return pool.execute(sql, p, function (err, res) {
            if (err) {
              buildError(err);
              return reject(err);
            }
            else {
              return resolve(res.affectedRows);
            }
          });
        })];
    });
  });
}
exports.execute = execute;
function query(pool, sql, args, m, bools) {
  return __awaiter(this, void 0, void 0, function () {
    var p;
    return __generator(this, function (_a) {
      p = toArray(args);
      return [2, new Promise(function (resolve, reject) {
          return pool.query(sql, p, function (err, results, fields) {
            if (err) {
              return reject(err);
            }
            else {
              if (results.length > 0) {
                var arrayResult = results.map(function (item) {
                  if (Array.isArray(item)) {
                    return formatData(fields, item);
                  }
                  else {
                    return item;
                  }
                });
                return resolve(handleResults(arrayResult, m, bools));
              }
              else {
                resolve([]);
              }
            }
          });
        })];
    });
  });
}
exports.query = query;
function queryOne(pool, sql, args, m, bools) {
  return query(pool, sql, args, m, bools).then(function (r) {
    return r && r.length > 0 ? r[0] : null;
  });
}
exports.queryOne = queryOne;
function executeScalar(pool, sql, args) {
  return queryOne(pool, sql, args).then(function (r) {
    if (!r) {
      return null;
    }
    else {
      var keys = Object.keys(r);
      return r[keys[0]];
    }
  });
}
exports.executeScalar = executeScalar;
function count(pool, sql, args) {
  return executeScalar(pool, sql, args);
}
exports.count = count;
function save(pool, obj, table, attrs, ver, buildParam, i) {
  var s = build_1.buildToSave(obj, table, attrs, ver, buildParam);
  if (!s.query) {
    return Promise.resolve(-1);
  }
  if (typeof pool === "function") {
    return pool(s.query, s.params);
  }
  else {
    return execute(pool, s.query, s.params);
  }
}
exports.save = save;
function saveBatch(pool, objs, table, attrs, ver, buildParam) {
  var s = build_1.buildToSaveBatch(objs, table, attrs, ver, buildParam);
  if (typeof pool === "function") {
    return pool(s);
  }
  else {
    return executeBatch(pool, s);
  }
}
exports.saveBatch = saveBatch;
function toArray(arr) {
  if (!arr || arr.length === 0) {
    return [];
  }
  var p = [];
  var l = arr.length;
  for (var i = 0; i < l; i++) {
    if (arr[i] === undefined || arr[i] == null) {
      p.push(null);
    }
    else {
      if (typeof arr[i] === "object") {
        if (arr[i] instanceof Date) {
          p.push(arr[i]);
        }
        else {
          if (resource.string) {
            var s = JSON.stringify(arr[i]);
            p.push(s);
          }
          else {
            p.push(arr[i]);
          }
        }
      }
      else {
        p.push(arr[i]);
      }
    }
  }
  return p;
}
exports.toArray = toArray;
function handleResults(r, m, bools) {
  if (m) {
    var res = mapArray(r, m);
    if (bools && bools.length > 0) {
      return handleBool(res, bools);
    }
    else {
      return res;
    }
  }
  else {
    if (bools && bools.length > 0) {
      return handleBool(r, bools);
    }
    else {
      return r;
    }
  }
}
exports.handleResults = handleResults;
function handleBool(objs, bools) {
  if (!bools || bools.length === 0 || !objs) {
    return objs;
  }
  for (var _i = 0, objs_1 = objs; _i < objs_1.length; _i++) {
    var obj = objs_1[_i];
    var o = obj;
    for (var _a = 0, bools_1 = bools; _a < bools_1.length; _a++) {
      var field = bools_1[_a];
      if (field.name) {
        var v = o[field.name];
        if (typeof v !== "boolean" && v != null && v !== undefined) {
          var b = field.true;
          if (b == null || b === undefined) {
            o[field.name] = "1" == v || "T" == v || "Y" == v || "true" == v || "on" == v;
          }
          else {
            o[field.name] = v == b ? true : false;
          }
        }
      }
    }
  }
  return objs;
}
exports.handleBool = handleBool;
function formatData(nameColumn, data, m) {
  var result = {};
  nameColumn.forEach(function (item, index) {
    var key = item.name;
    if (m) {
      key = m[item.name];
    }
    result[key] = data[index];
  });
  return result;
}
exports.formatData = formatData;
function map(obj, m) {
  if (!m) {
    return obj;
  }
  var mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return obj;
  }
  var o = {};
  var keys = Object.keys(obj);
  for (var _i = 0, keys_1 = keys; _i < keys_1.length; _i++) {
    var key = keys_1[_i];
    var k0 = m[key];
    if (!k0) {
      k0 = key;
    }
    o[k0] = obj[key];
  }
  return o;
}
exports.map = map;
function mapArray(results, m) {
  if (!m) {
    return results;
  }
  var mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return results;
  }
  var objs = [];
  var length = results.length;
  for (var i = 0; i < length; i++) {
    var obj = results[i];
    var obj2 = {};
    var keys = Object.keys(obj);
    for (var _i = 0, keys_2 = keys; _i < keys_2.length; _i++) {
      var key = keys_2[_i];
      var k0 = m[key];
      if (!k0) {
        k0 = key;
      }
      obj2[k0] = obj[key];
    }
    objs.push(obj2);
  }
  return objs;
}
exports.mapArray = mapArray;
function getFields(fields, all) {
  if (!fields || fields.length === 0) {
    return undefined;
  }
  var ext = [];
  if (all) {
    for (var _i = 0, fields_1 = fields; _i < fields_1.length; _i++) {
      var s = fields_1[_i];
      if (all.includes(s)) {
        ext.push(s);
      }
    }
    if (ext.length === 0) {
      return undefined;
    }
    else {
      return ext;
    }
  }
  else {
    return fields;
  }
}
exports.getFields = getFields;
function buildFields(fields, all) {
  var s = getFields(fields, all);
  if (!s || s.length === 0) {
    return "*";
  }
  else {
    return s.join(",");
  }
}
exports.buildFields = buildFields;
function getMapField(name, mp) {
  if (!mp) {
    return name;
  }
  var x = mp[name];
  if (!x) {
    return name;
  }
  if (typeof x === "string") {
    return x;
  }
  return name;
}
exports.getMapField = getMapField;
function isEmpty(s) {
  return !(s && s.length > 0);
}
exports.isEmpty = isEmpty;
var StringService = (function () {
  function StringService(pool, table, column) {
    this.pool = pool;
    this.table = table;
    this.column = column;
    this.load = this.load.bind(this);
    this.save = this.save.bind(this);
  }
  StringService.prototype.load = function (key, max) {
    var _this = this;
    var s = "select " + this.column + " from " + this.table + " where " + this.column + " like ? order by " + this.column + " limit " + max;
    return query(this.pool, s, ["" + key + "%"]).then(function (arr) {
      return arr.map(function (i) { return i[_this.column]; });
    });
  };
  StringService.prototype.save = function (values) {
    if (!values || values.length === 0) {
      return Promise.resolve(0);
    }
    var arr = [];
    for (var i = 1; i <= values.length; i++) {
      arr.push("(?)");
    }
    var s = "insert ignore into " + this.table + "(" + this.column + ")values" + arr.join(",");
    return execute(this.pool, s, values);
  };
  return StringService;
}());
exports.StringService = StringService;
function version(attrs) {
  var ks = Object.keys(attrs);
  for (var _i = 0, ks_1 = ks; _i < ks_1.length; _i++) {
    var k = ks_1[_i];
    var attr = attrs[k];
    if (attr.version) {
      attr.name = k;
      return attr;
    }
  }
  return undefined;
}
exports.version = version;
var MySQLWriter = (function () {
  function MySQLWriter(pool, table, attributes, oneIfSuccess, toDB, buildParam) {
    this.table = table;
    this.attributes = attributes;
    this.oneIfSuccess = oneIfSuccess;
    this.write = this.write.bind(this);
    if (typeof pool === "function") {
      this.execute = pool;
    }
    else {
      this.pool = pool;
    }
    this.param = buildParam;
    this.map = toDB;
    var x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  MySQLWriter.prototype.write = function (obj) {
    if (!obj) {
      return Promise.resolve(0);
    }
    var obj2 = obj;
    if (this.map) {
      obj2 = this.map(obj);
    }
    var stmt = build_1.buildToSave(obj2, this.table, this.attributes, this.version, this.param);
    if (stmt) {
      if (this.execute) {
        if (this.oneIfSuccess) {
          return this.execute(stmt.query, stmt.params).then(function (ct) { return (ct > 0 ? 1 : 0); });
        }
        else {
          return this.execute(stmt.query, stmt.params);
        }
      }
      else {
        if (this.oneIfSuccess) {
          return execute(this.pool, stmt.query, stmt.params).then(function (ct) { return (ct > 0 ? 1 : 0); });
        }
        else {
          return execute(this.pool, stmt.query, stmt.params);
        }
      }
    }
    else {
      return Promise.resolve(0);
    }
  };
  return MySQLWriter;
}());
exports.MySQLWriter = MySQLWriter;
var MySQLStreamWriter = (function () {
  function MySQLStreamWriter(pool, table, attributes, size, toDB, buildParam) {
    this.table = table;
    this.attributes = attributes;
    this.list = [];
    this.size = 0;
    this.write = this.write.bind(this);
    this.flush = this.flush.bind(this);
    if (typeof pool === "function") {
      this.executeBatch = pool;
    }
    else {
      this.pool = pool;
    }
    this.param = buildParam;
    this.map = toDB;
    var x = version(attributes);
    if (x) {
      this.version = x.name;
    }
    if (size) {
      this.size = size;
    }
  }
  MySQLStreamWriter.prototype.write = function (obj) {
    if (!obj) {
      return Promise.resolve(0);
    }
    var obj2 = obj;
    if (this.map) {
      obj2 = this.map(obj);
      this.list.push(obj2);
    }
    else {
      this.list.push(obj);
    }
    if (this.list.length < this.size) {
      return Promise.resolve(0);
    }
    else {
      return this.flush();
    }
  };
  MySQLStreamWriter.prototype.flush = function () {
    var _this = this;
    if (!this.list || this.list.length === 0) {
      return Promise.resolve(0);
    }
    else {
      var total_1 = this.list.length;
      var stmt = build_1.buildToSaveBatch(this.list, this.table, this.attributes, this.version, this.param);
      if (stmt) {
        if (this.executeBatch) {
          return this.executeBatch(stmt).then(function (r) {
            _this.list = [];
            return total_1;
          });
        }
        else {
          return executeBatch(this.pool, stmt).then(function (r) {
            _this.list = [];
            return total_1;
          });
        }
      }
      else {
        return Promise.resolve(0);
      }
    }
  };
  return MySQLStreamWriter;
}());
exports.MySQLStreamWriter = MySQLStreamWriter;
var MySQLBatchWriter = (function () {
  function MySQLBatchWriter(pool, table, attributes, oneIfSuccess, toDB, buildParam) {
    this.table = table;
    this.attributes = attributes;
    this.oneIfSuccess = oneIfSuccess;
    this.write = this.write.bind(this);
    if (typeof pool === "function") {
      this.execute = pool;
    }
    else {
      this.pool = pool;
    }
    this.param = buildParam;
    this.map = toDB;
    var x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  MySQLBatchWriter.prototype.write = function (objs) {
    if (!objs || objs.length === 0) {
      return Promise.resolve(0);
    }
    var list = objs;
    if (this.map) {
      list = [];
      for (var _i = 0, objs_2 = objs; _i < objs_2.length; _i++) {
        var obj = objs_2[_i];
        var obj2 = this.map(obj);
        list.push(obj2);
      }
    }
    var stmts = build_1.buildToSaveBatch(list, this.table, this.attributes, this.version, this.param);
    if (stmts && stmts.length > 0) {
      if (this.execute) {
        if (this.oneIfSuccess) {
          return this.execute(stmts).then(function (ct) { return stmts.length; });
        }
        else {
          return this.execute(stmts);
        }
      }
      else {
        if (this.oneIfSuccess) {
          return executeBatch(this.pool, stmts).then(function (ct) { return stmts.length; });
        }
        else {
          return executeBatch(this.pool, stmts);
        }
      }
    }
    else {
      return Promise.resolve(0);
    }
  };
  return MySQLBatchWriter;
}());
exports.MySQLBatchWriter = MySQLBatchWriter;
var MySQLChecker = (function () {
  function MySQLChecker(pool, service, timeout) {
    this.pool = pool;
    this.timeout = timeout ? timeout : 4200;
    this.service = service ? service : "mysql";
    this.check = this.check.bind(this);
    this.name = this.name.bind(this);
    this.build = this.build.bind(this);
  }
  MySQLChecker.prototype.check = function () {
    return __awaiter(this, void 0, void 0, function () {
      var obj, promise;
      var _this = this;
      return __generator(this, function (_a) {
        obj = {};
        promise = new Promise(function (resolve, reject) {
          _this.pool.query("select current_time", function (err, result) {
            if (err) {
              return reject(err);
            }
            else {
              resolve(obj);
            }
          });
        });
        if (this.timeout > 0) {
          return [2, promiseTimeOut(this.timeout, promise)];
        }
        else {
          return [2, promise];
        }
        return [2];
      });
    });
  };
  MySQLChecker.prototype.name = function () {
    return this.service;
  };
  MySQLChecker.prototype.build = function (data, err) {
    if (err) {
      if (!data) {
        data = {};
      }
      data["error"] = err;
    }
    return data;
  };
  return MySQLChecker;
}());
exports.MySQLChecker = MySQLChecker;
function promiseTimeOut(timeoutInMilliseconds, promise) {
  return Promise.race([
    promise,
    new Promise(function (resolve, reject) {
      setTimeout(function () {
        reject("Timed out in: " + timeoutInMilliseconds + " milliseconds!");
      }, timeoutInMilliseconds);
    }),
  ]);
}
var Exporter = (function () {
  function Exporter(connection, buildQuery, ft, write, end, attributes) {
    this.connection = connection;
    this.buildQuery = buildQuery;
    this.write = write;
    this.end = end;
    this.attributes = attributes;
    this.format = ft;
    if (attributes) {
      this.map = buildMap(attributes);
    }
    this.export = this.export.bind(this);
  }
  Exporter.prototype.export = function (ctx) {
    return __awaiter(this, void 0, void 0, function () {
      var idx, stmt, reader, er;
      var _this = this;
      return __generator(this, function (_a) {
        switch (_a.label) {
          case 0:
            idx = 0;
            return [4, this.buildQuery(ctx)];
          case 1:
            stmt = _a.sent();
            reader = this.connection.query(stmt.query, stmt.params);
            reader.on("error", function (err) { return (er = err); });
            if (this.map) {
              reader.on("result", function (row) { return __awaiter(_this, void 0, void 0, function () {
                var obj, data;
                return __generator(this, function (_a) {
                  ++idx;
                  this.connection.pause();
                  obj = mapOne(row, this.map);
                  data = this.format(obj);
                  this.write(data);
                  this.connection.resume();
                  return [2];
                });
              }); });
            }
            else {
              reader.on("result", function (row) { return __awaiter(_this, void 0, void 0, function () {
                var data;
                return __generator(this, function (_a) {
                  ++idx;
                  this.connection.pause();
                  data = this.format(row);
                  this.write(data);
                  this.connection.resume();
                  return [2];
                });
              }); });
            }
            return [2, new Promise(function (resolve, reject) {
                reader.on("end", function () {
                  _this.end();
                  if (er) {
                    reject(er);
                  }
                  else {
                    _this.connection.end(function (err) {
                      if (err) {
                        reject(err);
                      }
                      else {
                        resolve(idx);
                      }
                    });
                  }
                });
              })];
        }
      });
    });
  };
  return Exporter;
}());
exports.Exporter = Exporter;
var ExportService = (function () {
  function ExportService(connection, queryBuilder, formatter, writer, attributes) {
    this.connection = connection;
    this.queryBuilder = queryBuilder;
    this.formatter = formatter;
    this.writer = writer;
    this.attributes = attributes;
    if (attributes) {
      this.map = buildMap(attributes);
    }
    this.export = this.export.bind(this);
  }
  ExportService.prototype.export = function (ctx) {
    return __awaiter(this, void 0, void 0, function () {
      var idx, stmt, reader, er;
      var _this = this;
      return __generator(this, function (_a) {
        switch (_a.label) {
          case 0:
            idx = 0;
            return [4, this.queryBuilder.build(ctx)];
          case 1:
            stmt = _a.sent();
            reader = this.connection.query(stmt.query, stmt.params);
            reader.on("error", function (err) { return (er = err); });
            if (this.map) {
              reader.on("result", function (row) { return __awaiter(_this, void 0, void 0, function () {
                var obj, data;
                return __generator(this, function (_a) {
                  ++idx;
                  this.connection.pause();
                  obj = mapOne(row, this.map);
                  data = this.formatter.format(obj);
                  this.writer.write(data);
                  this.connection.resume();
                  return [2];
                });
              }); });
            }
            else {
              reader.on("result", function (row) { return __awaiter(_this, void 0, void 0, function () {
                var data;
                return __generator(this, function (_a) {
                  ++idx;
                  this.connection.pause();
                  data = this.formatter.format(row);
                  this.writer.write(data);
                  this.connection.resume();
                  return [2];
                });
              }); });
            }
            return [2, new Promise(function (resolve, reject) {
                reader.on("end", function () {
                  if (_this.writer.end) {
                    _this.writer.end();
                  }
                  else if (_this.writer.flush) {
                    _this.writer.flush();
                  }
                  if (er) {
                    reject(er);
                  }
                  else {
                    _this.connection.end(function (err) {
                      if (err) {
                        reject(err);
                      }
                      else {
                        resolve(idx);
                      }
                    });
                  }
                });
              })];
        }
      });
    });
  };
  return ExportService;
}());
exports.ExportService = ExportService;
function mapOne(results, m) {
  var obj = results;
  if (!m) {
    return obj;
  }
  var mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return obj;
  }
  var obj2 = {};
  var keys = Object.keys(obj);
  for (var _i = 0, keys_3 = keys; _i < keys_3.length; _i++) {
    var key = keys_3[_i];
    var k0 = m[key];
    if (!k0) {
      k0 = key;
    }
    obj2[k0] = obj[key];
  }
  return obj2;
}
exports.mapOne = mapOne;
function buildMap(attrs) {
  var mp = {};
  var ks = Object.keys(attrs);
  var isMap = false;
  for (var _i = 0, ks_2 = ks; _i < ks_2.length; _i++) {
    var k = ks_2[_i];
    var attr = attrs[k];
    attr.name = k;
    var field = attr.column ? attr.column : k;
    var s = field.toLowerCase();
    if (s !== k) {
      mp[s] = k;
      isMap = true;
    }
  }
  if (isMap) {
    return mp;
  }
  return undefined;
}
exports.buildMap = buildMap;
function select(table, attrs) {
  var cols = [];
  var ks = Object.keys(attrs);
  for (var _i = 0, ks_3 = ks; _i < ks_3.length; _i++) {
    var k = ks_3[_i];
    var attr = attrs[k];
    attr.name = k;
    var field = attr.column ? attr.column : k;
    cols.push(field);
  }
  return "select " + cols.join(",") + " from " + table;
}
exports.select = select;
