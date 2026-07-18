var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
  function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
  return new (P || (P = Promise))(function (resolve, reject) {
    function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
    function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
    function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
    step((generator = generator.apply(thisArg, _arguments || [])).next());
  });
};
import { createPool as createPool2 } from "mysql2";
import { buildToSave, buildToSaveBatch } from "./build";
export * from "./build";
export class resource {
}
export function createPool(conf) {
  if (conf.max && conf.max > 0 && !conf.connectionLimit) {
    conf.connectionLimit = conf.max;
  }
  const pool = createPool2(Object.assign(Object.assign({}, conf), { rowsAsArray: true }));
  return pool;
}
export function getConnection(pool) {
  return new Promise((resolve, reject) => {
    pool.getConnection((err, connection) => {
      if (err) {
        return reject(err);
      }
      return resolve(connection);
    });
  });
}
export function beginTransaction(connection, rollbackIfError) {
  return new Promise((resolve, reject) => {
    connection.beginTransaction((err) => {
      if (err) {
        if (!rollbackIfError) {
          return reject(err);
        }
        connection.rollback(() => {
          return reject(err);
        });
      }
      return resolve();
    });
  });
}
export function commit(connection, rollbackIfError) {
  return new Promise((resolve, reject) => {
    connection.commit((err) => {
      if (err) {
        if (!rollbackIfError) {
          return reject(err);
        }
        connection.rollback(() => {
          return reject(err);
        });
      }
      resolve();
    });
  });
}
export function rollback(connection) {
  return new Promise((resolve, reject) => {
    connection.rollback((err) => {
      if (err) {
        return reject(err);
      }
      resolve();
    });
  });
}
export class PoolManager {
  constructor(pool) {
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
  param(i) {
    return "?";
  }
  beginTransaction() {
    return new Promise((resolve, reject) => {
      this.pool.getConnection((er0, connection) => {
        if (er0) {
          return reject(er0);
        }
        connection.beginTransaction((er1) => {
          if (er1) {
            connection.rollback(() => {
              return reject(er1);
            });
          }
          const tx = new PoolConnectionManager(connection);
          return resolve(tx);
        });
      });
    });
  }
  execute(sql, args, ctx) {
    const p = ctx ? ctx : this.pool;
    return execute(p, sql, args);
  }
  executeBatch(statements, firstSuccess, ctx) {
    const p = ctx ? ctx : this.pool;
    return executeBatch(p, statements, firstSuccess);
  }
  query(sql, args, m, bools, ctx) {
    const p = ctx ? ctx : this.pool;
    return query(p, sql, args, m, bools);
  }
  queryOne(sql, args, m, bools, ctx) {
    const p = ctx ? ctx : this.pool;
    return queryOne(p, sql, args, m, bools);
  }
  executeScalar(sql, args, ctx) {
    const p = ctx ? ctx : this.pool;
    return executeScalar(p, sql, args);
  }
  count(sql, args, ctx) {
    const p = ctx ? ctx : this.pool;
    return count(p, sql, args);
  }
}
export class PoolConnectionManager {
  constructor(connection) {
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
  param(i) {
    return "?";
  }
  commit() {
    return new Promise((resolve, reject) => {
      this.connection.commit((er3) => {
        if (er3) {
          return reject(er3);
        }
        return resolve();
      });
    });
  }
  rollback() {
    return new Promise((resolve, reject) => {
      this.connection.rollback((er3) => {
        if (er3) {
          return reject(er3);
        }
        return resolve();
      });
    });
  }
  execute(sql, args, ctx) {
    const p = ctx ? ctx : this.connection;
    return execute(p, sql, args);
  }
  executeBatch(statements, firstSuccess, ctx) {
    const p = ctx ? ctx : this.connection;
    return executeBatchConnection(p, statements, firstSuccess);
  }
  query(sql, args, m, bools, ctx) {
    const p = ctx ? ctx : this.connection;
    return query(p, sql, args, m, bools);
  }
  queryOne(sql, args, m, bools, ctx) {
    const p = ctx ? ctx : this.connection;
    return queryOne(p, sql, args, m, bools);
  }
  executeScalar(sql, args, ctx) {
    const p = ctx ? ctx : this.connection;
    return executeScalar(p, sql, args);
  }
  count(sql, args, ctx) {
    const p = ctx ? ctx : this.connection;
    return count(p, sql, args);
  }
}
export function executeBatch(pool, statements, firstSuccess) {
  return __awaiter(this, void 0, void 0, function* () {
    if (!statements || statements.length === 0) {
      return Promise.resolve(0);
    }
    else if (statements.length === 1) {
      return execute(pool, statements[0].query, statements[0].params);
    }
    return new Promise((resolve, reject) => {
      pool.getConnection((er0, connection) => {
        if (er0) {
          return reject(er0);
        }
        return executeBatchConnectionTx(connection, statements, firstSuccess);
      });
    });
  });
}
export function executeBatchConnectionTx(connection, statements, firstSuccess) {
  return __awaiter(this, void 0, void 0, function* () {
    if (!statements || statements.length === 0) {
      return Promise.resolve(0);
    }
    else if (statements.length === 1) {
      return execute(connection, statements[0].query, statements[0].params);
    }
    if (resource.multipleStatements) {
      if (firstSuccess) {
        return new Promise((resolve, reject) => {
          connection.beginTransaction((er1) => {
            if (er1) {
              connection.rollback(() => {
                return reject(er1);
              });
            }
            else {
              connection.execute(statements[0].query, toArray(statements[0].params), (er2a, results0) => {
                if (er2a) {
                  connection.rollback(() => {
                    return reject(er2a);
                  });
                }
                else {
                  if (results0 && results0.affectedRows === 0) {
                    connection.commit((er3) => {
                      if (er3) {
                        connection.rollback(() => {
                          return reject(er3);
                        });
                      }
                    });
                    return 0;
                  }
                  else {
                    const queries = [];
                    const params = [];
                    const l = statements.length;
                    for (let j = 1; j < l; j++) {
                      const item = statements[j];
                      if (item.query.endsWith(";")) {
                        queries.push(item.query);
                      }
                      else {
                        queries.push(item.query + ";");
                      }
                      if (item.params && item.params.length > 0) {
                        for (const p of item.params) {
                          params.push(p);
                        }
                      }
                    }
                    connection.query(queries.join(""), toArray(params), (er2, results) => {
                      if (er2) {
                        connection.rollback(() => {
                          return reject(er2);
                        });
                      }
                      else {
                        connection.commit((er3) => {
                          if (er3) {
                            connection.rollback(() => {
                              return reject(er3);
                            });
                          }
                        });
                        let c = 0;
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
      }
      else {
        return new Promise((resolve, reject) => {
          connection.beginTransaction((er1) => {
            if (er1) {
              connection.rollback(() => {
                return reject(er1);
              });
            }
            else {
              const queries = [];
              const params = [];
              statements.forEach((item) => {
                if (item.query.endsWith(";")) {
                  queries.push(item.query);
                }
                else {
                  queries.push(item.query + ";");
                }
                if (item.params && item.params.length > 0) {
                  for (const p of item.params) {
                    params.push(p);
                  }
                }
              });
              connection.query(queries.join(""), toArray(params), (er2, results) => {
                if (er2) {
                  connection.rollback(() => {
                    buildError(er2);
                    return reject(er2);
                  });
                }
                else {
                  connection.commit((er3) => {
                    if (er3) {
                      connection.rollback(() => {
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
      }
    }
    else {
      if (firstSuccess) {
        try {
          yield beginTransaction(connection, true);
          let count = yield execute(connection, statements[0].query, toArray(statements[0].params));
          if (count === 0) {
            commit(connection, true);
            return 0;
          }
          const l = statements.length;
          for (let j = 1; j < l; j++) {
            const item = statements[j];
            const c = yield execute(connection, item.query, toArray(item.params));
            count = count + c;
          }
          commit(connection, true);
          return count;
        }
        catch (err) {
          rollback(connection);
          throw err;
        }
      }
      else {
        try {
          yield beginTransaction(connection, true);
          let count = 0;
          const l = statements.length;
          for (let j = 0; j < l; j++) {
            const item = statements[j];
            const c = yield execute(connection, item.query, toArray(item.params));
            count = count + c;
          }
          commit(connection, true);
          return count;
        }
        catch (err) {
          rollback(connection);
          throw err;
        }
      }
    }
  });
}
export function executeBatchConnection(connection, statements, firstSuccess) {
  return __awaiter(this, void 0, void 0, function* () {
    if (!statements || statements.length === 0) {
      return Promise.resolve(0);
    }
    else if (statements.length === 1) {
      return execute(connection, statements[0].query, statements[0].params);
    }
    if (resource.multipleStatements) {
      if (firstSuccess) {
        return new Promise((resolve, reject) => {
          connection.execute(statements[0].query, toArray(statements[0].params), (er2a, results0) => {
            if (er2a) {
              return reject(er2a);
            }
            else {
              if (results0 && results0.affectedRows === 0) {
                return 0;
              }
              else {
                const queries = [];
                const params = [];
                const l = statements.length;
                for (let j = 1; j < l; j++) {
                  const item = statements[j];
                  if (item.query.endsWith(";")) {
                    queries.push(item.query);
                  }
                  else {
                    queries.push(item.query + ";");
                  }
                  if (item.params && item.params.length > 0) {
                    for (const p of item.params) {
                      params.push(p);
                    }
                  }
                }
                connection.query(queries.join(""), toArray(params), (er2, results) => {
                  if (er2) {
                    return reject(er2);
                  }
                  else {
                    let c = 0;
                    c += results0.affectedRows + results.affectedRows;
                    return resolve(c);
                  }
                });
              }
            }
          });
        });
      }
      else {
        return new Promise((resolve, reject) => {
          const queries = [];
          const params = [];
          statements.forEach((item) => {
            if (item.query.endsWith(";")) {
              queries.push(item.query);
            }
            else {
              queries.push(item.query + ";");
            }
            if (item.params && item.params.length > 0) {
              for (const p of item.params) {
                params.push(p);
              }
            }
          });
          connection.query(queries.join(""), toArray(params), (er2, results) => {
            if (er2) {
              connection.rollback(() => {
                buildError(er2);
                return reject(er2);
              });
            }
            else {
              connection.commit((er3) => {
                if (er3) {
                  connection.rollback(() => {
                    return reject(er3);
                  });
                }
              });
              return resolve(results.affectedRows);
            }
          });
        });
      }
    }
    else {
      if (firstSuccess) {
        try {
          let count = yield execute(connection, statements[0].query, toArray(statements[0].params));
          if (count === 0) {
            return 0;
          }
          const l = statements.length;
          try {
            for (let j = 1; j < l; j++) {
              const item = statements[j];
              const c = yield execute(connection, item.query, toArray(item.params));
              count = count + c;
            }
            return count;
          }
          catch (er1) {
            throw er1;
          }
        }
        catch (er0) {
          throw er0;
        }
      }
      else {
        let count = 0;
        const l = statements.length;
        try {
          for (let j = 0; j < l; j++) {
            const item = statements[j];
            const c = yield execute(connection, item.query, toArray(item.params));
            count = count + c;
          }
          return count;
        }
        catch (err) {
          throw err;
        }
      }
    }
  });
}
function buildError(err) {
  if (err.errno === 1062 && err.code === "ER_DUP_ENTRY") {
    err.error = "duplicate";
  }
  return err;
}
export function execute(pool, sql, args) {
  return __awaiter(this, void 0, void 0, function* () {
    const p = toArray(args);
    return new Promise((resolve, reject) => {
      return pool.execute(sql, p, (err, res) => {
        if (err) {
          buildError(err);
          return reject(err);
        }
        else {
          return resolve(res.affectedRows);
        }
      });
    });
  });
}
export function query(pool, sql, args, m, bools) {
  return __awaiter(this, void 0, void 0, function* () {
    const p = toArray(args);
    return new Promise((resolve, reject) => {
      return pool.query(sql, p, (err, results, fields) => {
        if (err) {
          return reject(err);
        }
        else {
          if (results.length > 0) {
            const arrayResult = results.map((item) => {
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
    });
  });
}
export function queryOne(pool, sql, args, m, bools) {
  return query(pool, sql, args, m, bools).then((r) => {
    return r && r.length > 0 ? r[0] : null;
  });
}
export function executeScalar(pool, sql, args) {
  return queryOne(pool, sql, args).then((r) => {
    if (!r) {
      return null;
    }
    else {
      const keys = Object.keys(r);
      return r[keys[0]];
    }
  });
}
export function count(pool, sql, args) {
  return executeScalar(pool, sql, args);
}
export function save(pool, obj, table, attrs, ver, buildParam) {
  const s = buildToSave(obj, table, attrs, ver, buildParam);
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
export function saveBatch(pool, objs, table, attrs, ver, buildParam) {
  const s = buildToSaveBatch(objs, table, attrs, ver, buildParam);
  if (typeof pool === "function") {
    return pool(s);
  }
  else {
    return executeBatch(pool, s);
  }
}
export function toArray(arr) {
  if (!arr || arr.length === 0) {
    return [];
  }
  const p = [];
  const l = arr.length;
  for (let i = 0; i < l; i++) {
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
            const s = JSON.stringify(arr[i]);
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
export function handleResults(r, m, bools) {
  if (m) {
    const res = mapArray(r, m);
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
export function handleBool(objs, bools) {
  if (!bools || bools.length === 0 || !objs) {
    return objs;
  }
  for (const obj of objs) {
    const o = obj;
    for (const field of bools) {
      if (field.name) {
        const v = o[field.name];
        if (typeof v !== "boolean" && v != null && v !== undefined) {
          const b = field.true;
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
export function formatData(nameColumn, data, m) {
  const result = {};
  nameColumn.forEach((item, index) => {
    let key = item.name;
    if (m) {
      key = m[item.name];
    }
    result[key] = data[index];
  });
  return result;
}
export function map(obj, m) {
  if (!m) {
    return obj;
  }
  const mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return obj;
  }
  const o = {};
  const keys = Object.keys(obj);
  for (const key of keys) {
    let k0 = m[key];
    if (!k0) {
      k0 = key;
    }
    o[k0] = obj[key];
  }
  return o;
}
export function mapArray(results, m) {
  if (!m) {
    return results;
  }
  const mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return results;
  }
  const objs = [];
  const length = results.length;
  for (let i = 0; i < length; i++) {
    const obj = results[i];
    const obj2 = {};
    const keys = Object.keys(obj);
    for (const key of keys) {
      let k0 = m[key];
      if (!k0) {
        k0 = key;
      }
      obj2[k0] = obj[key];
    }
    objs.push(obj2);
  }
  return objs;
}
export function getFields(fields, all) {
  if (!fields || fields.length === 0) {
    return undefined;
  }
  const ext = [];
  if (all) {
    for (const s of fields) {
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
export function buildFields(fields, all) {
  const s = getFields(fields, all);
  if (!s || s.length === 0) {
    return "*";
  }
  else {
    return s.join(",");
  }
}
export function getMapField(name, mp) {
  if (!mp) {
    return name;
  }
  const x = mp[name];
  if (!x) {
    return name;
  }
  if (typeof x === "string") {
    return x;
  }
  return name;
}
export function isEmpty(s) {
  return !(s && s.length > 0);
}
export class StringService {
  constructor(pool, table, column) {
    this.pool = pool;
    this.table = table;
    this.column = column;
    this.load = this.load.bind(this);
    this.save = this.save.bind(this);
  }
  load(key, max) {
    const s = `select ${this.column} from ${this.table} where ${this.column} like ? order by ${this.column} limit ${max}`;
    return query(this.pool, s, ["" + key + "%"]).then((arr) => {
      return arr.map((i) => i[this.column]);
    });
  }
  save(values) {
    if (!values || values.length === 0) {
      return Promise.resolve(0);
    }
    const arr = [];
    for (let i = 1; i <= values.length; i++) {
      arr.push("(?)");
    }
    const s = `insert ignore into ${this.table}(${this.column})values${arr.join(",")}`;
    return execute(this.pool, s, values);
  }
}
export function version(attrs) {
  const ks = Object.keys(attrs);
  for (const k of ks) {
    const attr = attrs[k];
    if (attr.version) {
      attr.name = k;
      return attr;
    }
  }
  return undefined;
}
export class MySQLWriter {
  constructor(pool, table, attributes, oneIfSuccess, toDB, buildParam) {
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
    const x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  write(obj) {
    if (!obj) {
      return Promise.resolve(0);
    }
    let obj2 = obj;
    if (this.map) {
      obj2 = this.map(obj);
    }
    const stmt = buildToSave(obj2, this.table, this.attributes, this.version, this.param);
    if (stmt.query) {
      if (this.execute) {
        if (this.oneIfSuccess) {
          return this.execute(stmt.query, stmt.params).then((ct) => (ct > 0 ? 1 : 0));
        }
        else {
          return this.execute(stmt.query, stmt.params);
        }
      }
      else {
        if (this.oneIfSuccess) {
          return execute(this.pool, stmt.query, stmt.params).then((ct) => (ct > 0 ? 1 : 0));
        }
        else {
          return execute(this.pool, stmt.query, stmt.params);
        }
      }
    }
    else {
      return Promise.resolve(0);
    }
  }
}
export class MySQLStreamWriter {
  constructor(pool, table, attributes, size, toDB, buildParam) {
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
    const x = version(attributes);
    if (x) {
      this.version = x.name;
    }
    if (size) {
      this.size = size;
    }
  }
  write(obj) {
    if (!obj) {
      return Promise.resolve(0);
    }
    let obj2 = obj;
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
  }
  flush() {
    if (!this.list || this.list.length === 0) {
      return Promise.resolve(0);
    }
    else {
      const total = this.list.length;
      const stmt = buildToSaveBatch(this.list, this.table, this.attributes, this.version, this.param);
      if (stmt.length > 0) {
        if (this.executeBatch) {
          return this.executeBatch(stmt).then((r) => {
            this.list = [];
            return total;
          });
        }
        else {
          return executeBatch(this.pool, stmt).then((r) => {
            this.list = [];
            return total;
          });
        }
      }
      else {
        return Promise.resolve(0);
      }
    }
  }
}
export class MySQLBatchWriter {
  constructor(pool, table, attributes, oneIfSuccess, toDB, buildParam) {
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
    const x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  write(objs) {
    if (!objs || objs.length === 0) {
      return Promise.resolve(0);
    }
    let list = objs;
    if (this.map) {
      list = [];
      for (const obj of objs) {
        const obj2 = this.map(obj);
        list.push(obj2);
      }
    }
    const stmts = buildToSaveBatch(list, this.table, this.attributes, this.version, this.param);
    if (stmts.length > 0) {
      if (this.execute) {
        if (this.oneIfSuccess) {
          return this.execute(stmts).then((ct) => stmts.length);
        }
        else {
          return this.execute(stmts);
        }
      }
      else {
        if (this.oneIfSuccess) {
          return executeBatch(this.pool, stmts).then((ct) => stmts.length);
        }
        else {
          return executeBatch(this.pool, stmts);
        }
      }
    }
    else {
      return Promise.resolve(0);
    }
  }
}
export class MySQLChecker {
  constructor(pool, service, timeout) {
    this.pool = pool;
    this.timeout = timeout ? timeout : 4200;
    this.service = service ? service : "mysql";
    this.check = this.check.bind(this);
    this.name = this.name.bind(this);
    this.build = this.build.bind(this);
  }
  check() {
    return __awaiter(this, void 0, void 0, function* () {
      const obj = {};
      const promise = new Promise((resolve, reject) => {
        this.pool.query("select current_time", (err, result) => {
          if (err) {
            return reject(err);
          }
          else {
            resolve(obj);
          }
        });
      });
      if (this.timeout > 0) {
        return promiseTimeOut(this.timeout, promise);
      }
      else {
        return promise;
      }
    });
  }
  name() {
    return this.service;
  }
  build(data, err) {
    if (err) {
      if (!data) {
        data = {};
      }
      data["error"] = err;
    }
    return data;
  }
}
function promiseTimeOut(timeoutInMilliseconds, promise) {
  return Promise.race([
    promise,
    new Promise((resolve, reject) => {
      setTimeout(() => {
        reject(`Timed out in: ${timeoutInMilliseconds} milliseconds!`);
      }, timeoutInMilliseconds);
    }),
  ]);
}
export class Exporter {
  constructor(connection, buildQuery, ft, write, end, attributes) {
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
  export(ctx) {
    return __awaiter(this, void 0, void 0, function* () {
      let idx = 0;
      const stmt = yield this.buildQuery(ctx);
      const reader = this.connection.query(stmt.query, stmt.params);
      let er;
      reader.on("error", (err) => (er = err));
      if (this.map) {
        reader.on("result", (row) => __awaiter(this, void 0, void 0, function* () {
          ++idx;
          this.connection.pause();
          const obj = mapOne(row, this.map);
          const data = this.format(obj);
          this.write(data);
          this.connection.resume();
        }));
      }
      else {
        reader.on("result", (row) => __awaiter(this, void 0, void 0, function* () {
          ++idx;
          this.connection.pause();
          const data = this.format(row);
          this.write(data);
          this.connection.resume();
        }));
      }
      return new Promise((resolve, reject) => {
        reader.on("end", () => {
          this.end();
          if (er) {
            reject(er);
          }
          else {
            this.connection.end((err) => {
              if (err) {
                reject(err);
              }
              else {
                resolve(idx);
              }
            });
          }
        });
      });
    });
  }
}
export class ExportService {
  constructor(connection, queryBuilder, formatter, writer, attributes) {
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
  export(ctx) {
    return __awaiter(this, void 0, void 0, function* () {
      let idx = 0;
      const stmt = yield this.queryBuilder.build(ctx);
      const reader = this.connection.query(stmt.query, stmt.params);
      let er;
      reader.on("error", (err) => (er = err));
      if (this.map) {
        reader.on("result", (row) => __awaiter(this, void 0, void 0, function* () {
          ++idx;
          this.connection.pause();
          const obj = mapOne(row, this.map);
          const data = this.formatter.format(obj);
          this.writer.write(data);
          this.connection.resume();
        }));
      }
      else {
        reader.on("result", (row) => __awaiter(this, void 0, void 0, function* () {
          ++idx;
          this.connection.pause();
          const data = this.formatter.format(row);
          this.writer.write(data);
          this.connection.resume();
        }));
      }
      return new Promise((resolve, reject) => {
        reader.on("end", () => {
          if (this.writer.end) {
            this.writer.end();
          }
          else if (this.writer.flush) {
            this.writer.flush();
          }
          if (er) {
            reject(er);
          }
          else {
            this.connection.end((err) => {
              if (err) {
                reject(err);
              }
              else {
                resolve(idx);
              }
            });
          }
        });
      });
    });
  }
}
export function mapOne(results, m) {
  const obj = results;
  if (!m) {
    return obj;
  }
  const mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return obj;
  }
  const obj2 = {};
  const keys = Object.keys(obj);
  for (const key of keys) {
    let k0 = m[key];
    if (!k0) {
      k0 = key;
    }
    obj2[k0] = obj[key];
  }
  return obj2;
}
export function buildMap(attrs) {
  const mp = {};
  const ks = Object.keys(attrs);
  let isMap = false;
  for (const k of ks) {
    const attr = attrs[k];
    attr.name = k;
    const field = attr.column ? attr.column : k;
    const s = field.toLowerCase();
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
export function select(table, attrs) {
  const cols = [];
  const ks = Object.keys(attrs);
  for (const k of ks) {
    const attr = attrs[k];
    attr.name = k;
    const field = attr.column ? attr.column : k;
    cols.push(field);
  }
  return `select ${cols.join(",")} from ${table}`;
}
