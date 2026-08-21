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
export function param(i) {
  return "?";
}
export function params(length, from) {
  if (from == null) {
    from = 0;
  }
  const ps = [];
  for (let i = 1; i <= length; i++) {
    ps.push(param(i + from));
  }
  return ps;
}
export function metadata(attrs) {
  const mp = {};
  const ks = Object.keys(attrs);
  const ats = [];
  const bools = [];
  const fields = [];
  const m = { keys: ats, fields };
  let isMap = false;
  for (const k of ks) {
    const attr = attrs[k];
    attr.name = k;
    if (attr.key) {
      ats.push(attr);
    }
    if (!attr.ignored) {
      fields.push(k);
    }
    if (attr.type === "boolean") {
      bools.push(attr);
    }
    if (attr.version) {
      m.version = k;
    }
    const field = attr.column ? attr.column : k;
    const s = field.toLowerCase();
    if (s !== k) {
      mp[s] = k;
      isMap = true;
    }
  }
  if (isMap) {
    m.map = mp;
  }
  if (bools.length > 0) {
    m.bools = bools;
  }
  return m;
}
export function buildToSave(obj, table, attrs, buildParam, i) {
  if (!i) {
    i = 1;
  }
  if (!buildParam) {
    buildParam = param;
  }
  const ks = Object.keys(attrs);
  const pks = [];
  const cols = [];
  const values = [];
  const args = [];
  let isVersion = false;
  const o = obj;
  for (const k of ks) {
    const attr = attrs[k];
    attr.name = k;
    if (attr.key) {
      pks.push(attr);
    }
    let v = o[k];
    if (v == null) {
      v = attr.default;
    }
    if (v != null && !attr.ignored && !attr.noinsert) {
      const field = attr.column ? attr.column : k;
      cols.push(field);
      if (attr.version) {
        isVersion = true;
        values.push(`${1}`);
      }
      else {
        if (v === "") {
          values.push(`''`);
        }
        else if (typeof v === "number") {
          values.push(toString(v));
        }
        else {
          const p = buildParam(i++);
          values.push(p);
          if (typeof v === "boolean") {
            if (v === true) {
              const v2 = attr.true !== undefined ? attr.true : `'1'`;
              args.push(v2);
            }
            else {
              const v2 = attr.false !== undefined ? attr.false : `'0'`;
              args.push(v2);
            }
          }
          else {
            args.push(v);
          }
        }
      }
    }
  }
  if (pks.length === 0) {
    if (cols.length === 0) {
      return { query: "", params: args };
    }
    else {
      const q = `insert into ${table}(${cols.join(",")})values(${values.join(",")})`;
      return { query: q, params: args };
    }
  }
  else {
    const colSet = [];
    for (const k of ks) {
      const v = o[k];
      if (v !== undefined) {
        const attr = attrs[k];
        if (attr && !attr.key && !attr.ignored && !attr.noupdate) {
          const field = attr.column ? attr.column : k;
          let x;
          if (v === null) {
            x = "null";
          }
          else if (v === "") {
            x = `''`;
          }
          else if (typeof v === "number") {
            x = toString(v);
          }
          else {
            x = buildParam(i++);
            if (typeof v === "boolean") {
              if (v === true) {
                const v2 = attr.true !== undefined ? attr.true : `'1'`;
                args.push(v2);
              }
              else {
                const v2 = attr.false !== undefined ? attr.false : `'0'`;
                args.push(v2);
              }
            }
            else {
              args.push(v);
            }
          }
          colSet.push(`${field}=${x}`);
        }
      }
    }
    if (colSet.length === 0) {
      const q = `insert ignore into ${table}(${cols.join(",")})values(${values.join(",")})`;
      return { query: q, params: args };
    }
    else {
      const q = `insert into ${table}(${cols.join(",")})values(${values.join(",")}) on duplicate key update ${colSet.join(",")}`;
      return { query: q, params: args };
    }
  }
}
export function buildToSaveBatch(objs, table, attrs, buildParam) {
  if (!buildParam) {
    buildParam = param;
  }
  const sts = [];
  const ks = Object.keys(attrs);
  for (const obj of objs) {
    let i = 1;
    const cols = [];
    const values = [];
    const args = [];
    let isVersion = false;
    const o = obj;
    for (const k of ks) {
      const attr = attrs[k];
      let v = o[k];
      if (v == null) {
        v = attr.default;
      }
      if (v != null && !attr.ignored && !attr.noinsert) {
        const field = attr.column ? attr.column : k;
        cols.push(field);
        if (attr.version) {
          isVersion = true;
          values.push(`${1}`);
        }
        else {
          if (v === "") {
            values.push(`''`);
          }
          else if (typeof v === "number") {
            values.push(toString(v));
          }
          else {
            const p = buildParam(i++);
            values.push(p);
            if (typeof v === "boolean") {
              if (v === true) {
                const v2 = attr.true !== undefined ? attr.true : `'1'`;
                args.push(v2);
              }
              else {
                const v2 = attr.false !== undefined ? attr.false : `'0'`;
                args.push(v2);
              }
            }
            else {
              args.push(v);
            }
          }
        }
      }
    }
    const colSet = [];
    for (const k of ks) {
      const v = o[k];
      if (v !== undefined) {
        const attr = attrs[k];
        if (attr && !attr.key && !attr.ignored && !attr.noupdate) {
          const field = attr.column ? attr.column : k;
          let x;
          if (v === null) {
            x = "null";
          }
          else if (v === "") {
            x = `''`;
          }
          else if (typeof v === "number") {
            x = toString(v);
          }
          else {
            x = buildParam(i++);
            if (typeof v === "boolean") {
              if (v === true) {
                const v2 = attr.true !== undefined ? attr.true : `'1'`;
                args.push(v2);
              }
              else {
                const v2 = attr.false !== undefined ? attr.false : `'0'`;
                args.push(v2);
              }
            }
            else {
              args.push(v);
            }
          }
          colSet.push(`${field}=${x}`);
        }
      }
    }
    if (colSet.length === 0) {
      const q = `insert ignore into ${table}(${cols.join(",")})values(${values.join(",")});`;
      const smt = { query: q, params: args };
      sts.push(smt);
    }
    else {
      const q = `insert into ${table}(${cols.join(",")})values(${values.join(",")}) on duplicate key update ${colSet.join(",")};`;
      const smt = { query: q, params: args };
      sts.push(smt);
    }
  }
  return sts;
}
export function toString(v) {
  if (v === v && v !== Infinity && v !== -Infinity) {
    return "" + v;
  }
  return "null";
}
export class resource {
}
export function createPool(conf) {
  if (conf.max && conf.max > 0 && !conf.connectionLimit) {
    conf.connectionLimit = conf.max;
  }
  return createPool2(Object.assign(Object.assign({}, conf), { rowsAsArray: true }));
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
        return connection.rollback(() => {
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
        return connection.rollback(() => {
          return reject(err);
        });
      }
      return resolve();
    });
  });
}
export function rollback(connection) {
  return new Promise((resolve, reject) => {
    connection.rollback((err) => {
      if (err) {
        return reject(err);
      }
      return resolve();
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
      this.pool.getConnection((err, connection) => {
        if (err) {
          return reject(err);
        }
        connection.beginTransaction((beginError) => {
          if (beginError) {
            return connection.rollback(() => {
              connection.release();
              return reject(beginError);
            });
          }
          return resolve(new PoolConnectionManager(connection));
        });
      });
    });
  }
  execute(sql, args) {
    return execute(this.pool, sql, args);
  }
  executeBatch(statements, firstAffected) {
    return executeBatch(this.pool, statements, firstAffected);
  }
  query(sql, args, m, bools) {
    return query(this.pool, sql, args, m, bools);
  }
  queryOne(sql, args, m, bools) {
    return queryOne(this.pool, sql, args, m, bools);
  }
  executeScalar(sql, args) {
    return executeScalar(this.pool, sql, args);
  }
  count(sql, args) {
    return count(this.pool, sql, args);
  }
}
export class PoolConnectionManager {
  constructor(connection) {
    this.connection = connection;
    this.released = false;
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
      this.connection.commit((err) => {
        if (err) {
          return this.connection.rollback(() => {
            this.release();
            return reject(err);
          });
        }
        this.release();
        return resolve();
      });
    });
  }
  rollback() {
    return new Promise((resolve, reject) => {
      this.connection.rollback((err) => {
        this.release();
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }
  release() {
    if (this.released) {
      return;
    }
    this.released = true;
    this.connection.release();
  }
  execute(sql, args) {
    return execute(this.connection, sql, args);
  }
  executeBatch(statements, firstAffected) {
    return executeBatchConnection(this.connection, statements, firstAffected);
  }
  query(sql, args, m, bools, ctx) {
    return query(this.connection, sql, args, m, bools);
  }
  queryOne(sql, args, m, bools) {
    return queryOne(this.connection, sql, args, m, bools);
  }
  executeScalar(sql, args) {
    return executeScalar(this.connection, sql, args);
  }
  count(sql, args) {
    return count(this.connection, sql, args);
  }
}
export function executeBatch(pool, statements, firstAffected) {
  return __awaiter(this, void 0, void 0, function* () {
    if (!statements || statements.length === 0) {
      return 0;
    }
    if (statements.length === 1) {
      return execute(pool, statements[0].query, statements[0].params);
    }
    const connection = yield getConnection(pool);
    try {
      return yield executeBatchConnectionTx(connection, statements, firstAffected);
    }
    finally {
      connection.release();
    }
  });
}
export function executeBatchConnectionTx(connection, statements, firstAffected) {
  return __awaiter(this, void 0, void 0, function* () {
    if (!statements || statements.length === 0) {
      return 0;
    }
    if (statements.length === 1) {
      return execute(connection, statements[0].query, statements[0].params);
    }
    yield beginTransaction(connection, true);
    try {
      let count = 0;
      if (resource.multipleStatements) {
        if (firstAffected) {
          const firstCount = yield execute(connection, statements[0].query, statements[0].params);
          count = firstCount;
          if (firstCount === 0) {
            yield commit(connection, true);
            return 0;
          }
          const remaining = statements.slice(1);
          count += yield executeMultipleStatements(connection, remaining);
        }
        else {
          count = yield executeMultipleStatements(connection, statements);
        }
      }
      else {
        const start = firstAffected ? 1 : 0;
        if (firstAffected) {
          const firstCount = yield execute(connection, statements[0].query, statements[0].params);
          count = firstCount;
          if (firstCount === 0) {
            yield commit(connection, true);
            return 0;
          }
        }
        for (let i = start; i < statements.length; i++) {
          const item = statements[i];
          count += yield execute(connection, item.query, item.params);
        }
      }
      yield commit(connection, true);
      return count;
    }
    catch (err) {
      try {
        yield rollback(connection);
      }
      catch (_a) {
      }
      throw buildError(err);
    }
  });
}
export function executeBatchConnection(connection, statements, firstAffected) {
  return __awaiter(this, void 0, void 0, function* () {
    if (!statements || statements.length === 0) {
      return 0;
    }
    if (statements.length === 1) {
      return execute(connection, statements[0].query, statements[0].params);
    }
    if (resource.multipleStatements) {
      if (firstAffected) {
        const firstCount = yield execute(connection, statements[0].query, statements[0].params);
        if (firstCount === 0) {
          return 0;
        }
        return firstCount + (yield executeMultipleStatements(connection, statements.slice(1)));
      }
      return executeMultipleStatements(connection, statements);
    }
    let count = 0;
    const start = firstAffected ? 1 : 0;
    if (firstAffected) {
      const firstCount = yield execute(connection, statements[0].query, statements[0].params);
      count = firstCount;
      if (firstCount === 0) {
        return 0;
      }
    }
    for (let i = start; i < statements.length; i++) {
      const item = statements[i];
      count += yield execute(connection, item.query, item.params);
    }
    return count;
  });
}
function executeMultipleStatements(connection, statements) {
  return __awaiter(this, void 0, void 0, function* () {
    if (!statements || statements.length === 0) {
      return 0;
    }
    const queries = [];
    const params = [];
    for (const item of statements) {
      queries.push(ensureSemicolon(item.query));
      if (item.params && item.params.length > 0) {
        params.push(...item.params);
      }
    }
    return new Promise((resolve, reject) => {
      connection.query(queries.join(""), toArray(params), (err, results) => {
        if (err) {
          buildError(err);
          return reject(err);
        }
        return resolve(getAffectedRows(results));
      });
    });
  });
}
function ensureSemicolon(sql) {
  return sql.endsWith(";") ? sql : `${sql};`;
}
function getAffectedRows(results) {
  if (!results) {
    return 0;
  }
  if (Array.isArray(results)) {
    let count = 0;
    for (const result of results) {
      if (result && typeof result.affectedRows === "number") {
        count += result.affectedRows;
      }
    }
    return count;
  }
  if (typeof results.affectedRows === "number") {
    return results.affectedRows;
  }
  return 0;
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
      pool.execute(sql, p, (err, res) => {
        if (err) {
          buildError(err);
          return reject(err);
        }
        return resolve(res.affectedRows);
      });
    });
  });
}
export function query(pool, sql, args, m, bools) {
  return __awaiter(this, void 0, void 0, function* () {
    const p = toArray(args);
    return new Promise((resolve, reject) => {
      pool.query(sql, p, (err, results, fields) => {
        if (err) {
          return reject(err);
        }
        if (results.length === 0) {
          return resolve([]);
        }
        const arrayResult = results.map((item) => {
          if (Array.isArray(item)) {
            return formatData(fields, item);
          }
          return item;
        });
        return resolve(handleResults(arrayResult, m, bools));
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
    const keys = Object.keys(r);
    return r[keys[0]];
  });
}
export function count(pool, sql, args) {
  return executeScalar(pool, sql, args).then((res) => (res !== null ? res : 0));
}
export function save(pool, obj, table, attrs, buildParam) {
  const s = buildToSave(obj, table, attrs, buildParam);
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
export function saveBatch(pool, objs, table, attrs, buildParam) {
  const s = buildToSaveBatch(objs, table, attrs, buildParam);
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
  for (const value of arr) {
    if (value === undefined || value === null) {
      p.push(null);
      continue;
    }
    if (typeof value === "object") {
      if (value instanceof Date) {
        p.push(value);
        continue;
      }
      if (resource.string) {
        p.push(JSON.stringify(value));
      }
      else {
        p.push(value);
      }
      continue;
    }
    p.push(value);
  }
  return p;
}
export function handleResults(r, m, bools) {
  if (m) {
    const res = mapArray(r, m);
    if (bools && bools.length > 0) {
      return handleBool(res, bools);
    }
    return res;
  }
  if (bools && bools.length > 0) {
    return handleBool(r, bools);
  }
  return r;
}
export function handleBool(objs, bools) {
  if (!bools || bools.length === 0 || !objs) {
    return objs;
  }
  for (const obj of objs) {
    const o = obj;
    for (const field of bools) {
      if (!field.name) {
        continue;
      }
      const v = o[field.name];
      if (typeof v === "boolean" || v == null) {
        continue;
      }
      const b = field.true;
      if (b == null) {
        o[field.name] = "1" == v || "T" == v || "Y" == v || "true" == v || "on" == v;
      }
      else {
        o[field.name] = v == b;
      }
    }
  }
  return objs;
}
export function formatData(nameColumn, data, m) {
  const result = {};
  nameColumn.forEach((item, index) => {
    let key = item.name;
    if (m && m[item.name]) {
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
  for (const obj of results) {
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
export class StringAdapter {
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
export class MySQLWriter {
  constructor(pool, table, attributes, oneIfSuccess, map, buildParam) {
    this.pool = pool;
    this.table = table;
    this.attributes = attributes;
    this.oneIfSuccess = oneIfSuccess;
    this.map = map;
    this.write = this.write.bind(this);
    this.param = buildParam ? buildParam : param;
  }
  write(obj) {
    if (!obj) {
      return Promise.resolve(0);
    }
    let obj2 = obj;
    if (this.map) {
      obj2 = this.map(obj);
    }
    const stmt = buildToSave(obj2, this.table, this.attributes, this.param);
    if (stmt.query) {
      if (this.oneIfSuccess) {
        return execute(this.pool, stmt.query, stmt.params).then((ct) => (ct > 0 ? 1 : 0));
      }
      else {
        return execute(this.pool, stmt.query, stmt.params);
      }
    }
    else {
      return Promise.resolve(0);
    }
  }
}
export class MySQLStreamWriter {
  constructor(pool, table, attributes, size = 1000, map, buildParam) {
    this.pool = pool;
    this.table = table;
    this.attributes = attributes;
    this.size = size;
    this.map = map;
    this.list = [];
    this.write = this.write.bind(this);
    this.flush = this.flush.bind(this);
    this.param = buildParam ? buildParam : param;
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
      const stmt = buildToSaveBatch(this.list, this.table, this.attributes, this.param);
      if (stmt.length > 0) {
        return executeBatch(this.pool, stmt).then((r) => {
          this.list = [];
          return stmt.length;
        });
      }
      else {
        this.list = [];
        return Promise.resolve(0);
      }
    }
  }
}
export class MySQLBatchWriter {
  constructor(pool, table, attributes, oneIfSuccess, map, buildParam) {
    this.pool = pool;
    this.table = table;
    this.attributes = attributes;
    this.oneIfSuccess = oneIfSuccess;
    this.map = map;
    this.write = this.write.bind(this);
    this.param = buildParam ? buildParam : param;
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
    const stmts = buildToSaveBatch(list, this.table, this.attributes, this.param);
    if (stmts.length > 0) {
      if (this.oneIfSuccess) {
        return executeBatch(this.pool, stmts).then((ct) => stmts.length);
      }
      else {
        return executeBatch(this.pool, stmts);
      }
    }
    else {
      return Promise.resolve(0);
    }
  }
}
export class MySQLChecker {
  constructor(pool, service, timeout = 4500) {
    this.pool = pool;
    this.timeout = timeout;
    this.service = service || "mysql";
  }
  name() {
    return this.service;
  }
  build(data, error) {
    return Object.assign(Object.assign({ name: this.name(), healthy: error == null, timestamp: new Date().toISOString() }, data), (error && {
      error: {
        message: error.message,
        code: error.code,
      },
    }));
  }
  check() {
    return __awaiter(this, void 0, void 0, function* () {
      const started = Date.now();
      try {
        yield Promise.race([this.pool.query("SELECT 1"), this.timeoutPromise()]);
        return this.build({
          latency: Date.now() - started,
        }, null);
      }
      catch (err) {
        return this.build({
          latency: Date.now() - started,
        }, err);
      }
    });
  }
  timeoutPromise() {
    return new Promise((_, reject) => setTimeout(() => {
      const error = new Error("Health check timeout");
      error.code = "TIMEOUT";
      reject(error);
    }, this.timeout));
  }
}
export class Exporter {
  constructor(connection, filename, buildQuery, format, write, end, attributes, logInfo, progressSize = 10000) {
    this.connection = connection;
    this.filename = filename;
    this.buildQuery = buildQuery;
    this.format = format;
    this.write = write;
    this.end = end;
    this.attributes = attributes;
    this.logInfo = logInfo;
    this.progressSize = progressSize;
    if (attributes) {
      this.map = buildMap(attributes);
    }
    this.export = this.export.bind(this);
  }
  export(ctx) {
    return __awaiter(this, void 0, void 0, function* () {
      const stmt = yield this.buildQuery(ctx);
      const reader = this.connection.query(stmt.query, stmt.params);
      let er;
      let i = 0;
      let j = 0;
      reader.on("error", (err) => (er = err));
      if (this.map) {
        reader.on("result", (row) => __awaiter(this, void 0, void 0, function* () {
          ++i;
          j++;
          this.connection.pause();
          const obj = mapOne(row, this.map);
          const data = this.format(obj);
          this.write(data);
          this.connection.resume();
          if (j >= this.progressSize) {
            if (this.logInfo) {
              this.logInfo(`Progress: ${i} records processed of file '${this.filename}'`);
            }
            j = 0;
          }
        }));
      }
      else {
        reader.on("result", (row) => __awaiter(this, void 0, void 0, function* () {
          ++i;
          j++;
          this.connection.pause();
          const data = this.format(row);
          this.write(data);
          this.connection.resume();
          if (j >= this.progressSize) {
            if (this.logInfo) {
              this.logInfo(`Progress: ${i} records processed of file '${this.filename}'`);
            }
            j = 0;
          }
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
                resolve(i);
              }
            });
          }
        });
      });
    });
  }
}
export class ExportService {
  constructor(connection, filename, queryBuilder, formatter, writer, attributes, logInfo, progressSize = 10000) {
    this.connection = connection;
    this.filename = filename;
    this.queryBuilder = queryBuilder;
    this.formatter = formatter;
    this.writer = writer;
    this.attributes = attributes;
    this.logInfo = logInfo;
    this.progressSize = progressSize;
    if (attributes) {
      this.map = buildMap(attributes);
    }
    this.export = this.export.bind(this);
  }
  export(ctx) {
    return __awaiter(this, void 0, void 0, function* () {
      const stmt = yield this.queryBuilder.build(ctx);
      const reader = this.connection.query(stmt.query, stmt.params);
      let er;
      reader.on("error", (err) => (er = err));
      let i = 0;
      let j = 0;
      if (this.map) {
        reader.on("result", (row) => __awaiter(this, void 0, void 0, function* () {
          ++i;
          j++;
          this.connection.pause();
          const obj = mapOne(row, this.map);
          const data = this.formatter.format(obj);
          this.writer.write(data);
          this.connection.resume();
          if (j >= this.progressSize) {
            if (this.logInfo) {
              this.logInfo(`Progress: ${i} records processed of file '${this.filename}'`);
            }
            j = 0;
          }
        }));
      }
      else {
        reader.on("result", (row) => __awaiter(this, void 0, void 0, function* () {
          ++i;
          j++;
          this.connection.pause();
          const data = this.formatter.format(row);
          this.writer.write(data);
          this.connection.resume();
          if (j >= this.progressSize) {
            if (this.logInfo) {
              this.logInfo(`Progress: ${i} records processed of file '${this.filename}'`);
            }
            j = 0;
          }
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
                resolve(i);
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
