import { Connection, createPool as createPool2, FieldPacket, format, OkPacket, Pool, PoolConnection, ResultSetHeader } from 'mysql2';
import { buildToSave, buildToSaveBatch } from './build';
import { Attribute, Attributes, Manager, Statement, StringMap } from './metadata';

export * from './metadata';
export * from './build';

// tslint:disable-next-line:class-name
export class resource {
  static string?: boolean;
}
export interface Config {
  host?: string | undefined;
  port?: number;
  server?: string | undefined;
  database?: string | undefined;
  user?: string | undefined;
  password?: string | undefined;
  multipleStatements?: boolean | undefined;
  connectionLimit?: number | undefined;
  max?: number | undefined;
  min?: number | undefined;
  idleTimeoutMillis?: number | undefined;
}
export function createPool(conf: Config): Pool {
  if (conf.max && conf.max > 0 && !conf.connectionLimit) {
    conf.connectionLimit = conf.max;
  }
  const pool = createPool2({...conf, rowsAsArray: true});
  return pool;
}

// tslint:disable-next-line:max-classes-per-file
export class PoolManager implements Manager {
  constructor(public pool: Pool) {
    this.param = this.param.bind(this);
    this.exec = this.exec.bind(this);
    this.execBatch = this.execBatch.bind(this);
    this.query = this.query.bind(this);
    this.queryOne = this.queryOne.bind(this);
    this.execScalar = this.execScalar.bind(this);
    this.count = this.count.bind(this);
  }
  driver = 'mysql';
  param(i: number): string {
    return '?';
  }
  exec(sql: string, args?: any[], ctx?: any): Promise<number> {
    const p = (ctx ? ctx : this.pool);
    return exec(p, sql, args);
  }
  execBatch(statements: Statement[], firstSuccess?: boolean, ctx?: any): Promise<number> {
    const p = (ctx ? ctx : this.pool);
    return execBatch(p, statements, firstSuccess);
  }
  query<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[], ctx?: any): Promise<T[]> {
    const p = (ctx ? ctx : this.pool);
    return query(p, sql, args, m, bools);
  }
  queryOne<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[], ctx?: any): Promise<T | null> {
    const p = (ctx ? ctx : this.pool);
    return queryOne(p, sql, args, m, bools);
  }
  execScalar<T>(sql: string, args?: any[], ctx?: any): Promise<T> {
    const p = (ctx ? ctx : this.pool);
    return execScalar<T>(p, sql, args);
  }
  count(sql: string, args?: any[], ctx?: any): Promise<number> {
    const p = (ctx ? ctx : this.pool);
    return count(p, sql, args);
  }
}
export function execBatch(pool: Pool, statements: Statement[], firstSuccess?: boolean): Promise<number> {
  if (!statements || statements.length === 0) {
    return Promise.resolve(0);
  } else if (statements.length === 1) {
    return exec(pool, statements[0].query, statements[0].params);
  }
  if (firstSuccess) {
    return new Promise<number>((resolve, reject) => {
      pool.getConnection((er0, connection) => {
        if (er0) {
          return reject(er0);
        }
        connection.beginTransaction(er1 => {
          if (er1) {
            connection.rollback(() => {
              return reject(er1);
            });
          } else {
            let query0 = '';
            const queries: string[] = [];
            const l = statements.length;
            for (let j = 0; j < l; j++) {
              const item = statements[j];
              if (j === 0) {
                query0 = format(item.query, toArray(item.params));
              } else {
                if (item.query.endsWith(';')) {
                  queries.push(format(item.query, toArray(item.params)));
                } else {
                  queries.push(format(item.query + ';', toArray(item.params)));
                }
              }
            }
            connection.query<ResultSetHeader>(query0, (er2a, results0) => {
              if (er2a) {
                connection.rollback(() => {
                  return reject(er2a);
                });
              } else {
                if (results0 && results0.affectedRows === 0) {
                  return 0;
                } else {
                  connection.query<OkPacket[]>(queries.join(''), (er2, results) => {
                    if (er2) {
                      connection.rollback(() => {
                        return reject(er2);
                      });
                    } else {
                      connection.commit((er3) => {
                        if (er3) {
                          connection.rollback(() => {
                            return reject(er3);
                          });
                        }
                      });
                      let c = 0;
                      c += results0.affectedRows + results.reduce((prev, item) => prev + item.affectedRows, 0);
                      return resolve(c);
                    }
                  });
                }
              }
            });
          }
        });
      });
    });
  } else {
    return new Promise<number>((resolve, reject) => {
      pool.getConnection((er0, connection) => {
        if (er0) {
          return reject(er0);
        }
        connection.beginTransaction(er1 => {
          if (er1) {
            connection.rollback(() => {
              return reject(er1);
            });
          } else {
            const queries: string[] = [];
            statements.forEach(item => {
              if (item.query.endsWith(';')) {
                queries.push(format(item.query, toArray(item.params)));
              } else {
                queries.push(format(item.query + ';', toArray(item.params)));
              }
            });
            connection.query<OkPacket[]>(queries.join(''), (er2, results) => {
              if (er2) {
                connection.rollback(() => {
                  buildError(er2);
                  return reject(er2);
                });
              } else {
                connection.commit((er3) => {
                  if (er3) {
                    connection.rollback(() => {
                      return reject(er3);
                    });
                  }
                });
                return resolve(results.reduce((prev, item) => prev + item.affectedRows, 0));
              }
            });
          }
        });
      });
    });
  }
}
function buildError(err: any): any {
  if (err.errno === 1062 && err.code === 'ER_DUP_ENTRY') {
    err.error = 'duplicate';
  }
  return err;
}

export async function exec(pool: Pool, sql: string, args?: any[]): Promise<number> {
  const p = toArray(args);
  return new Promise<number>((resolve, reject) => {
    return pool.execute<ResultSetHeader>(sql, p, (err, res) => {
      if (err) {
        buildError(err);
        return reject(err);
      } else {
        return resolve(res.affectedRows);
      }
    });
  });
}
export async function query<T>(pool: Pool | PoolConnection, sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T[]> {
  const p = toArray(args);
  return new Promise((resolve, reject) => {
    return pool.query<T[] & ResultSetHeader>(sql, p, (err, results, fields) => {
      if (err) {
        return reject(err);
      } else {
        if (results.length > 0) {
          const arrayResult = results.map(item => {
            if (Array.isArray(item)) {
              return formatData<T>(fields, item);
            } else {
              return item;
            }
          });
          return resolve(handleResults(arrayResult, m, bools));
        } else {
          resolve([]);
        }
      }
    });
  });
}

export function queryOne<T>(pool: Pool, sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T | null> {
  return query<T>(pool, sql, args, m, bools).then(r => {
    return (r && r.length > 0 ? r[0] : null);
  });
}
export function execScalar<T>(pool: Pool, sql: string, args?: any[]): Promise<T> {
  return queryOne<T>(pool, sql, args).then(r => {
    if (!r) {
      return null;
    } else {
      const keys = Object.keys(r);
      return (r as any)[keys[0]];
    }
  });
}

export function count(pool: Pool, sql: string, args?: any[]): Promise<number> {
  return execScalar<number>(pool, sql, args);
}

export function save<T>(pool: Pool | ((sql: string, args?: any[]) => Promise<number>), obj: T, table: string, attrs: Attributes, ver?: string, buildParam?: (i: number) => string, i?: number): Promise<number> {
  const s = buildToSave(obj, table, attrs, ver, buildParam);
  if (!s) {
    return Promise.resolve(-1);
  }
  if (typeof pool === 'function') {
    return pool(s.query, s.params);
  } else {
    return exec(pool, s.query, s.params);
  }
}
export function saveBatch<T>(pool: Pool | ((statements: Statement[]) => Promise<number>), objs: T[], table: string, attrs: Attributes, ver?: string, buildParam?: (i: number) => string): Promise<number> {
  const s = buildToSaveBatch(objs, table, attrs, ver, buildParam);
  if (typeof pool === 'function') {
    return pool(s);
  } else {
    return execBatch(pool, s);
  }
}
export function toArray(arr?: any[]): any[] {
  if (!arr || arr.length === 0) {
    return [];
  }
  const p: any[] = [];
  const l = arr.length;
  for (let i = 0; i < l; i++) {
    if (arr[i] === undefined || arr[i] == null) {
      p.push(null);
    } else {
      if (typeof arr[i] === 'object') {
        if (arr[i] instanceof Date) {
          p.push(arr[i]);
        } else {
          if (resource.string) {
            const s: string = JSON.stringify(arr[i]);
            p.push(s);
          } else {
            p.push(arr[i]);
          }
        }
      } else {
        p.push(arr[i]);
      }
    }
  }
  return p;
}
export function handleResults<T>(r: T[], m?: StringMap, bools?: Attribute[]) {
  if (m) {
    const res = mapArray(r, m);
    if (bools && bools.length > 0) {
      return handleBool(res, bools);
    } else {
      return res;
    }
  } else {
    if (bools && bools.length > 0) {
      return handleBool(r, bools);
    } else {
      return r;
    }
  }
}
export function handleBool<T>(objs: T[], bools: Attribute[]) {
  if (!bools || bools.length === 0 || !objs) {
    return objs;
  }
  for (const obj of objs) {
    const o: any = obj;
    for (const field of bools) {
      if (field.name) {
        const v = o[field.name];
        if (typeof v !== 'boolean' && v != null && v !== undefined) {
          const b = field.true;
          if (b == null || b === undefined) {
            // tslint:disable-next-line:triple-equals
            o[field.name] = ('1' == v || 'T' == v || 'Y' == v || 'true' == v || 'on' == v);
          } else {
            // tslint:disable-next-line:triple-equals
            o[field.name] = (v == b ? true : false);
          }
        }
      }
    }
  }
  return objs;
}
export function formatData<T>(nameColumn: FieldPacket[], data: any, m?: StringMap): T {
  const result: any = {};
  nameColumn.forEach((item, index) => {
    let key = item.name;
    if (m) {
      key = m[item.name];
    }
    result[key] = data[index];
  });
  return result;
}
export function map<T>(obj: T, m?: StringMap): any {
  if (!m) {
    return obj;
  }
  const mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return obj;
  }
  const o: any = {};
  const keys = Object.keys(obj as any);
  for (const key of keys) {
    let k0 = m[key];
    if (!k0) {
      k0 = key;
    }
    o[k0] = (obj as any)[key];
  }
  return o;
}
export function mapArray<T>(results: T[], m?: StringMap): T[] {
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
    const obj2: any = {};
    const keys = Object.keys(obj as any);
    for (const key of keys) {
      let k0 = m[key];
      if (!k0) {
        k0 = key;
      }
      obj2[k0] = (obj as any)[key];
    }
    objs.push(obj2);
  }
  return objs;
}
export function getFields(fields: string[], all?: string[]): string[] | undefined {
  if (!fields || fields.length === 0) {
    return undefined;
  }
  const ext: string[] = [];
  if (all) {
    for (const s of fields) {
      if (all.includes(s)) {
        ext.push(s);
      }
    }
    if (ext.length === 0) {
      return undefined;
    } else {
      return ext;
    }
  } else {
    return fields;
  }
}
export function buildFields(fields: string[], all?: string[]): string {
  const s = getFields(fields, all);
  if (!s || s.length === 0) {
    return '*';
  } else {
    return s.join(',');
  }
}
export function getMapField(name: string, mp?: StringMap): string {
  if (!mp) {
    return name;
  }
  const x = mp[name];
  if (!x) {
    return name;
  }
  if (typeof x === 'string') {
    return x;
  }
  return name;
}
export function isEmpty(s: string): boolean {
  return !(s && s.length > 0);
}
// tslint:disable-next-line:max-classes-per-file
export class StringService {
  constructor(protected pool: Pool, public table: string, public column: string) {
    this.load = this.load.bind(this);
    this.save = this.save.bind(this);
  }
  load(key: string, max: number): Promise<string[]> {
    const s = `select ${this.column} from ${this.table} where ${this.column} like ? order by ${this.column} limit ${max}`;
    return query(this.pool, s, ['' + key + '%']).then(arr => {
      return arr.map(i => (i as any)[this.column] as string);
    });
  }
  save(values: string[]): Promise<number> {
    if (!values || values.length === 0) {
      return Promise.resolve(0);
    }
    const arr: string[] = [];
    for (let i = 1; i <= values.length; i++) {
      arr.push('(?)');
    }
    const s = `insert ignore into ${this.table}(${this.column})values${arr.join(',')}`;
    return exec(this.pool, s, values);
  }
}

export function version(attrs: Attributes): Attribute | undefined {
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
// tslint:disable-next-line:max-classes-per-file
export class MySQLWriter<T> {
  pool?: Pool;
  version?: string;
  exec?: (sql: string, args?: any[]) => Promise<number>;
  map?: (v: T) => T;
  param?: (i: number) => string;
  constructor(pool: Pool | ((sql: string, args?: any[]) => Promise<number>), public table: string, public attributes: Attributes, public oneIfSuccess?: boolean, toDB?: (v: T) => T, buildParam?: (i: number) => string) {
    this.write = this.write.bind(this);
    if (typeof pool === 'function') {
      this.exec = pool;
    } else {
      this.pool = pool;
    }
    this.param = buildParam;
    this.map = toDB;
    const x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  write(obj: T): Promise<number> {
    if (!obj) {
      return Promise.resolve(0);
    }
    let obj2: NonNullable<T> | T = obj;
    if (this.map) {
      obj2 = this.map(obj);
    }
    const stmt = buildToSave(obj2, this.table, this.attributes, this.version, this.param);
    if (stmt) {
      if (this.exec) {
        if (this.oneIfSuccess) {
          return this.exec(stmt.query, stmt.params).then(ct => ct > 0 ? 1 : 0);
        } else {
          return this.exec(stmt.query, stmt.params);
        }
      } else {
        if (this.oneIfSuccess) {
          return exec(this.pool as any, stmt.query, stmt.params).then(ct => ct > 0 ? 1 : 0);
        } else {
          return exec(this.pool as any, stmt.query, stmt.params);
        }
      }
    } else {
      return Promise.resolve(0);
    }
  }
}
// tslint:disable-next-line:max-classes-per-file
export class MySQLStreamWriter<T> {
  list: T[] = [];
  size = 0;
  pool?: Pool;
  version?: string;
  execBatch?: (statements: Statement[]) => Promise<number>;
  map?: (v: T) => T;
  param?: (i: number) => string;
  constructor(pool: Pool | ((statements: Statement[]) => Promise<number>), public table: string, public attributes: Attributes, size?: number, toDB?: (v: T) => T, buildParam?: (i: number) => string) {
    this.write = this.write.bind(this);
    this.flush = this.flush.bind(this);
    if (typeof pool === 'function') {
      this.execBatch = pool;
    } else {
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
  write(obj: T): Promise<number> {
    if (!obj) {
      return Promise.resolve(0);
    }
    let obj2: NonNullable<T> | T = obj;
    if (this.map) {
      obj2 = this.map(obj);
      this.list.push(obj2);
    } else {
      this.list.push(obj);
    }
    if (this.list.length < this.size) {
      return Promise.resolve(0);
    } else {
      return this.flush();
    }
  }
  flush(): Promise<number> {
    if (!this.list || this.list.length === 0) {
      return Promise.resolve(0);
    } else {
      const total = this.list.length;
      const stmt = buildToSaveBatch(this.list, this.table, this.attributes, this.version, this.param);
      if (stmt) {
        if (this.execBatch) {
          return this.execBatch(stmt).then(r => {
            this.list = [];
            return total;
          });
        } else {
          return execBatch(this.pool as any, stmt).then(r => {
            this.list = [];
            return total;
          });
        }
      } else {
        return Promise.resolve(0);
      }
    }
  }
}
// tslint:disable-next-line:max-classes-per-file
export class MySQLBatchWriter<T> {
  pool?: Pool;
  version?: string;
  execute?: (statements: Statement[]) => Promise<number>;
  map?: (v: T) => T;
  param?: (i: number) => string;
  constructor(pool: Pool | ((statements: Statement[]) => Promise<number>), public table: string, public attributes: Attributes, public oneIfSuccess?: boolean, toDB?: (v: T) => T, buildParam?: (i: number) => string) {
    this.write = this.write.bind(this);
    if (typeof pool === 'function') {
      this.execute = pool;
    } else {
      this.pool = pool;
    }
    this.param = buildParam;
    this.map = toDB;
    const x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  write(objs: T[]): Promise<number> {
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
    if (stmts && stmts.length > 0) {
      if (this.execute) {
        if (this.oneIfSuccess) {
          return this.execute(stmts).then(ct => stmts.length);
        } else {
          return this.execute(stmts);
        }
      } else {
        if (this.oneIfSuccess) {
          return execBatch(this.pool as any, stmts).then(ct => stmts.length);
        } else {
          return execBatch(this.pool as any, stmts);
        }
      }
    } else {
      return Promise.resolve(0);
    }
  }
}

export interface AnyMap {
  [key: string]: any;
}
// tslint:disable-next-line:max-classes-per-file
export class MySQLChecker {
  timeout: number;
  service: string;
  constructor(private pool: Pool, service?: string, timeout?: number) {
    this.timeout = (timeout ? timeout : 4200);
    this.service = (service ? service : 'mysql');
    this.check = this.check.bind(this);
    this.name = this.name.bind(this);
    this.build = this.build.bind(this);
  }
  async check(): Promise<AnyMap> {
    const obj = {} as AnyMap;
    const promise = new Promise<any>((resolve, reject) => {
      this.pool.query('select current_time', (err, result) => {
        if (err) {
          return reject(err);
        } else {
          resolve(obj);
        }
      });
    });
    if (this.timeout > 0) {
      return promiseTimeOut(this.timeout, promise);
    } else {
      return promise;
    }
  }
  name(): string {
    return this.service;
  }
  build(data: AnyMap, err: any): AnyMap {
    if (err) {
      if (!data) {
        data = {} as AnyMap;
      }
      data['error'] = err;
    }
    return data;
  }
}

function promiseTimeOut(timeoutInMilliseconds: number, promise: Promise<any>): Promise<any> {
  return Promise.race([
    promise,
    new Promise((resolve, reject) => {
      setTimeout(() => {
        reject(`Timed out in: ${timeoutInMilliseconds} milliseconds!`);
      }, timeoutInMilliseconds);
    })
  ]);
}
export interface Formatter<T> {
  format: (row: T) => string;
}
export interface FileWriter {
  write(chunk: string): boolean;
  flush?(cb?: () => void): void;
  end?(cb?: () => void): void;
}
export interface QueryBuilder {
  build(ctx?: any): Promise<Statement>;
}
// tslint:disable-next-line:max-classes-per-file
export class Exporter<T> {
  constructor(
    public connection: Connection,
    public buildQuery: (ctx?: any) => Promise<Statement>,
    ft: (row: T) => string,
    public write: (chunk: string) => boolean,
    public end: (cb?: () => void) => void,
    public attributes?: Attributes
  ) {
    this.format = ft;
    if (attributes) {
      this.map = buildMap(attributes);
    }
    this.export = this.export.bind(this);
  }
  map?: StringMap;
  format: (row: T) => string;
  async export(ctx?: any): Promise<number> {
    let idx = 0;
    const stmt = await this.buildQuery(ctx);
    const reader = this.connection.query(stmt.query, stmt.params);
    let er: any;
    reader.on('error', (err) => er = err);
    // (D2) WRITE ROW-BY-ROW
    if (this.map) {
      reader.on('result', async (row: any) => {
        ++idx;
        this.connection.pause();
        const obj = mapOne<T>(row, this.map);
        const data = this.format(obj);
        this.write(data);
        this.connection.resume();
      });
    } else {
      reader.on('result', async (row: any) => {
        ++idx;
        this.connection.pause();
        const data = this.format(row as T);
        this.write(data);
        this.connection.resume();
      });
    }
    // (D3) CLOSE CONNECTION + FILE
    return new Promise<number>((resolve, reject) => {
      reader.on('end', () => {
        this.end();
        if (er) {
          reject(er);
        } else {
          this.connection.end((err) => {
            if (err) {
              reject(err);
            } else {
              resolve(idx);
            }
          });
        }
      });
    });
  }
}
// tslint:disable-next-line:max-classes-per-file
export class ExportService<T> {
  constructor(
    public connection: Connection,
    public queryBuilder: QueryBuilder,
    public formatter: Formatter<T>,
    public writer: FileWriter,
    public attributes?: Attributes
  ) {
    if (attributes) {
      this.map = buildMap(attributes);
    }
    this.export = this.export.bind(this);
  }
  map?: StringMap;
  async export(ctx?: any): Promise<number> {
    let idx = 0;
    const stmt = await this.queryBuilder.build(ctx);
    const reader = this.connection.query(stmt.query, stmt.params);
    let er: any;
    reader.on('error', (err) => er = err);
    // (D2) WRITE ROW-BY-ROW
    if (this.map) {
      reader.on('result', async (row: any) => {
        ++idx;
        this.connection.pause();
        const obj = mapOne<T>(row, this.map);
        const data = this.formatter.format(obj);
        this.writer.write(data);
        this.connection.resume();
      });
    } else {
      reader.on('result', async (row: any) => {
        ++idx;
        this.connection.pause();
        const data = this.formatter.format(row as T);
        this.writer.write(data);
        this.connection.resume();
      });
    }
    // (D3) CLOSE CONNECTION + FILE
    return new Promise<number>((resolve, reject) => {
      reader.on('end', () => {
        if (this.writer.end) {
          this.writer.end();
        } else if (this.writer.flush) {
          this.writer.flush();
        }
        if (er) {
          reject(er);
        } else {
          this.connection.end((err) => {
            if (err) {
              reject(err);
            } else {
              resolve(idx);
            }
          });
        }
      });
    });
  }
}
export function mapOne<T>(results: any, m?: StringMap): T {
  const obj: any = results;
  if (!m) {
    return obj;
  }
  const mkeys = Object.keys(m as any);
  if (mkeys.length === 0) {
    return obj;
  }
  const obj2: any = {};
  const keys = Object.keys(obj);
  for (const key of keys) {
    let k0 = m[key];
    if (!k0) {
      k0 = key;
    }
    obj2[k0] = (obj)[key];
  }
  return obj2;
}
export function buildMap(attrs: Attributes): StringMap|undefined {
  const mp: StringMap = {};
  const ks = Object.keys(attrs);
  let isMap = false;
  for (const k of ks) {
    const attr = attrs[k];
    attr.name = k;
    const field = (attr.column ? attr.column : k);
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
export function select(table: string, attrs: Attributes): string {
  const cols: string[] = [];
  const ks = Object.keys(attrs);
  for (const k of ks) {
    const attr = attrs[k];
    attr.name = k;
    const field = (attr.column ? attr.column : k);
    cols.push(field);
  }
  return `select ${cols.join(',')} from ${table}`;
}
