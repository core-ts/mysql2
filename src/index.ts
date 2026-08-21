import { Connection, createPool as createPool2, FieldPacket, Pool, PoolConnection, ResultSetHeader } from "mysql2"
import { Pool as PromisePool } from "mysql2/promise"
import { buildToSave, buildToSaveBatch, param } from "./build"
import { Attribute, Attributes, DB, Statement, StringMap, Transaction } from "./metadata"

export * from "./build"
export * from "./metadata"

// tslint:disable-next-line:class-name
export class resource {
  static string?: boolean
  static multipleStatements?: boolean
}
export interface Config {
  host?: string | undefined
  port?: number
  server?: string | undefined
  database?: string | undefined
  user?: string | undefined
  password?: string | undefined
  multipleStatements?: boolean | undefined
  connectionLimit?: number | undefined
  max?: number | undefined
  min?: number | undefined
  idleTimeoutMillis?: number | undefined
}
export function createPool(conf: Config): Pool {
  if (conf.max && conf.max > 0 && !conf.connectionLimit) {
    conf.connectionLimit = conf.max
  }
  return createPool2({ ...conf, rowsAsArray: true })
}
export function getConnection(pool: Pool): Promise<PoolConnection> {
  return new Promise<PoolConnection>((resolve, reject) => {
    pool.getConnection((err, connection) => {
      if (err) {
        return reject(err)
      }
      return resolve(connection)
    })
  })
}
export function beginTransaction(connection: PoolConnection, rollbackIfError?: boolean): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    connection.beginTransaction((err) => {
      if (err) {
        if (!rollbackIfError) {
          return reject(err)
        }
        return connection.rollback(() => {
          return reject(err)
        })
      }
      return resolve()
    })
  })
}
export function commit(connection: PoolConnection, rollbackIfError?: boolean): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    connection.commit((err) => {
      if (err) {
        if (!rollbackIfError) {
          return reject(err)
        }
        return connection.rollback(() => {
          return reject(err)
        })
      }
      return resolve()
    })
  })
}
export function rollback(connection: PoolConnection): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    connection.rollback((err) => {
      if (err) {
        return reject(err)
      }
      return resolve()
    })
  })
}
// tslint:disable-next-line:max-classes-per-file
export class PoolManager implements DB {
  constructor(protected pool: Pool) {
    this.param = this.param.bind(this)
    this.execute = this.execute.bind(this)
    this.executeBatch = this.executeBatch.bind(this)
    this.query = this.query.bind(this)
    this.queryOne = this.queryOne.bind(this)
    this.executeScalar = this.executeScalar.bind(this)
    this.count = this.count.bind(this)
  }
  driver = "mysql"
  param(i: number): string {
    return "?"
  }
  beginTransaction(): Promise<Transaction> {
    return new Promise<Transaction>((resolve, reject) => {
      this.pool.getConnection((err, connection) => {
        if (err) {
          return reject(err)
        }
        connection.beginTransaction((beginError) => {
          if (beginError) {
            return connection.rollback(() => {
              connection.release()
              return reject(beginError)
            })
          }
          return resolve(new PoolConnectionManager(connection))
        })
      })
    })
  }
  execute(sql: string, args?: any[]): Promise<number> {
    return execute(this.pool, sql, args)
  }
  executeBatch(statements: Statement[], firstAffected?: boolean): Promise<number> {
    return executeBatch(this.pool, statements, firstAffected)
  }
  query<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T[]> {
    return query(this.pool, sql, args, m, bools)
  }
  queryOne<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T | null> {
    return queryOne(this.pool, sql, args, m, bools)
  }
  executeScalar<T>(sql: string, args?: any[]): Promise<T | null> {
    return executeScalar<T>(this.pool, sql, args)
  }
  count(sql: string, args?: any[]): Promise<number> {
    return count(this.pool, sql, args)
  }
}
// tslint:disable-next-line:max-classes-per-file
export class PoolConnectionManager implements Transaction {
  protected released = false
  constructor(protected connection: PoolConnection) {
    this.param = this.param.bind(this)
    this.execute = this.execute.bind(this)
    this.executeBatch = this.executeBatch.bind(this)
    this.query = this.query.bind(this)
    this.queryOne = this.queryOne.bind(this)
    this.executeScalar = this.executeScalar.bind(this)
    this.count = this.count.bind(this)
  }
  driver = "mysql"
  param(i: number): string {
    return "?"
  }
  commit(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.connection.commit((err) => {
        if (err) {
          return this.connection.rollback(() => {
            this.release()
            return reject(err)
          })
        }
        this.release()
        return resolve()
      })
    })
  }
  rollback(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.connection.rollback((err) => {
        this.release()
        if (err) {
          return reject(err)
        }
        return resolve()
      })
    })
  }
  protected release(): void {
    if (this.released) {
      return
    }
    this.released = true
    this.connection.release()
  }
  execute(sql: string, args?: any[]): Promise<number> {
    return execute(this.connection, sql, args)
  }
  executeBatch(statements: Statement[], firstAffected?: boolean): Promise<number> {
    return executeBatchConnection(this.connection, statements, firstAffected)
  }
  query<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[], ctx?: any): Promise<T[]> {
    return query(this.connection, sql, args, m, bools)
  }
  queryOne<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T | null> {
    return queryOne(this.connection, sql, args, m, bools)
  }
  executeScalar<T>(sql: string, args?: any[]): Promise<T | null> {
    return executeScalar<T>(this.connection, sql, args)
  }
  count(sql: string, args?: any[]): Promise<number> {
    return count(this.connection, sql, args)
  }
}
export async function executeBatch(pool: Pool, statements: Statement[], firstAffected?: boolean): Promise<number> {
  if (!statements || statements.length === 0) {
    return 0
  }
  if (statements.length === 1) {
    return execute(pool, statements[0].query, statements[0].params)
  }
  const connection = await getConnection(pool)
  try {
    return await executeBatchConnectionTx(connection, statements, firstAffected)
  } finally {
    connection.release()
  }
}
export async function executeBatchConnectionTx(connection: PoolConnection, statements: Statement[], firstAffected?: boolean): Promise<number> {
  if (!statements || statements.length === 0) {
    return 0
  }
  if (statements.length === 1) {
    return execute(connection, statements[0].query, statements[0].params)
  }
  await beginTransaction(connection, true)
  try {
    let count = 0

    if (resource.multipleStatements) {
      if (firstAffected) {
        const firstCount = await execute(connection, statements[0].query, statements[0].params)

        count = firstCount

        if (firstCount === 0) {
          await commit(connection, true)
          return 0
        }
        const remaining = statements.slice(1)
        count += await executeMultipleStatements(connection, remaining)
      } else {
        count = await executeMultipleStatements(connection, statements)
      }
    } else {
      const start = firstAffected ? 1 : 0
      if (firstAffected) {
        const firstCount = await execute(connection, statements[0].query, statements[0].params)
        count = firstCount
        if (firstCount === 0) {
          await commit(connection, true)
          return 0
        }
      }
      for (let i = start; i < statements.length; i++) {
        const item = statements[i]
        count += await execute(connection, item.query, item.params)
      }
    }

    await commit(connection, true)

    return count
  } catch (err) {
    try {
      await rollback(connection)
    } catch {
      // Preserve the original error.
    }
    throw buildError(err)
  }
}

export async function executeBatchConnection(connection: PoolConnection, statements: Statement[], firstAffected?: boolean): Promise<number> {
  if (!statements || statements.length === 0) {
    return 0
  }

  if (statements.length === 1) {
    return execute(connection, statements[0].query, statements[0].params)
  }

  if (resource.multipleStatements) {
    if (firstAffected) {
      const firstCount = await execute(connection, statements[0].query, statements[0].params)

      if (firstCount === 0) {
        return 0
      }

      return firstCount + (await executeMultipleStatements(connection, statements.slice(1)))
    }

    return executeMultipleStatements(connection, statements)
  }

  let count = 0
  const start = firstAffected ? 1 : 0

  if (firstAffected) {
    const firstCount = await execute(connection, statements[0].query, statements[0].params)

    count = firstCount

    if (firstCount === 0) {
      return 0
    }
  }

  for (let i = start; i < statements.length; i++) {
    const item = statements[i]

    count += await execute(connection, item.query, item.params)
  }

  return count
}

async function executeMultipleStatements(connection: PoolConnection, statements: Statement[]): Promise<number> {
  if (!statements || statements.length === 0) {
    return 0
  }

  const queries: string[] = []
  const params: any[] = []

  for (const item of statements) {
    queries.push(ensureSemicolon(item.query))

    if (item.params && item.params.length > 0) {
      params.push(...item.params)
    }
  }

  return new Promise<number>((resolve, reject) => {
    connection.query<any>(queries.join(""), toArray(params), (err, results) => {
      if (err) {
        buildError(err)
        return reject(err)
      }

      return resolve(getAffectedRows(results))
    })
  })
}
function ensureSemicolon(sql: string): string {
  return sql.endsWith(";") ? sql : `${sql};`
}
function getAffectedRows(results: any): number {
  if (!results) {
    return 0
  }

  if (Array.isArray(results)) {
    let count = 0

    for (const result of results) {
      if (result && typeof result.affectedRows === "number") {
        count += result.affectedRows
      }
    }

    return count
  }
  if (typeof results.affectedRows === "number") {
    return results.affectedRows
  }
  return 0
}
function buildError(err: any): any {
  if (err.errno === 1062 && err.code === "ER_DUP_ENTRY") {
    err.error = "duplicate"
  }
  return err
}

export async function execute(pool: Pool | PoolConnection, sql: string, args?: any[]): Promise<number> {
  const p = toArray(args)
  return new Promise<number>((resolve, reject) => {
    pool.execute<ResultSetHeader>(sql, p, (err, res) => {
      if (err) {
        buildError(err)
        return reject(err)
      }
      return resolve(res.affectedRows)
    })
  })
}
export async function query<T>(pool: Pool | PoolConnection, sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T[]> {
  const p = toArray(args)
  return new Promise<T[]>((resolve, reject) => {
    pool.query<T[] & ResultSetHeader>(sql, p, (err, results, fields) => {
      if (err) {
        return reject(err)
      }
      if (results.length === 0) {
        return resolve([])
      }
      const arrayResult = results.map((item) => {
        if (Array.isArray(item)) {
          return formatData<T>(fields, item)
        }

        return item
      })
      return resolve(handleResults(arrayResult, m, bools))
    })
  })
}

export function queryOne<T>(pool: Pool | PoolConnection, sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T | null> {
  return query<T>(pool, sql, args, m, bools).then((r) => {
    return r && r.length > 0 ? r[0] : null
  })
}
export function executeScalar<T>(pool: Pool | PoolConnection, sql: string, args?: any[]): Promise<T | null> {
  return queryOne<T>(pool, sql, args).then((r) => {
    if (!r) {
      return null
    }
    const keys = Object.keys(r)
    return (r as any)[keys[0]]
  })
}

export function count(pool: Pool | PoolConnection, sql: string, args?: any[]): Promise<number> {
  return executeScalar<number>(pool, sql, args).then((res) => (res !== null ? res : 0))
}

export function save<T>(pool: Pool | PoolConnection | ((sql: string, args?: any[]) => Promise<number>), obj: T, table: string, attrs: Attributes, buildParam?: (i: number) => string): Promise<number> {
  const s = buildToSave(obj, table, attrs, buildParam)
  if (!s.query) {
    return Promise.resolve(-1)
  }
  if (typeof pool === "function") {
    return pool(s.query, s.params)
  } else {
    return execute(pool, s.query, s.params)
  }
}
export function saveBatch<T>(pool: Pool | ((statements: Statement[]) => Promise<number>), objs: T[], table: string, attrs: Attributes, buildParam?: (i: number) => string): Promise<number> {
  const s = buildToSaveBatch(objs, table, attrs, buildParam)
  if (typeof pool === "function") {
    return pool(s)
  } else {
    return executeBatch(pool, s)
  }
}
export function toArray(arr?: any[]): any[] {
  if (!arr || arr.length === 0) {
    return []
  }
  const p: any[] = []
  for (const value of arr) {
    if (value === undefined || value === null) {
      p.push(null)
      continue
    }
    if (typeof value === "object") {
      if (value instanceof Date) {
        p.push(value)
        continue
      }
      if (resource.string) {
        p.push(JSON.stringify(value))
      } else {
        p.push(value)
      }
      continue
    }
    p.push(value)
  }
  return p
}
export function handleResults<T>(r: T[], m?: StringMap, bools?: Attribute[]): T[] {
  if (m) {
    const res = mapArray(r, m)
    if (bools && bools.length > 0) {
      return handleBool(res, bools)
    }
    return res
  }
  if (bools && bools.length > 0) {
    return handleBool(r, bools)
  }
  return r
}
export function handleBool<T>(objs: T[], bools: Attribute[]): T[] {
  if (!bools || bools.length === 0 || !objs) {
    return objs
  }
  for (const obj of objs) {
    const o: any = obj
    for (const field of bools) {
      if (!field.name) {
        continue
      }
      const v = o[field.name]
      if (typeof v === "boolean" || v == null) {
        continue
      }
      const b = field.true
      if (b == null) {
        // tslint:disable-next-line:triple-equals
        o[field.name] = "1" == v || "T" == v || "Y" == v || "true" == v || "on" == v
      } else {
        // tslint:disable-next-line:triple-equals
        o[field.name] = v == b
      }
    }
  }
  return objs
}
export function formatData<T>(nameColumn: FieldPacket[], data: any, m?: StringMap): T {
  const result: any = {}
  nameColumn.forEach((item, index) => {
    let key = item.name
    if (m && m[item.name]) {
      key = m[item.name]
    }
    result[key] = data[index]
  })
  return result
}
export function map<T>(obj: T, m?: StringMap): any {
  if (!m) {
    return obj
  }
  const mkeys = Object.keys(m)
  if (mkeys.length === 0) {
    return obj
  }
  const o: any = {}
  const keys = Object.keys(obj as any)
  for (const key of keys) {
    let k0 = m[key]
    if (!k0) {
      k0 = key
    }
    o[k0] = (obj as any)[key]
  }
  return o
}
export function mapArray<T>(results: T[], m?: StringMap): T[] {
  if (!m) {
    return results
  }
  const mkeys = Object.keys(m)
  if (mkeys.length === 0) {
    return results
  }
  const objs: T[] = []
  for (const obj of results) {
    const obj2: any = {}
    const keys = Object.keys(obj as any)
    for (const key of keys) {
      let k0 = m[key]
      if (!k0) {
        k0 = key
      }
      obj2[k0] = (obj as any)[key]
    }
    objs.push(obj2)
  }
  return objs
}
export function getFields(fields: string[], all?: string[]): string[] | undefined {
  if (!fields || fields.length === 0) {
    return undefined
  }
  const ext: string[] = []
  if (all) {
    for (const s of fields) {
      if (all.includes(s)) {
        ext.push(s)
      }
    }
    if (ext.length === 0) {
      return undefined
    } else {
      return ext
    }
  } else {
    return fields
  }
}
export function buildFields(fields: string[], all?: string[]): string {
  const s = getFields(fields, all)
  if (!s || s.length === 0) {
    return "*"
  } else {
    return s.join(",")
  }
}
export function getMapField(name: string, mp?: StringMap): string {
  if (!mp) {
    return name
  }
  const x = mp[name]
  if (!x) {
    return name
  }
  if (typeof x === "string") {
    return x
  }
  return name
}
export function isEmpty(s: string): boolean {
  return !(s && s.length > 0)
}
// tslint:disable-next-line:max-classes-per-file
export class StringAdapter {
  constructor(
    protected pool: Pool | PoolConnection,
    protected table: string,
    protected column: string,
  ) {
    this.load = this.load.bind(this)
    this.save = this.save.bind(this)
  }
  load(key: string, max: number): Promise<string[]> {
    const s = `select ${this.column} from ${this.table} where ${this.column} like ? order by ${this.column} limit ${max}`
    return query(this.pool, s, ["" + key + "%"]).then((arr) => {
      return arr.map((i) => (i as any)[this.column] as string)
    })
  }
  save(values: string[]): Promise<number> {
    if (!values || values.length === 0) {
      return Promise.resolve(0)
    }
    const arr: string[] = []
    for (let i = 1; i <= values.length; i++) {
      arr.push("(?)")
    }
    const s = `insert ignore into ${this.table}(${this.column})values${arr.join(",")}`
    return execute(this.pool, s, values)
  }
}

// tslint:disable-next-line:max-classes-per-file
export class MySQLWriter<T> {
  protected param?: (i: number) => string
  constructor(
    protected pool: Pool | PoolConnection,
    protected table: string,
    protected attributes: Attributes,
    protected oneIfSuccess?: boolean,
    protected map?: (v: T) => T,
    buildParam?: (i: number) => string,
  ) {
    this.write = this.write.bind(this)
    this.param = buildParam ? buildParam : param
  }
  write(obj: T): Promise<number> {
    if (!obj) {
      return Promise.resolve(0)
    }
    let obj2: NonNullable<T> | T = obj
    if (this.map) {
      obj2 = this.map(obj)
    }
    const stmt = buildToSave(obj2, this.table, this.attributes, this.param)
    if (stmt.query) {
      if (this.oneIfSuccess) {
        return execute(this.pool, stmt.query, stmt.params).then((ct) => (ct > 0 ? 1 : 0))
      } else {
        return execute(this.pool, stmt.query, stmt.params)
      }
    } else {
      return Promise.resolve(0)
    }
  }
}
// tslint:disable-next-line:max-classes-per-file
export class MySQLStreamWriter<T> {
  protected list: T[] = []
  protected param?: (i: number) => string
  constructor(
    protected pool: Pool,
    protected table: string,
    protected attributes: Attributes,
    protected size: number = 1000,
    protected map?: (v: T) => T,
    buildParam?: (i: number) => string,
  ) {
    this.write = this.write.bind(this)
    this.flush = this.flush.bind(this)
    this.param = buildParam ? buildParam : param
  }
  write(obj: T): Promise<number> {
    if (!obj) {
      return Promise.resolve(0)
    }
    let obj2: NonNullable<T> | T = obj
    if (this.map) {
      obj2 = this.map(obj)
      this.list.push(obj2)
    } else {
      this.list.push(obj)
    }
    if (this.list.length < this.size) {
      return Promise.resolve(0)
    } else {
      return this.flush()
    }
  }
  flush(): Promise<number> {
    if (!this.list || this.list.length === 0) {
      return Promise.resolve(0)
    } else {
      const stmt = buildToSaveBatch(this.list, this.table, this.attributes, this.param)
      if (stmt.length > 0) {
        return executeBatch(this.pool as any, stmt).then((r) => {
          this.list = []
          return stmt.length
        })
      } else {
        this.list = []
        return Promise.resolve(0)
      }
    }
  }
}
// tslint:disable-next-line:max-classes-per-file
export class MySQLBatchWriter<T> {
  protected param?: (i: number) => string
  constructor(
    protected pool: Pool,
    protected table: string,
    protected attributes: Attributes,
    protected oneIfSuccess?: boolean,
    protected map?: (v: T) => T,
    buildParam?: (i: number) => string,
  ) {
    this.write = this.write.bind(this)
    this.param = buildParam ? buildParam : param
  }
  write(objs: T[]): Promise<number> {
    if (!objs || objs.length === 0) {
      return Promise.resolve(0)
    }
    let list = objs
    if (this.map) {
      list = []
      for (const obj of objs) {
        const obj2 = this.map(obj)
        list.push(obj2)
      }
    }
    const stmts = buildToSaveBatch(list, this.table, this.attributes, this.param)
    if (stmts.length > 0) {
      if (this.oneIfSuccess) {
        return executeBatch(this.pool, stmts).then((ct) => stmts.length)
      } else {
        return executeBatch(this.pool, stmts)
      }
    } else {
      return Promise.resolve(0)
    }
  }
}

export interface AnyMap {
  [key: string]: any
}
// tslint:disable-next-line:max-classes-per-file
export interface HealthChecker {
  name(): string
  build(data: AnyMap, error: any): AnyMap
  check(): Promise<AnyMap>
}
export class MySQLChecker implements HealthChecker {
  protected readonly service: string
  constructor(
    protected readonly pool: PromisePool,
    service?: string,
    protected readonly timeout = 4500,
  ) {
    this.service = service || "mysql"
  }

  name(): string {
    return this.service
  }

  build(data: AnyMap, error: any): AnyMap {
    return {
      name: this.name(),
      healthy: error == null,
      timestamp: new Date().toISOString(),
      ...data,
      ...(error && {
        error: {
          message: error.message,
          code: error.code,
        },
      }),
    }
  }

  async check(): Promise<AnyMap> {
    const started = Date.now()
    try {
      await Promise.race([this.pool.query("SELECT 1"), this.timeoutPromise()])
      return this.build(
        {
          latency: Date.now() - started,
        },
        null,
      )
    } catch (err) {
      return this.build(
        {
          latency: Date.now() - started,
        },
        err,
      )
    }
  }

  private timeoutPromise(): Promise<never> {
    return new Promise((_, reject) =>
      setTimeout(() => {
        const error: any = new Error("Health check timeout")
        error.code = "TIMEOUT"
        reject(error)
      }, this.timeout),
    )
  }
}

export interface SimpleMap {
  [key: string]: string | number | boolean | Date
}
export interface Formatter<T> {
  format: (row: T) => string
}
export interface FileWriter {
  write(chunk: string): boolean
  flush?(cb?: () => void): void
  end?(cb?: () => void): void
}
export interface QueryBuilder {
  build(ctx?: any): Promise<Statement>
}
// tslint:disable-next-line:max-classes-per-file
export class Exporter<T> {
  constructor(
    protected connection: Connection,
    protected filename: string,
    protected buildQuery: (ctx?: any) => Promise<Statement>,
    protected format: (row: T) => string,
    protected write: (chunk: string) => boolean,
    protected end: (cb?: () => void) => void,
    protected attributes?: Attributes,
    protected logInfo?: (msg: string, m?: SimpleMap) => void,
    protected progressSize: number = 10000,
  ) {
    if (attributes) {
      this.map = buildMap(attributes)
    }
    this.export = this.export.bind(this)
  }
  map?: StringMap
  async export(ctx?: any): Promise<number> {
    const stmt = await this.buildQuery(ctx)
    const reader = this.connection.query(stmt.query, stmt.params)
    let er: any
    let i = 0
    let j = 0
    reader.on("error", (err) => (er = err))
    // (D2) WRITE ROW-BY-ROW
    if (this.map) {
      reader.on("result", async (row: any) => {
        ++i
        j++
        this.connection.pause()
        const obj = mapOne<T>(row, this.map)
        const data = this.format(obj)
        this.write(data)
        this.connection.resume()
        if (j >= this.progressSize) {
          if (this.logInfo) {
            this.logInfo(`Progress: ${i} records processed of file '${this.filename}'`)
          }
          j = 0
        }
      })
    } else {
      reader.on("result", async (row: any) => {
        ++i
        j++
        this.connection.pause()
        const data = this.format(row as T)
        this.write(data)
        this.connection.resume()
        if (j >= this.progressSize) {
          if (this.logInfo) {
            this.logInfo(`Progress: ${i} records processed of file '${this.filename}'`)
          }
          j = 0
        }
      })
    }
    // (D3) CLOSE CONNECTION + FILE
    return new Promise<number>((resolve, reject) => {
      reader.on("end", () => {
        this.end()
        if (er) {
          reject(er)
        } else {
          this.connection.end((err) => {
            if (err) {
              reject(err)
            } else {
              resolve(i)
            }
          })
        }
      })
    })
  }
}
// tslint:disable-next-line:max-classes-per-file
export class ExportService<T> {
  constructor(
    protected connection: Connection,
    protected filename: string,
    protected queryBuilder: QueryBuilder,
    protected formatter: Formatter<T>,
    protected writer: FileWriter,
    protected attributes?: Attributes,
    protected logInfo?: (msg: string, m?: SimpleMap) => void,
    protected progressSize: number = 10000,
  ) {
    if (attributes) {
      this.map = buildMap(attributes)
    }
    this.export = this.export.bind(this)
  }
  map?: StringMap
  async export(ctx?: any): Promise<number> {
    const stmt = await this.queryBuilder.build(ctx)
    const reader = this.connection.query(stmt.query, stmt.params)
    let er: any
    reader.on("error", (err) => (er = err))
    let i = 0
    let j = 0
    // (D2) WRITE ROW-BY-ROW
    if (this.map) {
      reader.on("result", async (row: any) => {
        ++i
        j++
        this.connection.pause()
        const obj = mapOne<T>(row, this.map)
        const data = this.formatter.format(obj)
        this.writer.write(data)
        this.connection.resume()
        if (j >= this.progressSize) {
          if (this.logInfo) {
            this.logInfo(`Progress: ${i} records processed of file '${this.filename}'`)
          }
          j = 0
        }
      })
    } else {
      reader.on("result", async (row: any) => {
        ++i
        j++
        this.connection.pause()
        const data = this.formatter.format(row as T)
        this.writer.write(data)
        this.connection.resume()
        if (j >= this.progressSize) {
          if (this.logInfo) {
            this.logInfo(`Progress: ${i} records processed of file '${this.filename}'`)
          }
          j = 0
        }
      })
    }
    // (D3) CLOSE CONNECTION + FILE
    return new Promise<number>((resolve, reject) => {
      reader.on("end", () => {
        if (this.writer.end) {
          this.writer.end()
        } else if (this.writer.flush) {
          this.writer.flush()
        }
        if (er) {
          reject(er)
        } else {
          this.connection.end((err) => {
            if (err) {
              reject(err)
            } else {
              resolve(i)
            }
          })
        }
      })
    })
  }
}
export function mapOne<T>(results: any, m?: StringMap): T {
  const obj: any = results
  if (!m) {
    return obj
  }
  const mkeys = Object.keys(m as any)
  if (mkeys.length === 0) {
    return obj
  }
  const obj2: any = {}
  const keys = Object.keys(obj)
  for (const key of keys) {
    let k0 = m[key]
    if (!k0) {
      k0 = key
    }
    obj2[k0] = obj[key]
  }
  return obj2
}
export function buildMap(attrs: Attributes): StringMap | undefined {
  const mp: StringMap = {}
  const ks = Object.keys(attrs)
  let isMap = false
  for (const k of ks) {
    const attr = attrs[k]
    attr.name = k
    const field = attr.column ? attr.column : k
    const s = field.toLowerCase()
    if (s !== k) {
      mp[s] = k
      isMap = true
    }
  }
  if (isMap) {
    return mp
  }
  return undefined
}
export function select(table: string, attrs: Attributes): string {
  const cols: string[] = []
  const ks = Object.keys(attrs)
  for (const k of ks) {
    const attr = attrs[k]
    attr.name = k
    const field = attr.column ? attr.column : k
    cols.push(field)
  }
  return `select ${cols.join(",")} from ${table}`
}
