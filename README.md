# mysql2-core

A lightweight TypeScript database abstraction for [mysql2](https://www.npmjs.com/package/mysql2), which has 4 main parts:
- A unified API for connection pools, transactions, SQL execution, queries, result mapping, batch execution, and common database utilities.
- Utilities for building and executing MySQL statements with [`mysql2`](https://www.npmjs.com/package/mysql2).
- A metadata-driven persistence layer with support for single records, batch operations, buffered streaming writes, transactions, and configurable value mapping.
- Health check
### Example
- [sql-modular-sample](https://github.com/source-code-template/sql-modular-sample): RESI API with express and MySQL

## Features

* MySQL connection pool management through `mysql2`
* Unified `Executor` abstraction for database operations
* Transaction support with automatic connection release
* Batch SQL execution
* Optional transactional batch execution
* Scalar and single-row queries
* Result-field mapping
* Boolean value conversion
* Dynamic SQL field helpers
* Support for JSON/string conversion of object parameters
* MySQL duplicate-key error normalization
* Compatible APIs for both `Pool` and `PoolConnection`

## Installation

```bash
npm install mysql2 mysql2-core
```

## Architecture

`mysql2-core` separates database operations from the underlying MySQL connection implementation.

```text
                    Executor
                       │
             ┌─────────┴─────────┐
             │                   │
            DB              Transaction
             │                   │
        PoolManager        PoolConnectionManager
             │                   │
            Pool            PoolConnection
```

### Executor

`Executor` defines the common database operations:

```ts
interface Executor {
  driver: string
  param(i: number): string
  execute(sql: string, args?: any[], ctx?: any): Promise<number>
  executeBatch(statements: Statement[], firstSuccess?: boolean, ctx?: any): Promise<number>
  query<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[], ctx?: any): Promise<T[]>
  queryOne<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[], ctx?: any): Promise<T | null>
  executeScalar<T>(sql: string, args?: any[], ctx?: any): Promise<T | null>
  count(sql: string, args?: any[], ctx?: any): Promise<number>
}
```

### DB

`DB` extends `Executor` with transaction creation:

```ts
interface DB extends Executor {
  beginTransaction(): Promise<Transaction>
}
```

### Transaction

`Transaction` extends `Executor` with:

```ts
interface Transaction extends Executor {
  commit(): Promise<void>
  rollback(): Promise<void>
}
```

## Creating a Connection Pool

```ts
import { createPool } from "mysql2-core"

const pool = createPool({
  host: "localhost",
  port: 3306,
  database: "app",
  user: "root",
  password: "password",
  connectionLimit: 10
})
```

`createPool()` creates a `mysql2` pool with `rowsAsArray: true`.

The library also supports the `max` option as an alias for `connectionLimit` when `connectionLimit` is not explicitly provided.

## Using `PoolManager`

`PoolManager` provides the `DB` implementation for a MySQL connection pool.

```ts
import { createPool, PoolManager} from "mysql2-core"

const pool = createPool({
  host: "localhost",
  database: "app",
  user: "root",
  password: "password"
})

const db = new PoolManager(pool)

const users = await db.query(
  "SELECT id, name FROM users WHERE active = ?",
  [1]
)
```

The MySQL driver identifier is:

```ts
db.driver
// "mysql"
```

Parameter placeholders are represented by:

```ts
db.param(0)
// "?"
```

## Execute SQL

`execute()` executes a parameterized statement and returns the affected-row count.

```ts
const affectedRows = await db.execute(
  "UPDATE users SET name = ? WHERE id = ?",
  ["John", 10]
)
```

For an `INSERT`, the returned value is the number of affected rows reported by MySQL.

## Query Rows

Use `query()` when multiple rows are expected.

```ts
const users = await db.query<User>(
  "SELECT id, name FROM users WHERE active = ?",
  [1]
)
```

The result is converted from MySQL's array-row representation into objects using the returned field metadata.

For example:

```sql
SELECT id, name FROM users
```

is represented as:

```ts
[
  {
    id: 1,
    name: "John"
  }
]
```

## Query One Row

`queryOne()` returns the first row or `null` when no row exists.

```ts
const user = await db.queryOne<User>(
  "SELECT id, name FROM users WHERE id = ?",
  [10]
)

if (!user) {
  // user does not exist
}
```

## Execute Scalar

`executeScalar()` returns the first column of the first row.

```ts
const username = await db.executeScalar<string>(
  "SELECT name FROM users WHERE id = ?",
  [10]
)
```

If there is no row, it returns `null`.

## Count

`count()` is a convenience method for scalar count queries.

```ts
const total = await db.count(
  "SELECT COUNT(*) FROM users WHERE active = ?",
  [1]
)
```

A missing scalar result is returned as `0`.

## Transactions

Create a transaction with:

```ts
const tx = await db.beginTransaction()

try {
  await tx.execute(
    "UPDATE accounts SET balance = balance - ? WHERE id = ?",
    [100, 1]
  )

  await tx.execute(
    "UPDATE accounts SET balance = balance + ? WHERE id = ?",
    [100, 2]
  )

  await tx.commit()
} catch (err) {
  await tx.rollback()
  throw err
}
```

`PoolManager.beginTransaction()` obtains a connection from the pool, starts a MySQL transaction, and returns a `PoolConnectionManager`.

The connection is released when `commit()` or `rollback()` completes.

## Batch Execution

Statements are represented by:

```ts
interface Statement {
  query: string
  params?: any[]
}
```

Example:

```ts
const statements = [
  {
    query: "UPDATE users SET active = ? WHERE id = ?",
    params: [1, 10]
  },
  {
    query: "INSERT INTO logs(user_id, action) VALUES (?, ?)",
    params: [10, "activate"]
  }
]

const affectedRows = await db.executeBatch(statements)
```

For multiple statements, the pool-level `executeBatch()` obtains a connection and executes the batch inside a transaction.

The connection is released after completion.

## `firstSuccess`

`executeBatch()` supports an optional `firstSuccess` flag:

```ts
await db.executeBatch(statements, true)
```

When enabled, the first statement is executed first. If it affects zero rows, the batch stops and returns `0`.

When the first statement affects one or more rows, the remaining statements are executed.

For example:

```ts
const statements = [
  {
    query: "UPDATE users SET active = ? WHERE id = ?",
    params: [1, 10]
  },
  {
    query: "INSERT INTO audit_logs(user_id, action) VALUES (?, ?)",
    params: [10, "activate"]
  }
]

await db.executeBatch(statements, true)
```

This allows subsequent operations to depend on the first statement modifying at least one row.

## Multiple Statements

When:

```ts
resource.multipleStatements = true
```

the batch implementation can combine multiple SQL statements into a single MySQL query.

```ts
import { resource } from "mysql2-core"

resource.multipleStatements = true
```

Each statement is terminated with a semicolon when necessary, and parameters are flattened into a single parameter array.

## Direct Connection Operations

The library also exposes functions that work directly with a `Pool` or `PoolConnection`.

```ts
import {
  execute,
  query,
  queryOne,
  executeScalar,
  count
} from "mysql2-core"
```

Example:

```ts
const rows = await query<User>(
  pool,
  "SELECT id, name FROM users"
)
```

For a transaction connection:

```ts
const rows = await query<User>(
  connection,
  "SELECT id, name FROM users"
)
```

## Connection Helpers

The library provides promise-based wrappers around MySQL connection operations.

### Get a connection

```ts
import { getConnection } from "mysql2-core"

const connection = await getConnection(pool)

try {
  // use connection
} finally {
  connection.release()
}
```

### Begin a transaction

```ts
import { beginTransaction } from "mysql2-core"

await beginTransaction(connection)
```

### Commit

```ts
import { commit } from "mysql2-core"

await commit(connection)
```

### Rollback

```ts
import { rollback } from "mysql2-core"

await rollback(connection)
```

## Result Mapping

`query()` accepts a field mapping:

```ts
const users = await db.query<User>(
  "SELECT user_id, user_name FROM users",
  [],
  {
    user_id: "id",
    user_name: "name"
  }
)
```

The result becomes:

```ts
[
  {
    id: 1,
    name: "John"
  }
]
```

The mapping is performed by `mapArray()`.

## Boolean Conversion

Boolean fields can be converted using `Attribute`.

```ts
const users = await db.query<User>(
  "SELECT id, active FROM users",
  [],
  undefined,
  [
    {
      name: "active"
    }
  ]
)
```

Without an explicit `true` value, the following values are interpreted as `true`:

```text
1
T
Y
true
on
```

Other non-null values are interpreted as `false`.

A custom true value can also be provided:

```ts
[
  {
    name: "active",
    true: "Y"
  }
]
```

## Parameter Conversion

Parameters are normalized by `toArray()`.

`undefined` and `null` are converted to SQL `NULL` parameters.

`Date` values are preserved as `Date` instances.

Objects can optionally be converted to JSON strings:

```ts
resource.string = true
```

For example:

```ts
await db.execute(
  "INSERT INTO documents(data) VALUES (?)",
  [{ name: "John" }]
)
```

With `resource.string = true`, the object is serialized using `JSON.stringify()`.

## Dynamic SQL Field Helpers

### `getFields()`

`getFields()` optionally filters a requested field list against an allowed list.

```ts
const fields = getFields(
  ["id", "name"],
  ["id", "name", "email"]
)
```

### `buildFields()`

`buildFields()` produces a comma-separated SQL field list.

```ts
const sqlFields = buildFields(
  ["id", "name"],
  ["id", "name", "email"]
)
```

Result:

```text
id,name
```

When no valid fields are available, `buildFields()` returns:

```text
*
```

### `getMapField()`

`getMapField()` returns a mapped field name when a mapping exists:

```ts
const field = getMapField(
  "user_id",
  {
    user_id: "id"
  }
)
```

Result:

```text
id
```

## Error Handling

Database errors are passed through `buildError()`.

MySQL duplicate-key errors matching:

```text
errno = 1062
code = ER_DUP_ENTRY
```

are additionally marked with:

```ts
error = "duplicate"
```

This allows application code to identify duplicate-key errors without depending entirely on the MySQL error code.

Example:

```ts
try {
  await db.execute(
    "INSERT INTO users(email) VALUES (?)",
    ["john@example.com"]
  )
} catch (err: any) {
  if (err.error === "duplicate") {
    // duplicate record
  }

  throw err
}
```

## API Overview

### Interfaces

```text
StringMap
Statement
Executor
Transaction
DB
Attribute
Attributes
Config
```

### Classes

```text
resource
PoolManager
PoolConnectionManager
```

### Main functions

```text
createPool()
getConnection()
beginTransaction()
commit()
rollback()

execute()
executeBatch()
executeBatchConnectionTx()
executeBatchConnection()

query()
queryOne()
executeScalar()
count()

toArray()
handleResults()
handleBool()
formatData()
mapArray()

getFields()
buildFields()
getMapField()
```

## Example

A simple repository-style implementation can be built on top of `DB`:

```ts
import { DB } from "mysql2-core"

export interface User {
  id: number
  name: string
  active: boolean
}

export class UserRepository {
  constructor(private readonly db: DB) {}

  async findById(id: number): Promise<User | null> {
    return this.db.queryOne<User>(
      `SELECT id, name, active FROM users WHERE id = ?`,
      [id],
      undefined,
      [
        {
          name: "active"
        }
      ]
    )
  }

  async countActive(): Promise<number> {
    return this.db.count(
      "SELECT COUNT(*) FROM users WHERE active = ?",
      [1]
    )
  }

  async updateName(id: number, name: string): Promise<number> {
    return this.db.execute(
      "UPDATE users SET name = ? WHERE id = ?",
      [name, id]
    )
  }
}
```

## Design Philosophy

`mysql2-core` keeps the database layer small while providing a consistent abstraction over the lower-level `mysql2` API.

The main design is based on three concepts:

```text
Executor
   │
   ├── DB
   │    └── beginTransaction()
   │
   └── Transaction
        ├── commit()
        └── rollback()
```

This allows application code to depend on `DB` and `Transaction` rather than directly depending on `Pool` and `PoolConnection`.

## SQL Builder

The library is built around three main concepts:

```text
Attributes
    │
    ▼
SQL Builder
    │
    ▼
Statement
    │
    ▼
mysql2
```

The high-level writers build on top of this:

```text
                   mysql2-core
                       │
          ┌────────────┼────────────┐
          │            │            │
       Writer       BatchWriter   StreamWriter
          │            │            │
          └────────────┴────────────┘
                       │
                 SQL / Execution
```

It is intentionally a small persistence utility rather than a full ORM.

## Attributes

The persistence mapping is described using `Attributes`.

```ts
import type { Attributes } from "mysql2-core"

const attributes: Attributes = {
  id: {
    type: "integer",
    key: true
  },

  name: {
    type: "string"
  },

  active: {
    type: "boolean"
  }
}
```

An `Attribute` supports:

| Property   | Description                             |
| ---------- | --------------------------------------- |
| `column`   | Database column name                    |
| `type`     | Declared data type                      |
| `default`  | Default value                           |
| `key`      | Marks a key attribute                   |
| `noinsert` | Excludes the attribute from inserts     |
| `noupdate` | Excludes the attribute from updates     |
| `version`  | Marks the version attribute             |
| `ignored`  | Excludes the attribute from persistence |
| `true`     | Value used when a boolean is `true`     |
| `false`    | Value used when a boolean is `false`    |

The supported `DataType` values include:

```text
ObjectId
date
datetime
time
boolean
number
integer
string
text
object
array
binary
primitives
booleans
numbers
integers
strings
dates
datetimes
times
```

## Column Mapping

A property can use a different database column name.

```ts
const attributes: Attributes = {
  userId: {
    column: "user_id",
    type: "integer",
    key: true
  },

  firstName: {
    column: "first_name",
    type: "string"
  }
}
```

This maps:

```text
userId     → user_id
firstName  → first_name
```

## SQL Generation

### Parameters

The default parameter builder returns `?`:

```ts
import { param, params } from "mysql2-core"

param(1)
// ?

params(3)
// ["?", "?", "?"]
```

A custom parameter builder can be supplied:

```ts
const buildParam = (i: number) => `$${i}`
```

This allows SQL generation to use a different parameter syntax when necessary.

### Statements

The library represents generated SQL as:

```ts
interface Statement {
  query: string
  params?: any[]
}
```

For example:

```text
{
  query: "insert into users(id,name)values(?,?)",
  params: [1, "John"]
}
```

## Insert and Upsert

`buildToSave()` generates an `INSERT` statement when no key attributes are defined.

```sql
insert into users(name,active)values(?,?)
```

When one or more attributes are marked with `key: true`, the generated statement uses MySQL's duplicate-key update syntax:

```sql
insert into users(id,name)values(?,?) on duplicate key update name=?
```

For example:

```ts
const attributes: Attributes = {
  id: {
    type: "integer",
    key: true
  },

  name: {
    type: "string"
  }
}
```

This allows the same metadata to represent both insert and upsert behavior.

## Boolean Values

Boolean values support both string and numeric representations.

The default representation is:

```text
true  → "1"
false → "0"
```

For example:

```ts
const attributes: Attributes = {
  active: {
    type: "boolean"
  }
}
```

The values are still passed through MySQL parameters.

Custom string values are supported:

```ts
const attributes: Attributes = {
  active: {
    type: "boolean",
    true: "Y",
    false: "N"
  }
}
```

Numeric values are supported as well:

```ts
const attributes: Attributes = {
  active: {
    type: "boolean",
    true: 1,
    false: 0
  }
}
```

Thus `Attribute.true` and `Attribute.false` can be either `string` or `number`.

## Default Values

An attribute can define a default value:

```ts
const attributes: Attributes = {
  status: {
    type: "string",
    default: "active"
  }
}
```

When a value is not provided, the configured default can be used when constructing the insert statement.

## Version

An attribute can be marked as a version field:

```ts
const attributes: Attributes = {
  id: {
    type: "integer",
    key: true
  },

  name: {
    type: "string"
  },

  version: {
    type: "integer",
    version: true
  }
}
```

When building a new record, the version field is initialized to `1`.

The source provides version-field identification and initialization; it does not implement a complete optimistic-locking mechanism.

## Ignored Fields

Use `ignored` to exclude a property from persistence:

```ts
const attributes: Attributes = {
  id: {
    type: "integer",
    key: true
  },

  name: {
    type: "string"
  },

  temporaryValue: {
    ignored: true
  }
}
```

## Insert and Update Controls

### `noinsert`

Prevent an attribute from being included in an insert:

```ts
const attributes: Attributes = {
  id: {
    type: "integer",
    key: true,
    noinsert: true
  }
}
```

### `noupdate`

Prevent an attribute from being included in duplicate-key updates:

```ts
const attributes: Attributes = {
  createdAt: {
    type: "datetime",
    noupdate: true
  }
}
```

## `MySQLWriter`

`MySQLWriter<T>` writes one object.

```ts
import { MySQLWriter } from "mysql2-core"

const writer = new MySQLWriter(pool, "users", attributes)

const count = await writer.write({
  id: 1,
  name: "John",
  active: true
})
```

The operation is:

```text
write()
  │
  ├── map()
  │
  ▼
buildToSave()
  │
  ▼
execute()
```

### Mapping

A mapping function can transform the object before the SQL statement is created:

```ts
const writer = new MySQLWriter(
  pool,
  "users",
  attributes,
  false,
  (user) => ({
    ...user,
    name: user.name.trim()
  })
)
```

### `oneIfSuccess`

When `oneIfSuccess` is enabled, the result is normalized to `0` or `1`:

```ts
const writer = new MySQLWriter(
  pool,
  "users",
  attributes,
  true
)
```

The result becomes:

```text
0 → zero affected rows
1 → one or more affected rows
```

## `MySQLBatchWriter`

`MySQLBatchWriter<T>` writes an array of objects.

```ts
import { MySQLBatchWriter } from "mysql2-core"

const writer = new MySQLBatchWriter(pool, "users", attributes)

const count = await writer.write([
  {
    id: 1,
    name: "John",
    active: true
  },
  {
    id: 2,
    name: "Jane",
    active: true
  }
])
```

The objects are converted into multiple statements using `buildToSaveBatch()` and executed by the batch execution layer.

## `MySQLStreamWriter`

`MySQLStreamWriter<T>` buffers objects and executes them when the configured buffer size is reached.

```ts
import { MySQLStreamWriter } from "mysql2-core"

const writer = new MySQLStreamWriter(pool, "users", attributes, 1000)

for (const user of users) {
  await writer.write(user)
}

await writer.flush()
```

The default size is `1000`.

The execution flow is:

```text
write()
   │
   ▼
buffer
   │
   ├── buffer < size ──► wait
   │
   └── buffer >= size
             │
             ▼
           flush()
             │
             ▼
      buildToSaveBatch()
             │
             ▼
        executeBatch()
```

The final buffered records must be flushed explicitly:

```ts
await writer.flush()
```

## Execution

### `execute`

Execute one SQL statement and return its affected-row count:

```ts
import { execute } from "mysql2-core"

const count = await execute(
  pool,
  "update users set name=? where id=?",
  ["John", 1]
)
```

`execute()` accepts either a `Pool` or `PoolConnection`.

## Batch Execution

### `executeBatch`

`executeBatch()` obtains a connection, runs the batch in a transaction, and releases the connection afterward.

```ts
import { executeBatch } from "mysql2-core"

const count = await executeBatch(
  pool,
  statements
)
```

Its lifecycle is:

```text
getConnection()
      │
      ▼
beginTransaction()
      │
      ▼
execute statements
      │
      ├── success ──► commit()
      │
      └── error ────► rollback()
      │
      ▼
connection.release()
```

The connection is released even when execution fails.

### `executeBatchConnectionTx`

Use `executeBatchConnectionTx()` when the caller already owns a `PoolConnection` but wants the function to manage the transaction.

```ts
await executeBatchConnectionTx(
  connection,
  statements
)
```

### `executeBatchConnection`

Use `executeBatchConnection()` when the caller owns both the connection and transaction lifecycle.

```ts
await executeBatchConnection(
  connection,
  statements
)
```

This allows the statements to participate in an existing transaction.

## Transactions

The library exposes transaction helpers:

```ts
import { beginTransaction, commit, rollback } from "mysql2-core"
```

Example:

```ts
const connection = await getConnection(pool)

try {
  await beginTransaction(connection)

  await execute(
    connection,
    "insert into users(name) values(?)",
    ["John"]
  )

  await commit(connection)
} catch (err) {
  await rollback(connection)
  throw err
} finally {
  connection.release()
}
```

## Multiple Statements

The batch transaction executor can optionally combine multiple statements into a single MySQL request.

Enable:

```ts
import { resource } from "mysql2-core"

resource.multipleStatements = true
```

The statements are combined and executed through `connection.query()`.

This can reduce the number of database round trips for large batches.

## Serialization

`toArray()` prepares parameter values before they are passed to `mysql2`.

`undefined` and `null` are normalized to `null`.

`Date` values remain `Date` objects.

Objects and arrays can optionally be serialized to JSON strings:

```ts
resource.string = true
```

When enabled:

```ts
const value = {
  name: "John",
  age: 30
}
```

is converted using:

```ts
JSON.stringify(value)
```

## Error Handling

MySQL duplicate-key errors are normalized by adding:

```ts
error = "duplicate"
```

to an error when it has:

```text
errno = 1062
code = ER_DUP_ENTRY
```

Example:

```ts
try {
  await writer.write(user)
} catch (err: any) {
  if (err.error === "duplicate") {
    // duplicate key
  }
}
```

The original error is otherwise propagated.

## API

### Types

```ts
StringMap
Statement
DataType
Attribute
Attributes
Metadata
```

### SQL Builders

```ts
param()
params()
metadata()
buildToSave()
buildToSaveBatch()
toString()
version()
toArray()
```

### Execution

```ts
execute()
executeBatch()
executeBatchConnectionTx()
executeBatchConnection()
```

### Connection and Transactions

```ts
getConnection()
beginTransaction()
commit()
rollback()
```

### Writers

```ts
MySQLWriter<T>
MySQLBatchWriter<T>
MySQLStreamWriter<T>
```

## Health Check

Built-in MySQL health checker.

Designed for cloud-native deployments.

Features:

* Connection validation
* Query validation
* Response time measurement
* Configurable timeout
* Kubernetes readiness and liveness probes

Example:

```typescript
const checker = new MySQLChecker(pool.promise());

const result = await checker.check();
```

Example response

```json
{
  "status": "UP",
  "details": {
    "mysql": {
      "status": "UP"
    }
  }
}
```

## Design Philosophy

`mysql2-core` sits between raw `mysql2` and a full ORM.

```text
Raw mysql2
    │
    ▼
mysql2-core
    │
    ├── metadata
    ├── SQL generation
    ├── persistence writers
    ├── batch execution
    └── transaction helpers
    │
    ▼
Application
```

It provides reusable persistence mechanics without introducing entity tracking, relationships, lazy loading, or a large ORM model.

## Requirements

* Node.js
* TypeScript
* MySQL
* [`mysql2`](https://www.npmjs.com/package/mysql2)

## License

MIT



## License

MIT



# mysql2-core

Lightweight TypeScript utilities for building and executing MySQL statements with [`mysql2`](https://www.npmjs.com/package/mysql2).

`mysql2-core` provides a metadata-driven persistence layer with support for single records, batch operations, buffered streaming writes, transactions, and configurable value mapping.


# mysql2-core

A lightweight MySQL database adapter built on top of **mysql2** for the **SQL Repository** framework.

This library provides connection management, transaction handling, query execution, object mapping, batch operations, streaming, exporting, and health checking while remaining completely independent from SQL generation.

Unlike traditional ORMs, this library **does not build SQL**. Instead, it executes SQL generated by the SQL Repository library (or any SQL source) and provides a clean, type-safe runtime layer.

### Example
- [sql-modular-sample](https://github.com/source-code-template/sql-modular-sample): RESI API with express and MySQL

---

# Philosophy

This library has one responsibility:

> **Execute SQL efficiently.**

It intentionally separates SQL execution from SQL generation.

```
                Application
                     │
                     ▼
            SQL Repository Library
              (SQL Generation)
                     │
                     ▼
              Executor Interface
                     │
                     ▼
                MySQL Adapter
                     │
                     ▼
                   mysql2
                     │
                     ▼
                   MySQL
```

This separation makes the entire stack easier to maintain, test, and extend.

---

# Features

- Built on top of **mysql2**
- Connection Pool Management
- Transaction Management
- Query Execution
- Command Execution
- Batch Execution
- Streaming Query Results
- Bulk Data Import
- CSV/Data Export
- Object Mapping
- Boolean Conversion
- Database Health Check
- Promise-based API
- Repository Friendly
- Zero ORM Dependencies

---

# Why Another MySQL Library?

Most MySQL libraries are either:

- Low-level drivers (`mysql2`)
- Full-featured ORMs (TypeORM, Prisma, Sequelize)

This library focuses on infrastructure.

It provides:

- Connection management
- Transactions
- Repository integration
- Batch execution
- Streaming

without hiding SQL from developers.

---

# Architecture

```
                  Application
                       │
                       ▼
              SQL Repository Layer
                       │
                       ▼
               Executor Interface
                       │
                       ▼
                 MySQL Adapter
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
    Connection     Transaction      Stream
        │              │              │
        └──────────────┼──────────────┘
                       ▼
                    mysql2
                       │
                       ▼
                     MySQL
```

---

# Installation

```bash
npm install mysql2
```

```bash
npm install mysql2-core
```

---

# Creating a Database

```ts
import { MySQLDB } from "mysql2-core"

const db = new MySQLDB({

    host: "localhost",

    port: 3306,

    database: "demo",

    user: "root",

    password: "password"

})
```

---

# Query

```ts
const users = await db.query<User>(
    `SELECT * FROM users WHERE age > ?`,
    [18]
)
```

---

# Execute

```ts
await db.execute(
    `UPDATE users SET active = ? WHERE id = ?`
    [true, 10]
)
```

---

# Transactions

```ts
const tx = await db.beginTransaction()

try {
    await tx.execute(
        `INSERT INTO users(name) VALUES(?)`,
        ["John"]
    )
    await tx.commit()
}
catch (err) {
    await tx.rollback()
}
```

---

# Batch Execution

```ts
await db.executeBatch([
    {
        query: "INSERT INTO users(name) VALUES(?)",
        params: ["John"]
    },
    {
        query: "INSERT INTO users(name) VALUES(?)",
        params: ["Jane"]
    }
])
```

---

# Streaming

Stream large result sets without loading everything into memory.

```ts
await db.stream(
    "SELECT * FROM users",
    [],
    async user => {
        console.log(user)
    }
)
```

Ideal for:

- ETL
- Migration
- Reporting
- Exporting millions of rows

---

# Bulk Writer

Insert data efficiently.

```ts
const writer = new MySQLWriter(db)

await writer.write(users)
```

---

# Batch Writer

```ts
const writer = new MySQLBatchWriter(db)

await writer.write(users)
```

---

# Stream Writer

```ts
const writer = new MySQLStreamWriter(db)

await writer.write(stream)
```

Useful for importing very large datasets.

---

# Export

Export query results without loading everything into memory.

```ts
await exporter.export(
    `SELECT * FROM users`,
    "users.csv"
)
```

---

# Object Mapping

Rows are automatically mapped into TypeScript objects.

```ts
interface User {

    id: number

    name: string

    active: boolean

}
```

```ts
const users = await db.query<User>(
    sql
)
```

---

# Boolean Conversion

Automatically converts database values.

```
0  -> false

1  -> true

NULL -> null
```

---

# Health Check

```ts
const ok = await checker.check()
```

Perfect for

- Kubernetes
- Docker
- Load Balancers
- Monitoring

---

# Repository Integration

Designed to work seamlessly with **SQL Repository**.

```
 Application

      │

      ▼

SqlRepository

      │

      ▼

  Executor

      │

      ▼

MySQL Adapter

      │

      ▼

   mysql2
```

The repository layer never depends directly on mysql2.

---

# Connection Pool

Internally the adapter manages a connection pool.

```
 Application

      │

      ▼

Pool Manager

      │

      ▼

 mysql2 Pool
```

Connection reuse improves performance.

---

# Transaction Model

```
 Application

      │

      ▼

 Transaction

      │

      ▼

PoolConnection

      │

      ▼

   mysql2
```

Transactions are isolated from application code.

---

# Error Handling

Database errors are propagated as Promise rejections.

```ts
try {

    await db.execute(sql)

}
catch(err){

    console.error(err)

}
```

---

# Performance

The adapter is designed for high throughput.

Features include:

- Connection Pooling
- Prepared Statement Parameters
- Batch Execution
- Streaming Queries
- Minimal Object Allocation
- Thin Wrapper over mysql2

---

# Design Goals

- Lightweight
- Predictable
- SQL-first
- Repository Friendly
- Driver Independent
- Easy to Debug
- Easy to Extend
- High Performance
- Low Memory Usage

---

# Comparison

| Feature | mysql2-core | mysql2 | TypeORM |
|----------|---------------|---------|----------|
| Connection Pool | ✅ | ✅ | ✅ |
| Transactions | ✅ | ✅ | ✅ |
| Streaming | ✅ | ✅ | Partial |
| Repository Support | ✅ | ❌ | ✅ |
| Batch Execution | ✅ | Manual | Partial |
| SQL Generation | ❌ | ❌ | ORM |
| ORM | ❌ | ❌ | ✅ |
| Lightweight | ✅ | ✅ | ❌ |

---

# Project Structure

```
MySQLDB
│
├── Connection Pool
├── Transactions
├── Query Execution
├── Execute
├── Batch Execute
├── Stream
├── Exporter
├── Object Mapper
├── Health Checker
└── Utilities
```

---

# Future Roadmap

Future versions will introduce:

- SQL AST Compiler integration
- PostgreSQL Adapter
- SQL Server Adapter
- Oracle Adapter
- SQLite Adapter
- Dialect Abstraction
- Prepared Statement Cache
- Retry Policies
- Metrics & Tracing
- Connection Observability

---

# Relationship with SQL Repository

This library is intended to be used together with the SQL Repository library.

```
                 SQL Repository
           (Metadata + SQL Builder)

                      │

                      ▼

               Executor Interface

                      │

        ┌─────────────┴─────────────┐

        ▼                           ▼

   MySQL Adapter             PostgreSQL Adapter

        ▼                           ▼

      mysql2                         pg
```

The SQL Repository library generates SQL.

This library executes SQL.

Together they provide a lightweight, SQL-first data access framework.

---

# Contributing

Contributions are welcome!

Feel free to submit issues, feature requests, or pull requests to improve the project.

---

# License

MIT License
