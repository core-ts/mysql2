import { Attribute, Attributes, Statement, StringMap } from "./metadata"

export function param(i: number): string {
  return "?"
}
export function params(length: number, from?: number): string[] {
  if (from == null) {
    from = 0
  }
  const ps: string[] = []
  for (let i = 1; i <= length; i++) {
    ps.push(param(i + from))
  }
  return ps
}
export interface Metadata {
  keys: Attribute[]
  bools?: Attribute[]
  map?: StringMap
  version?: string
  fields?: string[]
}
export function metadata(attrs: Attributes): Metadata {
  const mp: StringMap = {}
  const ks = Object.keys(attrs)
  const ats: Attribute[] = []
  const bools: Attribute[] = []
  const fields: string[] = []
  const m: Metadata = { keys: ats, fields }
  let isMap = false
  for (const k of ks) {
    const attr = attrs[k]
    attr.name = k
    if (attr.key) {
      ats.push(attr)
    }
    if (!attr.ignored) {
      fields.push(k)
    }
    if (attr.type === "boolean") {
      bools.push(attr)
    }
    if (attr.version) {
      m.version = k
    }
    const field = attr.column ? attr.column : k
    const s = field.toLowerCase()
    if (s !== k) {
      mp[s] = k
      isMap = true
    }
  }

  if (isMap) {
    m.map = mp
  }
  if (bools.length > 0) {
    m.bools = bools
  }
  return m
}
export function buildToSave<T>(obj: T, table: string, attrs: Attributes, pks?: Attribute[], ver?: string, buildParam?: (i: number) => string): Statement {
  if (!buildParam) {
    buildParam = param
  }
  const ks = Object.keys(attrs)
  if (!pks) {
    pks = []
    for (const k of ks) {
      const attr = attrs[k]
      attr.name = k
      if (attr.key) {
        pks.push(attr)
      }
      if (attr.version) {
        ver = k
      }
    }
  }

  let isUpdate = true
  let isVersion = false
  const cols: string[] = []
  const values: string[] = []
  const args: any[] = []
  const o: any = obj
  for (const k of pks) {
    if (k.name) {
      let v = o[k.name]
      if (v == null) {
        isUpdate = false
      }
    }
  }
  let i = 1
  for (const k of ks) {
    const attr = attrs[k]
    if (!attr) {
      continue
    }
    let v = o[k]
    if (v == null && attr.default !== undefined) {
      v = attr.default
    }
    if (v != null && !attr.ignored && !attr.noinsert) {
      const field = attr.column ? attr.column : k
      cols.push(field)
      if (k === ver) {
        isVersion = true
        ver = k
        values.push(`${1}`)
      } else {
        if (v === "") {
          values.push(`''`)
        } else if (typeof v === "number") {
          values.push(toString(v))
        } else {
          const p = buildParam(i++)
          values.push(p)
          if (typeof v === "boolean") {
            if (v === true) {
              const v2 = attr.true !== undefined ? attr.true : `1`
              args.push(v2)
            } else {
              const v2 = attr.false !== undefined ? attr.false : `0`
              args.push(v2)
            }
          } else {
            args.push(v)
          }
        }
      }
    }
  }
  if (cols.length > 0 && ver && isVersion === false) {
    const attr = attrs[ver]
    if (attr) {
      const field = attr.column ? attr.column : ver
      cols.push(field)
      values.push("1")
    }
  }
  if (isUpdate === false || pks.length === 0) {
    if (cols.length === 0) {
      return { query: "", params: args }
    } else {
      if (pks.length === 0) {
        const q = `insert into ${table}(${cols.join(",")})values(${values.join(",")})`
        return { query: q, params: args }
      } else {
        const q = `insert ignore into ${table}(${cols.join(",")})values(${values.join(",")})`
        return { query: q, params: args }
      }
    }
  } else {
    const colSet: string[] = []
    let version: string | null = null
    for (const k of ks) {
      const v = o[k]
      if (v !== undefined) {
        const attr = attrs[k]
        if (attr && !attr.key && !attr.ignored && !attr.noupdate) {
          const field = attr.column ? attr.column : k
          let x: string
          if (attr.version) {
            version = k
            x = `${field} + 1`
          } else {
            if (v === "") {
              x = `''`
            } else if (typeof v === "number") {
              x = toString(v)
            } else {
              x = buildParam(i++)
              if (typeof v === "boolean") {
                if (v === true) {
                  const v2 = attr.true !== undefined ? attr.true : `1`
                  args.push(v2)
                } else {
                  const v2 = attr.false !== undefined ? attr.false : `0`
                  args.push(v2)
                }
              } else {
                args.push(v)
              }
            }
          }
          colSet.push(`${field}=${x}`)
        }
      }
    }
    if (cols.length > 0) {
      if (colSet.length === 0) {
        const q = `insert ignore into ${table}(${cols.join(",")})values(${values.join(",")})`
        return { query: q, params: args }
      } else {
        const q = `insert into ${table}(${cols.join(",")})values(${values.join(",")}) on duplicate key update ${colSet.join(",")}`
        return { query: q, params: args }
      }
    } else {
      if (colSet.length === 0) {
        return { query: "", params: args }
      } else {
        const colQuery: string[] = []
        for (const attr of pks) {
          const field = attr.column ? attr.column : attr.name
          const na = attr.name ? attr.name : ""
          const v = o[na]
          let x: string
          if (v === "") {
            x = `''`
          } else if (typeof v === "number") {
            x = toString(v)
          } else {
            x = buildParam(i++)
            if (typeof v === "boolean") {
              if (v === true) {
                const v2 = attr.true !== undefined ? "" + attr.true : `1`
                args.push(v2)
              } else {
                const v2 = attr.false !== undefined ? "" + attr.false : `0`
                args.push(v2)
              }
            } else {
              args.push(v)
            }
          }
          colQuery.push(`${field}=${x}`)
        }
        if (version && version.length > 0) {
          const v = o[version]
          if (typeof v === "number" && !isNaN(v)) {
            const attr = attrs[version]
            if (attr) {
              const field = attr.column ? attr.column : version
              colQuery.push(`${field}=${v}`)
            }
          }
        }
        const q = `update ${table} set ${colSet.join(",")} where ${colQuery.join(" and ")}`
        return { query: q, params: args }
      }
    }
  }
}
export function buildToSaveBatch<T>(
  objs: T[],
  table: string,
  attrs: Attributes,
  pks?: Attribute[],
  ver?: string,
  buildParam?: (i: number) => string,
): Statement[] {
  if (!buildParam) {
    buildParam = param
  }
  const sts: Statement[] = []
  if (!pks) {
    pks = []
    const ks = Object.keys(attrs)
    for (const k of ks) {
      const attr = attrs[k]
      attr.name = k
      if (attr.key) {
        pks.push(attr)
      }
      if (attr.version) {
        ver = k
      }
    }
  }

  for (const obj of objs) {
    const smt = buildToSave(obj, table, attrs, pks, ver, buildParam)
    if (smt.query) {
      sts.push(smt)
    }
  }
  return sts
}
export function toString(v: number): string {
  if (v === v && v !== Infinity && v !== -Infinity) {
    return "" + v
  }
  return "null"
}
