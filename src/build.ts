import {Attribute, Attributes, Statement, StringMap} from './metadata';

export function param(i: number): string {
  return '?';
}
export function params(length: number, from?: number): string[] {
  if (from === undefined || from == null) {
    from = 0;
  }
  const ps: string[] = [];
  for (let i = 1; i <= length; i++) {
    ps.push(param(i + from));
  }
  return ps;
}
export interface Metadata {
  keys: Attribute[];
  bools?: Attribute[];
  map?: StringMap;
  version?: string;
  fields?: string[];
}
export function metadata(attrs: Attributes): Metadata {
  const mp: StringMap = {};
  const ks = Object.keys(attrs);
  const ats: Attribute[] = [];
  const bools: Attribute[] = [];
  const fields: string[] = [];
  const m: Metadata = {keys: ats, fields};
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
    if (attr.type === 'boolean') {
      bools.push(attr);
    }
    if (attr.version) {
      m.version = k;
    }
    const field = (attr.column ? attr.column : k);
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
export function buildToSave<T>(obj: T, table: string, attrs: Attributes, ver?: string, buildParam?: (i: number) => string, i?: number): Statement|undefined {
  if (!i) {
    i = 1;
  }
  if (!buildParam) {
    buildParam = param;
  }
  const ks = Object.keys(attrs);
  const pks: Attribute[] = [];
  const cols: string[] = [];
  const values: string[] = [];
  const args: any[] = [];
  let isVersion = false;
  const o: any = obj;
  for (const k of ks) {
    const attr = attrs[k];
    attr.name = k;
    if (attr.key) {
      pks.push(attr);
    }
    let v = o[k];
    if (v === undefined || v == null) {
      v = attr.default;
    }
    if (v !== undefined && v != null && !attr.ignored && !attr.noinsert) {
      const field = (attr.column ? attr.column : k);
      cols.push(field);
      if (k === ver) {
        isVersion = true;
        values.push(`${1}`);
      } else {
        if (v === '') {
          values.push(`''`);
        } else if (typeof v === 'number') {
          values.push(toString(v));
        } else {
          const p = buildParam(i++);
          values.push(p);
          if (typeof v === 'boolean') {
            if (v === true) {
              const v2 = (attr.true ? attr.true : `'1'`);
              args.push(v2);
            } else {
              const v2 = (attr.false ? attr.false : `'0'`);
              args.push(v2);
            }
          } else {
            args.push(v);
          }
        }
      }
    }
  }
  if (!isVersion && ver && ver.length > 0) {
    const attr = attrs[ver];
    const field = (attr.column ? attr.column : ver);
    cols.push(field);
    values.push(`${1}`);
  }
  if (pks.length === 0) {
    if (cols.length === 0) {
      return undefined;
    } else {
      const q = `insert into ${table}(${cols.join(',')})values(${values.join(',')})`;
      return { query: q, params: args };
    }
  } else {
    const colSet: string[] = [];
    for (const k of ks) {
      const v = o[k];
      if (v !== undefined) {
        const attr = attrs[k];
        if (attr && !attr.key && !attr.ignored && !attr.noupdate) {
          const field = (attr.column ? attr.column : k);
          let x: string;
          if (v == null) {
            x = 'null';
          } else if (v === '') {
            x = `''`;
          } else if (typeof v === 'number') {
            x = toString(v);
          } else {
            x = buildParam(i++);
            if (typeof v === 'boolean') {
              if (v === true) {
                const v2 = (attr.true ? '' + attr.true : `'1'`);
                args.push(v2);
              } else {
                const v2 = (attr.false ? '' + attr.false : `'0'`);
                args.push(v2);
              }
            } else {
              args.push(v);
            }
          }
          colSet.push(`${field}=${x}`);
        }
      }
    }
    if (colSet.length === 0) {
      const q = `insert ignore into ${table}(${cols.join(',')})values(${values.join(',')})`;
      return { query: q, params: args };
    } else {
      const q = `insert into ${table}(${cols.join(',')})values(${values.join(',')}) on duplicate key update ${colSet.join(',')}`;
      return { query: q, params: args };
    }
  }
}
export function buildToSaveBatch<T>(objs: T[], table: string, attrs: Attributes, ver?: string, buildParam?: (i: number) => string): Statement[] {
  if (!buildParam) {
    buildParam = param;
  }
  const sts: Statement[] = [];
  const ks = Object.keys(attrs);
  for (const obj of objs) {
    let i = 1;
    const cols: string[] = [];
    const values: string[] = [];
    const args: any[] = [];
    let isVersion = false;
    const o: any = obj;
    for (const k of ks) {
      const attr = attrs[k];
      let v = o[k];
      if (v === undefined || v == null) {
        v = attr.default;
      }
      if (v != null && v !== undefined && !attr.ignored && !attr.noinsert) {
        const field = (attr.column ? attr.column : k);
        cols.push(field);
        if (k === ver) {
          isVersion = true;
          values.push(`${1}`);
        } else {
          if (v === '') {
            values.push(`''`);
          } else if (typeof v === 'number') {
            values.push(toString(v));
          } else {
            const p = buildParam(i++);
            values.push(p);
            if (typeof v === 'boolean') {
              if (v === true) {
                const v2 = (attr.true ? attr.true : `'1'`);
                args.push(v2);
              } else {
                const v2 = (attr.false ? attr.false : `'0'`);
                args.push(v2);
              }
            } else {
              args.push(v);
            }
          }
        }
      }
    }
    if (!isVersion && ver && ver.length > 0) {
      const attr = attrs[ver];
      const field = (attr.column ? attr.column : ver);
      cols.push(field);
      values.push(`${1}`);
    }
    const colSet: string[] = [];
    for (const k of ks) {
      const v = o[k];
      if (v !== undefined) {
        const attr = attrs[k];
        if (attr && !attr.key && !attr.ignored && k !== ver && !attr.noupdate) {
          const field = (attr.column ? attr.column : k);
          let x: string;
          if (v == null) {
            x = 'null';
          } else if (v === '') {
            x = `''`;
          } else if (typeof v === 'number') {
            x = toString(v);
          } else {
            x = buildParam(i++);
            if (typeof v === 'boolean') {
              if (v === true) {
                const v2 = (attr.true ? '' + attr.true : `'1'`);
                args.push(v2);
              } else {
                const v2 = (attr.false ? '' + attr.false : `'0'`);
                args.push(v2);
              }
            } else {
              args.push(v);
            }
          }
          colSet.push(`${field}=${x}`);
        }
      }
    }
    if (colSet.length === 0) {
      const q = `insert ignore into ${table}(${cols.join(',')})values(${values.join(',')});`;
      const smt: Statement = { query: q, params: args };
      sts.push(smt);
    } else {
      const q = `insert into ${table}(${cols.join(',')})values(${values.join(',')}) on duplicate key update ${colSet.join(',')};`;
      const smt: Statement = { query: q, params: args };
      sts.push(smt);
    }
  }
  return sts;
}
const n = 'NaN';
export function toString(v: number): string {
  let x = '' + v;
  if (x === n) {
    x = 'null';
  }
  return x;
}
