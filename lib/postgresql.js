var Ddl = require("..")
var Fs = require("fs")
var unquote = require("./utils").unquote
var defineHiddenProperty = require("./utils").defineHiddenProperty
var SQL = Fs.readFileSync(__dirname + "/postgresql.sql", "utf8")
var constraints_SQL = Fs.readFileSync(__dirname + "/postgresql_constraints.sql", "utf8")
var indexes_SQL = Fs.readFileSync(__dirname + "/postgresql_indexes.sql", "utf8")
exports = module.exports = postgresql

/**
 * Queries a PostgreSQL database table for its data definition.
 *
 * Give it a [`Pg.Client`](https://github.com/brianc/node-postgres/wiki/Client)
 * for `connection`.
 * Calls `callback` with an `error` and a [`Ddl`](#Ddl) object with attributes.
 *
 * @example
 * var Ddl = require("ddl")
 * var Pg = require("pg")
 *
 * var db = new Pg.Client("postgresql://localhost/world")
 * db.connect()
 *
 * Ddl.postgresql(db, "people", console.log)
 *
 * @static
 * @method postgresql
 * @param connection
 * @param tableName
 * @param callback
 */
function postgresql(conn, table, options = {}, done) {
  return Promise.all([
    new Promise((resolve, reject) => {
      conn.query(SQL, [table], function(err, resp) {
        if (err) reject(err);
        resolve(attributify(resp.rows, table));
      })
    }),
    new Promise((resolve, reject) => {
      conn.query(constraints_SQL, [table], function(err, resp) {
        if (err) reject(err);
        resolve(constraintify(resp.rows));
      })
    }),
    new Promise((resolve, reject) => {
      conn.query(indexes_SQL, [table], function(err, resp) {
        if (err) reject(err);
        resolve(indexify(resp.rows));
      })
    }),
  ]).then(([attributes, constraints, indexes ])=>{
    done(null, {...attributes, constraints, indexes })
  }).catch((e) => done(e));
}

function constraintify(rows){
  const constraintType = {
    'FOREIGN KEY': 'foreign',
    'PRIMARY KEY': 'primary'
  };

  let constraintsMap = rows.reduce(function (constraints, row) {
    let isForeignKey = constraintType[row.constraint_type] === 'foreign';
    constraints[row.constraint_name]
      = Object.assign((constraints[row.column_name] || {}), {
      name:row.constraint_name,
      type: constraintType[row.constraint_type],
      reference_table: row.foreign_table_name,
      delete: isForeignKey ? (row.on_delete || '').toLowerCase() : undefined,
      update: isForeignKey ? (row.on_update || '').toLowerCase() : undefined,
      keys: [{
        key: row.column_name,
        reference_column: row.foreign_column_name,
      }].concat((constraints[row.constraint_name] && constraints[row.constraint_name].keys || []))
    })
    return constraints;
  }, {})
  return Object.keys(constraintsMap).map((key) => {
    let constraint = constraintsMap[key];
    if(constraint.type === 'primary'){
      let unique = {};
      constraint.keys = constraint.keys.reduce((acc, { key }) => {
        if(!unique[key]){
          acc.push({ key });
          unique[key] = true;
        }
        return acc;
      }, []);
    }
    return constraint;
  });
}

function indexify(rows){
  return rows.reduce(function (indexes, row) {
    return indexes.concat({
      key: row.column_name.split(',').reduce((key, name) => ({ ...key, [name]: 1}), {}),
      unique: row.is_unique,
      name: row.index_name
    })
  }, [])
}

exports.SQL = SQL
exports.attributify = attributify

function attributify(columns, table) {
  return columns.reduce(function(ddl, column) {
    if (column.type == "ARRAY") column.udt += repeat(column.dimensions-1, "[]")
    if(column.udt === 'integer' && column.nullable === false
      && column.default === `nextval('${table}_${column.name}_seq'::regclass)`){
      column.udt = 'serial';
    }
    var attr = ddl.properties[column.name] = typeify(column.udt, column.nullable)
    if(!column.nullable){
      ddl.required.push(column.name);
    }

    if(column.ref){
      console.log(column.ref);
    }

    if (column.type != "ARRAY" && column.default !== null && column.default !== undefined ) {
      attr.default = parseDefault(column.default, parseType(column.type))
    }

    if (column.length != null) attr.maxLength = column.length

    // Don't depend on _* properties being stable between minor versions.
    // They're experimental until I figure out how to pass custom types via
    // JSON Schema which has no concept of dates, times etc.
    defineHiddenProperty(attr, "_type", column.udt.toUpperCase())

    return ddl
  }, new Ddl)
}

// http://www.postgresql.org/docs/9.2/static/datatype.html
var TYPES = {
  BIGSERIAL: "integer",
  BOOLEAN: "boolean",
  "CHARACTER VARYING": "string",
  CHARACTER: "string",
  DATE: "string",
  BIGINT: "integer",
  "DOUBLE PRECISION": "number",
  INTEGER: "integer",
  JSON: "object",
  JSONB: "object",
  NUMERIC: "number",
  REAL: "number",
  SMALLINT: "integer",
  SMALLSERIAL: "integer",
  SERIAL: "integer",
  TEXT: "string",
  "TIME WITHOUT TIME ZONE": "string",
  "TIMESTAMP WITHOUT TIME ZONE": "string"
}

var FORMATS = {
  //"CHARACTER VARYING": "string",
  //CHARACTER: "string",
  DATE: "date",
 //TEXT: "string",
  "TIME WITHOUT TIME ZONE": "time",
  "TIMESTAMP WITHOUT TIME ZONE": "date-time",
  "TIMESTAMP WITH TIME ZONE": "date-time",
  SMALLSERIAL: "serial",
  SERIAL: "serial",
}

var ARRAY = /\[\]$/
var TRUES = require("./utils").TRUES
var FALSES = require("./utils").FALSES
var NUMERIC = require("./utils").NUMERIC

function typeify(pgType, nullable) {
  var isArray = pgType.match(ARRAY)
  var attr = {type: isArray ? "array" : parseType(pgType)}
  if (isArray) attr.items = typeify(pgType.replace(ARRAY, ""))
  if(attr.type === "string" || attr.type === "integer"){
    let format = parseFormat(pgType);
    if(format) attr.format = format;
  }
  return attr
}

function parseFormat(type) {
  type = type.match(/^([^(]+)/)
  return FORMATS[type && type[0].toUpperCase()]
}

function parseType(type) {
  type = type.match(/^([^(]+)/)
  return TYPES[type && type[0].toUpperCase()] || "string"
}

function parseDefault(value, type) {
  if (value == null) return null
  if (value.toLowerCase() == "null") return null
  value = stripCast(value)

  switch (type) {
    case "string":
      // PostgreSQL is precise about using only single qutoes for string
      // literals. As opposed to SQLite, for example.
      if (value[0] != "'") return undefined
      return unquote(value)

    case "integer":
    case "number":
      value = unquote(value)
      if (!value.match(NUMERIC)) return undefined
      return Number(value)

    case "boolean":
      value = unquote(value)
      if (~TRUES.indexOf(value)) return true
      if (~FALSES.indexOf(value)) return false
      return undefined

    default: return undefined
  }
}

function stripCast(val) {
  return val.match(/::[\w ]+$/) ? val.match(/^\(?(.*?)\)?::[\w ]+$/)[1] : val
}

function repeat(n, string) {
  if (n == 0) return ""
  if (n == 1) return string
  return new Array(n + 1).join(string)
}
