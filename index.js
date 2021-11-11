const { Pool } = require('pg');

/* Common Utilities */

/**
 * Common Result Handler: fetch the first row if exists.
 */
const first = rows => rows? rows[0]: null;

/**
 * Check is undefined.
 */
const isUndefined = o => typeof(o) === 'undefined';

/**
 * Check is string.
 */
const isString = o => typeof(o) === 'string';

/* Database Operators */

/**
 * Execute SQL and response result as json.
 */
const execute = (pool, { sql, handler }, response) => pool.query(sql)
      .then(rs => response.json(handler? handler(rs.rows): rs.rows))
      .catch(exception => response.status(400).json({ sql, exception }));

/* RESTful Actions */

/**
 * Index: list the resources.
 */
const index = table => ({ orders = [], page, size }) => {
    const values = [];

    let orderBy = '';
    if (orders.length > 0) {
        orderBy = ' order by ';
        orderBy += orders
            .map(order => isString(order)? { column: order }: order)
            .map(({ column, mode = 'asc' }) => `${column} ${mode}`)
            .join(', ');
    }
    
    let limitOffset = '';
    if (!isUndefined(page) || !isUndefined(size)) {
        if (!isUndefined(page)) {
            page = parseInt(page);
        }
        if (isUndefined(page) || page < 1) {
            page = 1;
        }

        if (!isUndefined(size)) {
            size = parseInt(size);
        }
        if (isUndefined(size) || size < 0) {
            size = 20;
        }

        limitOffset = ' limit $1 offset $2';
        values.push(size);
        values.push((page - 1) * size);
    }

    return {
        sql: {
            text: `
with statistics as (
  select
    count(*) as total
  from
    ${table}
), records as (
  select
    to_jsonb(${table}) as record
  from
    ${table}
  ${orderBy}
  ${limitOffset}
), page as (
  select
    jsonb_agg(record) as records
  from
    records
)
select
  jsonb_build_object(
    'total', statistics.total,
    'records', page.records
  ) as "result"
from
  statistics,
  page
`,
            values
        },
        handler: rows => {
            const result = rows[0].result;
            if (!isUndefined(page)) {
                result['page'] = page;
            }
            if (!isUndefined(size)) {
                result['size'] = size;
            }
            return result;
        }
    };
};

/**
 * Show: returns specified record.
 */
const show = (table, id) => () => ({
    sql: {
        text: `select * from ${table} where id = $1`,
        values: [id]
    },
    handler: first
});

/**
 * Create: inserts and returns new `record`.
 */
const create = table => record => {
    const keys = Object.keys(record)
    const columns = keys.join(', ');
    const placeholders = keys.keys()
          .map(index => `\$${index + 1}`)
          .join(', ');

    const values = Object.values(record);

    return {
        sql: {
            text: `insert into ${table} (${columns}) values (${placeholders}) returning *`,
            values
        },
        handler: first
    };
};

/**
 * Update: overwrite specified fields of `record`.
 */
const update = (table, id) => record => {
    const columns = Object.keys(record)
          .map((column, index) => `${column} = \$${index + 1}`)
          .join(', ');

    const values = Object.values(record);
    values.push(id);

    return {
        sql: {
            text: `update ${table} set ${columns} where id = \$${values.length} returning *`,
            values
        },
        handler: first
    };
};

/**
 * Patch: same with update.
 */
const patch = update;

/**
 * Delete: delete the specified record.
 */
const remove = (table, id) => () => ({
    sql: {
        text: `delete from ${table} where id = $1 returning *`,
        values: [id]
    },
    handler: first
});

/* Routers */

/**
 * Dispatch request to RESTful action.
 */
const dispatch = (method, table, id) => {
    const isSingle = !isUndefined(id);
    if (isSingle) {
        id = parseInt(id);
    }

    if (method === 'GET' && !isSingle) {
        return index(table);
    } else if (method === 'GET' && isSingle) {
        return show(table, id);
    } else if (method === 'POST' && !isSingle) {
        return create(table);
    } else if (method === 'PUT' && isSingle) {
        return update(table, id);
    } else if (method === 'PATCH' && isSingle) {
        return patch(table, id);
    } else if (method === 'DELETE' && isSingle) {
        return remove(table, id);
    }

    return null;
};

/**
 * Create RESTful request matcher.
 * - Request accepts json type response.
 * - Request URL matches [prefix]<resource>[/<id>].
 */
const matcherOf = (prefix, tables) => {
    const resources = new Set(tables);
    return request => {
        if (request.accepts('json')) {
            let path = request.path;

            // strip prefix
            if (path.startsWith(prefix)) {
                path = path.substring(prefix.length);
            } else {
                return false;
            }

            const [resource, id] = path.split('/', 2);
            if (resources.has(resource)
                && (isUndefined(id) || /^\d+$/.test(id))) {
                return dispatch(request.method, resource, id);
            } else {
                return false;
            }
        }
        return false;
    }
};

/**
 * Create RESTful router.
 */
const router = (pool, matcher) => (request, response, next) => {
    const action = matcher(request);
    if (action) {
        execute(pool, action(request.body), response);
    } else {
        next();
    }
};

// Middleware

/**
 * PostgreSQL Proxy Middleware.
 * - prefix?: optional url prefix. '/' default.
 * - pgConfig: PostgreSQL connection information.
 * - ...tables: list of table names.
 */
const proxy = (...params) => {
    let prefix, pgConfig, tables;
    if (isString(params[0])) {
        [prefix, pgConfig, ...tables] = params;
    } else {
        prefix = '/';
        [pgConfig, ...tables] = params;
    }

    const pool = new Pool(pgConfig);
    const matcher = matcherOf(prefix, tables);
    return router(pool, matcher);
};

module.exports = proxy;
