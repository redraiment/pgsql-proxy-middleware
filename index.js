// Database wrapper

const { Pool } = require('pg');

const executor = (pool, response) => (({ sql, wrapper }) => (
    pool.query(sql)
        .then(rs => response.json(wrapper? wrapper(rs.rows): rs.rows))
        .catch(exception => response.status(400).json({ sql, exception }))
));

const first = rows => rows? rows[0]: null;

// RESTful actions

const split = parameters => ({
    keys: Object.keys(parameters),
    values: Object.values(parameters)
});

const index = (table) => (() => ({
    sql: `select * from ${table}`
}));

const show = (table, id) => (() => ({
    sql: {
        text: `select * from ${table} where id = $1`,
        values: [id]
    },
    wrapper: first
}));

const create = (table) => (parameters => {
    const { keys, values } = split(parameters);
    return {
        sql: {
            text: `insert into ${table} (${keys.join(', ')}) values (${keys.map((key, index) => `\$${index + 1}`).join(', ')}) returning *`,
            values
        },
        wrapper: first
    };
});

const update = (table, id) => (parameters => {
    const { keys, values } = split(parameters);
    values.push(id);
    return {
        sql: {
            text: `update ${table} set ${keys.map((key, index) => `${key} = \$${index + 1}`).join(', ')} where id = \$${values.length} returning *`,
            values
        },
        wrapper: first
    };
});

const patch = update;

const remove = (table, id) => ((params) => ({
    sql: {
        text: `delete from ${table} where id = $1 returning *`,
        values: [id]
    },
    wrapper: first
}));

// url routers

const regexp_quote = s => s? s.replace(/([\[\]\^\$\|\(\)\\\+\*\?\{\}\=\!])/gi, '\\$1'): s;

const url_path_pattern = tables => new RegExp(`^/(${tables.map(regexp_quote).join('|')})(?:/(\\d+))?/?\$`, 'i');

const router = (method, table, id) => {
    const is_single = id !== undefined;
    if (method === 'GET' && !is_single) {
        return index(table);
    } else if (method === 'GET' && is_single) {
        return show(table, id);
    } else if (method === 'POST' && !is_single) {
        return create(table);
    } else if (method === 'PUT' && is_single) {
        return update(table, id);
    } else if (method === 'PATCH' && is_single) {
        return patch(table, id);
    } else if (method === 'DELETE' && is_single) {
        return remove(table, id);
    } else {
        return null;
    }
};

const match = (pattern, request) => {
    if (request.accepts('json')) {
        const matcher = pattern.exec(request.path);
        if (matcher) {
            const table = matcher[1];
            const id = matcher[2];
            return router(request.method, table, id);
        }
    }
    return null;
}

// middleware

const proxy = (pgConfig, ...tables) => {
    const pool = new Pool(pgConfig);
    const pattern = url_path_pattern(tables);
    return (request, response, next) => {
        const action = match(pattern, request);
        if (action) {
            const { sql, wrapper } = action(request.body);
            pool.query(sql)
                .then(rs => response.json(wrapper? wrapper(rs.rows): rs.rows))
                .catch(exception => response.status(400).json({ sql, exception }))
        } else {
            next();
        }
    };
};

module.exports = proxy;
