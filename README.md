# pgsql-proxy-middleware

The one-liner node.js RESTful middleware for PostgreSQL tables.

# Install

## npm

```sh
npm install pgsql-proxy-middleware
```

## yarn

```sh
yarn add pgsql-proxy-middleware
```

# Config

```js
const bodyParser = require('body-parser');
const proxy = require('pgsql-proxy-middleware');

module.exports = function(app) {
    app.use(bodyParser.json(), proxy({
        database: '<database name>',
        user: '<username>',
        ...
    }, '<table-name>'));
};
```
