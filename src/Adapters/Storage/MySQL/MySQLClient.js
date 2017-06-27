
const parser = require('./MySQLConfigParser');
const mysql = require('mysql2/promise');
const bluebird = require('bluebird');

export function createClient(uri, databaseOptions) {
  let dbOptions = {};
  databaseOptions = databaseOptions || {};

  if (uri) {
    dbOptions = parser.getDatabaseOptionsFromURI(uri);
  }

  for (const key in databaseOptions) {
    dbOptions[key] = databaseOptions[key];
  }
  dbOptions['Promise'] = bluebird;
  console.log(dbOptions);
  return mysql.createConnection(dbOptions);
}
