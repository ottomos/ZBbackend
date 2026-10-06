// init-sls-tables.js - Create the sls schema, DealerExecutions tables and sel_ procedures locally.
//
// simulators/sls.DealerExecutions.sql is a verbatim copy of the script that runs on the bank's
// server, so it must NOT be edited. This loader adapts it to the local environment instead:
//   - `USE StreambaseLogs;` is stripped (we connect to DB_DATABASE from .env, e.g. TestDB)
//   - CREATE TABLE statements are skipped when the table already exists, so re-running is safe
//     (the supplied script has no IF NOT EXISTS guard on its tables)
// The procedures use CREATE OR ALTER, so they are re-runnable as-is.

require('dotenv').config();
const sql = require('mssql');
const fs = require('fs');
const path = require('path');

const dbConfig = {
  user: process.env.DB_USER || 'testuser',
  password: process.env.DB_PASSWORD || 'testpassword',
  server: (process.env.DB_SERVER || '').trim(),
  database: process.env.DB_DATABASE || 'TestDB',
  port: parseInt(process.env.DB_PORT || '1433'),
  connectionTimeout: 5000,
  requestTimeout: 10000,
  options: {
    encrypt: process.env.DB_ENCRYPT === 'true',
    trustServerCertificate: process.env.DB_TRUST_CERTIFICATE === 'true'
  }
};

if (process.env.DB_INSTANCE) {
  dbConfig.options.instanceName = process.env.DB_INSTANCE;
}

// Comment-free view of a statement, used only for inspection (never executed).
function stripComments(statement) {
  return statement.replace(/\/\*[\s\S]*?\*\//g, ' ').replace(/--[^\n]*/g, ' ');
}

async function initializeSlsObjects() {
  let pool = null;
  try {
    console.log('Initializing sls DealerExecutions objects');
    console.log(`Connecting to MSSQL at ${dbConfig.server}:${dbConfig.port}/${dbConfig.database}...`);
    pool = await sql.connect(dbConfig);
    console.log('Connected');

    const sqlFile = path.join(__dirname, 'simulators', 'sls.DealerExecutions.sql');
    const sqlContent = fs.readFileSync(sqlFile, 'utf-8');

    const statements = sqlContent.split(/^\s*GO\s*$/gm);

    for (const raw of statements) {
      // Drop any USE <database> line so everything lands in DB_DATABASE.
      const statement = raw.replace(/^[ \t]*USE\s+\[?\w+\]?[ \t]*;?[ \t]*$/gim, '').trim();

      if (stripComments(statement).trim().length === 0) continue;

      const createTable = /CREATE\s+TABLE\s+\[?(\w+)\]?\.\[?(\w+)\]?/i.exec(stripComments(statement));
      if (createTable) {
        const [, schema, table] = createTable;
        const exists = await pool
          .request()
          .input('objectName', sql.NVarChar(256), `${schema}.${table}`)
          .query("SELECT OBJECT_ID(@objectName, 'U') AS id");
        if (exists.recordset[0].id) {
          console.log(`  skip  ${schema}.${table} (already exists)`);
          continue;
        }
        await pool.request().query(statement);
        console.log(`  table ${schema}.${table} created`);
        continue;
      }

      await pool.request().query(statement);
    }

    console.log('\nVerifying...');
    const objects = await pool.request().query(`
      SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS name, 'table' AS kind
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_NAME LIKE 'DealerExecutions%'
      UNION ALL
      SELECT SCHEMA_NAME(schema_id) + '.' + name, 'proc'
      FROM sys.procedures
      WHERE name LIKE 'sel_DealerExecutions%'
      ORDER BY kind, name
    `);
    objects.recordset.forEach(row => console.log(`  ${row.kind.padEnd(5)} ${row.name}`));

    await pool.close();
    console.log('\nDone');
  } catch (err) {
    console.error('Failed:', err.message);
    if (pool) await pool.close();
    process.exitCode = 1;
  }
}

initializeSlsObjects();
