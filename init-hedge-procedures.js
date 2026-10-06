// init-hedge-procedures.js - Initialize APM Hedge Stored Procedures
require('dotenv').config();
const sql = require('mssql');
const fs = require('fs');
const path = require('path');

const dbConfig = {
  user: process.env.DB_USER || 'testuser',
  password: process.env.DB_PASSWORD || 'testpassword',
  server: process.env.DB_SERVER,
  database: process.env.DB_DATABASE || 'TestDB',
  port: parseInt(process.env.DB_PORT || '1433'),
  connectionTimeout: 5000,
  requestTimeout: 10000,
  options: {
    encrypt: process.env.DB_ENCRYPT === 'true',
    trustServerCertificate: process.env.DB_TRUST_CERTIFICATE === 'true'
  }
};

async function initializeHedgeProcedures() {
  let pool = null;
  try {
    console.log('📋 Initializing APM Hedge Stored Procedures');
    console.log(`🔄 Connecting to MSSQL at ${dbConfig.server}:${dbConfig.port}...`);
    pool = await sql.connect(dbConfig);
    console.log('✅ Connected to MSSQL TestDB');

    const sqlFiles = [
      {
        file: 'apm.sel_PeriodMatrixReport.sql',
        label: 'Period Matrix Report',
        verifyLike: 'sel_PeriodMatrixReport%',
      },
      {
        file: 'apm.sel_HedgePerformanceReport.sql',
        label: 'APM Hedge Performance',
        verifyLike: 'sel_HedgePerformanceReport%',
      },
      {
        file: 'apm.sel_MatchedvsHedgedReport.sql',
        label: 'Hedge Routing by Currency Pair (MatchedvsHedged)',
        verifyLike: 'sel_MatchedvsHedgedReport%',
      },
      {
        file: 'apm.sel_VenueSummaryReport.sql',
        label: 'Interbank LP Distribution (VenueSummaryReport)',
        verifyLike: 'sel_VenueSummaryReport%',
      },
      {
        file: 'apm.sel_LpReport.sql',
        label: 'Interbank Counterparty Distribution (LpReport)',
        verifyLike: 'sel_LpReport%',
      },
    ];

    for (const entry of sqlFiles) {
      console.log(`\n📄 Reading ${entry.label}...`);
      const sqlFile = path.join(__dirname, 'simulators', entry.file);
      const sqlContent = fs.readFileSync(sqlFile, 'utf-8');

      console.log(`🔨 Executing ${entry.label} procedures...`);
      const statements = sqlContent
        .split(/^\s*GO\s*$/gm)
        .map(s => s.trim())
        .filter(s => s.length > 0 && !s.startsWith('--'));

      for (const statement of statements) {
        if (statement.length > 0) {
          await pool.request().query(statement);
        }
      }
      console.log(`✅ ${entry.label} procedures created/updated successfully`);

      console.log(`🔍 Verifying ${entry.label} procedures...`);
      const result = await pool.request().query(`
        SELECT name FROM sys.procedures
        WHERE name LIKE '${entry.verifyLike}'
        ORDER BY name
      `);
      if (result.recordset && result.recordset.length > 0) {
        result.recordset.forEach(row => {
          console.log(`  ✓ apm.${row.name}`);
        });
      } else {
        console.log(`  ⚠️  No procedures found for ${entry.label}`);
      }
    }

    await pool.close();
    console.log('\n✅ All hedge procedures initialized successfully');
    process.exit(0);
  } catch (err) {
    console.error('❌ Error:', err.message);
    if (pool) {
      try { await pool.close(); } catch (e) {}
    }
    process.exit(1);
  }
}

if (require.main === module) {
  initializeHedgeProcedures();
}
