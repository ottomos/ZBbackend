// init-msdb.js - Initialize MSSQL TestDB
// NOTE: This script must be run from Windows PowerShell, not WSL
// The mssql npm module has native bindings that hang on WSL

const sql = require('mssql');

const dbConfig = {
  user: 'testuser',
  password: 'testpassword',
  server: '192.168.68.101',
  database: 'TestDB',
  port: 1433,
  connectionTimeout: 5000,
  requestTimeout: 5000,
  options: {
    encrypt: false,
    trustServerCertificate: true
  }
};

const passwordHash = '$2b$10$IGRbKbrhHY59YqPPsaZeH.qHI8ctFRCfCP9On20YRMCbYXQLOOc3e';
const users = [
  { id: 'cm4v1w2x10001z8x7y9z8a1b2c3d4e5f6', username: 'admin', email: 'admin@ktgui.com', password: passwordHash, role: 'CoE', status: 'Active', entity: 'All', gsm: '+965123456789', name: 'Admin', surname: 'User', permissions: JSON.stringify(['User Management', 'View Trading Data']), createdAt: '2025-12-12T10:00:00.000Z', updatedAt: '2025-12-12T10:00:00.000Z' },
  { id: 'cm4v1w2x10002z8x7y9z8a1b2c3d4e5f7', username: 'trader1', email: 'trader1@ktgui.com', password: passwordHash, role: 'Entity Trader', status: 'Active', entity: 'KT', gsm: '+965987654321', name: 'Trader', surname: 'One', permissions: JSON.stringify(['Trading Operations', 'View Reports']), createdAt: '2025-12-12T10:01:00.000Z', updatedAt: '2025-12-12T10:01:00.000Z' },
  { id: 'cm4v1w2x10003z8x7y9z8a1b2c3d4e5f8', username: 'manager1', email: 'manager1@ktgui.com', password: passwordHash, role: 'Management', status: 'Active', entity: 'KFH', gsm: '+965456789123', name: 'Manager', surname: 'One', permissions: JSON.stringify(['View Only', 'Analytics', 'Reports']), createdAt: '2025-12-12T10:02:00.000Z', updatedAt: '2025-12-12T10:02:00.000Z' },
  { id: 'cm4v1w2x10004z8x7y9z8a1b2c3d4e5f9', username: 'viewer', email: 'viewer@ktgui.com', password: passwordHash, role: 'Entity Viewer', status: 'Inactive', entity: 'AUB', gsm: '+965789012345', name: 'Viewer', surname: 'User', permissions: JSON.stringify(['View Only']), createdAt: '2025-12-12T10:03:00.000Z', updatedAt: '2025-12-12T10:03:00.000Z' },
  { id: 'cm4v1w2x10005z8x7y9z8a1b2c3d4e5fa', username: 'kfh.trader', email: 'kfh.trader@ktgui.com', password: passwordHash, role: 'Entity Trader', status: 'Active', entity: 'KFH', gsm: '+965321098765', name: 'KFH', surname: 'Trader', permissions: JSON.stringify(['Trading Operations', 'View Reports']), createdAt: '2025-12-12T10:04:00.000Z', updatedAt: '2025-12-12T10:04:00.000Z' },
  { id: 'cm4v1w2x10006z8x7y9z8a1b2c3d4e5fb', username: 'aub.manager', email: 'aub.manager@ktgui.com', password: passwordHash, role: 'Management', status: 'Active', entity: 'AUB', gsm: '+965147852369', name: 'AUB', surname: 'Manager', permissions: JSON.stringify(['Management Reports', 'Analytics', 'Oversight']), createdAt: '2025-12-12T10:05:00.000Z', updatedAt: '2025-12-12T10:05:00.000Z' },
  { id: 'cm4v1w2x10007z8x7y9z8a1b2c3d4e5fc', username: 'john.wilson', email: 'john.wilson@ktgui.com', password: passwordHash, role: 'Entity Viewer', status: 'Active', entity: 'KT', gsm: '+965258369147', name: 'John', surname: 'Wilson', permissions: JSON.stringify(['View Only']), createdAt: '2025-12-12T10:06:00.000Z', updatedAt: '2025-12-12T10:06:00.000Z' },
  { id: 'cm4v1w2x10008z8x7y9z8a1b2c3d4e5fd', username: 'sarah.brown', email: 'sarah.brown@ktgui.com', password: passwordHash, role: 'Management', status: 'Inactive', entity: 'AUB', gsm: '+965258369147', name: 'Sarah', surname: 'Brown', permissions: JSON.stringify(['Management Reports', 'Analytics', 'Oversight']), createdAt: '2025-12-12T10:07:00.000Z', updatedAt: '2025-12-12T10:07:00.000Z' },
  { id: 'cm4v1w2x10009z8x7y9z8a1b2c3d4e5fe', username: 'michael.head', email: 'michael.head@ktgui.com', password: passwordHash, role: 'Entity Trader', status: 'Active', entity: 'KFH', gsm: '+965369258147', name: 'Michael', surname: 'Head', permissions: JSON.stringify(['Trading Operations', 'View Reports']), createdAt: '2025-12-12T10:08:00.000Z', updatedAt: '2025-12-12T10:08:00.000Z' }
];

const session = { id: 'dev-session-001', token: 'dev-session-token', userId: 'cm4v1w2x10003z8x7y9z8a1b2c3d4e5f8', expiresAt: '2025-12-19T21:33:20.000Z', createdAt: '2025-12-12T21:33:20.000Z' };

async function initializeMsDatabase() {
  let pool = null;
  try {
    console.log('📋 MSSQL Database Initialization');
    console.log('🔄 Connecting to MSSQL at 192.168.68.101:1433...');
    pool = await sql.connect(dbConfig);
    console.log('✅ Connected to MSSQL TestDB');

    console.log('🔨 Creating tables...');
    await pool.request().query(`IF SCHEMA_ID('apm') IS NULL EXEC('CREATE SCHEMA apm')`);
    await pool.request().query(`IF OBJECT_ID('apm.users', 'U') IS NULL AND OBJECT_ID('dbo.users', 'U') IS NOT NULL ALTER SCHEMA apm TRANSFER dbo.users`);
    await pool.request().query(`IF OBJECT_ID('apm.sessions', 'U') IS NULL AND OBJECT_ID('dbo.sessions', 'U') IS NOT NULL ALTER SCHEMA apm TRANSFER dbo.sessions`);
    await pool.request().query(`IF OBJECT_ID('apm.audit_logs', 'U') IS NULL AND OBJECT_ID('dbo.audit_logs', 'U') IS NOT NULL ALTER SCHEMA apm TRANSFER dbo.audit_logs`);

    await pool.request().query(`IF OBJECT_ID('apm.users', 'U') IS NULL CREATE TABLE apm.users (id NVARCHAR(50) PRIMARY KEY, username NVARCHAR(50) UNIQUE NOT NULL, email NVARCHAR(100) UNIQUE NOT NULL, password NVARCHAR(255) NOT NULL, role NVARCHAR(50) DEFAULT 'user', status NVARCHAR(50) DEFAULT 'Active', entity NVARCHAR(50), gsm NVARCHAR(20), name NVARCHAR(100), surname NVARCHAR(100), permissions NVARCHAR(MAX), createdAt DATETIME, updatedAt DATETIME)`);
    console.log('  ✓ users table created');

    await pool.request().query(`IF OBJECT_ID('apm.sessions', 'U') IS NULL CREATE TABLE apm.sessions (id NVARCHAR(50) PRIMARY KEY, token NVARCHAR(255) UNIQUE NOT NULL, userId NVARCHAR(50) NOT NULL, expiresAt DATETIME NOT NULL, createdAt DATETIME DEFAULT GETDATE(), FOREIGN KEY (userId) REFERENCES apm.users(id) ON DELETE CASCADE)`);
    console.log('  ✓ sessions table created');

    await pool.request().query(`IF OBJECT_ID('apm.audit_logs', 'U') IS NULL CREATE TABLE apm.audit_logs (id NVARCHAR(50) PRIMARY KEY, userId NVARCHAR(50) NOT NULL, action NVARCHAR(50) NOT NULL, tableName NVARCHAR(50) NOT NULL, recordId NVARCHAR(50) NOT NULL, changedFields NVARCHAR(MAX), timestamp DATETIME DEFAULT GETDATE(), ipAddress NVARCHAR(50), userAgent NVARCHAR(255), FOREIGN KEY (userId) REFERENCES apm.users(id))`);
    console.log('  ✓ audit_logs table created');

    console.log('🔄 Clearing existing data...');
    await pool.request().query('DELETE FROM apm.sessions');
    await pool.request().query('DELETE FROM apm.users');

    console.log('📥 Seeding user data...');
    for (const user of users) {
      await pool.request()
        .input('id', sql.NVarChar(50), user.id)
        .input('username', sql.NVarChar(50), user.username)
        .input('email', sql.NVarChar(100), user.email)
        .input('password', sql.NVarChar(255), user.password)
        .input('role', sql.NVarChar(50), user.role)
        .input('status', sql.NVarChar(50), user.status)
        .input('entity', sql.NVarChar(50), user.entity)
        .input('gsm', sql.NVarChar(20), user.gsm)
        .input('name', sql.NVarChar(100), user.name)
        .input('surname', sql.NVarChar(100), user.surname)
        .input('permissions', sql.NVarChar(sql.MAX), user.permissions)
        .input('createdAt', sql.DateTime, new Date(user.createdAt))
        .input('updatedAt', sql.DateTime, new Date(user.updatedAt))
        .query(`INSERT INTO apm.users (id, username, email, password, role, status, entity, gsm, name, surname, permissions, createdAt, updatedAt) VALUES (@id, @username, @email, @password, @role, @status, @entity, @gsm, @name, @surname, @permissions, @createdAt, @updatedAt)`);
    }
    console.log(`  ✓ ${users.length} users seeded`);

    console.log('📥 Seeding session data...');
    await pool.request()
      .input('id', sql.NVarChar(50), session.id)
      .input('token', sql.NVarChar(255), session.token)
      .input('userId', sql.NVarChar(50), session.userId)
      .input('expiresAt', sql.DateTime, new Date(session.expiresAt))
      .input('createdAt', sql.DateTime, new Date(session.createdAt))
      .query(`INSERT INTO apm.sessions (id, token, userId, expiresAt, createdAt) VALUES (@id, @token, @userId, @expiresAt, @createdAt)`);
    console.log('  ✓ Session data seeded');

    await pool.close();
    console.log('✅ MSSQL database initialized successfully');
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
  initializeMsDatabase();
}

module.exports = { initializeMsDatabase };
