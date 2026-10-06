// Copy of db-api.js for MSSQL integration
// Next: Replace SQLite with MSSQL connection

const express = require('express');
const bcrypt = require('bcrypt');
const path = require('path');
const cors = require('cors');
const dotenv = require('dotenv');

// Load environment variables from .env file
dotenv.config();

console.log('✅ All modules loaded successfully (MSSQL version)');

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(express.json());
app.use(cors());

// POST /api/users/:id/auth - Authenticate user password (MSSQL)
app.post('/api/users/:id/auth', async (req, res) => {
  const id = req.params.id;
  const { password } = req.body;
  const ipAddress = req.ip || req.connection?.remoteAddress || 'unknown';
  const userAgent = req.get('user-agent') || 'unknown';

  if (!password) {
    return res.status(400).json({
      success: false,
      error: 'Password is required'
    });
  }
  try {
    const pool = await poolPromise;
    const result = await pool.request()
      .input('id', sql.NVarChar(50), id)
      .query('SELECT password FROM apm.users WHERE id = @id');
    if (!result.recordset[0]) {
      // Log failed login - user not found
      const auditId = generateAuditId();
      const timestamp = new Date().toISOString();
      await pool.request()
        .input('id', sql.NVarChar(50), auditId)
        .input('userId', sql.NVarChar(50), id)
        .input('action', sql.NVarChar(50), 'LOGIN_FAILED')
        .input('recordId', sql.NVarChar(50), id)
        .input('eventDetails', sql.NVarChar(sql.MAX), JSON.stringify({ reason: 'User not found', timestamp }))
        .input('ipAddress', sql.NVarChar(50), ipAddress)
        .input('userAgent', sql.NVarChar(255), userAgent)
        .input('timestamp', sql.DateTime, timestamp)
        .query(`
          INSERT INTO apm.audit_logs (id, userId, action, tableName, recordId, changedFields, ipAddress, userAgent, timestamp)
          VALUES (@id, @userId, @action, 'auth_events', @recordId, @eventDetails, @ipAddress, @userAgent, @timestamp)
        `)
        .catch(err => console.error('⚠️  Failed to log audit:', err.message));

      return res.status(401).json({
        success: false,
        error: 'Incorrect Username/Password'
      });
    }
    const hash = result.recordset[0].password;
    const isValid = await bcrypt.compare(password, hash);
    
    // Log failed login if password is invalid
    if (!isValid) {
      const auditId = generateAuditId();
      const timestamp = new Date().toISOString();
      await pool.request()
        .input('id', sql.NVarChar(50), auditId)
        .input('userId', sql.NVarChar(50), id)
        .input('action', sql.NVarChar(50), 'LOGIN_FAILED')
        .input('recordId', sql.NVarChar(50), id)
        .input('eventDetails', sql.NVarChar(sql.MAX), JSON.stringify({ reason: 'Invalid password', timestamp }))
        .input('ipAddress', sql.NVarChar(50), ipAddress)
        .input('userAgent', sql.NVarChar(255), userAgent)
        .input('timestamp', sql.DateTime, timestamp)
        .query(`
          INSERT INTO apm.audit_logs (id, userId, action, tableName, recordId, changedFields, ipAddress, userAgent, timestamp)
          VALUES (@id, @userId, @action, 'auth_events', @recordId, @eventDetails, @ipAddress, @userAgent, @timestamp)
        `)
        .catch(err => console.error('⚠️  Failed to log audit:', err.message));
      return res.status(401).json({
        success: false,
        error: 'Incorrect Username/Password'
      });
    }

    // Successful authentication
    res.json({
      success: true,
      message: 'Authentication successful'
    });
  } catch (err) {
    console.error('Error authenticating user:', err);
    res.status(500).json({
      success: false,
      error: 'Authentication error',
      details: err.message
    });
  }
});

// MSSQL connection setup
const sql = require('mssql');

const dbConfig = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER,
  database: process.env.DB_DATABASE,
  port: parseInt(process.env.DB_PORT || '1433'),
  options: {
    encrypt: process.env.DB_ENCRYPT === 'true',
    trustServerCertificate: process.env.DB_TRUST_CERTIFICATE === 'true'
  }
};

// Add named instance if configured (e.g., 'YAKUT' for SRDEV\\YAKUT)
if (process.env.DB_INSTANCE) {
  dbConfig.options.instanceName = process.env.DB_INSTANCE;
}

// Add NTLM authentication if configured
if (process.env.DB_AUTH_TYPE === 'ntlm' && process.env.DB_AUTH_DOMAIN && process.env.DB_AUTH_USERNAME) {
  dbConfig.authentication = {
    // type: 'ntlm',
    options: {
      domain: process.env.DB_AUTH_DOMAIN,
      userName: process.env.DB_AUTH_USERNAME,
      password: process.env.DB_AUTH_PASSWORD
    }
  };
}

console.log(`📡 Database Config: ${dbConfig.server}:${dbConfig.port}/${dbConfig.database}`);

// Create a connection pool
const poolPromise = new sql.ConnectionPool(dbConfig)
  .connect()
  .then(async pool => {
    console.log('✅ Connected to MSSQL');
    // Non-destructive migration: make sure the forced-password-change flag exists.
    // Safe/idempotent — adds the column in place, existing users default to 0.
    try {
      await pool.request().query(`IF COL_LENGTH('apm.users','mustChangePassword') IS NULL ALTER TABLE apm.users ADD mustChangePassword BIT NOT NULL DEFAULT 0`);
      console.log('  ✓ users.mustChangePassword column ensured');
    } catch (e) {
      console.error('⚠️  Failed to ensure users.mustChangePassword column:', e.message);
    }
    return pool;
  })
  .catch(err => {
    console.error('❌ Database Connection Failed! Bad Config: ', err);
    throw err;
  });

// ============================================================================
// AUDIT LOGGING UTILITIES
// ============================================================================

/**
 * Generate unique ID for audit log entries
 */
function generateAuditId() {
  return 'audit_' + Date.now().toString(36) + Math.random().toString(36).substr(2, 8);
}

/**
 * Timestamps persisted in the users/sessions tables are written in Kuwait local
 * time (UTC+03:00) instead of UTC, so the stored values read as T+3. The value
 * is emitted without a zone suffix so SQL Server stores the clock time as-is
 * (a suffixed offset would be converted back to UTC on insert into DATETIME).
 */
const LOCAL_UTC_OFFSET_HOURS = 3;
function localIso(date = new Date()) {
  const shifted = new Date(date.getTime() + LOCAL_UTC_OFFSET_HOURS * 60 * 60 * 1000);
  return shifted.toISOString().slice(0, 23);
}

/**
 * Log user changes to audit_logs table
 * @param {string} userId - ID of user making the change
 * @param {string} action - Action type: 'CREATE', 'UPDATE', 'DELETE'
 * @param {string} recordId - ID of the record being modified
 * @param {object} changedFields - Object with field changes: {fieldName: {old: value, new: value}}
 * @param {object} req - Express request object (for IP address)
 */
async function logAudit(userId, action, recordId, changedFields, req) {
  try {
    const pool = await poolPromise;
    const auditId = generateAuditId();
    const timestamp = new Date().toISOString();
    const ipAddress = req?.ip || req?.connection?.remoteAddress || 'unknown';
    const userAgent = req?.get('user-agent') || 'unknown';
    const changedFieldsJson = JSON.stringify(changedFields);

    console.log(`🔍 Attempting audit log: userId=${userId}, action=${action}, recordId=${recordId}`);

    const result = await pool.request()
      .input('id', sql.NVarChar(50), auditId)
      .input('userId', sql.NVarChar(50), userId)
      .input('action', sql.NVarChar(50), action)
      .input('tableName', sql.NVarChar(50), 'users')
      .input('recordId', sql.NVarChar(50), recordId)
      .input('changedFields', sql.NVarChar(sql.MAX), changedFieldsJson)
      .input('timestamp', sql.DateTime, timestamp)
      .input('ipAddress', sql.NVarChar(50), ipAddress)
      .input('userAgent', sql.NVarChar(255), userAgent)
      .query(`
        INSERT INTO apm.audit_logs (id, userId, action, tableName, recordId, changedFields, timestamp, ipAddress, userAgent)
        VALUES (@id, @userId, @action, @tableName, @recordId, @changedFields, @timestamp, @ipAddress, @userAgent)
      `);

    console.log(`✅ Audit logged: ${action} on user ${recordId} by ${userId}`);
  } catch (err) {
    console.error('⚠️  Failed to log audit:', err);
    console.error('Error details:', err.message);
    // Don't throw - audit logging failure shouldn't break the operation
  }
}

// ============================================================================

// GET /api/users - Get all users (MSSQL)
app.get('/api/users', async (req, res) => {
  try {
    const pool = await poolPromise;
    const result = await pool.request().query('SELECT * FROM apm.users');
    // Remove password and parse permissions for each user
    const users = result.recordset.map(user => {
      const { password, permissions, ...userWithoutPassword } = user;
      return {
        ...userWithoutPassword,
        permissions: permissions ? JSON.parse(permissions) : []
      };
    });
    res.json({
      success: true,
      data: users,
      count: users.length
    });
  } catch (err) {
    console.error('Error fetching users:', err);
    res.status(500).json({
      success: false,
      error: 'Failed to read users',
      details: err.message
    });
  }
});
// GET /db-sales-pnl - Execute stored procedure to return Sales PnL (WTD)
app.get('/db-sales-pnl', async (req, res) => {
  const profile = String(req.query.profile || 'KFH').toUpperCase();
  try {
    const pool = await poolPromise;
    const request = pool.request();
    // Stored procedure expects @Period parameter; using 'WTD' per request
    request.input('Period', sql.NVarChar(10), 'WTD');

    // Choose stored procedure suffix based on requested profile (KT/AUB/KFH)
    let suffix;
    if (profile === 'KT') suffix = 'KT';
    else if (profile === 'AUB') suffix = 'AUB';
    else suffix = 'KFH';
    const procName = `apm.sel_ClientFlowSummaryReport_${suffix}`;
    const result = await request.execute(procName);

    // Debug option: return the raw result object to help inspect what's coming back from the proc
    const debug = req.query.debug === '1' || req.query.debug === 'true';
    if (debug) {
      return res.json({ success: true, profile, result });
    }

    let salesPnl = null;

    // Prefer aggregating SalesPnL from recordset when present (matches stored-proc behavior)
    if (result.recordset && result.recordset.length > 0) {
      const firstRow = result.recordset[0];
      const keys = Object.keys(firstRow || {});
      const preferredSalesKeys = ['SalesPnL', 'Sales PnL', 'Sales_PnL', 'Sales PNL', 'SalesPNL', 'Sales'];
      const summaryKeys = ['Symbol', 'symbol', 'CurrencyPair', 'currencyPair', 'Pair', 'pair', 'Currency', 'currency'];
      const isTotalSummaryRow = (row) => summaryKeys.some((key) => {
        const value = row?.[key];
        return typeof value === 'string' && /^total\b/i.test(value.trim());
      });

      // find a key that matches preferred names or a normalized 'salespnl'
      let salesKey = keys.find(k => preferredSalesKeys.includes(k));
      if (!salesKey) {
        salesKey = keys.find(k => k.toLowerCase().replace(/[^a-z0-9]/g, '') === 'salespnl');
      }

      if (salesKey) {
        const detailRows = result.recordset.filter((row) => !isTotalSummaryRow(row));
        const salesPnlRows = detailRows.length > 0 ? detailRows : result.recordset;
        salesPnl = salesPnlRows.reduce((acc, row) => {
          const v = row[salesKey];
          const n = (typeof v === 'number') ? v : parseFloat(String(v).replace(/[^0-9eE+.-]+/g, ''));
          return acc + (Number.isNaN(n) ? 0 : n);
        }, 0);
      }
    }

    // If we couldn't find a SalesPnL column, fall back to other sources
    if (salesPnl === null) {
      // 1) Prefer explicit returnValue when present and non-zero
      if (typeof result.returnValue === 'number' && result.returnValue !== 0) {
        salesPnl = result.returnValue;
      }

      // 2) Check output parameters (if stored proc uses OUTPUT params)
      if (salesPnl === null && result.output) {
        for (const k of Object.keys(result.output)) {
          const v = result.output[k];
          if (typeof v === 'number') { salesPnl = v; break; }
          if (typeof v === 'string') {
            const n = parseFloat(String(v).replace(/[^0-9eE+.-]+/g, ''));
            if (!Number.isNaN(n)) { salesPnl = n; break; }
          }
        }
      }

      // 3) Look for a column specifically named like 'Return Value'
      if (salesPnl === null && result.recordset && result.recordset.length > 0) {
        const row = result.recordset[0];
        const preferredKeys = ['Return Value', 'ReturnValue', 'returnValue', 'Return_Value', 'return_value', 'Return'];
        for (const pk of preferredKeys) {
          if (Object.prototype.hasOwnProperty.call(row, pk)) {
            const v = row[pk];
            if (typeof v === 'number') { salesPnl = v; break; }
            if (typeof v === 'string') {
              const n = parseFloat(String(v).replace(/[^0-9eE+.-]+/g, ''));
              if (!Number.isNaN(n)) { salesPnl = n; break; }
            }
          }
        }
        // 4) Fallback: pick first numeric-looking column
        if (salesPnl === null) {
          for (const k of Object.keys(row)) {
            const v = row[k];
            if (typeof v === 'number') { salesPnl = v; break; }
            if (typeof v === 'string') {
              const n = parseFloat(String(v).replace(/[^0-9eE+.-]+/g, ''));
              if (!Number.isNaN(n)) { salesPnl = n; break; }
            }
          }
        }
      }
    }

    res.json({ entity: profile, salesPnl: salesPnl ?? 0, source: 'db', lastUpdated: new Date().toISOString() });
  } catch (err) {
    console.error('[msdb-api] /db-sales-pnl error:', err);
    res.status(500).json({ success: false, error: 'db_error', details: String(err) });
  }
});
// GET /api/users/:id - Get specific user (MSSQL)
app.get('/api/users/:id', async (req, res) => {
  const id = req.params.id;
  try {
    const pool = await poolPromise;
    const result = await pool.request()
      .input('id', sql.NVarChar(50), id)
      .query('SELECT * FROM apm.users WHERE id = @id');
    if (!result.recordset[0]) {
      return res.status(404).json({
        success: false,
        error: 'User not found'
      });
    }
    const { password, permissions, ...userWithoutPassword } = result.recordset[0];
    const user = {
      ...userWithoutPassword,
      permissions: permissions ? JSON.parse(permissions) : []
    };
    res.json({
      success: true,
      data: user
    });
  } catch (err) {
    console.error('Error fetching user:', err);
    res.status(500).json({
      success: false,
      error: 'Failed to read user',
      details: err.message
    });
  }
});

// Proxy POST /api/users to POST /api/users/:id if id is in body (for frontend compatibility)
app.post('/api/users', (req, res, next) => {
  const { id } = req.body;
  console.log('Proxy POST /api/users: received id =', id);
  if (!id) {
    return next();
  }
  req.url = `/api/users/${id}`;
  next();
});

// POST /api/users - Create new user (MSSQL)
app.post('/api/users', async (req, res) => {
  const { username, email, password, role, status, entity, gsm, name, surname, permissions } = req.body;
  if (!username || !email || !password) {
    return res.status(400).json({
      success: false,
      error: 'Username, email, and password are required'
    });
  }
  try {
    const pool = await poolPromise;
    // Check if username or email already exists
    const exists = await pool.request()
      .input('username', sql.NVarChar(50), username)
      .input('email', sql.NVarChar(100), email)
      .query('SELECT id FROM apm.users WHERE username = @username OR email = @email');
    if (exists.recordset.length > 0) {
      return res.status(400).json({
        success: false,
        error: 'Username or email already exists'
      });
    }
    const newId = 'cm4v1w2x1000' + Date.now().toString(36) + Math.random().toString(36).substr(2, 8);
    const now = localIso();
    const permissionsJson = JSON.stringify(permissions || []);
    const hashedPassword = bcrypt.hashSync(password, 10);
    const roleToInsert = (role !== undefined ? role : 'user');
    await pool.request()
      .input('id', sql.NVarChar(50), newId)
      .input('username', sql.NVarChar(50), username)
      .input('email', sql.NVarChar(100), email)
      .input('password', sql.NVarChar(255), hashedPassword)
      .input('role', sql.NVarChar(50), roleToInsert)
      .input('status', sql.NVarChar(50), status || 'Active')
      .input('entity', sql.NVarChar(100), entity || null)
      .input('gsm', sql.NVarChar(50), gsm || null)
      .input('name', sql.NVarChar(100), name || null)
      .input('surname', sql.NVarChar(100), surname || null)
      .input('permissions', sql.NVarChar, permissionsJson)
      .input('createdAt', sql.NVarChar(50), now)
      .input('updatedAt', sql.NVarChar(50), now)
      .input('mustChangePassword', sql.Bit, 1)
      .query(`INSERT INTO apm.users (id, username, email, password, role, status, entity, gsm, name, surname, permissions, createdAt, updatedAt, mustChangePassword)
        VALUES (@id, @username, @email, @password, @role, @status, @entity, @gsm, @name, @surname, @permissions, @createdAt, @updatedAt, @mustChangePassword)`);
    const newUser = {
      id: newId,
      username,
      email,
      role: roleToInsert,
      status: status || 'Active',
      entity: entity || null,
      gsm: gsm || null,
      name: name || null,
      surname: surname || null,
      permissions: permissions || [],
      createdAt: now,
      updatedAt: now,
      mustChangePassword: 1
    };

    // Log audit trail for user creation
    const changedFields = {
      username: { old: null, new: username },
      email: { old: null, new: email },
      role: { old: null, new: roleToInsert },
      status: { old: null, new: status || 'Active' },
      entity: { old: null, new: entity || null },
      gsm: { old: null, new: gsm || null },
      name: { old: null, new: name || null },
      surname: { old: null, new: surname || null },
      permissions: { old: null, new: permissions || [] }
    };
    
    // Get authenticated user ID from request body, headers, or session
    const authenticatedUserId = req.body.authenticatedUserId || req.headers['x-user-id'] || req.user?.id || req.session?.userId || 'system';
    await logAudit(authenticatedUserId, 'CREATE', newId, changedFields, req);

    res.status(201).json({
      success: true,
      data: newUser,
      message: 'User created successfully'
    });
  } catch (err) {
    console.error('Error creating user:', err);
    res.status(500).json({
      success: false,
      error: 'Failed to save user',
      details: err.message
    });
  }
});

// POST /api/users/:id/change-password - Verify current password, set a new one, clear the forced-change flag
app.post('/api/users/:id/change-password', async (req, res) => {
  const id = req.params.id;
  const { currentPassword, newPassword } = req.body;
  if (!currentPassword || !newPassword) {
    return res.status(400).json({ success: false, error: 'Current and new password are required' });
  }
  try {
    const pool = await poolPromise;
    const result = await pool.request()
      .input('id', sql.NVarChar(50), id)
      .query('SELECT password FROM apm.users WHERE id = @id');
    if (!result.recordset[0]) {
      return res.status(404).json({ success: false, error: 'User not found' });
    }
    const valid = await bcrypt.compare(currentPassword, result.recordset[0].password);
    if (!valid) {
      return res.status(401).json({ success: false, error: 'Current password is incorrect' });
    }
    const hashed = bcrypt.hashSync(newPassword, 10);
    const now = localIso();
    await pool.request()
      .input('id', sql.NVarChar(50), id)
      .input('password', sql.NVarChar(255), hashed)
      .input('updatedAt', sql.NVarChar(50), now)
      .query('UPDATE apm.users SET password = @password, mustChangePassword = 0, updatedAt = @updatedAt WHERE id = @id');
    res.json({ success: true, message: 'Password changed successfully' });
  } catch (err) {
    console.error('Error changing password:', err);
    res.status(500).json({ success: false, error: 'Failed to change password', details: err.message });
  }
});

// POST /api/users/:id - Update user (MSSQL)
app.post('/api/users/:id', async (req, res) => {
  const id = req.params.id;
  const { username, email, password, role, status, entity, gsm, name, surname, permissions, mustChangePassword } = req.body;
  // Compatibility: accept POST with action='delete' (some proxies block DELETE)
  if (req.body && (req.body.action === 'delete' || req.body._method === 'DELETE' || req.body.delete === true)) {
    try {
      // Reuse delete logic from DELETE /api/users/:id
      const pool = await poolPromise;
      // First get the user to return in response
      const result = await pool.request()
        .input('id', sql.NVarChar(50), id)
        .query('SELECT * FROM apm.users WHERE id = @id');
      if (!result.recordset[0]) {
        return res.status(404).json({ success: false, error: 'User not found' });
      }
      const userToDelete = result.recordset[0];
      await pool.request().input('id', sql.NVarChar(50), id).query('DELETE FROM apm.users WHERE id = @id');

      // Log the deletion
      const auditId = generateAuditId();
      const timestamp = new Date().toISOString();
      const changedFields = {
        username: { old: userToDelete.username, new: null },
        email: { old: userToDelete.email, new: null },
        role: { old: userToDelete.role, new: null },
        status: { old: userToDelete.status, new: null }
      };
      const authenticatedUserId = req.user?.id || req.session?.userId || 'system';
      try {
        await pool.request()
          .input('id', sql.NVarChar(50), auditId)
          .input('userId', sql.NVarChar(50), authenticatedUserId)
          .input('action', sql.NVarChar(50), 'DELETE')
          .input('tableName', sql.NVarChar(50), 'users')
          .input('recordId', sql.NVarChar(50), id)
          .input('changedFields', sql.NVarChar(sql.MAX), JSON.stringify(changedFields))
          .input('timestamp', sql.DateTime, timestamp)
          .input('ipAddress', sql.NVarChar(50), req.ip || req.connection?.remoteAddress || 'unknown')
          .input('userAgent', sql.NVarChar(255), req.get('user-agent') || 'unknown')
          .query(`
            INSERT INTO apm.audit_logs (id, userId, action, tableName, recordId, changedFields, timestamp, ipAddress, userAgent)
            VALUES (@id, @userId, @action, @tableName, @recordId, @changedFields, @timestamp, @ipAddress, @userAgent)
          `);
        console.log(`✅ Audit logged: DELETE on user ${id} by ${authenticatedUserId}`);
      } catch (auditErr) {
        console.error('⚠️  Failed to log audit:', auditErr.message);
      }

      const { password: pw, permissions: perms, ...userWithoutPassword } = userToDelete;
      const user = { ...userWithoutPassword, permissions: perms ? JSON.parse(perms) : [] };
      return res.json({ success: true, data: user, message: 'User deleted successfully' });
    } catch (err) {
      console.error('Error deleting user via POST-compat:', err);
      return res.status(500).json({ success: false, error: 'Failed to delete user', details: err.message });
    }
  }
  try {
    console.log('POST /api/users/:id handler: req.params.id =', id);
    const pool = await poolPromise;
    
    // Fetch original user data for audit comparison
    const originalUserResult = await pool.request()
      .input('id', sql.NVarChar(50), id)
      .query('SELECT * FROM apm.users WHERE id = @id');
    
    if (!originalUserResult.recordset[0]) {
      return res.status(404).json({
        success: false,
        error: 'User not found'
      });
    }
    
    const originalUser = originalUserResult.recordset[0];
    
    // Build dynamic update query
    const updates = [];
    const values = {};
    if (username !== undefined) { updates.push('username = @username'); values.username = username; }
    if (email !== undefined) { updates.push('email = @email'); values.email = email; }
    if (password !== undefined) {
      const hashedPassword = bcrypt.hashSync(password, 10);
      updates.push('password = @password'); values.password = hashedPassword;
    }
    if (role !== undefined) { updates.push('role = @role'); values.role = role; }
    if (status !== undefined) { updates.push('status = @status'); values.status = status; }
    if (entity !== undefined) { updates.push('entity = @entity'); values.entity = entity; }
    if (gsm !== undefined) { updates.push('gsm = @gsm'); values.gsm = gsm; }
    if (name !== undefined) { updates.push('name = @name'); values.name = name; }
    if (surname !== undefined) { updates.push('surname = @surname'); values.surname = surname; }
    if (permissions !== undefined) { updates.push('permissions = @permissions'); values.permissions = JSON.stringify(permissions); }
    if (mustChangePassword !== undefined) { updates.push('mustChangePassword = @mustChangePassword'); values.mustChangePassword = mustChangePassword ? 1 : 0; }
    updates.push('updatedAt = @updatedAt'); values.updatedAt = localIso();
    if (updates.length === 1) {
      return res.status(400).json({
        success: false,
        error: 'No fields to update'
      });
    }
    let setClause = updates.join(', ');
    let request = pool.request().input('id', sql.NVarChar(50), id);
    // Use correct types for each field
    if (values.username !== undefined) request = request.input('username', sql.NVarChar(50), values.username);
    if (values.email !== undefined) request = request.input('email', sql.NVarChar(100), values.email);
    if (values.password !== undefined) request = request.input('password', sql.NVarChar(255), values.password);
    if (values.role !== undefined) request = request.input('role', sql.NVarChar(50), values.role);
    if (values.status !== undefined) request = request.input('status', sql.NVarChar(50), values.status);
    if (values.entity !== undefined) request = request.input('entity', sql.NVarChar(100), values.entity);
    if (values.gsm !== undefined) request = request.input('gsm', sql.NVarChar(50), values.gsm);
    if (values.name !== undefined) request = request.input('name', sql.NVarChar(100), values.name);
    if (values.surname !== undefined) request = request.input('surname', sql.NVarChar(100), values.surname);
    if (values.permissions !== undefined) request = request.input('permissions', sql.NVarChar, values.permissions);
    if (values.mustChangePassword !== undefined) request = request.input('mustChangePassword', sql.Bit, values.mustChangePassword);
    if (values.updatedAt !== undefined) request = request.input('updatedAt', sql.NVarChar(50), values.updatedAt);
    const result = await request.query(`UPDATE apm.users SET ${setClause} WHERE id = @id; SELECT * FROM apm.users WHERE id = @id`);
    // Try to get the updated user from the second recordset, fallback to first if needed
    let updatedUserRow = (result.recordsets && result.recordsets[1] && result.recordsets[1][0])
      || (result.recordset && result.recordset[0]);
    if (!updatedUserRow) {
      return res.status(404).json({
        success: false,
        error: 'User not found'
      });
    }
    const { password: pw, permissions: dbPermissions, ...userWithoutPassword } = updatedUserRow;
    const user = {
      ...userWithoutPassword,
      permissions: dbPermissions ? JSON.parse(dbPermissions) : []
    };

    // Build changedFields object comparing original with new values
    const changedFields = {};
    if (username !== undefined && username !== originalUser.username) {
      changedFields.username = { old: originalUser.username, new: username };
    }
    if (email !== undefined && email !== originalUser.email) {
      changedFields.email = { old: originalUser.email, new: email };
    }
    if (password !== undefined) {
      changedFields.password = { old: '[hashed]', new: '[hashed - updated]' };
    }
    if (role !== undefined && role !== originalUser.role) {
      changedFields.role = { old: originalUser.role, new: role };
    }
    if (status !== undefined && status !== originalUser.status) {
      changedFields.status = { old: originalUser.status, new: status };
    }
    if (entity !== undefined && entity !== originalUser.entity) {
      changedFields.entity = { old: originalUser.entity, new: entity };
    }
    if (gsm !== undefined && gsm !== originalUser.gsm) {
      changedFields.gsm = { old: originalUser.gsm, new: gsm };
    }
    if (name !== undefined && name !== originalUser.name) {
      changedFields.name = { old: originalUser.name, new: name };
    }
    if (surname !== undefined && surname !== originalUser.surname) {
      changedFields.surname = { old: originalUser.surname, new: surname };
    }
    if (permissions !== undefined) {
      const originalPermissions = originalUser.permissions ? JSON.parse(originalUser.permissions) : [];
      if (JSON.stringify(permissions) !== JSON.stringify(originalPermissions)) {
        changedFields.permissions = { old: originalPermissions, new: permissions };
      }
    }

    // Only log audit if there were actual changes
    if (Object.keys(changedFields).length > 0) {
      const authenticatedUserId = req.user?.id || req.session?.userId || 'system';
      await logAudit(authenticatedUserId, 'UPDATE', id, changedFields, req);
    }

    res.json({
      success: true,
      data: user,
      message: 'User updated successfully'
    });
  } catch (err) {
    console.error('Error updating user:', err);
    res.status(500).json({
      success: false,
      error: 'Failed to update user',
      details: err.message
    });
  }
});

// DELETE /api/users/:id - Delete user (MSSQL)
app.delete('/api/users/:id', async (req, res) => {
  const id = req.params.id;
  try {
    const pool = await poolPromise;
    // First get the user to return in response
    const result = await pool.request()
      .input('id', sql.NVarChar(50), id)
      .query('SELECT * FROM apm.users WHERE id = @id');
    if (!result.recordset[0]) {
      return res.status(404).json({
        success: false,
        error: 'User not found'
      });
    }
    
    const userToDelete = result.recordset[0];
    
    await pool.request().input('id', sql.NVarChar(50), id).query('DELETE FROM apm.users WHERE id = @id');
    
    // Log the deletion
    const auditId = generateAuditId();
    const timestamp = new Date().toISOString();
    const changedFields = {
      username: { old: userToDelete.username, new: null },
      email: { old: userToDelete.email, new: null },
      role: { old: userToDelete.role, new: null },
      status: { old: userToDelete.status, new: null }
    };
    const authenticatedUserId = req.user?.id || req.session?.userId || 'system';
    
    try {
      await pool.request()
        .input('id', sql.NVarChar(50), auditId)
        .input('userId', sql.NVarChar(50), authenticatedUserId)
        .input('action', sql.NVarChar(50), 'DELETE')
        .input('tableName', sql.NVarChar(50), 'users')
        .input('recordId', sql.NVarChar(50), id)
        .input('changedFields', sql.NVarChar(sql.MAX), JSON.stringify(changedFields))
        .input('timestamp', sql.DateTime, timestamp)
        .input('ipAddress', sql.NVarChar(50), req.ip || req.connection?.remoteAddress || 'unknown')
        .input('userAgent', sql.NVarChar(255), req.get('user-agent') || 'unknown')
        .query(`
          INSERT INTO apm.audit_logs (id, userId, action, tableName, recordId, changedFields, timestamp, ipAddress, userAgent)
          VALUES (@id, @userId, @action, @tableName, @recordId, @changedFields, @timestamp, @ipAddress, @userAgent)
        `);
      console.log(`✅ Audit logged: DELETE on user ${id} by ${authenticatedUserId}`);
    } catch (auditErr) {
      console.error('⚠️  Failed to log audit:', auditErr.message);
    }
    
    const { password, permissions, ...userWithoutPassword } = userToDelete;
    const user = {
      ...userWithoutPassword,
      permissions: permissions ? JSON.parse(permissions) : []
    };
    res.json({
      success: true,
      data: user,
      message: 'User deleted successfully'
    });
  } catch (err) {
    console.error('Error deleting user:', err);
    res.status(500).json({
      success: false,
      error: 'Failed to delete user',
      details: err.message
    });
  }
});

// GET /api/sessions - Get all sessions (MSSQL)
app.get('/api/sessions', async (req, res) => {
  try {
    const pool = await poolPromise;
    const result = await pool.request().query('SELECT * FROM apm.sessions');
    res.json({
      success: true,
      data: result.recordset,
      count: result.recordset.length
    });
  } catch (err) {
    console.error('Error fetching sessions:', err);
    res.status(500).json({
      success: false,
      error: 'Failed to read sessions',
      details: err.message
    });
  }
});

// POST /api/sessions - Create new session (MSSQL)
app.post('/api/sessions', async (req, res) => {
  const { token, userId, expiresAt } = req.body;
  if (!token || !userId) {
    return res.status(400).json({
      success: false,
      error: 'Token and userId are required'
    });
  }
  try {
    const pool = await poolPromise;
    // Check if user exists
    const userResult = await pool.request().input('userId', sql.NVarChar(50), userId).query('SELECT id FROM apm.users WHERE id = @userId');
    if (!userResult.recordset[0]) {
      return res.status(400).json({
        success: false,
        error: 'User not found'
      });
    }
    const newId = 'cm4v1w2x1000' + Date.now().toString(36) + Math.random().toString(36).substr(2, 8);
    const defaultExpiry = localIso(new Date(Date.now() + 7 * 24 * 60 * 60 * 1000));
    const now = localIso();
    await pool.request()
      .input('id', sql.NVarChar(50), newId)
      .input('token', sql.NVarChar, token)
      .input('userId', sql.NVarChar(50), userId)
      .input('expiresAt', sql.NVarChar(50), expiresAt || defaultExpiry)
      .input('createdAt', sql.NVarChar(50), now)
      .query('INSERT INTO apm.sessions (id, token, userId, expiresAt, createdAt) VALUES (@id, @token, @userId, @expiresAt, @createdAt)');
    const newSession = {
      id: newId,
      token,
      userId,
      expiresAt: expiresAt || defaultExpiry,
      createdAt: now
    };
    res.status(201).json({
      success: true,
      data: newSession,
      message: 'Session created successfully'
    });
  } catch (err) {
    console.error('Error creating session:', err);
    res.status(500).json({
      success: false,
      error: 'Failed to save session',
      details: err.message
    });
  }
});

// DELETE /api/sessions/:id - Delete session (MSSQL)
app.delete('/api/sessions/:id', async (req, res) => {
  const id = req.params.id;
  try {
    const pool = await poolPromise;
    // First get the session to return in response
    const result = await pool.request().input('id', sql.NVarChar(50), id).query('SELECT * FROM apm.sessions WHERE id = @id');
    if (!result.recordset[0]) {
      return res.status(404).json({
        success: false,
        error: 'Session not found'
      });
    }
    await pool.request().input('id', sql.NVarChar(50), id).query('DELETE FROM apm.sessions WHERE id = @id');
    res.json({
      success: true,
      data: result.recordset[0],
      message: 'Session deleted successfully'
    });
  } catch (err) {
    console.error('Error deleting session:', err);
    res.status(500).json({
      success: false,
      error: 'Failed to delete session',
      details: err.message
    });
  }
});

// GET /api/overview/client-flow-pair - Client Flow Summary (By Pair)
app.get('/api/overview/client-flow-pair', async (req, res) => {
  try {
    const pool = await poolPromise;

    const profileRaw = req.query.profile || 'KFH';
    const periodRaw = req.query.period || 'TODAY';
    const startDateRaw = req.query.startDate;
    const endDateRaw = req.query.endDate;

    const profile = String(profileRaw).trim().toUpperCase();
    const period = String(periodRaw).trim().toUpperCase();

    let suffix;
    if (profile === 'KT') suffix = 'KT';
    else if (profile === 'AUB') suffix = 'AUB';
    else suffix = 'KFH';

    const procName = `apm.sel_ClientFlowSummaryReport_${suffix}`;

    const request = pool
      .request()
      .input('Period', sql.NVarChar(10), period)
      .input('StartDate', sql.Date, startDateRaw ? new Date(String(startDateRaw)) : null)
      .input('EndDate', sql.Date, endDateRaw ? new Date(String(endDateRaw)) : null);

    const result = await request.execute(procName);
    const rows = result.recordset || [];

    const data = rows.map(row => ({
      Symbol: row.CurrencyPair,
      'Total Amount': row.TotalAmount,
      'Client Buy Amount': row.ClientBuyAmount,
      'Client Sell Amount': row.ClientSellAmount,
      'Net Amount': row.NetAmount,
      'Sales PnL': row.SalesPnL,
      'Total Amount USD': row.TotalAmountUSD,
    }));

    res.json({
      success: true,
      data,
      count: data.length,
      profile,
      period,
    });
  } catch (err) {
    console.error('Error fetching client flow summary (by pair):', err);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch client flow summary (by pair)',
      details: err.message,
    });
  }
});

// GET /api/overview/client-flow-currency - Client Flow Summary (By Currency)
app.get('/api/overview/client-flow-currency', async (req, res) => {
  try {
    const pool = await poolPromise;

    const profileRaw = req.query.profile || 'KFH';
    const periodRaw = req.query.period || 'TODAY';
    const startDateRaw = req.query.startDate;
    const endDateRaw = req.query.endDate;

    const profile = String(profileRaw).trim().toUpperCase();
    const period = String(periodRaw).trim().toUpperCase();

    let suffix;
    if (profile === 'KT') suffix = 'KT';
    else if (profile === 'AUB') suffix = 'AUB';
    else suffix = 'KFH';

    // Call dedicated Currency Summary stored procedure
    const procName = `apm.sel_CurrencySummaryReport_${suffix}`;

    const request = pool
      .request()
      .input('Period', sql.NVarChar(10), period)
      .input('StartDate', sql.Date, startDateRaw ? new Date(String(startDateRaw)) : null)
      .input('EndDate', sql.Date, endDateRaw ? new Date(String(endDateRaw)) : null);

    const result = await request.execute(procName);
    const rows = result.recordset || [];

    const data = rows.map(r => ({
      Symbol: (r.Currency ?? r.currency ?? r.Symbol ?? r.symbol) || (r.CurrencyPair ? String(r.CurrencyPair).split(/[\/\-]/)[0] : ''),
      'Total Buy Amount': r.TotalBuyAmount,
      'Total Sell Amount': r.TotalSellAmount,
      'Net Amount': r.NetAmount
    })).sort((a, b) => String(a.Symbol || '').localeCompare(String(b.Symbol || '')));

    res.json({ success: true, data, count: data.length, profile, period });
  } catch (err) {
    console.error('Error fetching client flow summary (by currency):', err);
    res.status(500).json({ success: false, error: 'Failed to fetch client flow summary (by currency)', details: err.message });
  }
});

// GET /api/overview/interbank-execution - Interbank Execution Summary
app.get('/api/overview/interbank-execution', async (req, res) => {
  try {
    const pool = await poolPromise;

    const profileRaw = req.query.profile || 'KFH';
    const periodRaw = req.query.period || 'TODAY';
    const startDateRaw = req.query.startDate;
    const endDateRaw = req.query.endDate;

    const profile = String(profileRaw).trim().toUpperCase();
    const period = String(periodRaw).trim().toUpperCase();

    let suffix;
    if (profile === 'KT') suffix = 'KT';
    else if (profile === 'AUB') suffix = 'AUB';
    else suffix = 'KFH';

    const procName = `apm.sel_InterbankExecutionSummaryReport_${suffix}`;

    const request = pool
      .request()
      .input('Period', sql.NVarChar(10), period)
      .input('StartDate', sql.Date, startDateRaw ? new Date(String(startDateRaw)) : null)
      .input('EndDate', sql.Date, endDateRaw ? new Date(String(endDateRaw)) : null);

    const result = await request.execute(procName);
    const rows = result.recordset || [];

    const data = rows.map(row => ({
      Symbol: row.Symbol,
      'Total Amount': row.TotalAmount,
      'Total Buy Amount': row.TotalBuy,
      'Total Sell Amount': row.TotalSell,
      'Net Amount': row.NetAmount,
      'Total Amount USD': row.TotalAmountUSD,
    }));

    res.json({
      success: true,
      data,
      count: data.length,
      profile,
      period,
    });
  } catch (err) {
    console.error('Error fetching interbank execution summary:', err);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch interbank execution summary',
      details: err.message,
    });
  }
});

// GET /api/overview/position-pnl - PnL Summary Report (By Pair)
app.get('/api/overview/position-pnl', async (req, res) => {
  try {
    const pool = await poolPromise;

    const profileRaw = req.query.profile || 'KFH';
    const periodRaw = req.query.period || 'TODAY';
    const startDateRaw = req.query.startDate;
    const endDateRaw = req.query.endDate;

    const profile = String(profileRaw).trim().toUpperCase();
    const period = String(periodRaw).trim().toUpperCase();

    let suffix;
    if (profile === 'KT') suffix = 'KT';
    else if (profile === 'AUB') suffix = 'AUB';
    else suffix = 'KFH';

    const procName = `apm.sel_PnLSummaryReport_${suffix}`;

    const request = pool
      .request()
      .input('Period', sql.NVarChar(10), period)
      .input('StartDate', sql.Date, startDateRaw ? new Date(String(startDateRaw)) : null)
      .input('EndDate', sql.Date, endDateRaw ? new Date(String(endDateRaw)) : null);

    const result = await request.execute(procName);
    const rows = result.recordset || [];

    const data = rows.map(r => ({
      Symbol: r.CurrencyPair,
      'Matching PnL': r.MatchingPNL,
      'Position PnL': r.PositionPNL,
      'Total PnL': r.TotalPNL
    })).sort((a, b) => String(a.Symbol || '').localeCompare(String(b.Symbol || '')));

    res.json({ success: true, data, count: data.length, profile, period });
  } catch (err) {
    console.error('Error fetching PnL summary (by pair):', err);
    res.status(500).json({ success: false, error: 'Failed to fetch PnL summary (by pair)', details: err.message });
  }
});

// GET /api/reports/apm-hedge-performance - APM Hedge Performance
app.get('/api/reports/apm-hedge-performance', async (req, res) => {
  try {
    const pool = await poolPromise;

    const profileRaw = req.query.profile || 'KFH';
    const periodRaw = req.query.period || 'TODAY';
    const startDateRaw = req.query.startDate;
    const endDateRaw = req.query.endDate;

    const profile = String(profileRaw).trim().toUpperCase();
    const period = String(periodRaw).trim().toUpperCase();

    let suffix;
    if (profile === 'KT') suffix = 'KT';
    else if (profile === 'AUB') suffix = 'AUB';
    else suffix = 'KFH';

    const procName = `apm.sel_HedgePerformanceReport_${suffix}`;

    const request = pool
      .request()
      .input('Period', sql.NVarChar(10), period)
      .input('StartDate', sql.Date, startDateRaw ? new Date(String(startDateRaw)) : null)
      .input('EndDate', sql.Date, endDateRaw ? new Date(String(endDateRaw)) : null);

    const result = await request.execute(procName);
    const rows = result.recordset || [];

    const data = rows.map(r => ({
      Symbol: String(r.Symbol || ''),
      HedgeType: r.HedgeType,
      ExecutionAmountUSD: Number(r.ExecutionAmountUSD) || 0,
      ExecutionCount: Number(r.ExecutionCount) || 0,
      PositionPnL: Number(r.PositionPnL) || 0,
    }));

    res.json({
      success: true,
      data,
      count: data.length,
      profile,
      period,
    });
  } catch (err) {
    console.error('Error fetching APM hedge performance report:', err);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch APM hedge performance report',
      details: err.message,
    });
  }
});

// GET /api/reports/lp-distribution - Interbank LP Distribution & Trade Success
app.get('/api/reports/lp-distribution', async (req, res) => {
  try {
    const pool = await poolPromise;

    const profileRaw = req.query.profile || 'KFH';
    const periodRaw = req.query.period || 'TODAY';
    const startDateRaw = req.query.startDate;
    const endDateRaw = req.query.endDate;

    const profile = String(profileRaw).trim().toUpperCase();
    const period = String(periodRaw).trim().toUpperCase();

    let suffix;
    if (profile === 'KT') suffix = 'KT';
    else if (profile === 'AUB') suffix = 'AUB';
    else suffix = 'KFH';

    const procName = `apm.sel_VenueSummaryReport_${suffix}`;

    const request = pool
      .request()
      .input('Period', sql.NVarChar(10), period)
      .input('StartDate', sql.Date, startDateRaw ? new Date(String(startDateRaw)) : null)
      .input('EndDate', sql.Date, endDateRaw ? new Date(String(endDateRaw)) : null);

    const result = await request.execute(procName);
    const rows = result.recordset || [];

    const data = rows.map((r) => ({
      Venue: String(r.Venue || ''),
      Symbol: String(r.Symbol || ''),
      ExecutionAmountUSD: Number(r.ExecutionAmountUSD) || 0,
      ExecutionCount: Number(r.ExecutionCount) || 0,
      RejectCount: Number(r.RejectCount) || 0,
    }));

    res.json({ success: true, data, count: data.length, profile, period });
  } catch (err) {
    console.error('Error fetching LP distribution report:', err);
    res.status(500).json({ success: false, error: 'Failed to fetch LP distribution report', details: err.message });
  }
});

// GET /api/reports/counterparty-distribution - Interbank Counterparty Distribution (volume only)
app.get('/api/reports/counterparty-distribution', async (req, res) => {
  try {
    const pool = await poolPromise;

    const profileRaw = req.query.profile || 'KFH';
    const periodRaw = req.query.period || 'TODAY';
    const startDateRaw = req.query.startDate;
    const endDateRaw = req.query.endDate;

    const profile = String(profileRaw).trim().toUpperCase();
    const period = String(periodRaw).trim().toUpperCase();

    let suffix;
    if (profile === 'KT') suffix = 'KT';
    else if (profile === 'AUB') suffix = 'AUB';
    else suffix = 'KFH';

    const procName = `apm.sel_LpReport_${suffix}`;

    const request = pool
      .request()
      .input('Period', sql.NVarChar(10), period)
      .input('StartDate', sql.Date, startDateRaw ? new Date(String(startDateRaw)) : null)
      .input('EndDate', sql.Date, endDateRaw ? new Date(String(endDateRaw)) : null);

    const result = await request.execute(procName);
    const rows = result.recordset || [];

    const data = rows.map((r) => ({
      LpName: String(r.LpName || ''),
      Symbol: String(r.Symbol || ''),
      Amount: Number(r.Amount) || 0,
    }));

    res.json({ success: true, data, count: data.length, profile, period });
  } catch (err) {
    console.error('Error fetching counterparty distribution report:', err);
    res.status(500).json({ success: false, error: 'Failed to fetch counterparty distribution report', details: err.message });
  }
});

// GET /api/reports/hedge-routing-by-currency-pair - Matched vs Hedged by Symbol
app.get('/api/reports/hedge-routing-by-currency-pair', async (req, res) => {
  try {
    const pool = await poolPromise;

    const profileRaw = req.query.profile || 'KFH';
    const periodRaw = req.query.period || 'TODAY';
    const startDateRaw = req.query.startDate;
    const endDateRaw = req.query.endDate;

    const profile = String(profileRaw).trim().toUpperCase();
    const period = String(periodRaw).trim().toUpperCase();

    let suffix;
    if (profile === 'KT') suffix = 'KT';
    else if (profile === 'AUB') suffix = 'AUB';
    else suffix = 'KFH';

    const procName = `apm.sel_MatchedvsHedgedReport_${suffix}`;

    const request = pool
      .request()
      .input('Period', sql.NVarChar(10), period)
      .input('StartDate', sql.Date, startDateRaw ? new Date(String(startDateRaw)) : null)
      .input('EndDate', sql.Date, endDateRaw ? new Date(String(endDateRaw)) : null);

    const result = await request.execute(procName);
    const rows = result.recordset || [];

    const data = rows.map((r) => ({
      Symbol: String(r.Symbol || ''),
      MatchedAmountUSD: Number(r.MatchedAmountUSD) || 0,
      InGroupHedgeAmountUSD: Number(r.InGroupHedgeAmountUSD) || 0,
      ExternalHedgeAmountUSD: Number(r.ExternalHedgeAmountUSD) || 0,
    }));

    res.json({
      success: true,
      data,
      count: data.length,
      profile,
      period,
    });
  } catch (err) {
    console.error('Error fetching hedge routing by currency pair report:', err);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch hedge routing by currency pair report',
      details: err.message,
    });
  }
});

// GET /api/reports/period-matrix - Client Volume & PnL Trend matrix (3 x 10)
app.get('/api/reports/period-matrix', async (req, res) => {
  try {
    const pool = await poolPromise;

    const profileRaw = req.query.profile || 'KFH';
    const periodRaw = req.query.period || 'TODAY';
    const startDateRaw = req.query.startDate;
    const endDateRaw = req.query.endDate;

    const profile = String(profileRaw).trim().toUpperCase();
    const period = String(periodRaw).trim().toUpperCase();

    let suffix;
    if (profile === 'KT') suffix = 'KT';
    else if (profile === 'AUB') suffix = 'AUB';
    else suffix = 'KFH';

    const procName = `apm.sel_PeriodMatrixReport_${suffix}`;

    const request = pool
      .request()
      .input('Period', sql.NVarChar(10), period)
      .input('StartDate', sql.NVarChar(20), startDateRaw ? String(startDateRaw) : null)
      .input('EndDate', sql.NVarChar(20), endDateRaw ? String(endDateRaw) : null);

    const result = await request.execute(procName);
    const rows = result.recordset || [];

    // SP format: one row per period with
    //   StartDate, EndDate, ClientVolume, PnL, ParamChange
    const toDate = (value) => {
      if (value instanceof Date && !Number.isNaN(value.getTime())) return value;
      if (typeof value === 'string') {
        const parsed = new Date(value);
        if (!Number.isNaN(parsed.getTime())) return parsed;
      }
      return null;
    };

    // Optional pair filter: ?symbols=EUR/USD,GBP/USD  (absent or "ALL" => all pairs)
    const symbolsRaw = req.query.symbols;
    const symbolFilter = (() => {
      if (symbolsRaw === undefined || symbolsRaw === null) return null;
      const text = String(symbolsRaw).trim();
      if (!text || text.toUpperCase() === 'ALL') return null;
      return new Set(text.split(',').map((s) => s.trim().toUpperCase()).filter(Boolean));
    })();
    const matchesFilter = (sym) => symbolFilter === null || symbolFilter.has(String(sym || '').trim().toUpperCase());

    const parseParamChangeCount = (raw) => {
      if (raw === null || raw === undefined) return 0;
      if (typeof raw === 'number') return Number.isFinite(raw) ? Math.trunc(Math.abs(raw)) : 0;
      const text = String(raw).trim();
      if (!text || text.toUpperCase() === 'NULL') return 0;
      const asNumber = Number(text);
      if (Number.isFinite(asNumber)) return Math.trunc(Math.abs(asNumber));
      // Text payloads like "(USD/TRY LongPositionLimit old ... new ...)": count "old" tokens.
      const explicitChanges = (text.match(/\bold\b/gi) || []).length;
      return explicitChanges > 0 ? explicitChanges : 1;
    };

    // Labels from StartDate. TODAY: HH:mm, other periods: dd MMM. Fixed UTC to avoid offset shifts.
    const upperPeriod = period.toUpperCase();
    const hhmmFormatter = new Intl.DateTimeFormat('en-GB', { hour: '2-digit', minute: '2-digit', hour12: false, timeZone: 'UTC' });
    const dayMonthFormatter = new Intl.DateTimeFormat('en-GB', { day: '2-digit', month: 'short', timeZone: 'UTC' });
    const formatLabel = (start) => {
      if (!start) return '';
      return upperPeriod === 'TODAY' ? hhmmFormatter.format(start) : dayMonthFormatter.format(start);
    };

    // Distinct pairs present (unfiltered) — used to populate the UI dropdown.
    const symbolsPresent = Array.from(new Set(rows.map((r) => String(r.Symbol || '').trim()).filter(Boolean)))
      .sort((a, b) => a.localeCompare(b));

    // Group rows by time bucket (StartDate); within each bucket, sum the pairs that pass the filter.
    // Every bucket is kept (so the time axis stays complete); non-matching buckets simply sum to 0.
    const buckets = new Map();
    for (const r of rows) {
      const date = toDate(r.StartDate);
      const key = date ? date.getTime() : String(r.StartDate);
      let bucket = buckets.get(key);
      if (!bucket) {
        bucket = { sortKey: date ? date.getTime() : (Number(r.StartDate) || 0), label: formatLabel(date), volume: 0, pnl: 0, paramChanges: 0 };
        buckets.set(key, bucket);
      }
      if (!matchesFilter(r.Symbol)) continue;
      const v = Number(r.ClientVolume);
      const p = Number(r.PnL);
      bucket.volume += Number.isFinite(v) ? v : 0;
      bucket.pnl += Number.isFinite(p) ? p : 0;
      bucket.paramChanges += parseParamChangeCount(r.ParamChange);
    }

    const orderedBuckets = Array.from(buckets.values()).sort((a, b) => a.sortKey - b.sortKey);
    const volume = orderedBuckets.map((b) => b.volume);
    const pnl = orderedBuckets.map((b) => b.pnl);
    const labels = orderedBuckets.map((b) => b.label);
    const paramChanges = orderedBuckets.map((b) => b.paramChanges);

    res.json({
      success: true,
      profile,
      period,
      data: {
        volume,
        pnl,
        labels,
        paramChanges,
        symbols: symbolsPresent,
      },
    });
  } catch (err) {
    console.error('Error fetching period matrix report:', err);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch period matrix report',
      details: err.message,
    });
  }
});

// GET /api/sales/dealer-executions - Sales > Deal blotter (sls.sel_DealerExecutions_<entity>)
// Both @BeginDate and @EndDate are optional; when omitted the procedure defaults to
// the current week up to the end of today.
app.get('/api/sales/dealer-executions', async (req, res) => {
  try {
    const pool = await poolPromise;

    const profileRaw = req.query.profile || 'KFH';
    const beginDateRaw = req.query.beginDate;
    const endDateRaw = req.query.endDate;

    const profile = String(profileRaw).trim().toUpperCase();

    let suffix;
    if (profile === 'KT') suffix = 'KT';
    else if (profile === 'AUB') suffix = 'AUB';
    else suffix = 'KFH';

    const procName = `sls.sel_DealerExecutions_${suffix}`;

    const request = pool
      .request()
      .input('BeginDate', sql.DateTime, beginDateRaw ? new Date(String(beginDateRaw)) : null)
      .input('EndDate', sql.DateTime, endDateRaw ? new Date(String(endDateRaw)) : null);

    const result = await request.execute(procName);
    const rows = result.recordset || [];

    const data = rows.map(row => ({
      DealerExecutionID: row.DealerExecutionID,
      DealID: row.DealID,
      Symbol: row.Symbol,
      Side: row.Side,
      BaseAmount: row.BaseAmount,
      TranPrice: row.TranPrice,
      Price: row.Price,
      CustomerID: row.CustomerID,
      ValueDate: row.ValueDate,
      Type: row.Type,
      Producer: row.Producer,
      SystemDate: row.SystemDate,
      User: row.User,
    }));

    res.json({
      success: true,
      data,
      count: data.length,
      profile,
    });
  } catch (err) {
    console.error('Error fetching dealer executions:', err);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch dealer executions',
      details: err.message,
    });
  }
});

// GET /api/random - Get a random value
app.get('/api/random', (req, res) => {
  const randomValue = Math.round((Math.random() * 1000) * 100) / 100;
  res.json({
    success: true,
    data: {
      value: randomValue,
      timestamp: new Date().toISOString(),
    },
    message: 'Random value generated'
  });
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    success: true,
    service: 'MSDB API',
    status: 'running',
    port: PORT,
    timestamp: new Date().toISOString()
  });
});

// Root endpoint
app.get('/', (req, res) => {
  res.json({
    success: true,
    message: 'Simple MSDB API is running',
    endpoints: [
      'GET /health - Health check',
      'GET /api/users - Get all users',
      'GET /api/users/:id - Get user by ID',
      'POST /api/users - Create new user (requires: username, email, password)',
      'POST /api/users/:id - Update user',
      'DELETE /api/users/:id - Delete user',
      'POST /api/users/:id/auth - Authenticate user password',
      'GET /api/sessions - Get all sessions',
      'POST /api/sessions - Create new session (requires: token, userId)',
      'DELETE /api/sessions/:id - Delete session',
      'GET /api/audit-logs - Get audit logs (optional filters: recordId, userId, action, limit, offset)',
      'GET /api/audit-logs/user/:userId - Get all changes made by a specific user',
      'GET /api/audit-logs/record/:recordId - Get all changes made to a specific user record',
      'GET /api/random - Get random value'
    ],
    port: PORT
  });
});

// Start the server
app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 MSDB API Server running on port ${PORT}`);
  console.log(`🌐 Access at: http://0.0.0.0:${PORT}`);
  console.log(`💾 Health check: http://0.0.0.0:${PORT}/health`);
  console.log('🔄 Server is running and listening for requests...');
});

// Error handling
process.on('uncaughtException', (error) => {
  console.error('❌ Uncaught Exception:', error);
  console.error('Stack:', error.stack);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('exit', (code) => {
  console.log(`🔄 Process exiting with code: ${code}`);
});

process.on('SIGTERM', () => {
  console.log('🛑 SIGTERM received, shutting down gracefully...');
  process.exit(0);
});

// GET /api/audit-logs - Retrieve audit logs with optional filtering
app.get('/api/audit-logs', async (req, res) => {
  try {
    const { recordId, userId, action, limit = 100, offset = 0 } = req.query;
    const pool = await poolPromise;
    
    let query = 'SELECT * FROM apm.audit_logs WHERE 1=1';
    let request = pool.request();
    
    if (recordId) {
      query += ' AND recordId = @recordId';
      request = request.input('recordId', sql.NVarChar(50), recordId);
    }
    if (userId) {
      query += ' AND userId = @userId';
      request = request.input('userId', sql.NVarChar(50), userId);
    }
    if (action) {
      query += ' AND action = @action';
      request = request.input('action', sql.NVarChar(50), action);
    }
    
    query += ' ORDER BY timestamp DESC OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY';
    request = request
      .input('offset', sql.Int, parseInt(offset))
      .input('limit', sql.Int, parseInt(limit));
    
    const result = await request.query(query);
    
    // Parse changedFields JSON for each record
    const auditLogs = result.recordset.map(log => ({
      ...log,
      changedFields: log.changedFields ? JSON.parse(log.changedFields) : null
    }));
    
    res.json({
      success: true,
      data: auditLogs,
      count: auditLogs.length
    });
  } catch (err) {
    console.error('Error retrieving audit logs:', err);
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve audit logs',
      details: err.message
    });
  }
});

// GET /api/audit-logs/user/:userId - Get all audit logs for changes made by a specific user
app.get('/api/audit-logs/user/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    const { limit = 100, offset = 0 } = req.query;
    const pool = await poolPromise;
    
    const result = await pool.request()
      .input('userId', sql.NVarChar(50), userId)
      .input('offset', sql.Int, parseInt(offset))
      .input('limit', sql.Int, parseInt(limit))
      .query(`SELECT * FROM apm.audit_logs 
        WHERE userId = @userId 
        ORDER BY timestamp DESC 
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY`);
    
    // Parse changedFields JSON for each record
    const auditLogs = result.recordset.map(log => ({
      ...log,
      changedFields: log.changedFields ? JSON.parse(log.changedFields) : null
    }));
    
    res.json({
      success: true,
      data: auditLogs,
      count: auditLogs.length
    });
  } catch (err) {
    console.error('Error retrieving user audit logs:', err);
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve user audit logs',
      details: err.message
    });
  }
});

// GET /api/audit-logs/record/:recordId - Get all audit logs for changes to a specific user record
app.get('/api/audit-logs/record/:recordId', async (req, res) => {
  try {
    const { recordId } = req.params;
    const { limit = 100, offset = 0 } = req.query;
    const pool = await poolPromise;
    
    const result = await pool.request()
      .input('recordId', sql.NVarChar(50), recordId)
      .input('offset', sql.Int, parseInt(offset))
      .input('limit', sql.Int, parseInt(limit))
      .query(`SELECT * FROM apm.audit_logs 
        WHERE recordId = @recordId 
        ORDER BY timestamp DESC 
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY`);
    
    // Parse changedFields JSON for each record
    const auditLogs = result.recordset.map(log => ({
      ...log,
      changedFields: log.changedFields ? JSON.parse(log.changedFields) : null
    }));
    
    res.json({
      success: true,
      data: auditLogs,
      count: auditLogs.length
    });
  } catch (err) {
    console.error('Error retrieving record audit logs:', err);
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve record audit logs',
      details: err.message
    });
  }
});

// POST /api/audit-events - Log authentication/security events (login failures, password resets, etc.)
app.post('/api/audit-events', async (req, res) => {
  const { userId, action, recordId, reason, details, attemptedEmail, ipAddress, userAgent } = req.body;

  if (!userId || !action) {
    return res.status(400).json({
      success: false,
      error: 'userId and action are required'
    });
  }

  try {
    const pool = await poolPromise;
    const eventId = 'event_' + Date.now().toString(36) + Math.random().toString(36).substr(2, 8);
    const timestamp = new Date().toISOString();

    const eventDetails = JSON.stringify({
      reason,
      details,
      attemptedEmail,
      timestamp
    });

    await pool.request()
      .input('id', sql.NVarChar(50), eventId)
      .input('userId', sql.NVarChar(50), userId)
      .input('action', sql.NVarChar(50), action)
      .input('recordId', sql.NVarChar(50), recordId || 'N/A')
      .input('eventDetails', sql.NVarChar(sql.MAX), eventDetails)
      .input('ipAddress', sql.NVarChar(50), ipAddress || 'unknown')
      .input('userAgent', sql.NVarChar(255), userAgent || 'unknown')
      .input('timestamp', sql.DateTime, timestamp)
      .query(`
        INSERT INTO apm.audit_logs (id, userId, action, tableName, recordId, changedFields, ipAddress, userAgent, timestamp)
        VALUES (@id, @userId, @action, 'auth_events', @recordId, @eventDetails, @ipAddress, @userAgent, @timestamp)
      `);

    console.log(`✅ Security event logged: ${action} for user ${userId}`);
    res.status(201).json({
      success: true,
      eventId,
      message: 'Event logged successfully'
    });
  } catch (err) {
    console.error('⚠️  Failed to log security event:', err.message);
    res.status(500).json({
      success: false,
      error: 'Failed to log event',
      details: err.message
    });
  }
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\n🛑 MSDB API Server shutting down gracefully...');
  process.exit(0);
});
