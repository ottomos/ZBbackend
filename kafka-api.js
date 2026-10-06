const express = require('express');
const cors = require('cors');
const { Kafka } = require('kafkajs');
const bodyParser = require('body-parser');
const dotenv = require('dotenv');
const Redis = require('ioredis');

// Load environment variables from .env file
dotenv.config();

// ─── Redis Setup ──────────────────────────────────────────────────────────────
const REDIS_INSTANCE = process.env.REDIS_INSTANCE_NAME || 'kth';

function createRedisClient() {
  const clusterNodes = (process.env.REDIS_CLUSTER_NODES || '').trim();
  const password = process.env.REDIS_PASSWORD || undefined;

  if (clusterNodes) {
    const nodes = clusterNodes.split(',').map(n => {
      const [host, port] = n.trim().split(':');
      return { host, port: parseInt(port || '6379', 10) };
    });
    console.log(`🔴 Redis: connecting to cluster (${nodes.length} nodes)`);
    return new Redis.Cluster(nodes, {
      redisOptions: { password },
      clusterRetryStrategy: times => Math.min(times * 200, 3000)
    });
  }

  const host = process.env.REDIS_HOST || '127.0.0.1';
  const port = parseInt(process.env.REDIS_PORT || '6379', 10);
  console.log(`🔴 Redis: connecting to ${host}:${port}`);
  return new Redis({ host, port, password, retryStrategy: times => Math.min(times * 200, 3000) });
}

const redis = createRedisClient();
redis.on('connect', () => console.log('✅ Redis connected'));
redis.on('error', err => console.error('❌ Redis error:', err.message));

// Redis key helpers
const rk = {
  hash:      (entity, type) => `${REDIS_INSTANCE}:${entity}:${type}`,
  list:      (entity, type) => `${REDIS_INSTANCE}:${entity}:${type}`,
  pnlSum:    (entity)       => `${REDIS_INSTANCE}:${entity}:salesPnlSum`,
  pnlCutoff: (entity)       => `${REDIS_INSTANCE}:${entity}:salesPnlCutoff`,
};

// Fire-and-forget Redis write helpers
async function redisHSet(entity, type, field, obj) {
  try { await redis.hset(rk.hash(entity, type), field, JSON.stringify(obj)); }
  catch (e) { console.error(`Redis hset error [${entity}:${type}]:`, e.message); }
}

async function redisListAppend(entity, type, obj, maxLen = 1000) {
  try {
    const key = rk.list(entity, type);
    await redis.rpush(key, JSON.stringify(obj));
    await redis.ltrim(key, -maxLen, -1);
  } catch (e) { console.error(`Redis list error [${entity}:${type}]:`, e.message); }
}

async function redisSavePnl(entity) {
  try {
    await redis.set(rk.pnlSum(entity), String(salesPnlSums[entity] || 0));
    await redis.set(rk.pnlCutoff(entity), salesPnlCutoff[entity].toISOString());
  } catch (e) { console.error(`Redis pnl save error [${entity}]:`, e.message); }
}

// Restore all dataStores from Redis on startup
async function restoreFromRedis() {
  for (const entity of ['KFH', 'KT', 'AUB']) {
    try {
      // Hash tables: positions, strategies, riskMonitor
      for (const type of ['positions', 'strategies', 'riskMonitor']) {
        const hash = await redis.hgetall(rk.hash(entity, type));
        if (hash) {
          for (const [field, val] of Object.entries(hash)) {
            try { dataStores[entity][type][field] = JSON.parse(val); } catch {}
          }
        }
      }
      // Lists: executions, customerTransactions, internalTransactions
      for (const type of ['executions', 'customerTransactions', 'internalTransactions']) {
        const items = await redis.lrange(rk.list(entity, type), 0, -1);
        if (items && items.length) {
          dataStores[entity][type] = items
            .map(v => { try { return JSON.parse(v); } catch { return null; } })
            .filter(Boolean);
        }
      }
      // Sales PnL
      const savedSum    = await redis.get(rk.pnlSum(entity));
      const savedCutoff = await redis.get(rk.pnlCutoff(entity));
      if (savedSum !== null) salesPnlSums[entity] = parseFloat(savedSum) || 0;
      if (savedCutoff) {
        const dt = new Date(savedCutoff);
        if (!isNaN(dt.getTime())) salesPnlCutoff[entity] = dt;
      }
      console.log(
        `📥 [${entity}] Restored from Redis → pos:${Object.keys(dataStores[entity].positions).length}` +
        ` strat:${Object.keys(dataStores[entity].strategies).length}` +
        ` exec:${dataStores[entity].executions.length}` +
        ` cust:${dataStores[entity].customerTransactions.length}` +
        ` int:${dataStores[entity].internalTransactions.length}` +
        ` pnl:${salesPnlSums[entity].toFixed(2)}`
      );
    } catch (e) {
      console.error(`❌ Redis restore failed for ${entity}:`, e.message);
    }
  }
}
// ─────────────────────────────────────────────────────────────────────────────

const app = express();
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'],
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With'],
  credentials: false
}));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Generate unique group ID with timestamp for each instance
function generateTimestamp() {
  const now = new Date();
  return now.getFullYear() + '-' + 
    String(now.getMonth() + 1).padStart(2, '0') + '-' + 
    String(now.getDate()).padStart(2, '0') + '_' + 
    String(now.getHours()).padStart(2, '0') + ':' + 
    String(now.getMinutes()).padStart(2, '0') + ':' + 
    String(now.getSeconds()).padStart(2, '0') + '_';
}

let timestamp = generateTimestamp();
console.log(`📌 Instance ID: ${timestamp}`);

function isLikelyTimestamp(value) {
  return /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:?\d{2})?$/.test(String(value || '').trim());
}

function isFeedLikeStatus(value) {
  const normalized = String(value || '').trim().toLowerCase();
  return normalized === 'healthy' || normalized === 'disconnected' || normalized === 'closed' || normalized === 'true' || normalized === 'false';
}

function stripRiskPayloadMeta(fields) {
  const statuses = fields.slice(1).map((field) => String(field || '').trim());

  if (statuses.length > 0 && isLikelyTimestamp(statuses[statuses.length - 1])) {
    statuses.pop();
  }

  if (statuses.length > 0) {
    const tail = String(statuses[statuses.length - 1] || '').trim().toUpperCase();
    if (tail === 'STREAMBASE' || tail === 'PYTHON' || tail === 'NODE' || tail === 'NEXTJS') {
      statuses.pop();
    }
  }

  return statuses;
}

function parseRiskRowByEntity(entity, fields) {
  if (!Array.isArray(fields) || fields.length < 2) return null;

  const symbol = String(fields[0] || '').trim();
  if (!symbol) return null;

  const statuses = stripRiskPayloadMeta(fields);
  if (statuses.length < 3) return null;

  if (entity === 'KT') {
    const hasFeed360 = statuses.length >= 4 && isFeedLikeStatus(statuses[3]);
    if (statuses.length === 8 && hasFeed360) {
      return {
        Symbol: symbol,
        'KFH Feed Status': statuses[0] || '',
        'Integral Feed Status': statuses[1] || '',
        'Tradair Feed Status': statuses[2] || '',
        'T360T Feed Status': statuses[3] || '',
        'Strategy Status': '',
        'Position Control': statuses[4] || '',
        'Last Order Status': statuses[5] || '',
        'Spread Check': statuses[6] || '',
        'Position Flow Check': statuses[7] || '',
      };
    }

    return {
      Symbol: symbol,
      'KFH Feed Status': statuses[0] || '',
      'Integral Feed Status': statuses[1] || '',
      'Tradair Feed Status': statuses[2] || '',
      'T360T Feed Status': statuses[3] || '',
      'Strategy Status': statuses[4] || '',
      'Position Control': statuses[5] || '',
      'Last Order Status': statuses[6] || '',
      'Spread Check': statuses[7] || '',
      'Position Flow Check': statuses[8] || '',
    };
  }

  if (entity === 'AUB') {
    const hasExtraFeedBeforeStrategy = statuses.length >= 9 && isFeedLikeStatus(statuses[3]);
    const strategyIndex = hasExtraFeedBeforeStrategy ? 4 : 3;

    return {
      Symbol: symbol,
      'KFH Feed Status': statuses[0] || '',
      'FXAll Feed Status': statuses[1] || '',
      'KT Feed Status': statuses[2] || '',
      'Strategy Status': statuses[strategyIndex] || '',
      'Position Control': statuses[strategyIndex + 1] || '',
      'Last Order Status': statuses[strategyIndex + 2] || '',
      'Spread Check': statuses[strategyIndex + 3] || '',
      'Position Flow Check': statuses[strategyIndex + 4] || '',
    };
  }

  const hasExtraFeedBeforeStrategy = statuses.length >= 9 && isFeedLikeStatus(statuses[3]);
  const strategyIndex = hasExtraFeedBeforeStrategy ? 4 : 3;
  return {
    Symbol: symbol,
    'KT Feed Status': statuses[0] || '',
    'T360T Feed Status': statuses[1] || '',
    'FXAll Feed Status': statuses[2] || '',
    'Strategy Status': statuses[strategyIndex] || '',
    'Position Control': statuses[strategyIndex + 1] || '',
    'Last Order Status': statuses[strategyIndex + 2] || '',
    'Spread Check': statuses[strategyIndex + 3] || '',
    'Position Flow Check': statuses[strategyIndex + 4] || '',
  };
}

function parseNumericValue(value) {
  const raw = String(value ?? '').trim();
  if (!raw) return NaN;

  let normalized = raw.replace(/\s+/g, '').replace(/,/g, '');
  normalized = normalized.replace(/[^0-9eE+\-.]/g, '');

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : NaN;
}

// Multi-broker Kafka configuration with resilient settings
const kafkaClients = {
  KFH: new Kafka({
    clientId: 'nextjs-kafka-kfh',
    brokers: process.env.KAFKA_BROKER_KFH
      ? process.env.KAFKA_BROKER_KFH.split(',').map(broker => broker.trim()).filter(Boolean)
      : ['localhost:9093'],
    retry: {
      initialRetryTime: 100,
      retries: 8,
      factor: 2,
      multiplier: 2,
      maxRetryTime: 30000
    },
    connectionTimeout: 10000,
    requestTimeout: 30000
  }),
  KT: new Kafka({
    clientId: 'nextjs-kafka-kt', 
    brokers: process.env.KAFKA_BROKER_KT
      ? process.env.KAFKA_BROKER_KT.split(',').map(broker => broker.trim()).filter(Boolean)
      : ['localhost:9093'],
    retry: {
      initialRetryTime: 100,
      retries: 8,
      factor: 2,
      multiplier: 2,
      maxRetryTime: 30000
    },
    connectionTimeout: 10000,
    requestTimeout: 30000
  }),
  AUB: new Kafka({
    clientId: 'nextjs-kafka-aub',
    brokers: process.env.KAFKA_BROKER_AUB
      ? process.env.KAFKA_BROKER_AUB.split(',').map(broker => broker.trim()).filter(Boolean)
      : ['localhost:9093'],
    retry: {
      initialRetryTime: 100,
      retries: 8,
      factor: 2,
      multiplier: 2,
      maxRetryTime: 30000
    },
    connectionTimeout: 10000,
    requestTimeout: 30000
  })
};

// Entity-specific consumers for each broker
const consumers = {
  KFH: {
    position:      kafkaClients.KFH.consumer({ groupId: 'nextjs-kfh-position-group' }),
    strategy:      kafkaClients.KFH.consumer({ groupId: 'nextjs-kfh-strategy-group' }),
    provider:      kafkaClients.KFH.consumer({ groupId: 'nextjs-kfh-provider-group' }),
    execution:     kafkaClients.KFH.consumer({ groupId: 'nextjs-kfh-execution-group' }),
    risk:          kafkaClients.KFH.consumer({ groupId: 'nextjs-kfh-risk-group' }),
    closePosition: kafkaClients.KFH.consumer({ groupId: 'nextjs-kfh-closeposition-group' }),
    salesRates:    kafkaClients.KFH.consumer({ groupId: 'nextjs-kfh-salesrates-group', autoCommitInterval: 1000, autoCommitThreshold: 10 })
  },
  KT: {
    position:      kafkaClients.KT.consumer({ groupId: 'nextjs-kt-position-group' }),
    strategy:      kafkaClients.KT.consumer({ groupId: 'nextjs-kt-strategy-group' }),
    provider:      kafkaClients.KT.consumer({ groupId: 'nextjs-kt-provider-group' }),
    execution:     kafkaClients.KT.consumer({ groupId: 'nextjs-kt-execution-group' }),
    risk:          kafkaClients.KT.consumer({ groupId: 'nextjs-kt-risk-group' }),
    closePosition: kafkaClients.KT.consumer({ groupId: 'nextjs-kt-closeposition-group' }),
    salesRates:    kafkaClients.KT.consumer({ groupId: 'nextjs-kt-salesrates-group', autoCommitInterval: 1000, autoCommitThreshold: 10 })
  },
  AUB: {
    position:      kafkaClients.AUB.consumer({ groupId: 'nextjs-aub-position-group' }),
    strategy:      kafkaClients.AUB.consumer({ groupId: 'nextjs-aub-strategy-group' }),
    provider:      kafkaClients.AUB.consumer({ groupId: 'nextjs-aub-provider-group' }),
    execution:     kafkaClients.AUB.consumer({ groupId: 'nextjs-aub-execution-group' }),
    risk:          kafkaClients.AUB.consumer({ groupId: 'nextjs-aub-risk-group' }),
    closePosition: kafkaClients.AUB.consumer({ groupId: 'nextjs-aub-closeposition-group' }),
    salesRates:    kafkaClients.AUB.consumer({ groupId: 'nextjs-aub-salesrates-group', autoCommitInterval: 1000, autoCommitThreshold: 10 })
  }
};

// SalesRates updates are independent by partition. Process several partitions
// at once so a busy partition cannot hold the other rate updates behind it.
const SALES_RATES_PARTITIONS_CONCURRENTLY = 3;

// One long-lived producer per entity. `kafka.producer()` builds a NEW instance
// on every call, so creating one per request (as /api/kafka used to) leaked a
// fully connected producer for every KT or AUB message ever sent.
const producers = {
  KFH: kafkaClients.KFH.producer(),
  KT: kafkaClients.KT.producer(),
  AUB: kafkaClients.AUB.producer()
};

// Entities whose producer is currently connected. Cleared on disconnect so the
// next send reconnects the same instance instead of building another one.
const connectedProducers = new Set();
// In-flight connect() per entity, so a burst of deals cannot start several
// concurrent connects on the same producer.
const connectingProducers = new Map();

for (const [entity, entityProducer] of Object.entries(producers)) {
  entityProducer.on(entityProducer.events.DISCONNECT, () => {
    connectedProducers.delete(entity);
    console.warn(`⚠️  ${entity} producer disconnected`);
  });
}

async function getConnectedProducer(entity) {
  const entityProducer = producers[entity];
  if (connectedProducers.has(entity)) return entityProducer;

  let pending = connectingProducers.get(entity);
  if (!pending) {
    pending = entityProducer
      .connect()
      .then(() => connectedProducers.add(entity))
      .finally(() => connectingProducers.delete(entity));
    connectingProducers.set(entity, pending);
  }
  await pending;
  return entityProducer;
}

// Entity a message belongs to: an explicit profile wins, otherwise fall back to
// the topic suffix. Anything unrecognised stays on KFH, as before.
// `profile` is caller-supplied, so match own keys only - a plain `producers[x]`
// lookup would treat inherited Object properties as valid entities.
function resolveProducerEntity(profile, topic) {
  const normalized = String(profile || '').trim().toUpperCase();
  if (Object.hasOwn(producers, normalized)) return normalized;
  if (topic?.includes('_KT')) return 'KT';
  if (topic?.includes('_AUB')) return 'AUB';
  return 'KFH';
}

// Topic configuration for each entity (matching simulator output)
const topicConfig = {
  KFH: {
    position: 'PanelPosition_KFH',
    strategy: 'PanelStrategy_KFH',
    provider: 'PanelProvider_KFH',
    execution: 'PanelExecutionResult_KFH',
    risk: 'PanelRiskMonitor_KFH',
    closePosition: 'PanelClosePosition_KFH',
    customer: 'PanelCustomerData_KFH',
    internalTrade: 'PanelCustomerData_KFH_InternalTrade',
    salesRates: 'PanelSalesRates_KFH'
  },
  KT: {
    position: 'PanelPosition_KT',
    strategy: 'PanelStrategy_KT',
    provider: 'PanelProvider_KT',
    execution: 'PanelExecutionResult_KT',
    risk: 'PanelRiskMonitor_KT',
    closePosition: 'PanelClosePosition_KT',
    customer: 'PanelCustomerData_KT',
    internalTrade: 'PanelCustomerData_KT_InternalTrade',
    salesRates: 'PanelSalesRates_KT'
  },
  AUB: {
    position: 'PanelPosition_AUB',
    strategy: 'PanelStrategy_AUB',
    provider: 'PanelProvider_AUB',
    execution: 'PanelExecutionResult_AUB',
    risk: 'PanelRiskMonitor_AUB',
    closePosition: 'PanelClosePosition_AUB',
    customer: 'PanelCustomerData_AUB',
    internalTrade: 'PanelCustomerData_AUB_InternalTrade',
    salesRates: 'PanelSalesRates_AUB'
  }
};

// Entity-specific data stores
const dataStores = {
  KFH: {
    positions: {},
    strategies: {},
    providers: {},
    executions: [],
    riskMonitor: {},
    customerTransactions: [],
    internalTransactions: [],
    salesRates: {}
  },
  KT: {
    positions: {},
    strategies: {},
    providers: {},
    executions: [],
    riskMonitor: {},
    customerTransactions: [],
    internalTransactions: [],
    salesRates: {}
  },
  AUB: {
    positions: {},
    strategies: {},
    providers: {},
    executions: [],
    riskMonitor: {},
    customerTransactions: [],
    internalTransactions: [],
    salesRates: {}
  }
};

// Rolling Sales PnL sums per entity (since last Saturday 09:00)
const salesPnlSums = {
  KFH: 0,
  KT: 0,
  AUB: 0
};

// Track the current cutoff (last Saturday 09:00) per entity
const salesPnlCutoff = {
  KFH: getLastSaturdayNine(),
  KT: getLastSaturdayNine(),
  AUB: getLastSaturdayNine()
};

function getLastSaturdayNine() {
  const now = new Date();
  const day = now.getDay();
  const daysSinceSaturday = (day >= 6) ? day - 6 : day + 1;
  const lastSaturday = new Date(now);
  lastSaturday.setDate(now.getDate() - daysSinceSaturday);
  lastSaturday.setHours(9, 0, 0, 0);
  if (now.getDay() === 6 && now.getHours() < 9) {
    lastSaturday.setDate(lastSaturday.getDate() - 7);
  }
  return lastSaturday;
}

// Broker connection status tracking
const brokerStatus = {
  KFH: 'disconnected',
  KT: 'disconnected',
  AUB: 'disconnected'
};

// Universal consumer setup for all entities
async function setupConsumers() {
  console.log('🔌 Setting up multi-broker consumers...');
  
  for (const [entity, entityConsumers] of Object.entries(consumers)) {
    const maxRetries = 10;
    let retryCount = 0;
    
    while (retryCount < maxRetries) {
      try {
        // If retrying, recreate consumers with the same static group IDs
        if (retryCount > 0) {
          console.log(`🔄 Recreating ${entity} consumers (retry ${retryCount})`);
          const ent = entity.toLowerCase();
          consumers[entity] = {
            position:      kafkaClients[entity].consumer({ groupId: `nextjs-${ent}-position-group` }),
            strategy:      kafkaClients[entity].consumer({ groupId: `nextjs-${ent}-strategy-group` }),
            provider:      kafkaClients[entity].consumer({ groupId: `nextjs-${ent}-provider-group` }),
            execution:     kafkaClients[entity].consumer({ groupId: `nextjs-${ent}-execution-group` }),
            risk:          kafkaClients[entity].consumer({ groupId: `nextjs-${ent}-risk-group` }),
            closePosition: kafkaClients[entity].consumer({ groupId: `nextjs-${ent}-closeposition-group` }),
            salesRates:    kafkaClients[entity].consumer({ groupId: `nextjs-${ent}-salesrates-group`, autoCommitInterval: 1000, autoCommitThreshold: 10 })
          };
        }
        
        await setupEntityConsumers(entity, consumers[entity]);
        brokerStatus[entity] = 'connected';
        console.log(`✅ ${entity} broker connected successfully`);
        break; // Success, exit retry loop
      } catch (error) {
        retryCount++;
        console.error(`❌ ${entity} broker failed to connect (attempt ${retryCount}/${maxRetries}):`, error.message);
        brokerStatus[entity] = 'disconnected';
        
        if (retryCount < maxRetries) {
          // Exponential backoff: 2s, 4s, 8s, 16s, 32s
          const delay = Math.pow(2, retryCount) * 1000;
          console.log(`⏳ Retrying ${entity} in ${delay / 1000}s with new consumer group...`);
          await new Promise(resolve => setTimeout(resolve, delay));
        } else {
          console.warn(`⚠️  ${entity} broker failed after ${maxRetries} attempts, continuing with other brokers...`);
        }
      }
    }
  }
}

async function setupEntityConsumers(entity, entityConsumers) {
  const topics = topicConfig[entity];

  function updateCutoffAndRecompute(entity) {
    const newCutoff = getLastSaturdayNine();
    if (+newCutoff !== +salesPnlCutoff[entity]) {
      // boundary crossed, reset and recompute from current stored transactions
      salesPnlCutoff[entity] = newCutoff;
      let total = 0;
      const rows = (dataStores[entity].customerTransactions || []).concat(dataStores[entity].internalTransactions || []);
      for (const row of rows) {
        const tsRaw = (row.Timestamp || row.SystemDate || '').toString();
        const parsed = Date.parse(tsRaw);
        if (isNaN(parsed)) continue;
        const dt = new Date(parsed);
        if (dt >= salesPnlCutoff[entity]) {
          total += parseFloat(row.SalesPnL || 0) || 0;
        }
      }
      salesPnlSums[entity] = total;
      console.log(`🔁 [${entity}] Sales PnL cutoff updated to ${salesPnlCutoff[entity].toISOString()}, recomputed sum=${salesPnlSums[entity]}`);
    }
  }
  
  // Position consumer
  await entityConsumers.position.connect();
  await entityConsumers.position.subscribe({ topic: topics.position, fromBeginning: false });
  await entityConsumers.position.run({
    eachMessage: async ({ message }) => {
      const value = message.value.toString();
      const fields = value.split(',');
      if (fields.length >= 12) {
        const symbol = fields[0].trim();
        const row = {
          Symbol: symbol,
          Position: fields[1].trim(),
          'Position Value': fields[2].trim(),
          'Avg Cost Rate': fields[3].trim(),
          'Unrealized PNL': fields[4].trim(),
          'Realized PNL': fields[5].trim(),
          Bid: fields[6].trim(),
          Ask: fields[7].trim(),
          'Bid Venue': fields[8].trim(),
          'Ask Venue': fields[9].trim(),
        };
        dataStores[entity].positions[symbol] = row;
        redisHSet(entity, 'positions', symbol, row);
        // Limit to 100 symbols per entity
        if (Object.keys(dataStores[entity].positions).length > 100) {
          delete dataStores[entity].positions[Object.keys(dataStores[entity].positions)[0]];
        }
      }
    },
  });

  // Strategy consumer
  await entityConsumers.strategy.connect();
  await entityConsumers.strategy.subscribe({ topic: topics.strategy, fromBeginning: true });
  await entityConsumers.strategy.run({
    eachMessage: async ({ message }) => {
      const value = message.value.toString();
      const fields = value.split(',');
      if (fields.length >= 10) {
        const symbol = fields[0].trim();
        const transferAmount = parseFloat(fields[2].trim()) || 0;
        const row = {
          Symbol: symbol,
          Status: fields[1].trim(),
          TransferAccount: transferAmount.toLocaleString(),
          LongPositionLimit: parseFloat(fields[3].trim()) || 0,
          ShortPositionLimit: parseFloat(fields[4].trim()) || 0,
          MinHedge: parseFloat(fields[5].trim()) || 0,
          HedgeRatio: parseFloat(fields[6].trim()) || 0,
          TakeProfit: parseFloat(fields[7].trim()) || 0,
          StopLoss: parseFloat(fields[8].trim()) || 0,
          SpreadCheck: parseFloat(fields[9].trim()) || 0,
        };
        dataStores[entity].strategies[symbol] = row;
        redisHSet(entity, 'strategies', symbol, row);
        if (Object.keys(dataStores[entity].strategies).length > 100) {
          delete dataStores[entity].strategies[Object.keys(dataStores[entity].strategies)[0]];
        }
      }
    },
  });

  // Execution consumer
  await entityConsumers.execution.connect();
  await entityConsumers.execution.subscribe({ topic: topics.execution, fromBeginning: true });
  await entityConsumers.execution.run({
    eachMessage: async ({ message }) => {
      const value = message.value.toString();
      const fields = value.split(',');
      if (fields.length >= 11) {
        const parsedQuantity = parseNumericValue(fields[1]);
        const row = {
          Symbol: fields[0].trim(),
          Quantity: Number.isFinite(parsedQuantity) ? Math.round(parsedQuantity) : 0,
          Price: parseFloat(fields[2].trim()) || 0,
          Side: fields[3].trim(),
          Venue: fields[4].trim(),
          'Counter Party': fields[5].trim(),
          Timestamp: fields[6].trim(),
          Strategy: fields[7].trim(),
          'Realized PnL': parseFloat(fields[8].trim()) || 0,
        };
        dataStores[entity].executions.push(row);
        redisListAppend(entity, 'executions', row);
        // Keep only last 1000 executions per entity
        if (dataStores[entity].executions.length > 1000) {
          dataStores[entity].executions = dataStores[entity].executions.slice(-1000);
        }
      }
    },
  });

  // Risk Monitor consumer
  await entityConsumers.risk.connect();
  await entityConsumers.risk.subscribe({ topic: topics.risk, fromBeginning: false });
  await entityConsumers.risk.run({
    eachMessage: async ({ message }) => {
      const value = message.value.toString();
      const fields = value.split(',');
      const row = parseRiskRowByEntity(entity, fields);
      if (row) {
        const symbol = row.Symbol;
        dataStores[entity].riskMonitor[symbol] = row;
        redisHSet(entity, 'riskMonitor', symbol, row);
        if (Object.keys(dataStores[entity].riskMonitor).length > 100) {
          delete dataStores[entity].riskMonitor[Object.keys(dataStores[entity].riskMonitor)[0]];
        }
      }
    },
  });

  // Close Position consumer
  await entityConsumers.closePosition.connect();
  await entityConsumers.closePosition.subscribe({ topic: topics.closePosition, fromBeginning: false });
  await entityConsumers.closePosition.run({
    eachMessage: async ({ message }) => {
      const value = message.value.toString();
      const fields = value.split(',');
      if (fields.length >= 4) {
        const symbol = fields[0].trim();
        const closeFlag = fields[1].trim(); // True/False
        const producer = fields[2].trim();
        const timestamp = fields[3].trim();
        
        console.log(`🔄 [${entity}] Close Position Request: ${symbol} (${closeFlag}) at ${timestamp}`);
        
        // Here you could add logic to actually close the position
        // For now, just log the close request
      }
    },
  });

  // Sales Rates consumer (Sales > Deal rate cards)
  // Payload: Symbol, SpotDate, SpotBid, SpotAsk, SwapBid, SwapAsk, TodayDate,
  //          TodayBid, TodayAsk, Producer, SystemDate
  // Prices are kept as strings so the trailing zeros coming from the feed
  // (e.g. "55.7530060") survive untouched - the rate cards split them into
  // head / big figure / pips and need the exact precision.
  await entityConsumers.salesRates.connect();
  await entityConsumers.salesRates.subscribe({ topic: topics.salesRates, fromBeginning: false });
  await entityConsumers.salesRates.run({
    partitionsConsumedConcurrently: SALES_RATES_PARTITIONS_CONCURRENTLY,
    eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
      const latestRows = new Map();

      for (const [messageIndex, message] of batch.messages.entries()) {
        resolveOffset(message.offset);

        const value = message.value.toString();
        const fields = value.split(',');
        if (fields.length < 11) continue;

        const symbol = fields[0].trim();
        if (!symbol) continue;

        latestRows.set(symbol, {
          Symbol: symbol,
          SpotDate: fields[1].trim(),
          SpotBid: fields[2].trim(),
          SpotAsk: fields[3].trim(),
          SwapBid: fields[4].trim(),
          SwapAsk: fields[5].trim(),
          TodayDate: fields[6].trim(),
          TodayBid: fields[7].trim(),
          TodayAsk: fields[8].trim(),
          Producer: fields[9].trim(),
          SystemDate: fields[10].trim(),
        });

        if ((messageIndex + 1) % 100 === 0) await heartbeat();
      }

      for (const [symbol, row] of latestRows) {
        dataStores[entity].salesRates[symbol] = row;
      }
    },
  });

  // Customer Transaction consumer
  const customerConsumer = kafkaClients[entity].consumer({ groupId: `nextjs-${entity.toLowerCase()}-customer-group` });
  await customerConsumer.connect();
  await customerConsumer.subscribe({ topic: topics.customer, fromBeginning: false });
  await customerConsumer.run({
    eachMessage: async ({ message }) => {
      const value = message.value.toString();
      const fields = value.split(',');
      if (fields.length >= 12) {
        const row = {
          Symbol: fields[0].trim(),
          Quantity: parseFloat(fields[1].trim()) || 0,
          Side: fields[2].trim(),
          TranPrice: parseFloat(fields[3].trim()) || 0,
          Timestamp: fields[4].trim(),
          CustomerID: fields[5].trim(),
          BidPrice: parseFloat(fields[6].trim()) || 0,
          AskPrice: parseFloat(fields[7].trim()) || 0,
          SalesPnL: parseFloat(fields[8].trim()) || 0,
          TranID: fields[9].trim(),
          Producer: fields[10].trim(),
          SystemDate: fields[11].trim()
        };
        dataStores[entity].customerTransactions.push(row);
        redisListAppend(entity, 'customerTransactions', row, 30000);
        if (dataStores[entity].customerTransactions.length > 30000) {
          dataStores[entity].customerTransactions = dataStores[entity].customerTransactions.slice(-30000);
        }
        // update cutoff if needed and add to rolling sum if within window
        try {
          updateCutoffAndRecompute(entity);
          const tsRaw = (row.Timestamp || row.SystemDate || '').toString();
          const parsed = Date.parse(tsRaw);
          if (!isNaN(parsed)) {
            const dt = new Date(parsed);
            if (dt >= salesPnlCutoff[entity]) {
              salesPnlSums[entity] = (salesPnlSums[entity] || 0) + (parseFloat(row.SalesPnL) || 0);
              redisSavePnl(entity);
            }
          }
        } catch (e) {
          console.error('Error updating sales sum (customer):', e);
        }
      }
    }
  });

  // Internal Trade consumer
  const internalConsumer = kafkaClients[entity].consumer({ groupId: `nextjs-${entity.toLowerCase()}-internal-group` });
  await internalConsumer.connect();
  await internalConsumer.subscribe({ topic: topics.internalTrade, fromBeginning: false });
  await internalConsumer.run({
    eachMessage: async ({ message }) => {
      const value = message.value.toString();
      const fields = value.split(',');
      if (fields.length >= 12) {
        const row = {
          Symbol: fields[0].trim(),
          Quantity: parseFloat(fields[1].trim()) || 0,
          Side: fields[2].trim(),
          TranPrice: parseFloat(fields[3].trim()) || 0,
          Timestamp: fields[4].trim(),
          CustomerID: fields[5].trim(),
          BidPrice: parseFloat(fields[6].trim()) || 0,
          AskPrice: parseFloat(fields[7].trim()) || 0,
          SalesPnL: parseFloat(fields[8].trim()) || 0,
          TranID: fields[9].trim(),
          Producer: fields[10].trim(),
          SystemDate: fields[11].trim()
        };
        dataStores[entity].internalTransactions.push(row);
        redisListAppend(entity, 'internalTransactions', row, 30000);
        if (dataStores[entity].internalTransactions.length > 30000) {
          dataStores[entity].internalTransactions = dataStores[entity].internalTransactions.slice(-30000);
        }
        // update cutoff if needed and add to rolling sum if within window
        try {
          updateCutoffAndRecompute(entity);
          const tsRaw = (row.Timestamp || row.SystemDate || '').toString();
          const parsed = Date.parse(tsRaw);
          if (!isNaN(parsed)) {
            const dt = new Date(parsed);
            if (dt >= salesPnlCutoff[entity]) {
              salesPnlSums[entity] = (salesPnlSums[entity] || 0) + (parseFloat(row.SalesPnL) || 0);
              redisSavePnl(entity);
            }
          }
        } catch (e) {
          console.error('Error updating sales sum (internal):', e);
        }
      }
    }
  });
}

async function runProvider() {
  await providerConsumer.connect();
  await providerConsumer.subscribe({ topic: providerTopic, fromBeginning: false });

  await providerConsumer.run({
    eachMessage: async ({ message }) => {
      const value = message.value.toString();
      const fields = value.split(',');
      if (fields.length >= 5) {
        const symbol = fields[0].trim();
        const row = {
          Symbol: symbol,
          KFH: fields[1].trim(),
          FIX360T: fields[2].trim(),
          Integral: fields[3].trim(),
          Tradair: fields[4].trim(),
        };
        latestProviderRowsBySymbol[symbol] = row;
        // Optionally limit to 100 symbols:
        if (Object.keys(latestProviderRowsBySymbol).length > 100) {
          delete latestProviderRowsBySymbol[Object.keys(latestProviderRowsBySymbol)[0]];
        }
      }
    },
  });
}

async function runExecution() {
  await executionConsumer.connect();
  await executionConsumer.subscribe({ topic: executionTopic, fromBeginning: false });

  await executionConsumer.run({
    eachMessage: async ({ message }) => {
      const value = message.value.toString();
      const fields = value.split(',');
      if (fields.length >= 9) {
        const sideValue = fields[3].trim();
        const side = sideValue === '1' ? 'Buy' : sideValue === '2' ? 'Sell' : sideValue;
        
        const row = {
          Symbol: fields[0].trim(),
          Quantity: parseFloat(fields[1].trim()) || 0,
          Price: parseFloat(fields[2].trim()) || 0,
          Side: side,
          Venue: fields[4].trim(),
          'Counter Party': fields[5].trim(),
          Timestamp: fields[6].trim(),
          Strategy: fields[7].trim(),
          'Realized PnL': parseFloat(fields[8].trim()) || 0,
        };
        
        // Add to beginning of array to show latest executions first
        latestExecutionRows.unshift(row);
        
        // Keep only last 1000 executions to prevent memory issues
        if (latestExecutionRows.length > 1000) {
          latestExecutionRows = latestExecutionRows.slice(0, 1000);
        }
      }
    },
  });
}

async function runRisk() {
  await riskConsumer.connect();
  await riskConsumer.subscribe({ topic: riskTopic, fromBeginning: false });

  await riskConsumer.run({
    eachMessage: async ({ message }) => {
      const value = message.value.toString();
      const fields = value.split(',');
      const statuses = stripRiskPayloadMeta(fields);
      if (fields.length >= 2 && statuses.length >= 4) {
        const symbol = String(fields[0] || '').trim();
        if (!symbol) return;
        const row = {
          Symbol: symbol,
          KFHFeedStatus: statuses[0] || '',
          IntegralFeedStatus: statuses[1] || '',
          TradairFeedStatus: statuses[2] || '',
          Feed360TStatus: statuses[3] || '',
          StrategyStatus: statuses[4] || '',
          PositionControl: statuses[5] || '',
          LastOrderStatus: statuses[6] || '',
          SpreadCheck: statuses[7] || '',
          PositionFlowCheck: statuses[8] || '',
        };
        latestRiskRowsBySymbol[symbol] = row;
        if (Object.keys(latestRiskRowsBySymbol).length > 100) {
          delete latestRiskRowsBySymbol[Object.keys(latestRiskRowsBySymbol)[0]];
        }
      }
    },
  });
}

// Helper function to get entity data
function getEntityData(profile, dataType) {
  const entity = profile || 'KFH';
  
  if (!dataStores[entity]) {
    console.warn(`Unknown entity: ${entity}, using KFH as fallback`);
    return dataStores['KFH'][dataType] || {};
  }
  
  return dataStores[entity][dataType] || {};
}

// API Endpoints with multi-broker support
app.get('/api/positions', (req, res) => {
  const profile = req.query.profile || 'KFH';
  const data = getEntityData(profile, 'positions');
  const sortedRows = Object.values(data).sort((a, b) => a.Symbol?.localeCompare(b.Symbol) || 0);
  
  res.json({
    entity: profile,
    brokerStatus: brokerStatus[profile] || 'unknown',
    data: sortedRows,
    lastUpdated: new Date().toISOString()
  });
});

app.get('/api/strategy', (req, res) => {
  const profile = req.query.profile || 'KFH';
  const data = getEntityData(profile, 'strategies');
  const sortedRows = Object.values(data).sort((a, b) => a.Symbol?.localeCompare(b.Symbol) || 0);
  
  res.json({
    entity: profile,
    brokerStatus: brokerStatus[profile] || 'unknown',
    data: sortedRows,
    lastUpdated: new Date().toISOString()
  });
});

app.get('/api/execution', (req, res) => {
  const profile = req.query.profile || 'KFH';
  const data = getEntityData(profile, 'executions');
  
  res.json({
    entity: profile,
    brokerStatus: brokerStatus[profile] || 'unknown',
    data: Array.isArray(data) ? data : [],
    lastUpdated: new Date().toISOString()
  });
});

// Sales > Deal rate cards (PanelSalesRates_<entity>)
app.get('/api/sales-rates', (req, res) => {
  const profile = req.query.profile || 'KFH';
  const data = getEntityData(profile, 'salesRates');
  const sortedRows = Object.values(data).sort((a, b) => a.Symbol?.localeCompare(b.Symbol) || 0);

  res.json({
    entity: profile,
    brokerStatus: brokerStatus[profile] || 'unknown',
    data: sortedRows,
    lastUpdated: new Date().toISOString()
  });
});

app.get('/api/risk-monitor', (req, res) => {
  const profile = req.query.profile || 'KFH';
  const data = getEntityData(profile, 'riskMonitor');
  const sortedRows = Object.values(data).sort((a, b) => a.Symbol?.localeCompare(b.Symbol) || 0);
  
  res.json({
    entity: profile,
    brokerStatus: brokerStatus[profile] || 'unknown',
    data: sortedRows,
    lastUpdated: new Date().toISOString()
  });
});

app.get('/api/customer-transactions', (req, res) => {
  const profile = req.query.profile || 'KFH';
  const data = dataStores[profile]?.customerTransactions || [];
  res.json({
    entity: profile,
    brokerStatus: brokerStatus[profile] || 'unknown',
    data: data,
    lastUpdated: new Date().toISOString()
  });
});

app.get('/api/internal-transactions', (req, res) => {
  const profile = req.query.profile || 'KFH';
  const data = dataStores[profile]?.internalTransactions || [];
  res.json({
    entity: profile,
    brokerStatus: brokerStatus[profile] || 'unknown',
    data: data,
    lastUpdated: new Date().toISOString()
  });
});

// POST endpoint for sending messages to Kafka (for strategy edits and controls)
app.post('/api/kafka', async (req, res) => {
  try {
    const { topic, key, value, profile } = req.body;

    const entity = resolveProducerEntity(profile, topic);

    // Send message with retry logic for leadership election issues
    let retries = 3;
    let lastError = null;
    
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        const targetProducer = await getConnectedProducer(entity);
        await targetProducer.send({
          topic: topic,
          messages: [{
            key: key,
            value: value,
            timestamp: Date.now().toString()
          }]
        });

        // Try to pretty-print JSON values, otherwise print raw string
        let displayValue = value;
        try {
          if (typeof value === 'string') {
            const parsed = JSON.parse(value);
            displayValue = JSON.stringify(parsed, null, 2);
          }
        } catch (e) {
          // not JSON, leave as-is
        }

        console.log(`📤 Message sent to topic: ${topic}, key: ${key}${attempt > 1 ? ` (attempt ${attempt})` : ''}`);
        console.log('--- Message payload ---');
        console.log(displayValue);
        console.log('-----------------------');
        res.json({ 
          success: true, 
          message: 'Strategy update sent successfully',
          topic: topic,
          profile: profile || 'KFH',
          attempt: attempt
        });
        return; // Success, exit retry loop
        
      } catch (error) {
        lastError = error;
        
        // Check if it's a leadership election error
        if (error.message?.includes('leadership election') || error.message?.includes('no leader')) {
          console.log(`⚠️  Leadership election in progress for ${topic}, attempt ${attempt}/${retries}`);
          
          if (attempt < retries) {
            // Wait before retry (exponential backoff)
            const delay = Math.pow(2, attempt) * 1000; // 2s, 4s, 8s
            await new Promise(resolve => setTimeout(resolve, delay));
            continue;
          }
        } else {
          // Non-retryable error, throw immediately
          throw error;
        }
      }
    }
    
    // If we get here, all retries failed
    throw lastError;
    
  } catch (error) {
    console.error('❌ Error sending message to Kafka:', error);
    res.status(500).json({ 
      success: false, 
      error: error.message,
      details: 'Failed to send strategy update to Kafka broker'
    });
  }
});

// Broker status endpoint
app.get('/api/broker-status', (req, res) => {
  res.json({
    brokers: brokerStatus,
    entities: Object.keys(topicConfig),
    lastChecked: new Date().toISOString()
  });
});

// Legacy endpoints for backward compatibility
app.get('/api/table', (req, res) => {
  const profile = req.query.profile || 'KFH';
  const data = getEntityData(profile, 'positions');
  const sortedRows = Object.values(data).sort((a, b) => a.Symbol?.localeCompare(b.Symbol) || 0);
  res.json(sortedRows);
});

app.get('/api/risk', (req, res) => {
  const profile = req.query.profile || 'KFH';
  const data = getEntityData(profile, 'riskMonitor');
  const sortedRows = Object.values(data).sort((a, b) => a.Symbol?.localeCompare(b.Symbol) || 0);
  res.json(sortedRows);
});

// Start the multi-broker consumer setup
async function startServer() {
  console.log('🚀 Starting Multi-Broker Kafka API Server...');
  
  // Connect every entity producer up front so request handlers only ever reuse
  // them. A failure here is not fatal - getConnectedProducer() retries lazily on
  // the first send.
  for (const entity of Object.keys(producers)) {
    for (let i = 0; i < 3; i++) {
      try {
        await getConnectedProducer(entity);
        console.log(`✅ ${entity} producer connected`);
        break;
      } catch (error) {
        console.error(`❌ Failed to connect ${entity} producer (attempt ${i + 1}/3):`, error.message);
        if (i < 2) {
          await new Promise(resolve => setTimeout(resolve, 2000));
        } else {
          console.warn(`⚠️  ${entity} producer connection failed, will retry on first send...`);
        }
      }
    }
  }
  
  // Restore last-known state from Redis before consumers start
  console.log('📥 Restoring state from Redis...');
  await restoreFromRedis();
  
  // Setup all entity consumers (with built-in retry logic)
  await setupConsumers();
  
  console.log('🎯 Multi-broker setup complete!');
  console.log(`📊 Available entities: ${Object.keys(topicConfig).join(', ')}`);
  console.log('🔗 API endpoints:');
  console.log('   • GET /api/positions?profile=KFH|KT|AUB');
  console.log('   • GET /api/strategy?profile=KFH|KT|AUB');
  console.log('   • GET /api/execution?profile=KFH|KT|AUB');
  console.log('   • GET /api/risk-monitor?profile=KFH|KT|AUB');
  console.log('   • POST /api/kafka (for strategy edits & controls)');
  console.log('   • GET /api/broker-status');
}

app.listen(4000, '0.0.0.0', async () => {
  console.log('🌐 Kafka API server running on http://0.0.0.0:4000');
  await startServer();
});