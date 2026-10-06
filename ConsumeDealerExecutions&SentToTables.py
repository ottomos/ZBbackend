import os

import pyodbc
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

# ----------------------------------------------------------------------
# 1) DB BAGLANTISI (.env'den)
# ----------------------------------------------------------------------
server = os.getenv("DB_SERVER", "").strip()
instance = os.getenv("DB_INSTANCE", "").strip()
database = os.getenv("DB_DATABASE", "").strip()
port = os.getenv("DB_PORT", "1433").strip()
user = os.getenv("DB_USER", "").strip()
password = os.getenv("DB_PASSWORD", "")

# Named instance -> Server=host\instance, aksi halde Server=host,port
if instance:
    server_part = f"{server}\\{instance}"
else:
    server_part = f"{server},{port}" if port else server

conn_str = (
    "Driver={ODBC Driver 17 for SQL Server};"
    f"Server={server_part};"
    f"Database={database};"
)

# DB_USER doluysa SQL auth, bos ise Windows auth kullan.
if user:
    conn_str += f"UID={user};PWD={password};"
else:
    conn_str += "Trusted_Connection=yes;"

conn_str += "TrustServerCertificate=yes;"

# Sifreyi ekrana basma
print(
    f"DB -> {server_part} / {database} ({'SQL auth: ' + user if user else 'Windows auth'})"
)

conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()
print("DB baglantisi kuruldu.")

# ----------------------------------------------------------------------
# 2) SQL
# ----------------------------------------------------------------------
INSERT_SQL = """
INSERT INTO sls.DealerExecutions_KT
    ([DealID],[Symbol],[Side],[BaseAmount],[TranPrice],[Price],
     [CustomerID],[ValueDate],[Type],[Producer],[SystemDate],[User])
VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
"""

UPDATE_SQL = """
UPDATE sls.DealerExecutions_KT
   SET [Symbol]       = ?,
       [Side]         = ?,
       [BaseAmount]  = ?,
       [TranPrice]   = ?,
       [Price]        = ?,
       [CustomerID]  = ?,
       [ValueDate]   = ?,
       [Type]         = ?,
       [Producer]     = ?,
       [SystemDate]   = ?,
       [User]         = ?
 WHERE [DealID] = ?
"""

# ----------------------------------------------------------------------
# 3) KAFKA CONSUMER
# ----------------------------------------------------------------------
KAFKA_BROKERS = [
    broker.strip()
    for broker in os.getenv(
        "KAFKA_BROKER_KT",
        "localhost:9092,localhost:9093,localhost:9094",
    ).split(",")
    if broker.strip()
]

print(f"Kafka -> {KAFKA_BROKERS}")

consumer = KafkaConsumer(
    "PanelDealerExecutions_KT",
    bootstrap_servers=KAFKA_BROKERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="DealerExecutions_KT_consumer",
    value_deserializer=lambda m: m.decode("utf-8"),
)

print("Waiting for messages from Kafka...")

# ----------------------------------------------------------------------
# 4) DINLE - AYRISTIR - INSERT / UPDATE
# ----------------------------------------------------------------------
for message in consumer:
    raw = message.value.strip()
    if not raw:
        continue

    try:
        p = [x.strip() for x in raw.split(",")]

        if len(p) != 12:
            print(f"HATALI ALAN SAYISI ({len(p)}): {raw}")
            continue

        DealID = p[0]
        Symbol = p[1]
        Side = p[2]
        BaseAmount = float(p[3])
        TranPrice = float(p[4])
        Price = float(p[5])
        CustomerID = p[6]
        ValueDate = p[7]
        Type = p[8]
        Producer = p[9]
        SystemDate = p[10]
        User = p[11]

        islem = Type.strip().lower()

        # ---------------- INSERT ----------------
        if islem == "insert":
            cursor.execute(
                INSERT_SQL,
                DealID,
                Symbol,
                Side,
                BaseAmount,
                TranPrice,
                Price,
                CustomerID,
                ValueDate,
                Type,
                Producer,
                SystemDate,
                User,
            )
            print(f"INSERT -> {DealID} | {Symbol} | {Side} | {BaseAmount}")

        # ---------------- UPDATE ----------------
        elif islem == "update":
            cursor.execute(
                UPDATE_SQL,
                Symbol,
                Side,
                BaseAmount,
                TranPrice,
                Price,
                CustomerID,
                ValueDate,
                Type,
                Producer,
                SystemDate,
                User,
                DealID,
            )

            if cursor.rowcount == 0:
                cursor.execute(
                    INSERT_SQL,
                    DealID,
                    Symbol,
                    Side,
                    BaseAmount,
                    TranPrice,
                    Price,
                    CustomerID,
                    ValueDate,
                    Type,
                    Producer,
                    SystemDate,
                    User,
                )
                print(f"UPDATE->INSERT -> {DealID} (kayit bulunamadi, eklendi)")
            else:
                print(f"UPDATE -> {DealID} | {Symbol} | {Side} | {BaseAmount}")

        # ---------------- BILINMEYEN ----------------
        else:
            print(f"BILINMEYEN TYPE '{Type}': {raw}")

    except Exception as e:
        print(f"HATA: {e} | Mesaj: {raw}")
