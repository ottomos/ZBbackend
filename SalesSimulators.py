import os
import random
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()


KAFKA_BROKERS = [
    broker.strip()
    for broker in os.getenv(
        "KAFKA_BROKER_KT",
        "localhost:9092,localhost:9093,localhost:9094",
    ).split(",")
    if broker.strip()
]


TOPICS = ["PanelSalesRates_KT", "PanelSalesRates_KFH", "PanelSalesRates_AUB"]


CURRENCY_PAIRS = [
    "EUR/TRY",
    "USD/JPY",
    "EUR/CAD",
    "XPT/USD",
    "XAU/EUR",
    "GBP/USD",
    "XPD/EUR",
    "CAD/TRY",
    "SEK/TRY",
    "XPT/TRY",
    "USD/DKK",
    "USD/AED",
    "JPY/TRY",
    "USD/TRY",
    "XPD/USD",
    "EUR/SAR",
    "AUD/USD",
    "AED/TRY",
    "EUR/KWD",
    "XPT/EUR",
    "USD/NOK",
    "CHF/TRY",
    "USD/CAD",
    "DKK/TRY",
    "USD/QAR",
    "GBP/TRY",
    "XAG/TRY",
    "EUR/CHF",
    "EUR/AUD",
    "XAU/TRY",
    "USD/RUB",
    "EUR/QAR",
    "AUD/TRY",
    "EUR/DKK",
    "EUR/USD",
    "USD/KWD",
    "EUR/AED",
    "XAG/USD",
    "USD/CHF",
    "USD/SEK",
    "NOK/TRY",
    "EUR/RUB",
    "KWD/TRY",
    "EUR/JPY",
    "SAR/TRY",
    "XAG/EUR",
    "USD/SAR",
    "EUR/GBP",
    "XPD/TRY",
    "RUB/TRY",
    "XAU/USD",
    "QAR/TRY",
]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: v.encode("utf-8"),
    acks=1,
    linger_ms=0,
    batch_size=16384,
    max_block_ms=10000,
)


def generate_rate(pair):
    today_date = datetime.now().strftime("%Y%m%d")
    spot_date = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")
    system_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    today_bid = round(random.uniform(1, 100), 7)
    spread = random.uniform(0.001, 0.05)
    today_ask = round(today_bid + spread, 7)

    spot_bid = round(today_bid + random.uniform(-1, 1), 5)
    spot_ask = round(spot_bid + spread, 5)

    swap_bid = round(random.uniform(-5000, 5000), 3)
    swap_ask = round(swap_bid + random.uniform(10, 100), 3)

    return (
        f"{pair}, {spot_date}, "
        f"{spot_bid:.5f}, {spot_ask:.5f}, "
        f"{swap_bid:.3f}, {swap_ask:.3f}, "
        f"{today_date}, "
        f"{today_bid:.7f}, {today_ask:.7f},"
        f"Streambase, {system_date}"
    )


print("Simulator started...")

while True:
    try:
        for pair in CURRENCY_PAIRS:
            message = generate_rate(pair)

            for topic in TOPICS:
                producer.send(topic, value=message)
                # time.sleep(0.5)
                # print(topic,message)

        producer.flush()
        # print("Batch sent")

        time.sleep(0.3)

    except Exception as e:
        print(f"ERROR: {e}")
