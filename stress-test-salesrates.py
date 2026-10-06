#!/usr/bin/env python3
"""
Stress test: feed PanelSalesRates_KT at ~495 msgs/sec and measure consumer lag.

Loads the .venv's kafka-python and the same .env brokers, produces a steady
stream of realistic rate messages, then samples the consumer-group lag via
kafka-consumer-groups (run inside the KT broker container).

Usage:
    ./.venv/bin/python stress-test-salesrates.py [--duration 30] [--rate 495]
"""

import argparse
import os
import random
import subprocess
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

BROKERS = [
    broker.strip()
    for broker in os.getenv(
        "KAFKA_BROKER_KT",
        "localhost:9092,localhost:9093,localhost:9094",
    ).split(",")
    if broker.strip()
]
TOPIC = "PanelSalesRates_KT"
GROUP = "nextjs-kt-salesrates-group"
CONTAINER = "kthgui-backend-kafka-kt-1"

PAIRS = [
    "EUR/TRY",
    "USD/JPY",
    "EUR/CAD",
    "XPT/USD",
    "XAU/EUR",
    "GBP/USD",
    "CAD/TRY",
    "SEK/TRY",
    "USD/TRY",
    "EUR/USD",
    "USD/KWD",
    "USD/AED",
    "CHF/TRY",
    "NOK/TRY",
    "AUD/USD",
    "GBP/TRY",
    "XAU/TRY",
    "USD/SAR",
    "DKK/TRY",
    "EUR/CHF",
    "USD/CAD",
    "EUR/GBP",
    "JPY/TRY",
    "QAR/TRY",
]


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
        f"{pair}, {spot_date}, {spot_bid:.5f}, {spot_ask:.5f}, "
        f"{swap_bid:.3f}, {swap_ask:.3f}, {today_date}, "
        f"{today_bid:.7f}, {today_ask:.7f}, Streambase, {system_date}"
    )


def current_lag():
    try:
        out = subprocess.run(
            [
                "docker",
                "exec",
                CONTAINER,
                "kafka-consumer-groups",
                "--bootstrap-server",
                "kafka-kt:29093",
                "--describe",
                "--group",
                GROUP,
            ],
            capture_output=True,
            text=True,
            timeout=20,
        ).stdout
        for line in out.splitlines():
            if TOPIC in line:
                cols = line.split()
                if len(cols) >= 6:
                    return int(cols[5])
    except Exception as error:
        print(f"  (lag query error: {error})")
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=30, help="seconds to run")
    parser.add_argument("--rate", type=int, default=495, help="messages per second")
    args = parser.parse_args()

    print(f"Stress test: {args.rate} msg/s for {args.duration}s to {TOPIC}")
    print(f"Producer -> {BROKERS}, consumer group = {GROUP}")

    producer = KafkaProducer(
        bootstrap_servers=BROKERS,
        value_serializer=lambda value: value.encode("utf-8"),
        acks=1,
        linger_ms=0,
        batch_size=16384,
        max_block_ms=10000,
    )

    batch = max(1, args.rate // 2)
    chunk_interval = batch / args.rate
    start = time.time()
    sent = 0
    last_sample = start
    lag_samples = []

    print(f"{'t(s)':<8}{'sent':<10}{'lag':<8}")
    while time.time() - start < args.duration:
        for _ in range(batch):
            producer.send(TOPIC, value=generate_rate(random.choice(PAIRS)))
        sent += batch
        time.sleep(chunk_interval)

        if time.time() - last_sample >= 5:
            lag = current_lag()
            lag_samples.append((int(time.time() - start), sent, lag))
            print(
                f"{time.time() - start:<8.0f}{sent:<10}{lag if lag is not None else '?':<8}"
            )
            last_sample = time.time()

    producer.flush()
    producer.close()
    time.sleep(3)
    final_lag = current_lag()
    print(
        f"\nDone. Sent {sent} messages in {int(time.time() - start)}s "
        f"(avg {sent / (time.time() - start):.1f}/s). Final lag = {final_lag}"
    )
    print("Lag samples:", lag_samples)


if __name__ == "__main__":
    main()
