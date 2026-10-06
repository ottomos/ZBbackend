#!/usr/bin/env python3

"""
Multi-Broker Kafka Producer Simulator
Creates realistic 3-broker simulation with entity-specific connections:
- KFH (Kuwait): broker1 (localhost:9092)
- KT (Turkey): broker2 (localhost:9093)
- AUB (Bahrain): broker3 (localhost:9094)

Each entity sends to its own _sim topics with entity-specific data variations.
"""

import os
import random
import re
import threading
import time
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

KAFKA_BROKERS = [
    broker.strip()
    for broker in os.getenv(
        "KAFKA_BROKER_KFH", "localhost:9092,localhost:9093,localhost:9094"
    ).split(",")
    if broker.strip()
]


def sci_fmt(v, sig=8):
    s = f"{v:.{sig}E}"
    s = re.sub(r"E\+0*", "E", s)
    s = re.sub(r"E-0*", "E-", s)
    return s


# Entity configurations with entity-specific simulation settings
ENTITIES = {
    "KFH": {
        "country": "Kuwait",
        "timezone": "+0300",
        "position_offset": 0,
        "venues": ["KFH", "Integral", "T360T", "Tradair"],
        "primary_venue": "KFH",
    },
    "KT": {
        "country": "Turkey",
        "timezone": "+0300",
        "position_offset": -1000,
        "venues": ["KT", "Integral", "T360T", "Tradair"],
        "primary_venue": "KT",
    },
    "AUB": {
        "country": "Bahrain",
        "timezone": "+0300",
        "position_offset": 1000,
        "venues": ["AUB", "Integral", "T360T", "Tradair"],
        "primary_venue": "AUB",
    },
}

# Common data pools
symbols = [
    "EUR/USD",
    "GBP/USD",
    "USD/JPY",
    "AUD/USD",
    "USD/CHF",
    "USD/CAD",
    "NZD/USD",
    "EUR/GBP",
    "EUR/JPY",
    "GBP/JPY",
    "XAU/USD",
    "XAG/USD",
    "USD/TRY",
    "EUR/TRY",
    "GBP/TRY",
    "XPT/USD",
    "XPD/USD",
    "USD/SAR",
]

currencies = ["USD", "EUR", "GBP", "JPY", "AUD", "CHF", "CAD", "NZD", "TRY", "SAR"]
sides = ["1", "2"]  # 1=Buy, 2=Sell
strategies = ["Strategy1", "Strategy2", "Strategy3", "Strategy4"]
long_counterparties = {
    "KFH": "KFH Global Liquidity",
    "KT": "KT Interbank Flow Counterparty",
    "AUB": "AUB Treasury CrossBorder",
}

# Force zero Sales PnL for this symbol to make it visible in the UI
ZERO_PNL_SYMBOL = "USD/JPY"


class EntityProducer:
    """Handles Kafka production for a single entity"""

    def __init__(self, entity_code, config):
        self.entity_code = entity_code
        self.config = config
        self.producer = None
        self.count = 0
        self.symbolcount = 18
        self.running = False
        # Assign test email per entity
        if entity_code == "KFH":
            self.test_email = "test1@test.com"
        elif entity_code == "KT":
            self.test_email = "test2@test.com"
        elif entity_code == "AUB":
            self.test_email = "test3@test.com"
        else:
            self.test_email = "testX@test.com"

    def connect(self):
        """Establish connection to the shared Kafka cluster"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: v.encode("utf-8"),
            )
            print(
                f"✅ {self.entity_code} connected to {KAFKA_BROKERS} ({self.config['country']})"
            )
            return True
        except Exception as e:
            print(f"❌ {self.entity_code} failed to connect to {KAFKA_BROKERS}: {e}")
            return False

    def send_initial_data(self):
        """Send initial data for all symbols"""
        print(f"\n🔄 {self.entity_code}: Sending initial data for all symbols...")

        for i, symbol in enumerate(symbols):
            # Strategy data
            self._send_strategy_data(symbol)

            # Provider data
            self._send_provider_data(symbol)

            # Position data
            self._send_position_data(symbol)

            # Risk Monitor data
            self._send_risk_monitor_data(symbol)

            time.sleep(0.1)  # Small delay between symbols

    def _send_strategy_data(self, symbol):
        """Send strategy configuration data"""
        status = random.choice(["Running", "Stopped"])
        transferamount = int(random.randint(10000, 1000000))
        minhedge = int(random.randint(10000, 1000000))
        longpositionlimit = int(random.randint(100000, 1000000)) + minhedge
        shortpositionlimit = int(random.randint(-1000000, -10000)) - minhedge
        hedgeratio = round(random.uniform(0, 1), 2)
        takeprofit = round(random.uniform(0, 1), 4)
        stoploss = round(random.uniform(-1, 0), 4)
        spreadcheck = round(random.uniform(0, 1), 2)
        producer1 = "Streambase"
        systemdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        message = f"{symbol},{status},{transferamount},{longpositionlimit},{shortpositionlimit},{minhedge},{hedgeratio},{takeprofit},{stoploss},{spreadcheck},{producer1},{systemdate}"
        topic = f"PanelStrategy_{self.entity_code}"

        self.producer.send(topic, value=message)
        print(f"[{self.entity_code}] Strategy: {symbol} -> {status}")

    def _send_provider_data(self, symbol):
        """Send provider status data"""
        # Entity-specific provider statuses
        if self.entity_code == "KFH":
            kfh = "True"
            t360t = random.choice(["True", "False"])
            integral = random.choice(["True", "False"])
            tradeair = random.choice(["True", "False"])
        elif self.entity_code == "KT":
            kfh = random.choice(["True", "False"])
            t360t = "True"  # KT has strong T360T connection
            integral = random.choice(["True", "False"])
            tradeair = random.choice(["True", "False"])
        else:  # AUB
            kfh = random.choice(["True", "False"])
            t360t = random.choice(["True", "False"])
            integral = "True"  # AUB has strong Integral connection
            tradeair = random.choice(["True", "False"])

        producer1 = "Streambase"
        systemdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        message = (
            f"{symbol},{kfh},{t360t},{integral},{tradeair},{producer1},{systemdate}"
        )
        topic = f"PanelProvider_{self.entity_code}"

        self.producer.send(topic, value=message)

    def _send_position_data(self, symbol):
        """Send position data with entity-specific variations"""
        # For TRY pairs produce much larger positions and values so UI shows very high numbers
        if "TRY" in symbol:
            base_position = int(random.randint(-100000000, 100000000))
            position = base_position + self.config["position_offset"]
            positionValue = int(random.randint(-150000000, 150000000))
            averageCostRate = round(random.uniform(1, 100), 3)
            # Log TRY position output for verification
            print(
                f"[{self.entity_code}] Position (TRY): {symbol} pos={position} posValue={positionValue}"
            )
        else:
            base_position = int(random.randint(-1000000, 1000000))
            position = base_position + self.config["position_offset"]
            positionValue = int(random.randint(-1000000, 1000000))
            averageCostRate = round(random.uniform(1, 100), 3)
        unrealizedPNL = round(random.uniform(-1000, 1000), 2)
        realizedPNL = round(random.uniform(-20000, 20000), 2)
        bid = round(random.uniform(1, 100), 3)
        ask = round(random.uniform(1, 100), 3)

        # Prefer entity's primary venue
        if random.random() < 0.6:  # 60% chance of using primary venue
            bidVenue = self.config["primary_venue"]
            askVenue = self.config["primary_venue"]
        else:
            bidVenue = random.choice(self.config["venues"])
            askVenue = random.choice(self.config["venues"])

        producer1 = "Streambase"
        systemdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        # For TRY pairs format position and value in scientific notation
        if "TRY" in symbol:
            try_symbol = "USD/TRY"
            pos_str = sci_fmt(position)
            posval_str = sci_fmt(abs(positionValue))
            message = f"{try_symbol},{pos_str},{posval_str},{averageCostRate},{unrealizedPNL},{realizedPNL},{bid},{ask},{bidVenue},{askVenue},{producer1},{systemdate}"
        else:
            message = f"{symbol},{position},{positionValue},{averageCostRate},{unrealizedPNL},{realizedPNL},{bid},{ask},{bidVenue},{askVenue},{producer1},{systemdate}"
        topic = f"PanelPosition_{self.entity_code}"

        self.producer.send(topic, value=message)

    def _send_risk_monitor_data(self, symbol):
        """Send risk monitor data with entity-specific feed statuses"""
        # Entity-specific feed health patterns
        if self.entity_code == "KFH":
            kfhfeedstatus = random.choice(
                ["Healthy", "Healthy", "Disconnected"]
            )  # Usually healthy
            integralfeedstatus = random.choice(["Disconnected", "Healthy", "Closed"])
            tradairfeedstatus = random.choice(["Disconnected", "Healthy", "Closed"])
            t360tfeedstatus = random.choice(["Disconnected", "Healthy", "Closed"])
        elif self.entity_code == "KT":
            kfhfeedstatus = random.choice(["Disconnected", "Healthy", "Closed"])
            integralfeedstatus = random.choice(["Disconnected", "Healthy", "Closed"])
            tradairfeedstatus = random.choice(["Disconnected", "Healthy", "Closed"])
            t360tfeedstatus = random.choice(
                ["Healthy", "Healthy", "Disconnected"]
            )  # Usually healthy
        else:  # AUB
            kfhfeedstatus = random.choice(["Disconnected", "Healthy", "Closed"])
            integralfeedstatus = random.choice(
                ["Healthy", "Healthy", "Disconnected"]
            )  # Usually healthy
            tradairfeedstatus = random.choice(["Disconnected", "Healthy", "Closed"])
            t360tfeedstatus = random.choice(["Disconnected", "Healthy", "Closed"])

        strategystatus = random.choice(["Running", "Stopped"])
        positioncontrol = random.choice(["OK", "Above Limit"])
        lastorderstatus = random.choice(["Filled", "No Order", "Pending", "Rejected"])
        spreadcheck = random.choice(["OK", "Above Limit"])
        positionflowcheck = random.choice(["Active", "Delayed"])
        producer1 = "Streambase"
        systemdate = (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            + self.config["timezone"]
        )

        message = f"{symbol},{kfhfeedstatus},{integralfeedstatus},{tradairfeedstatus},{t360tfeedstatus},{strategystatus},{positioncontrol},{lastorderstatus},{spreadcheck},{positionflowcheck},{producer1},{systemdate}"
        topic = f"PanelRiskMonitor_{self.entity_code}"

        self.producer.send(topic, value=message)

    def _generate_transaction_price(self, symbol: str) -> float:
        decimals = 3 if str(symbol).upper().startswith("X") else 5
        value = round(random.uniform(1, 100), decimals)
        return value

    def _send_customer_transaction_data(self):
        """Send customer transaction data"""
        symbol = random.choice(symbols)
        quantity = round(random.uniform(0, 100000), 2)
        side = random.choice(["CustomerBuy", "CustomerSell"])
        tranprice = self._generate_transaction_price(symbol)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        customerid = int(random.randint(10000, 1000000))
        bidprice = round(random.uniform(1, 100), 3)
        askprice = round(random.uniform(1, 100), 3)
        # Allow forcing zero PnL for a specific symbol to aid UI testing
        if symbol == ZERO_PNL_SYMBOL:
            salespnl = 0.0
        else:
            salespnl = round(random.uniform(0, 1000), 2)
        tranid = round(random.uniform(-2000, 2000), 2)
        producer1 = "Streambase"
        systemdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        message = f"{symbol},{quantity},{side},{tranprice},{timestamp},{customerid},{bidprice},{askprice},{salespnl},{tranid},{producer1},{systemdate}"
        topic = f"PanelCustomerData_{self.entity_code}"
        self.producer.send(topic, value=message)
        print(
            f"[{self.entity_code}] CustomerData: {symbol} {side} {quantity} @ {tranprice}"
        )

    def _send_customer_internal_trade_data(self):
        """Send customer internal trade data"""
        symbol = random.choice(symbols)
        quantity = round(random.uniform(0, 100000), 2)
        side = random.choice(["CustomerBuy", "CustomerSell"])
        tranprice = self._generate_transaction_price(symbol)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        customerid = int(random.randint(10000, 1000000))
        bidprice = round(random.uniform(1, 100), 3)
        askprice = round(random.uniform(1, 100), 3)
        # Allow forcing zero PnL for a specific symbol to aid UI testing
        if symbol == ZERO_PNL_SYMBOL:
            salespnl = 0.0
        else:
            salespnl = round(random.uniform(0, 1000), 2)
        tranid = round(random.uniform(-2000, 2000), 2)
        producer1 = "Streambase"
        systemdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        message = f"{symbol},{quantity},{side},{tranprice},{timestamp},{customerid},{bidprice},{askprice},{salespnl},{tranid},{producer1},{systemdate}"
        topic = f"PanelCustomerData_{self.entity_code}_InternalTrade"
        self.producer.send(topic, value=message)
        print(
            f"[{self.entity_code}] CustomerData_InternalTrade: {symbol} {side} {quantity} @ {tranprice}"
        )

    def start_continuous_simulation(self):
        """Start continuous data generation"""
        self.running = True
        print(f"\n🚀 {self.entity_code}: Starting continuous simulation...")

        while self.running:
            self.count += 1

            # Position updates (every iteration)
            symbol = random.choice(symbols)
            self._send_position_data(symbol)

            # Execution data (every 2nd iteration)
            if self.count % 2 == 0:
                self._send_execution_data()

            # Risk monitor updates (every iteration)
            symbol = random.choice(symbols)
            self._send_risk_monitor_data(symbol)

            # Strategy updates (every 4th iteration)
            if self.count % 4 == 0:
                self.symbolcount += 1
                symbol_index = self.symbolcount % len(symbols)
                symbol = symbols[symbol_index]
                self._send_strategy_data(symbol)

            # Customer transaction data (every iteration)
            self._send_customer_transaction_data()
            # Customer internal trade data (every iteration)
            self._send_customer_internal_trade_data()
            # Summary data (every 5th iteration)
            if self.count % 5 == 0:
                self._send_summary_data()

            time.sleep(1)  # 1 second between cycles

    def _send_execution_data(self):
        """Send execution result data"""
        symbol = random.choice(symbols)
        quantity = int(random.randint(10000, 1000000))
        price = round(random.uniform(1, 100), 3)
        side = random.choice(sides)
        venue = random.choice(self.config["venues"])
        counterparty = (
            long_counterparties.get(
                self.entity_code, random.choice(self.config["venues"])
            )
            if random.random() < 0.35
            else random.choice(self.config["venues"])
        )
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        strategy = random.choice(strategies)
        realizedpnl = round(random.uniform(-2000, 2000), 2)
        producer1 = "Streambase"
        systemdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        email = self.test_email
        message = f"{symbol},{quantity},{price},{side},{venue},{counterparty},{timestamp},{strategy},{realizedpnl},{producer1},{systemdate},{email}"
        topic = f"PanelExecutionResult_{self.entity_code}"
        self.producer.send(topic, value=message)
        print(
            f"[{self.entity_code}] 💰 EXECUTION: {symbol} {side} {quantity:,} @ {price} | {email}"
        )

    def _send_summary_data(self):
        """Send summary data for various panels"""
        symbol = random.choice(symbols)

        # Execution Summary
        total_buy = int(random.randint(1000000, 10000000))
        total_sell = int(random.randint(1000000, 10000000))
        total_amount = total_buy + total_sell
        net_amount = total_buy - total_sell

        message = f"{symbol},{total_amount},{total_buy},{total_sell},{net_amount}"
        topic = f"PanelExecutionSummary_{self.entity_code}"
        self.producer.send(topic, value=message)

        # Position PNL Summary
        currency = random.choice(currencies)
        exposure = random.randint(-100000, 100000)
        AvgRate = random.randint(10000, 30000) / 10000
        USDEquivalent = exposure / AvgRate
        UnrealizedPNL = random.randint(-100, 1000)
        SpreadPNL = random.randint(-500, 20000)
        MatchingPNL = random.randint(10000, 20000)
        PositionPNL = random.randint(-500, 20000)
        TotalPNL = SpreadPNL + MatchingPNL + PositionPNL

        message = f"{currency},{exposure},{USDEquivalent},{AvgRate},{UnrealizedPNL},{SpreadPNL},{MatchingPNL},{PositionPNL},{TotalPNL}"
        topic = f"PanelPositionPNLSummary_{self.entity_code}"
        self.producer.send(topic, value=message)

        # Currency Flow Summary
        currency = symbol[-3:] if "/" in symbol else random.choice(currencies)
        Sum_Buy = random.randint(500000, 10000000)
        Sum_Sell = random.randint(500000, 10000000)
        Net = Sum_Buy - Sum_Sell

        message = f"{currency},{Sum_Buy},{Sum_Sell},{Net}"
        topic = f"PanelCurrencyFlowSummary_{self.entity_code}"
        self.producer.send(topic, value=message)

    def stop(self):
        """Stop the simulation"""
        self.running = False
        if self.producer:
            self.producer.flush()
            self.producer.close()
        print(f"🛑 {self.entity_code}: Simulation stopped")


def main():
    """Main simulation controller"""
    print("🌍 Multi-Broker Kafka Simulator Starting...")
    print("=" * 60)

    # Create entity producers
    producers = {}
    threads = {}

    # Connect all entities
    for entity_code, config in ENTITIES.items():
        producer = EntityProducer(entity_code, config)
        if producer.connect():
            producers[entity_code] = producer
        else:
            print(f"⚠️  Failed to connect {entity_code}, skipping...")

    if not producers:
        print("❌ No entities connected. Exiting...")
        return

    print(f"\n✅ Connected to {len(producers)} entities")

    # Send initial data for all connected entities
    print("\n📊 Sending initial data...")
    for entity_code, producer in producers.items():
        producer.send_initial_data()

    print("\n⏱️  Initial data sent, starting continuous simulation in 3 seconds...")
    time.sleep(3)

    # Start continuous simulation threads
    try:
        for entity_code, producer in producers.items():
            thread = threading.Thread(
                target=producer.start_continuous_simulation,
                name=f"{entity_code}_simulator",
            )
            thread.daemon = True
            thread.start()
            threads[entity_code] = thread

        print(f"\n🔥 All {len(threads)} entity simulators running!")
        print("Press Ctrl+C to stop simulation...")

        # Keep main thread alive
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n\n🛑 Stopping all simulations...")

        # Stop all producers
        for producer in producers.values():
            producer.stop()

        # Wait for threads to finish
        for thread in threads.values():
            thread.join(timeout=2)

        print("✅ All simulators stopped")


if __name__ == "__main__":
    main()
