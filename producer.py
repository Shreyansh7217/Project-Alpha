import os
import sys
import time
import json
import uuid
import random
import logging
import argparse
import math
from datetime import datetime, timezone, timedelta
from faker import Faker

try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable, KafkaError
except (ImportError, AttributeError):
    try:
        from kafka import KafkaProducer
    except Exception:
        pass
    class NoBrokersAvailable(Exception):
        pass
    class KafkaError(Exception):
        pass

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092,localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "financial-transactions")
DEFAULT_STREAM_DELAY = float(os.getenv("STREAM_DELAY_SECONDS", "1.0"))
CONTROL_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "simulation_control.json")

IST_TZ = timezone(timedelta(hours=5, minutes=30))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("IndianKafkaProducer")
fake = Faker("en_IN")

INDIAN_MERCHANTS = [
    {"merchant": "Zomato Food Delivery", "category": "dining", "min_inr": 120.0, "max_inr": 1400.0, "weight": 0.22, "is_pos": False},
    {"merchant": "Swiggy Instamart", "category": "dining", "min_inr": 150.0, "max_inr": 1800.0, "weight": 0.18, "is_pos": False},
    {"merchant": "Blinkit Quick Grocery", "category": "grocery", "min_inr": 90.0, "max_inr": 1500.0, "weight": 0.14, "is_pos": False},
    {"merchant": "Zepto 10-Min Delivery", "category": "grocery", "min_inr": 80.0, "max_inr": 1200.0, "weight": 0.10, "is_pos": False},
    {"merchant": "Amazon India", "category": "clothing", "min_inr": 350.0, "max_inr": 8500.0, "weight": 0.08, "is_pos": False},
    {"merchant": "Flipkart SuperMall", "category": "clothing", "min_inr": 450.0, "max_inr": 9500.0, "weight": 0.07, "is_pos": False},
    {"merchant": "Myntra Fashion", "category": "clothing", "min_inr": 600.0, "max_inr": 6500.0, "weight": 0.05, "is_pos": False},
    {"merchant": "JioMart Supermarket", "category": "grocery", "min_inr": 300.0, "max_inr": 3800.0, "weight": 0.04, "is_pos": False},
    {"merchant": "Apollo Pharmacy 24/7", "category": "health_beauty", "min_inr": 150.0, "max_inr": 2800.0, "weight": 0.04, "is_pos": True},
    {"merchant": "BookMyShow Movies", "category": "entertainment", "min_inr": 250.0, "max_inr": 1600.0, "weight": 0.03, "is_pos": False},
    {"merchant": "Petrol Pump IOCL", "category": "transport", "min_inr": 300.0, "max_inr": 4500.0, "weight": 0.04, "is_pos": True},
    {"merchant": "HPCL Fuel Station", "category": "transport", "min_inr": 250.0, "max_inr": 4200.0, "weight": 0.03, "is_pos": True},
    {"merchant": "IRCTC Railways Booking", "category": "transport", "min_inr": 250.0, "max_inr": 4800.0, "weight": 0.02, "is_pos": False},
    {"merchant": "IndiGo Airlines", "category": "travel_flight", "min_inr": 3200.0, "max_inr": 28000.0, "weight": 0.01, "is_pos": False},
    {"merchant": "MakeMyTrip Holidays", "category": "travel_flight", "min_inr": 4500.0, "max_inr": 45000.0, "weight": 0.008, "is_pos": False},
    {"merchant": "Croma Electronics Store", "category": "electronics", "min_inr": 1800.0, "max_inr": 95000.0, "weight": 0.01, "is_pos": True},
    {"merchant": "Reliance Digital Tech", "category": "electronics", "min_inr": 2200.0, "max_inr": 120000.0, "weight": 0.008, "is_pos": True},
    {"merchant": "Tanishq Jewellers", "category": "luxury_goods", "min_inr": 15000.0, "max_inr": 250000.0, "weight": 0.004, "is_pos": True},
    {"merchant": "Kalyan Jewellers", "category": "luxury_goods", "min_inr": 12000.0, "max_inr": 220000.0, "weight": 0.003, "is_pos": True},
    {"merchant": "CoinDCX Crypto Portal", "category": "crypto_exchange", "min_inr": 5000.0, "max_inr": 180000.0, "weight": 0.003, "is_pos": False},
    {"merchant": "WazirX Digital Asset", "category": "crypto_exchange", "min_inr": 4000.0, "max_inr": 160000.0, "weight": 0.002, "is_pos": False},
    {"merchant": "UPI P2P Merchant Hub", "category": "utilities", "min_inr": 50.0, "max_inr": 25000.0, "weight": 0.04, "is_pos": False},
]

MERCHANT_WEIGHTS = [m["weight"] for m in INDIAN_MERCHANTS]

INDIAN_CITIES = {
    "Connaught Place, New Delhi": {"lat": 28.6315, "lon": 77.2167, "state": "Delhi"},
    "South Mumbai, Mumbai": {"lat": 18.9220, "lon": 72.8347, "state": "Maharashtra"},
    "Indiranagar, Bengaluru": {"lat": 12.9784, "lon": 77.6408, "state": "Karnataka"},
    "T Nagar, Chennai": {"lat": 13.0418, "lon": 80.2341, "state": "Tamil Nadu"},
    "Banjara Hills, Hyderabad": {"lat": 17.4156, "lon": 78.4357, "state": "Telangana"},
    "Salt Lake, Kolkata": {"lat": 22.5867, "lon": 88.4178, "state": "West Bengal"},
    "Koregaon Park, Pune": {"lat": 18.5362, "lon": 73.8940, "state": "Maharashtra"},
    "C.G. Road, Ahmedabad": {"lat": 23.0338, "lon": 72.5606, "state": "Gujarat"},
    "Civil Lines, Jaipur": {"lat": 26.9048, "lon": 75.7873, "state": "Rajasthan"},
    "Sector 17, Chandigarh": {"lat": 30.7398, "lon": 76.7827, "state": "Punjab/Haryana"},
}

DISTANT_FRAUD_DESTINATIONS = {
    "Madurai Meenakshi, Tamil Nadu": {"lat": 9.9195, "lon": 78.1193},
    "Guwahati Paltan Bazar, Assam": {"lat": 26.1824, "lon": 91.7519},
    "Dimapur Commercial, Nagaland": {"lat": 25.9094, "lon": 93.7266},
    "Port Blair Marina, Andaman": {"lat": 11.6234, "lon": 92.7265},
    "International Dubai Mall, UAE": {"lat": 25.1972, "lon": 55.2744},
    "International London Oxford St, UK": {"lat": 51.5154, "lon": -0.1419},
}

CITY_NAMES = list(INDIAN_CITIES.keys())

CARD_POOL = [f"4532-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}" for _ in range(400)]
CARD_PROFILES = {}

for card in CARD_POOL:
    home_city = random.choice(CITY_NAMES)
    CARD_PROFILES[card] = {
        "home_city": home_city,
        "home_coords": INDIAN_CITIES[home_city],
        "historical_avg_inr": random.choice([350.0, 500.0, 850.0, 1200.0]),
        "last_swipe_time": None,
        "last_location": home_city,
    }


def calculate_haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return round(R * c, 2)


def get_simulation_config() -> dict:
    config = {
        "attack_mode": os.getenv("ATTACK_MODE", "0") == "1",
        "attack_type": "velocity_burst",
        "stream_delay": DEFAULT_STREAM_DELAY,
    }
    if os.path.exists(CONTROL_FILE):
        try:
            with open(CONTROL_FILE, "r") as f:
                saved = json.load(f)
                config.update(saved)
        except Exception:
            pass
    return config


def create_kafka_producer(max_retries: int = 10, retry_delay: int = 3) -> KafkaProducer:
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Connecting to Kafka broker at '{KAFKA_BOOTSTRAP_SERVERS}' (Attempt {attempt}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
            )
            logger.info("Successfully connected to Kafka cluster.")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka broker not ready. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)
        except Exception as e:
            logger.error(f"Error connecting to Kafka: {e}. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)
    
    logger.critical("Failed to connect to Kafka. Exiting.")
    sys.exit(1)


def generate_indian_transaction(
    card_id: str = None,
    fraud_scenario: str = None,
) -> dict:
    if not card_id:
        card_id = random.choice(CARD_POOL)
    
    profile = CARD_PROFILES[card_id]
    home_city = profile["home_city"]
    home_lat = profile["home_coords"]["lat"]
    home_lon = profile["home_coords"]["lon"]

    now_ist = datetime.now(IST_TZ)

    if fraud_scenario == "impossible_travel":
        dest_name, dest_data = random.choice(list(DISTANT_FRAUD_DESTINATIONS.items()))
        distance_km = calculate_haversine_distance(home_lat, home_lon, dest_data["lat"], dest_data["lon"])
        
        merchant_item = random.choice([
            {"merchant": "Croma Electronics Store", "category": "electronics", "min_inr": 45000.0, "max_inr": 140000.0},
            {"merchant": "Tanishq Jewellers", "category": "luxury_goods", "min_inr": 65000.0, "max_inr": 220000.0},
        ])
        amount = round(random.uniform(merchant_item["min_inr"], merchant_item["max_inr"]), 2)
        location = dest_name
        is_foreign = 1 if "International" in dest_name else 0
        velocity_flag = 0
        device_risk = round(random.uniform(0.85, 0.99), 2)
        merchant_brand = merchant_item["merchant"]
        merchant_category = merchant_item["category"]

    elif fraud_scenario == "velocity_burst":
        aggregator = random.choice([
            {"merchant": "Flipkart SuperMall", "category": "electronics", "min_inr": 18000.0, "max_inr": 75000.0},
            {"merchant": "Croma Electronics Store", "category": "electronics", "min_inr": 25000.0, "max_inr": 115000.0},
            {"merchant": "Tanishq Jewellers", "category": "luxury_goods", "min_inr": 45000.0, "max_inr": 185000.0},
            {"merchant": "CoinDCX Crypto Portal", "category": "crypto_exchange", "min_inr": 35000.0, "max_inr": 150000.0},
        ])
        amount = round(random.uniform(aggregator["min_inr"], aggregator["max_inr"]), 2)
        location = home_city
        distance_km = round(random.uniform(0.5, 12.0), 2)
        is_foreign = 0
        velocity_flag = 1
        device_risk = round(random.uniform(0.78, 0.98), 2)
        merchant_brand = aggregator["merchant"]
        merchant_category = aggregator["category"]

    elif fraud_scenario == "night_amount_outlier":
        merchant_item = random.choice([
            {"merchant": "Tanishq Jewellers Flagship", "category": "luxury_goods", "min_inr": 85000.0, "max_inr": 248000.0},
            {"merchant": "CoinDCX Instant Crypto", "category": "crypto_exchange", "min_inr": 60000.0, "max_inr": 195000.0},
        ])
        amount = round(random.uniform(merchant_item["min_inr"], merchant_item["max_inr"]), 2)
        location = home_city
        distance_km = round(random.uniform(1.0, 25.0), 2)
        is_foreign = 0
        velocity_flag = 0
        device_risk = round(random.uniform(0.88, 0.99), 2)
        merchant_brand = merchant_item["merchant"]
        merchant_category = merchant_item["category"]
        now_ist = now_ist.replace(hour=3, minute=random.randint(15, 45))

    else:
        merchant_info = random.choices(INDIAN_MERCHANTS, weights=MERCHANT_WEIGHTS, k=1)[0]
        amount = round(random.uniform(merchant_info["min_inr"], merchant_info["max_inr"]), 2)
        merchant_brand = merchant_info["merchant"]
        merchant_category = merchant_info["category"]

        if random.random() < 0.05:
            travel_city = random.choice([c for c in CITY_NAMES if c != home_city])
            location = travel_city
            dest_coords = INDIAN_CITIES[travel_city]
            distance_km = calculate_haversine_distance(home_lat, home_lon, dest_coords["lat"], dest_coords["lon"])
            is_foreign = 0
        else:
            location = home_city
            distance_km = round(random.uniform(0.2, 14.0), 2)
            is_foreign = 0

        velocity_flag = 0
        device_risk = round(random.betavariate(1.2, 9.5), 2)

    payload = {
        "transaction_id": f"TXN-IN-{uuid.uuid4().hex[:10].upper()}",
        "card_id": card_id,
        "timestamp": now_ist.strftime("%Y-%m-%d %H:%M:%S"),
        "amount": amount,
        "currency": "INR",
        "merchant_brand": merchant_brand,
        "merchant_category": merchant_category,
        "location": location,
        "distance_from_home": distance_km,
        "velocity_flag": velocity_flag,
        "foreign_transaction": is_foreign,
        "device_risk_score": device_risk,
    }

    profile["last_location"] = location
    profile["last_swipe_time"] = now_ist
    return payload


def stream_transactions(cli_storm: bool = False, cli_scenario: str = None):
    producer = create_kafka_producer()
    logger.info(f"Starting Indian Financial Telemetry Stream to topic '{KAFKA_TOPIC}'...")
    print("\n" + "=" * 90)
    print(" 🇮🇳 INDIAN FINANCIAL STREAMING TELEMETRY ENGINE RUNNING (₹ / INR)")
    print(f" Target Topic: {KAFKA_TOPIC} | Control File: {CONTROL_FILE}")
    print("=" * 90 + "\n")

    tx_count = 0
    try:
        while True:
            config = get_simulation_config()
            attack_active = cli_storm or config.get("attack_mode", False)
            scenario = cli_scenario or config.get("attack_type", "velocity_burst")
            stream_delay = float(config.get("stream_delay", DEFAULT_STREAM_DELAY))

            if attack_active:
                attack_card = random.choice(CARD_POOL)
                burst_size = random.randint(4, 6) if scenario == "velocity_burst" else random.randint(2, 3)
                
                print(f"\n⚡ [🔥 SIMULATED FRAUD STORM ACTIVE] Injecting {burst_size} '{scenario}' attacks on {attack_card} ({CARD_PROFILES[attack_card]['home_city']})")
                
                for _ in range(burst_size):
                    tx = generate_indian_transaction(card_id=attack_card, fraud_scenario=scenario)
                    producer.send(KAFKA_TOPIC, key=tx["card_id"], value=tx)
                    tx_count += 1
                    print(
                        f"  🚨 [{tx['timestamp']}] #{tx_count:05d} | {tx['transaction_id']} | "
                        f"Card: {tx['card_id']} | ₹{tx['amount']:>10,.2f} | "
                        f"{tx['merchant_brand']:<26} | Loc: {tx['location']:<28} | "
                        f"Dist: {tx['distance_from_home']:>6.1f}km | Velocity: {tx['velocity_flag']} | DevRisk: {tx['device_risk_score']:.2f}"
                    )
                    time.sleep(0.12)
                producer.flush()
            else:
                tx = generate_indian_transaction()
                producer.send(KAFKA_TOPIC, key=tx["card_id"], value=tx)
                producer.flush()
                tx_count += 1
                
                print(
                    f"  💳 [{tx['timestamp']}] #{tx_count:05d} | {tx['transaction_id']} | "
                    f"Card: {tx['card_id']} | ₹{tx['amount']:>10,.2f} | "
                    f"{tx['merchant_brand']:<26} | Loc: {tx['location']:<28} | "
                    f"Dist: {tx['distance_from_home']:>6.1f}km | DevRisk: {tx['device_risk_score']:.2f}"
                )

            time.sleep(stream_delay)

    except KeyboardInterrupt:
        logger.info("\nStreaming interrupted by user. Closing Kafka producer...")
    except Exception as e:
        logger.error(f"Streaming error: {e}", exc_info=True)
    finally:
        producer.close()
        logger.info("Kafka Producer cleanly stopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Indian Financial Kafka Stream Producer")
    parser.add_argument("--storm", action="store_true", help="Launch continuous simulated fraud storm")
    parser.add_argument("--scenario", choices=["impossible_travel", "velocity_burst", "night_amount_outlier"], help="Specific fraud vector scenario to inject")
    parser.add_argument("--delay", type=float, default=1.0, help="Stream interval in seconds (default: 1.0)")
    args = parser.parse_args()

    if args.delay:
        DEFAULT_STREAM_DELAY = args.delay

    stream_transactions(cli_storm=args.storm, cli_scenario=args.scenario)
