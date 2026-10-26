import os
import sys
import time
import json
import logging
import joblib
import numpy as np
import pandas as pd
import psycopg2
from datetime import datetime
try:
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable, KafkaError
except (ImportError, AttributeError):
    try:
        from kafka import KafkaConsumer
    except Exception:
        pass
    class NoBrokersAvailable(Exception):
        pass
    class KafkaError(Exception):
        pass

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092,localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "financial-transactions")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "fraud-ml-inference-engine")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "fraud_detection")
POSTGRES_USER = os.getenv("POSTGRES_USER", "fraud_admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "fraud_password")

MODEL_PATH = os.getenv("MODEL_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "fraud_model.pkl"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLConsumerEngine")

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


class DatabaseManager:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self._connect()
        self._initialize_schema()

    def _connect(self, max_retries: int = 10, retry_delay: int = 3):
        ports_to_try = [self.port, 5433, 5432]
        ports_to_try = list(dict.fromkeys(ports_to_try))
        
        for attempt in range(1, max_retries + 1):
            for port in ports_to_try:
                try:
                    logger.info(f"Probing PostgreSQL at {self.host}:{port}/{self.database} (Attempt {attempt}/{max_retries})...")
                    self.connection = psycopg2.connect(
                        host=self.host,
                        port=port,
                        dbname=self.database,
                        user=self.user,
                        password=self.password,
                        connect_timeout=3,
                    )
                    self.connection.autocommit = True
                    self.port = port
                    logger.info(f"PostgreSQL connection established on port {port}.")
                    return
                except psycopg2.OperationalError as e:
                    last_err = e
                    continue
                except Exception as e:
                    last_err = e
                    continue
            logger.warning(f"Database not ready on tested ports {ports_to_try}: {last_err}. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)
        
        logger.critical("Could not connect to PostgreSQL after multiple attempts. Terminating.")
        sys.exit(1)

    def ensure_connection(self):
        if self.connection is None or self.connection.closed:
            self._connect()
        else:
            try:
                with self.connection.cursor() as cur:
                    cur.execute("SELECT 1;")
            except (psycopg2.OperationalError, psycopg2.DatabaseError):
                logger.warning("Database connection dropped. Re-establishing connection...")
                self._connect()

    def _initialize_schema(self):
        self.ensure_connection()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS transactions_log (
            id SERIAL PRIMARY KEY,
            transaction_id VARCHAR(64) UNIQUE NOT NULL,
            card_id VARCHAR(64) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            amount NUMERIC(10, 2) NOT NULL,
            merchant_category VARCHAR(64) NOT NULL,
            location VARCHAR(128),
            distance_from_home NUMERIC(8, 2) NOT NULL,
            velocity_flag INTEGER NOT NULL,
            foreign_transaction INTEGER NOT NULL,
            device_risk_score NUMERIC(4, 2) NOT NULL,
            fraud_prediction INTEGER NOT NULL,
            risk_score NUMERIC(6, 4) NOT NULL,
            processing_time_ms NUMERIC(8, 2) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_txn_timestamp ON transactions_log(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_txn_fraud ON transactions_log(fraud_prediction);
        CREATE INDEX IF NOT EXISTS idx_txn_card_id ON transactions_log(card_id);
        """
        try:
            with self.connection.cursor() as cur:
                cur.execute(create_table_query)
            logger.info("Table 'transactions_log' and associated indexes verified.")
        except Exception as e:
            logger.error(f"Error initializing schema: {e}")
            raise

    def insert_transaction(self, record: dict):
        self.ensure_connection()
        insert_query = """
        INSERT INTO transactions_log (
            transaction_id, card_id, timestamp, amount, merchant_category,
            location, distance_from_home, velocity_flag, foreign_transaction,
            device_risk_score, fraud_prediction, risk_score, processing_time_ms
        ) VALUES (
            %(transaction_id)s, %(card_id)s, %(timestamp)s, %(amount)s, %(merchant_category)s,
            %(location)s, %(distance_from_home)s, %(velocity_flag)s, %(foreign_transaction)s,
            %(device_risk_score)s, %(fraud_prediction)s, %(risk_score)s, %(processing_time_ms)s
        )
        ON CONFLICT (transaction_id) DO NOTHING;
        """
        try:
            with self.connection.cursor() as cur:
                cur.execute(insert_query, record)
        except Exception as e:
            logger.error(f"Database insert error for transaction {record.get('transaction_id')}: {e}")
            self.connection.rollback()


class MLFraudEngine:
    def __init__(self, model_file: str):
        self.model_file = model_file
        self.model = None
        self.feature_names = []
        self.category_risk_map = {}
        self.threshold = 0.60
        self._load_model()

    def _load_model(self):
        if not os.path.exists(self.model_file):
            logger.critical(
                f"Model file '{self.model_file}' not found! "
                f"Please run 'python model_trainer.py' first to train and serialize the model."
            )
            sys.exit(1)

        try:
            logger.info(f"Loading ML model artifact from '{self.model_file}'...")
            artifact = joblib.load(self.model_file)
            
            if isinstance(artifact, dict) and "model" in artifact:
                self.model = artifact["model"]
                self.feature_names = artifact.get("feature_names", [])
                self.category_risk_map = artifact.get("category_risk_map", {})
                self.threshold = artifact.get("threshold", 0.60)
            else:
                self.model = artifact
                self.feature_names = [
                    "amount", "hour_of_day", "category_risk", "distance_from_home",
                    "velocity_flag", "foreign_transaction", "device_risk_score"
                ]
            logger.info(f"Model successfully loaded. Inference threshold set to {self.threshold:.2f}.")
        except Exception as e:
            logger.critical(f"Failed to load model artifact: {e}", exc_info=True)
            sys.exit(1)

    def extract_features(self, payload: dict) -> pd.DataFrame:
        ts_str = payload.get("timestamp", "")
        try:
            hour_of_day = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").hour
        except Exception:
            hour_of_day = datetime.utcnow().hour

        merchant_cat = payload.get("merchant_category", "grocery")
        cat_risk = self.category_risk_map.get(merchant_cat, 0.10)

        feature_dict = {
            "amount": float(payload.get("amount", 0.0)),
            "hour_of_day": int(hour_of_day),
            "category_risk": float(cat_risk),
            "distance_from_home": float(payload.get("distance_from_home", 0.0)),
            "velocity_flag": int(payload.get("velocity_flag", 0)),
            "foreign_transaction": int(payload.get("foreign_transaction", 0)),
            "device_risk_score": float(payload.get("device_risk_score", 0.0)),
        }

        return pd.DataFrame([feature_dict])[self.feature_names]

    def predict(self, payload: dict) -> tuple[int, float]:
        features_df = self.extract_features(payload)
        risk_proba = float(self.model.predict_proba(features_df)[0, 1])
        prediction = 1 if risk_proba >= self.threshold else 0
        return prediction, risk_proba


def create_kafka_consumer(max_retries: int = 10, retry_delay: int = 3) -> KafkaConsumer:
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Subscribing to topic '{KAFKA_TOPIC}' on '{KAFKA_BOOTSTRAP_SERVERS}' (Attempt {attempt}/{max_retries})...")
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.info("Kafka consumer initialized successfully.")
            return consumer
        except NoBrokersAvailable:
            logger.warning(f"Kafka broker not available. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)
        except Exception as e:
            logger.error(f"Error connecting to Kafka: {e}. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)
            
    logger.critical("Could not connect Kafka consumer. Exiting.")
    sys.exit(1)


def start_engine():
    db_manager = DatabaseManager(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    
    ml_engine = MLFraudEngine(MODEL_PATH)
    consumer = create_kafka_consumer()

    print("\n" + "=" * 90)
    print(f"{BOLD}{CYAN} 🛡️ REAL-TIME FRAUD DETECTION ML CONSUMER ENGINE ACTIVE{RESET}")
    print(f" Listening on: {KAFKA_TOPIC} | Persisting to: PostgreSQL ({POSTGRES_DB})")
    print("=" * 90 + "\n")

    total_processed = 0
    total_fraud = 0

    try:
        for message in consumer:
            start_time = time.perf_counter()
            tx_payload = message.value

            pred_label, risk_score = ml_engine.predict(tx_payload)
            latency_ms = round((time.perf_counter() - start_time) * 1000, 2)

            record = {
                **tx_payload,
                "fraud_prediction": pred_label,
                "risk_score": round(risk_score, 4),
                "processing_time_ms": latency_ms,
            }

            db_manager.insert_transaction(record)

            total_processed += 1
            if pred_label == 1:
                total_fraud += 1

            fraud_rate = (total_fraud / total_processed) * 100
            if pred_label == 1:
                status_tag = f"{RED}{BOLD}[🚨 FRAUD DETECTED]{RESET}"
                risk_tag = f"{RED}Risk: {risk_score * 100:>5.1f}%{RESET}"
            else:
                status_tag = f"{GREEN}[✓ LEGITIMATE]{RESET}"
                risk_tag = f"{GREEN}Risk: {risk_score * 100:>5.1f}%{RESET}"

            print(
                f"{status_tag} {record['transaction_id']} | "
                f"Card: {record['card_id']} | ${record['amount']:>8.2f} | "
                f"{record['merchant_category']:<16} | {risk_tag} | "
                f"Latency: {latency_ms:>5.2f}ms | Stats: [{total_fraud}/{total_processed} Fraud: {fraud_rate:.1f}%]"
            )

    except KeyboardInterrupt:
        logger.info("\nConsumer engine shutdown requested by user.")
    except Exception as e:
        logger.error(f"Fatal error in consumer engine: {e}", exc_info=True)
    finally:
        consumer.close()
        if db_manager.connection:
            db_manager.connection.close()
        logger.info("ML Consumer Engine safely stopped.")


if __name__ == "__main__":
    start_engine()
