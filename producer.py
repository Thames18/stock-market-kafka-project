import logging
from kafka import KafkaProducer
from time import sleep
from json import dumps
import pandas as pd

# Configuration
KAFKA_TOPIC = 'demo_test'
KAFKA_BOOTSTRAP_SERVERS = ['<Your Public IP>:9092']  # Use your public IPV4
CSV_FILE = 'stockData.csv'
SLEEP_INTERVAL = 0.1  # in seconds

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )
        logging.info("Kafka producer initialized successfully.")
        return producer
    except Exception as e:
        logging.error(f"Failed to initialize Kafka producer: {e}")
        return None


def load_csv_data(filepath):
    try:
        df = pd.read_csv(filepath)
        if df.empty:
            raise ValueError("CSV file is empty.")
        logging.info(f"CSV loaded successfully with {len(df)} records.")
        return df
    except Exception as e:
        logging.error(f"Error loading CSV file: {e}")
        return None


def send_data_loop(producer, df):
    try:
        while True:
            sample_data = df.sample(1).to_dict(orient='records')[0]
            producer.send(KAFKA_TOPIC, value=sample_data)
            logging.info(f"Sent: {sample_data}")
            sleep(SLEEP_INTERVAL)
    except KeyboardInterrupt:
        logging.info("Interrupted by user. Shutting down...")
    except Exception as e:
        logging.error(f"Error while sending data to Kafka: {e}")


def main():
    producer = create_kafka_producer()
    df = load_csv_data(CSV_FILE)

    if not producer or df is None:
        logging.error("Producer or data not available. Exiting...")
        return

    send_data_loop(producer, df)

    try:
        producer.flush()
        logging.info("Kafka producer flushed successfully.")
    except Exception as e:
        logging.error(f"Error flushing producer: {e}")


if __name__ == '__main__':
    main()
