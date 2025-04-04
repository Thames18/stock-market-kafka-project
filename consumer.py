import logging
from cassandra.cluster import Cluster
from kafka import KafkaConsumer
from json import loads, dumps

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_TOPIC = 'demo_test'
KAFKA_BOOTSTRAP_SERVERS = ['<Your Public IP>:9092']  # Use your public IPV4
CASSANDRA_CONTACT_POINTS = ['<Your Public IP>']      # Use your public IPV4

def create_cassandra_session():
    try:
        cluster = Cluster(CASSANDRA_CONTACT_POINTS)
        session = cluster.connect()

        # Create keyspace and table if not exists
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS stockmarket 
            WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
        """)
        session.set_keyspace("stockmarket")

        session.execute("""
            CREATE TABLE IF NOT EXISTS stock_market_data (
                id int PRIMARY KEY,
                "index" text,
                date text,
                open float,
                high float,
                low float,
                close float,
                "adj close" float,
                volume bigint,
                closeUSD float
            );
        """)
        logging.info("Cassandra keyspace and table are ready.")
        return session
    except Exception as e:
        logging.error(f"Error initializing Cassandra: {e}")
        return None

def create_kafka_consumer():
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: loads(x.decode('utf-8'))
        )
        logging.info("Kafka consumer initialized.")
        return consumer
    except Exception as e:
        logging.error(f"Error initializing Kafka consumer: {e}")
        return None

def main():
    session = create_cassandra_session()
    consumer = create_kafka_consumer()

    if not session or not consumer:
        logging.error("Could not initialize either Kafka or Cassandra. Exiting.")
        return

    message_id = 0
    for message in consumer:
        if message.value:
            try:
                message_id += 1
                new_data = {'id': message_id}
                new_data.update(message.value)
                json_data = dumps(new_data).replace("'", "''")  # Escape single quotes for CQL

                session.execute(f"INSERT INTO stock_market_data JSON '{json_data}';")
                logging.info(f"Inserted record ID {message_id}")
            except Exception as e:
                logging.error(f"Error inserting data into Cassandra: {e}")

if __name__ == '__main__':
    main()
