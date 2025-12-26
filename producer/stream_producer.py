import time
import json
import pickle
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime

# Configuration
KAFKA_BROKER = 'kafka:9092' 
TOPIC = 'flight_events'

# Load the trained model
print("Loading Statistical Model...")
with open('model.pkl', 'rb') as f:
    model = pickle.load(f)

# Setup Kafka - Wait for Kafka to be ready
print("Waiting for Kafka to be ready...")
producer = None
max_retries = 30
retry_count = 0

while producer is None and retry_count < max_retries:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            api_version=(0, 10, 1),
            retries=5,
            request_timeout_ms=10000
        )
        print("Kafka connection established!")
        break
    except NoBrokersAvailable:
        retry_count += 1
        print(f"Waiting for Kafka... (attempt {retry_count}/{max_retries})")
        if retry_count < max_retries:
            time.sleep(2)
        else:
            print("Failed to connect to Kafka after maximum retries")
            raise
    except Exception as e:
        # Other errors - might mean Kafka is up but something else is wrong
        print(f"Unexpected error connecting to Kafka: {e}")
        retry_count += 1
        if retry_count < max_retries:
            time.sleep(2)
        else:
            raise

if producer is None:
    raise RuntimeError("Failed to create Kafka producer")

print("Starting Stream...")

while True:
    try:
        # Generate 1 batch of flights (5 flights)
        synthetic_data = model.sample(num_rows=5)
        
        # Convert to list of dicts
        records = synthetic_data.to_dict(orient='records')
        
        for record in records:
            # Add real-time timestamp (The model generates Month/Day, but we need exact time)
            record['timestamp'] = datetime.now().isoformat()
            # Send to Kafka
            producer.send(TOPIC, value=record)
            print(f"Sent: {record['AIRLINE_CODE']} flight from {record['ORIGIN']} to {record['DEST']}")

        producer.flush()
        
        # Sleep to simulate real-time (1 batch every 2 seconds)
        time.sleep(2)
        
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(5) # Wait 5 seconds before retrying if Kafka isn't ready
