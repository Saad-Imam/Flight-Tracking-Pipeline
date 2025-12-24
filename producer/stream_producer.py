import time
import json
import pickle
import random
from kafka import KafkaProducer
from datetime import datetime

# Configuration
KAFKA_BROKER = 'kafka:9092' # Matches service name in docker-compose
TOPIC = 'flight_events'

# 1. Load the trained model
print("Loading Statistical Model...")
with open('model.pkl', 'rb') as f:
    model = pickle.load(f)

# 2. Setup Kafka
# retries=5 helps if the script starts before Kafka is fully ready
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    retries=5 
)

print("Starting Stream...")

while True:
    try:
        # 3. Generate 1 batch of flights (e.g., 5 flights)
        synthetic_data = model.sample(num_rows=5)
        
        # 4. Convert to list of dicts
        records = synthetic_data.to_dict(orient='records')
        
        for record in records:
            # Add real-time timestamp (The model generates Month/Day, but we need exact time)
            record['timestamp'] = datetime.now().isoformat()
            
            # Note: The model might generate 'Month' and 'DayOfWeek' as columns.
            # We can keep them or let Spark handle the date logic. 
            # For now, we send everything the model generated.

            # Send to Kafka
            producer.send(TOPIC, value=record)
            
            # Updated print statement to use the correct 'Updated Header' name
            print(f"Sent: {record['AIRLINE_CODE']} flight from {record['ORIGIN']} to {record['DEST']}")

        producer.flush()
        
        # Sleep to simulate real-time (e.g., 1 batch every 2 seconds)
        time.sleep(2)
        
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(5) # Wait 5 seconds before retrying if Kafka isn't ready