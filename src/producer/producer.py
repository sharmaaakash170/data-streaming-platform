from kafka import KafkaProducer
import json
import time
from datetime import datetime

# Kafka configuration
kafka_broker = "kafka-service.default.svc.cluster.local:9093"
topic = "test-topic"

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_broker,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_message():
    message = {
        "key": "value",
        "message": "Hello Kafka!",
        "timestamp": str(datetime.now())
    }
    producer.send(topic, message)
    producer.flush()
    print(f"[{datetime.now()}] Message sent to topic {topic}")

# Run forever with 1 minute interval
if __name__ == "__main__":
    try:
        print("Kafka producer started. Press Ctrl+C to exit.")
        while True:
            send_message()
            time.sleep(60)  # 60 seconds = 1 minute
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()