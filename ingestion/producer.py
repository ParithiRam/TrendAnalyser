import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime

# Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'raw-tweets'

# Simulated Data Sources
KEYWORDS = ['AI', 'Python', 'Java', 'Kafka', 'Crypto', 'Election', 'Sports', 'Music', 'Tech', 'Space']
HASHTAGS = ['#coding', '#news', '#trending', '#update', '#viral', '#tech', '#future', '#innovation']

def generate_tweet():
    """Generates a simulated tweet."""
    keyword = random.choice(KEYWORDS)
    hashtag = random.choice(HASHTAGS)
    text = f"Just saw some great news about {keyword}! It is really {hashtag} right now."
    return {
        'id': random.randint(100000, 999999),
        'text': text,
        'timestamp': datetime.now().isoformat(),
        'user': f"user_{random.randint(1, 100)}"
    }

def main():
    print(f"Starting Kafka Producer to {KAFKA_BROKER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connected to Kafka.")
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return

    try:
        while True:
            tweet = generate_tweet()
            producer.send(TOPIC_NAME, tweet)
            print(f"Sent: {tweet}")
            time.sleep(1) # Simulate real-time stream
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
