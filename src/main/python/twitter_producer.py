from kafka import KafkaProducer
import tweepy
import json
import time

# Twitter API credentials
CONSUMER_KEY = "your-consumer-key"
CONSUMER_SECRET = "your-consumer-secret"
ACCESS_TOKEN = "your-access-token"
ACCESS_SECRET = "your-access-token-secret"
BEARER_TOKEN = "your-bearer-token"

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Function to create and restart the Twitter stream
def start_stream():
    class TwitterStream(tweepy.StreamingClient):
        def on_tweet(self, tweet):
            print(f"Tweet: {tweet.text}")
            producer.send("twitter-stream", {"text": tweet.text})
        
        def on_connection_error(self):
            print("⚠️ Connection lost. Restarting stream in 10 seconds...")
            time.sleep(10)
            start_stream()  # Restart the stream on failure

    try:
        listener = TwitterStream(BEARER_TOKEN)
        listener.add_rules(tweepy.StreamRule("Super Bowl"))  # Set keyword here
        print("Starting Twitter stream...")
        listener.filter()  # Start streaming
    except Exception as e:
        print(f" Error: {e}. Restarting in 10 seconds...")
        time.sleep(10)
        start_stream()  # Restart if it crashes

# Start streaming tweets
if __name__ == "__main__":
    start_stream()