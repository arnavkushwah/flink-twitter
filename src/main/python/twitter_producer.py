from kafka import KafkaProducer
import tweepy
import json
import time


BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAJxqzgEAAAAAavVBhIkKIOL9P2j7ktVLI8NbArQ%3D4Hi3P3dvAUsDinkSUg6S72UA4GSitilwlu9cgtbzT1QQopWVfG"


TWEET_LIMIT = 10  # minimum 10 due to twitter's API restriction
TWEET_LIMIT = max(10, min(100, TWEET_LIMIT))


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_tweets(query, max_results=TWEET_LIMIT):
    client = tweepy.Client(bearer_token=BEARER_TOKEN)
    while True:
        try:
            response = client.search_recent_tweets(
                query=query,
                max_results=max_results,
                tweet_fields=["created_at", "text"]
            )
            if response.data:
                for tweet in response.data:
                    print(f" Tweet: {tweet.text}")
                    producer.send("twitter-stream", {
                        "text": tweet.text,
                        "created_at": str(tweet.created_at)
                    })
                producer.flush() 
                print(f" Retrieved {len(response.data)} tweets.")
            else:
                print("No tweets found.")
            break
        except tweepy.TooManyRequests:
            print("⚠️ Hit Twitter rate limit. Waiting 15 minutes before retrying...")
            time.sleep(15 * 60)
        except tweepy.TweepyException as e:
            print(f"Error fetching tweets: {e}")
            break

if __name__ == "__main__":
    fetch_tweets("Super Bowl")  # change keyword as needed
