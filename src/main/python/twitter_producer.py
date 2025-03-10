from kafka import KafkaProducer
import tweepy
import json
import time
import openai

openai.api_key = "ENTER KEY HERE"
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAJxqzgEAAAAAavVBhIkKIOL9P2j7ktVLI8NbArQ%3D4Hi3P3dvAUsDinkSUg6S72UA4GSitilwlu9cgtbzT1QQopWVfG"


TWEET_LIMIT = 10  # minimum 10 due to twitter's API restriction
TWEET_LIMIT = max(10, min(100, TWEET_LIMIT))


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_tweets(query, max_results=TWEET_LIMIT):
    client = tweepy.Client(bearer_token=BEARER_TOKEN)
    tweets = ""
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
                    tweets += tweet.text + ";"
                    producer.send("twitter-stream", {
                        "text": tweet.text,
                        "created_at": str(tweet.created_at)
                    })
                producer.flush() 
                print(f" Retrieved {len(response.data)} tweets.")
            else:
                print("No tweets found.")
            tweets = tweets[:-1]
            return tweets
        except tweepy.TooManyRequests:
            print("⚠️ Hit Twitter rate limit. Waiting 15 minutes before retrying...")
            time.sleep(15 * 60)
        except tweepy.TweepyException as e:
            print(f"Error fetching tweets: {e}")
            break

def summarize_and_analyze_sentiment(tweet_text):
    prompt = f"""Analyze the following collection of tweets. Provide:
    1. A concise summary explaining the key event.
    2. The overall sentiment (Positive, Negative, or Neutral) based on the general tone of the tweets.
    
    Tweets:
    {tweet_text}
    
    Response format:
    Summary: <summary>
    Overall Sentiment: <sentiment>
    """

    client = openai.OpenAI(api_key="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")  

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You analyze social media trends and extract key insights."},
            {"role": "user", "content": prompt}
        ]
    )

    return response.choices[0].message.content.strip()

if __name__ == "__main__":
    start_time = time.time()
    
    tweets = fetch_tweets("Super Bowl")  # change keyword as needed
    result = summarize_and_analyze_sentiment(tweets)
    print(result)
    
    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} s")

