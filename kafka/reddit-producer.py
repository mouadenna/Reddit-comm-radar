import praw
from confluent_kafka import Producer
import json
import time

# --- Reddit Setup ---
reddit = praw.Reddit(
    client_id="WE9cMa51atcAuT5vk82o4w",
    client_secret="gKSbsR83IF45C2jruOi6gsCQ8fyLPw",
    user_agent="Reddit Kafka Producer"
)

# --- Kafka Producer Setup ---
producer = Producer({'bootstrap.servers': 'localhost:9092'})  # Ensure Kafka is running




def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# --- Stream and Produce to Kafka ---
subreddit = reddit.subreddit("Morocco")  # Change subreddit here

for submission in subreddit.stream.submissions(skip_existing=True):
    data = {
        "id": submission.id,
        "title": submission.title,
        "author": str(submission.author),
        "score": submission.score,
        "created_utc": submission.created_utc,
        "url": submission.url,
        "num_comments": submission.num_comments
    }

    print(f"Sending submission: {submission.title}")
    producer.produce(
        topic='reddit-morocco',
        key=submission.id,
        value=json.dumps(data),
        callback=delivery_report
    )
    producer.poll(0)  # Trigger callbacks for delivery

# Optional: flush if you add signal handling for clean exit
# producer.flush()
