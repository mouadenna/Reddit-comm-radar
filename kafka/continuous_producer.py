from confluent_kafka import Producer
import json
import time
from datetime import datetime
import random

# Sample data for generating dummy Reddit posts
SUBREDDITS = ['Morocco', 'Casablanca', 'Rabat', 'Marrakech', 'Fez']
POST_TYPES = ['text', 'link', 'image']
TOPICS = ['Culture', 'Food', 'Travel', 'News', 'Sports', 'Technology', 'Politics', 'Education']
SAMPLE_TITLES = [
    "What's your favorite traditional Moroccan dish?",
    "Beautiful sunset in {city} today!",
    "Question about living in {city}",
    "Latest developments in Moroccan {topic}",
    "Looking for recommendations in {city}",
    "Interesting historical fact about {city}",
    "Discussion: Future of {topic} in Morocco",
    "Need advice about {topic} in {city}"
]

SAMPLE_TEXTS = [
    "I've been wondering about this for a while. Any recommendations would be appreciated!",
    "Just captured this amazing moment and wanted to share with everyone.",
    "I'm planning to move here soon and would love some local insights.",
    "Let's discuss the recent changes and their impact on our community.",
    "Has anyone had experience with this? Would love to hear your thoughts.",
    "Found this interesting piece of information and thought I'd share.",
    "What are your thoughts on the current state and future prospects?",
    "First time visitor here, looking for some local expertise."
]

def generate_dummy_post():
    """Generate a dummy Reddit post with realistic-looking data"""
    subreddit = random.choice(SUBREDDITS)
    post_type = random.choice(POST_TYPES)
    topic = random.choice(TOPICS)
    current_time = int(time.time())
    
    # Generate title by filling in template
    title_template = random.choice(SAMPLE_TITLES)
    title = title_template.format(city=random.choice(SUBREDDITS), topic=topic.lower())
    
    # Generate random engagement metrics
    score = random.randint(1, 1000)
    comments = random.randint(0, min(score, 100))
    
    return {
        'id': f'post_{current_time}_{random.randint(1000, 9999)}',
        'title': title,
        'author': f'dummy_user_{random.randint(1, 100)}',
        'score': score,
        'created_utc': current_time,
        'url': f'https://reddit.com/r/{subreddit}/comments/{current_time}',
        'num_comments': comments,
        'selftext': random.choice(SAMPLE_TEXTS),
        'subreddit': subreddit,
        'timestamp': current_time
    }

def delivery_callback(err, msg):
    if err:
        print(f'ERROR: Message failed delivery: {err}')
    else:
        print(f"✓ Message delivered to topic {msg.topic()} partition [{msg.partition()}] @ offset {msg.offset()}")

def run_producer():
    # Producer configuration
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'reddit-continuous-producer'
    }
    
    producer = Producer(conf)
    
    print("=== Starting Continuous Reddit Producer ===")
    print("Generating one dummy post per minute. Press Ctrl+C to stop.")
    
    message_count = 0
    
    try:
        while True:
            # Generate and send dummy post
            post = generate_dummy_post()
            
            # Print preview of the post
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Producing post {message_count + 1}:")
            print(f"Title: {post['title']}")
            print(f"Author: u/{post['author']}")
            print(f"Subreddit: r/{post['subreddit']}")
            print(f"Score: {post['score']}, Comments: {post['num_comments']}")
            
            # Produce message
            producer.produce(
                'reddit-morocco',
                json.dumps(post).encode('utf-8'),
                callback=delivery_callback
            )
            
            # Flush and increment counter
            producer.flush()
            message_count += 1
            
            # Wait for next minute
            time.sleep(60)
            
    except KeyboardInterrupt:
        print("\nProducer stopped by user")
    except Exception as e:
        print(f"\nError in producer: {str(e)}")
    finally:
        producer.flush()
        print(f"\n=== Producer finished after sending {message_count} messages ===")

if __name__ == "__main__":
    run_producer() 