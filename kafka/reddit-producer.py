import praw
from confluent_kafka import Producer
import json
import time
import logging
import os
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RedditProducer:
    """
    Reddit data producer that streams posts to Kafka
    """
    
    def __init__(self, subreddit_name="Morocco", topic_name="reddit-morocco"):
        self.subreddit_name = subreddit_name
        self.topic_name = topic_name
        
        # Initialize Reddit API
        self.reddit = praw.Reddit(
            client_id="WE9cMa51atcAuT5vk82o4w",
            client_secret="gKSbsR83IF45C2jruOi6gsCQ8fyLPw",
            user_agent="Reddit Kafka Producer"
        )
        
        # Initialize Kafka Producer
        self.producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'reddit-producer'
        })
        
        logger.info(f"Reddit producer initialized for subreddit: {subreddit_name}")
        logger.info(f"Kafka topic: {topic_name}")
    
    def delivery_report(self, err, msg):
        """Delivery report callback for Kafka producer"""
        if err:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    
    def format_submission_data(self, submission):
        """Format Reddit submission data for Kafka"""
        try:
            data = {
                "id": submission.id,
                "title": submission.title,
                "author": str(submission.author) if submission.author else "deleted",
                "score": submission.score,
                "created_utc": submission.created_utc,
                "url": submission.url,
                "num_comments": submission.num_comments,
                "selftext": submission.selftext[:1000] if submission.selftext else "",  # Limit length
                "subreddit": submission.subreddit.display_name,
                "timestamp": time.time()
            }
            return data
        except Exception as e:
            logger.error(f"Error formatting submission data: {str(e)}")
            return None
    
    def start_streaming(self):
        """Start streaming Reddit data to Kafka"""
        try:
            subreddit = self.reddit.subreddit(self.subreddit_name)
            logger.info(f"Starting to stream from r/{self.subreddit_name}")
            
            for submission in subreddit.stream.submissions(skip_existing=True):
                try:
                    # Format the data
                    data = self.format_submission_data(submission)
                    
                    if data is None:
                        continue
                    
                    # Send to Kafka
                    logger.info(f"Sending submission: {submission.title[:50]}...")
                    self.producer.produce(
                        topic=self.topic_name,
                        key=submission.id,
                        value=json.dumps(data),
                        callback=self.delivery_report
                    )
                    
                    # Poll for delivery reports
                    self.producer.poll(0)
                    
                    # Small delay to avoid overwhelming the system
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error processing submission: {str(e)}")
                    continue
                    
        except KeyboardInterrupt:
            logger.info("Streaming stopped by user")
        except Exception as e:
            logger.error(f"Error in streaming: {str(e)}")
        finally:
            # Flush any remaining messages
            self.producer.flush()
            logger.info("Producer shutdown complete")

def main():
    """Main function to run the Reddit producer"""
    producer = RedditProducer()
    producer.start_streaming()

if __name__ == "__main__":
    main()
