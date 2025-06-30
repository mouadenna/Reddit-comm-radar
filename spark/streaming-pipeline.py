import os
import json
import logging
from pathlib import Path
from confluent_kafka import Consumer, KafkaError
import numpy as np
import torch
import pandas as pd
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from sentence_transformers import SentenceTransformer
import time
from collections import deque
from threading import Lock, Thread
import signal

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MessageBuffer:
    """Thread-safe message buffer with size limit"""
    
    def __init__(self, max_size=1000):
        self.buffer = deque(maxlen=max_size)
        self.lock = Lock()
        self.max_size = max_size
    
    def add(self, message):
        """Add a message to the buffer"""
        with self.lock:
            self.buffer.append(message)
            return len(self.buffer)
    
    def get_batch(self, batch_size):
        """Get a batch of messages from the buffer"""
        messages = []
        with self.lock:
            while len(messages) < batch_size and self.buffer:
                messages.append(self.buffer.popleft())
        return messages
    
    def size(self):
        """Get current buffer size"""
        with self.lock:
            return len(self.buffer)
    
    def clear(self):
        """Clear the buffer"""
        with self.lock:
            self.buffer.clear()

class RedditStreamProcessor:
    """
    A streaming processor that consumes Reddit data from Kafka and applies 
    sentiment analysis and topic modeling in real-time using a custom consumer.
    """
    
    def __init__(self):
        self.sentiment_model = None
        self.sentiment_tokenizer = None
        self.embedding_model = None
        # Force CPU usage
        self.device = torch.device("cpu")
        self.sentiment_labels = {0: "negative", 1: "neutral", 2: "positive"}
        
        # Output directory for results
        self.output_dir = Path("data/streaming_results")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize message buffer
        self.message_buffer = MessageBuffer(max_size=10000)
        
        # Initialize Kafka consumer
        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'reddit_processor',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000
        })
        
        # Control flag for the consumer thread
        self.running = False
        self.consumer_thread = None

    def consumer_loop(self):
        """Consumer thread function to continuously fetch messages"""
        while self.running:
            msg = self.consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info("Reached end of partition")
                else:
                    logger.error(f"Error: {msg.error()}")
                continue
            
            # Add message to buffer
            buffer_size = self.message_buffer.add(msg)
            if buffer_size % 100 == 0:  # Log every 100 messages
                logger.info(f"Buffer size: {buffer_size}")

    def start_consumer(self):
        """Start the consumer thread"""
        self.running = True
        self.consumer_thread = Thread(target=self.consumer_loop)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()

    def stop_consumer(self):
        """Stop the consumer thread"""
        self.running = False
        if self.consumer_thread:
            self.consumer_thread.join()
        self.consumer.close()
        logger.info("Consumer stopped and closed")

    def initialize_models(self):
        """Initialize sentiment analysis and embedding models"""
        try:
            # Initialize sentiment analysis model
            model_name = "cardiffnlp/twitter-roberta-base-sentiment"
            self.sentiment_tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.sentiment_model = AutoModelForSequenceClassification.from_pretrained(model_name)
            self.sentiment_model.to(self.device)
            logger.info(f"Sentiment model initialized on {self.device}")
            
            # Initialize embedding model for topic modeling
            self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
            self.embedding_model.to(self.device)
            logger.info("Embedding model initialized")
            
        except Exception as e:
            logger.error(f"Error initializing models: {str(e)}")
            raise
    
    def analyze_sentiment_batch(self, texts):
        """
        Analyze sentiment for a batch of texts
        
        Args:
            texts: List of texts to analyze
            
        Returns:
            List of sentiment results
        """
        results = []
        
        for text in texts:
            if not text or not isinstance(text, str):
                results.append({"sentiment": "neutral", "score": 0.5})
                continue
            
            try:
                # Truncate text if too long
                max_length = 512
                if len(text) > max_length * 4:
                    text = text[:max_length * 4]
                
                # Tokenize and get sentiment
                inputs = self.sentiment_tokenizer(text, return_tensors="pt", truncation=True, max_length=max_length).to(self.device)
                with torch.no_grad():
                    outputs = self.sentiment_model(**inputs)
                
                # Get probabilities
                probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
                probs = probs.cpu().numpy()[0]
                
                # Get predicted sentiment
                sentiment_idx = np.argmax(probs)
                sentiment = self.sentiment_labels[sentiment_idx]
                score = float(probs[sentiment_idx])
                
                results.append({"sentiment": sentiment, "score": score})
            
            except Exception as e:
                logger.warning(f"Error analyzing sentiment: {str(e)}")
                results.append({"sentiment": "neutral", "score": 0.5})
        
        return results
    
    def generate_embeddings_batch(self, texts):
        """
        Generate embeddings for a batch of texts
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List of embeddings
        """
        try:
            # Filter out empty texts
            valid_texts = [text if text and isinstance(text, str) else "" for text in texts]
            embeddings = self.embedding_model.encode(valid_texts)
            return embeddings.tolist()
        except Exception as e:
            logger.warning(f"Error generating embeddings: {str(e)}")
            return [[0.0] * 384 for _ in texts]  # Return zero embeddings as fallback
    
    def process_messages(self, messages):
        """
        Process a batch of messages
        
        Args:
            messages: List of Kafka messages
        """
        if not messages:
            return
        
        batch_id = int(time.time())
        logger.info(f"Processing batch {batch_id} with {len(messages)} messages")
        
        # Parse messages
        records = []
        failed_messages = []
        for msg in messages:
            try:
                value = json.loads(msg.value().decode('utf-8'))
                # Validate required fields
                required_fields = ['id', 'title', 'author', 'score', 'created_utc', 
                                 'url', 'num_comments', 'selftext', 'subreddit', 'timestamp']
                if all(field in value for field in required_fields):
                    records.append(value)
                else:
                    logger.warning(f"Message missing required fields: {value.get('id', 'unknown')}")
                    failed_messages.append(msg)
            except Exception as e:
                logger.error(f"Error parsing message: {str(e)}")
                failed_messages.append(msg)
                continue
        
        if not records:
            return
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(records)
            
            # Combine title and selftext for analysis
            df['analysis_text'] = df.apply(
                lambda x: f"{x['title']} {x['selftext']}" if x['selftext'] else x['title'], 
                axis=1
            )
            
            # Extract texts for analysis
            texts = df['analysis_text'].fillna('').tolist()
            
            # Perform sentiment analysis
            logger.info(f"Performing sentiment analysis for batch {batch_id}")
            sentiment_results = self.analyze_sentiment_batch(texts)
            
            # Add sentiment results to DataFrame
            df['sentiment'] = [r['sentiment'] for r in sentiment_results]
            df['sentiment_score'] = [r['score'] for r in sentiment_results]
            
            # Generate embeddings for topic modeling
            logger.info(f"Generating embeddings for batch {batch_id}")
            embeddings = self.generate_embeddings_batch(texts)
            
            # Save batch results
            batch_output_path = self.output_dir / f"batch_{batch_id}.json"
            embeddings_output_path = self.output_dir / f"embeddings_batch_{batch_id}.json"
            
            # Prepare output data (excluding analysis_text field)
            df = df.drop('analysis_text', axis=1)
            output_data = df.to_dict('records')
            
            # Save main results
            with open(batch_output_path, 'w') as f:
                json.dump(output_data, f, indent=2, default=str)
            
            # Save embeddings separately
            embeddings_data = {
                'batch_id': batch_id,
                'embeddings': embeddings,
                'texts': texts,
                'post_ids': df['id'].tolist()
            }
            
            with open(embeddings_output_path, 'w') as f:
                json.dump(embeddings_data, f, indent=2)
            
            logger.info(f"Batch {batch_id} results saved to {batch_output_path}")
            
            # Print summary statistics
            sentiment_counts = df['sentiment'].value_counts()
            logger.info(f"Batch {batch_id} sentiment distribution: {sentiment_counts.to_dict()}")
            
            # Log engagement metrics
            logger.info(f"Batch {batch_id} average score: {df['score'].mean():.2f}")
            logger.info(f"Batch {batch_id} average comments: {df['num_comments'].mean():.2f}")
            
        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {str(e)}")
            # Return failed messages to buffer
            for msg in messages:
                self.message_buffer.add(msg)
    
    def start_streaming(self):
        """Start the streaming pipeline"""
        try:
            # Initialize models
            self.initialize_models()
            
            # Subscribe to topic
            self.consumer.subscribe(['reddit-morocco'])
            
            # Start consumer thread
            self.start_consumer()
            
            logger.info("Starting Reddit streaming pipeline...")
            logger.info("Press Ctrl+C to stop the pipeline")
            
            batch_size = 10
            batch_timeout = 30  # seconds
            
            while True:
                start_time = time.time()
                
                # Wait for either batch size or timeout
                while (self.message_buffer.size() < batch_size and 
                       (time.time() - start_time) < batch_timeout):
                    time.sleep(0.1)  # Small sleep to prevent CPU spinning
                
                # Get batch from buffer
                messages = self.message_buffer.get_batch(batch_size)
                
                # Process collected messages
                if messages:
                    self.process_messages(messages)
                
        except KeyboardInterrupt:
            logger.info("Streaming pipeline stopped by user")
        except Exception as e:
            logger.error(f"Error in streaming pipeline: {str(e)}")
        finally:
            self.stop_consumer()
            logger.info("Pipeline shutdown complete")

def main():
    """Main function to run the streaming pipeline"""
    processor = RedditStreamProcessor()
    processor.start_streaming()

if __name__ == "__main__":
    main()