"""
Main streaming pipeline for Reddit data analysis.
"""

import os
import json
import logging
import time
import pandas as pd
from pathlib import Path
from confluent_kafka import Consumer, KafkaError
from threading import Thread
from glob import glob
from databricks import sql

from processors.base_processor import BaseProcessor
from processors.message_buffer import MessageBuffer
from processors.sentiment_processor import SentimentProcessor
from processors.topic_processor import TopicProcessor
from processors.keyword_processor import KeywordProcessor

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RedditStreamProcessor(BaseProcessor):
    """
    A streaming processor that consumes Reddit data from Kafka and applies 
    sentiment analysis, topic modeling, and keyword extraction in real-time.
    """
    
    def __init__(self, output_dir: Path = None, min_batch_size: int = 1, use_databricks=True):
        """
        Initialize the streaming processor.
        
        Args:
            output_dir: Directory to save output files
            min_batch_size: Minimum number of new messages to accumulate before processing
            use_databricks: Whether to store data in Databricks
        """
        super().__init__(output_dir)
        
        # Initialize processors
        self.sentiment_processor = SentimentProcessor(self.output_dir)
        self.topic_processor = TopicProcessor(self.output_dir)
        self.keyword_processor = KeywordProcessor(self.output_dir)
        
        # Initialize message buffer
        self.message_buffer = MessageBuffer(max_size=10000)
        
        # Set minimum batch size for processing
        self.min_batch_size = min_batch_size
        
        # Store historical data
        self.historical_data = pd.DataFrame()
        
        # Ensure processed data directory exists
        self.processed_dir = self.output_dir / "processed_data"
        self.processed_dir.mkdir(exist_ok=True)
        
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
        
        # Databricks connection settings
        self.use_databricks = use_databricks
        self.databricks_connection = None
        self.databricks_cursor = None
        if self.use_databricks:
            self.initialize_databricks_connection()
            
    def initialize_databricks_connection(self):
        """Initialize connection to Databricks SQL warehouse."""
        try:
            logger.info("Initializing Databricks connection...")
            self.databricks_connection = sql.connect(
                server_hostname="",
                http_path="",
                access_token=""
            )
            self.databricks_cursor = self.databricks_connection.cursor()
            logger.info("Databricks connection established successfully")
        except Exception as e:
            logger.error(f"Error connecting to Databricks: {str(e)}")
            self.use_databricks = False
            
    def close_databricks_connection(self):
        """Close the Databricks connection."""
        if self.databricks_cursor:
            self.databricks_cursor.close()
        if self.databricks_connection:
            self.databricks_connection.close()
        logger.info("Databricks connection closed")

    def initialize_models(self):
        """Initialize all models."""
        logger.info("Initializing models...")
        self.sentiment_processor.initialize_model()
        self.topic_processor.initialize_models()
        self.keyword_processor.initialize_model()
        logger.info("Models initialized successfully")

    def load_historical_data(self, historical_dir: str):
        """
        Load historical data from JSON files in the specified directory.
        
        Args:
            historical_dir: Directory containing historical JSON files
        """
        logger.info(f"Loading historical data from {historical_dir}")
        
        historical_records = []
        for json_file in glob(os.path.join(historical_dir, "*.json")):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for record in data:
                        # Transform historical data format to match streaming format
                        processed_record = {
                            'id': record['id'],
                            'title': '',  # Historical data doesn't have separate title
                            'author': record['author'],
                            'score': record['score'],
                            'created_utc': record['created_utc'],
                            'url': record['permalink'],
                            'num_comments': record['num_comments'],
                            'selftext': record['post_text'],
                            'subreddit': record['subreddit'],
                            'timestamp': record['created_utc']
                        }
                        historical_records.append(processed_record)
                logger.info(f"Loaded {len(historical_records)} records from {json_file}")
            except Exception as e:
                logger.error(f"Error loading historical data from {json_file}: {str(e)}")
                
        if historical_records:
            self.historical_data = pd.DataFrame(historical_records)
            logger.info(f"Total historical records loaded: {len(self.historical_data)}")
            
            # Process historical data
            self.process_dataframe(self.historical_data)
        else:
            logger.warning("No historical data was loaded")

    def process_dataframe(self, df: pd.DataFrame):
        """
        Process a DataFrame of Reddit posts.
        
        Args:
            df: DataFrame containing Reddit posts
        """
        try:
            batch_id = int(time.time())
            logger.info(f"Processing batch {batch_id} with {len(df)} records")
            
            # Combine title and selftext for analysis
            df['analysis_text'] = df.apply(
                lambda x: f"{x['title']} {x['selftext']}" if x['selftext'] else x['title'], 
                axis=1
            )
            
            # Extract texts for analysis
            texts = df['analysis_text'].fillna('').tolist()
            
            # Perform sentiment analysis
            logger.info(f"Performing sentiment analysis for batch {batch_id}")
            sentiment_results = self.sentiment_processor.analyze_batch(texts)
            df['sentiment'] = [r['sentiment'] for r in sentiment_results]
            df['sentiment_score'] = [r['score'] for r in sentiment_results]
            
            # Generate embeddings and perform topic modeling
            logger.info(f"Performing topic modeling for batch {batch_id}")
            embeddings = self.topic_processor.generate_embeddings(texts)
            topics, probs = self.topic_processor.fit_transform(texts, embeddings=embeddings)
            df['topic'] = topics
            # Handle both single probability values and probability arrays
            df['topic_probability'] = [prob if isinstance(prob, (float, int)) else max(prob) if hasattr(prob, '__iter__') else 0.0 for prob in probs]
            
            # Extract keywords
            logger.info(f"Extracting keywords for batch {batch_id}")
            keywords_list = self.keyword_processor.extract_batch(texts)
            
            # Convert keywords list to string for table storage
            df['keywords'] = [','.join(kw_list) for kw_list in keywords_list]
            
            # Prepare final data
            df = df.drop('analysis_text', axis=1)
            
            # Get topic metadata
            topic_info = self.topic_processor.get_topic_info()
            topic_keywords = self.topic_processor.get_topic_keywords()
            
            # Create topic mapping DataFrame
            topic_df = pd.DataFrame(topic_info)
            
            # Create topic keywords DataFrame
            topic_keywords_data = []
            for topic_id, keywords in topic_keywords.items():
                if isinstance(keywords, (list, tuple)):
                    for keyword_item in keywords:
                        # Handle different possible formats of keyword data
                        if isinstance(keyword_item, (list, tuple)):
                            # If it's a tuple/list, assume first item is keyword and last is score
                            keyword = keyword_item[0]
                            score = keyword_item[-1] if len(keyword_item) > 1 else 1.0
                        else:
                            # If it's a single item, use it as keyword with default score
                            keyword = str(keyword_item)
                            score = 1.0
                        
                        topic_keywords_data.append({
                            'topic_id': topic_id,
                            'keyword': keyword,
                            'score': float(score)
                        })
                elif isinstance(keywords, dict):
                    # Handle dictionary format
                    for keyword, score in keywords.items():
                        topic_keywords_data.append({
                            'topic_id': topic_id,
                            'keyword': str(keyword),
                            'score': float(score)
                        })
                else:
                    # Handle single string/value
                    topic_keywords_data.append({
                        'topic_id': topic_id,
                        'keyword': str(keywords),
                        'score': 1.0
                    })
            
            topic_keywords_df = pd.DataFrame(topic_keywords_data)
            
            # Save all DataFrames as CSV files
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            
            # Save posts data
            posts_file = self.processed_dir / "posts.csv"
            df.to_csv(posts_file, index=False)
            
            # Save topic mapping
            topics_file = self.processed_dir / "topics.csv"
            topic_df.to_csv(topics_file, index=False)
            
            # Save topic keywords
            topic_keywords_file = self.processed_dir / "topic_keywords.csv"
            topic_keywords_df.to_csv(topic_keywords_file, index=False)
            
            # Save to Databricks if enabled
            if self.use_databricks and self.databricks_connection:
                try:
                    logger.info("Saving data to Databricks...")
                    self._save_to_databricks_table(df, "posts")
                    self._save_to_databricks_table(topic_df, "topics")
                    self._save_to_databricks_table(topic_keywords_df, "topic_keywords")
                    logger.info("Data successfully saved to Databricks")
                except Exception as e:
                    logger.error(f"Error saving to Databricks: {str(e)}")
                    # Try to reconnect
                    self.initialize_databricks_connection()
            
            # Save metadata about the latest processing
            metadata = {
                'last_updated': time.strftime('%Y-%m-%d %H:%M:%S'),
                'total_posts': len(df),
                'total_topics': len(topic_df),
                'total_topic_keywords': len(topic_keywords_df),
                'files': {
                    'posts': str(posts_file),
                    'topics': str(topics_file),
                    'topic_keywords': str(topic_keywords_file)
                }
            }
            metadata_file = self.processed_dir / "metadata.json"
            self.save_json(metadata, metadata_file)
            
            # Update our historical data with the latest processed data
            self.historical_data = df
            
            # Log statistics
            logger.info(f"Data processed and saved to {self.processed_dir}")
            logger.info("Current statistics:")
            logger.info(f"- Total posts: {len(df)}")
            logger.info(f"- Unique topics: {len(topic_df)}")
            logger.info(f"- Average sentiment score: {df['sentiment_score'].mean():.2f}")
            logger.info(f"- Average keywords per post: {sum(len(kw.split(',')) for kw in df['keywords']) / len(df):.2f}")
            
        except Exception as e:
            logger.error(f"Error processing data: {str(e)}")
            raise
            
    def _save_to_databricks_table(self, df, table_name):
        """
        Save DataFrame to Databricks table.
        
        Args:
            df: DataFrame to save
            table_name: Name of the table
        """
        if df.empty:
            logger.warning(f"Empty DataFrame, not saving to {table_name}")
            return
            
        try:
            # Make a copy of the DataFrame to avoid modifying the original
            df_copy = df.copy()
            
            # Handle specific data type conversions based on table
            if table_name == "posts":
                # Convert timestamp to proper numeric format if it's a string
                if 'timestamp' in df_copy.columns and df_copy['timestamp'].dtype == 'object':
                    try:
                        # Try to convert to float directly
                        df_copy['timestamp'] = df_copy['timestamp'].astype(float)
                    except:
                        # If that fails, try to handle date strings by converting to epoch
                        df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp']).astype('int64') // 10**9
                
                # Ensure numeric columns are properly typed
                numeric_cols = ['score', 'num_comments', 'sentiment_score', 'topic_probability']
                for col in numeric_cols:
                    if col in df_copy.columns:
                        df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
            
            # Convert DataFrame to list of records
            records = df_copy.to_dict('records')
            
            # Use batched inserts for better performance
            batch_size = 10
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                
                # Process each record in the batch
                batch_values = []
                for record in batch:
                    # Process each record individually
                    try:
                        # Build SQL for this record
                        columns = ', '.join(df_copy.columns)
                        placeholders = []
                        
                        for col in df_copy.columns:
                            value = record[col]
                            
                            # Handle different data types
                            if value is None:
                                placeholders.append("NULL")
                            elif isinstance(value, (int, float)):
                                placeholders.append(str(value))
                            elif isinstance(value, bool):
                                placeholders.append("TRUE" if value else "FALSE")
                            else:
                                # Clean and escape string values
                                str_val = str(value).replace("'", "''")
                                placeholders.append(f"'{str_val}'")
                        
                        # Create the SQL query with values directly inserted
                        values_str = ', '.join(placeholders)
                        query = f"INSERT INTO data_engineering_projects.reddit_comm_radar.{table_name} ({columns}) VALUES ({values_str})"
                        
                        # Execute the query
                        self.databricks_cursor.execute(query)
                    except Exception as e:
                        logger.error(f"Error inserting record in {table_name}: {str(e)}")
                        logger.error(f"Problematic record: {record}")
                        # Continue with other records
                
            logger.info(f"Saved {len(records)} records to table {table_name}")
            
        except Exception as e:
            logger.error(f"Error saving to table {table_name}: {str(e)}")
            raise

    def process_messages(self, messages):
        """
        Process a batch of messages.
        
        Args:
            messages: List of Kafka messages
        """
        if not messages:
            return
        
        # Parse messages
        records = []
        failed_messages = []
        for msg in messages:
            try:
                value = json.loads(msg.value().decode('utf-8'))
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
            new_df = pd.DataFrame(records)
            
            # Combine with historical data, keeping only unique posts based on 'id'
            if not self.historical_data.empty:
                combined_df = pd.concat([self.historical_data, new_df])
                # Drop duplicates keeping the latest version of each post
                combined_df = combined_df.drop_duplicates(subset=['id'], keep='last')
            else:
                combined_df = new_df
            
            # Process the combined data
            self.process_dataframe(combined_df)
            
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            # Return failed messages to buffer
            for msg in messages:
                self.message_buffer.add(msg)

    def consumer_loop(self):
        """Consumer thread function to continuously fetch messages."""
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
        """Start the consumer thread."""
        self.running = True
        self.consumer_thread = Thread(target=self.consumer_loop)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        
    def stop_consumer(self):
        """Stop the consumer thread."""
        self.running = False
        if self.consumer_thread:
            self.consumer_thread.join()
        self.consumer.close()
        
        # Close Databricks connection
        if self.use_databricks:
            self.close_databricks_connection()
            
        logger.info("Consumer stopped and closed")
        
    def start_streaming(self, historical_dir: str = None):
        """
        Start the streaming pipeline.
        
        Args:
            historical_dir: Optional directory containing historical data to load
        """
        try:
            # Initialize models
            self.initialize_models()
            
            # Load historical data if provided
            if historical_dir:
                self.load_historical_data(historical_dir)
            
            # Subscribe to topic
            self.consumer.subscribe(['reddit-morocco'])
            
            # Start consumer thread
            self.start_consumer()
            
            logger.info("Starting Reddit streaming pipeline...")
            logger.info(f"Using minimum batch size of {self.min_batch_size}")
            logger.info("Press Ctrl+C to stop the pipeline")
            
            while True:
                # Wait for minimum batch size
                while self.message_buffer.size() < self.min_batch_size:
                    time.sleep(0.1)  # Small sleep to prevent CPU spinning
                
                # Get batch from buffer
                messages = self.message_buffer.get_batch(self.min_batch_size)
                
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
    """Main function to run the streaming pipeline."""
    processor = RedditStreamProcessor(min_batch_size=10, use_databricks=True)
    processor.start_streaming(historical_dir="data/historical")

if __name__ == "__main__":
    main() 