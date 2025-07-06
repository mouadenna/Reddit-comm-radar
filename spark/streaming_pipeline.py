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
from dotenv import load_dotenv

from processors.base_processor import BaseProcessor
from processors.message_buffer import MessageBuffer
from processors.sentiment_processor import SentimentProcessor
from processors.topic_processor import TopicProcessor
from processors.keyword_processor import KeywordProcessor

load_dotenv()

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
        kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", 'localhost:9092')
        self.consumer = Consumer({
            'bootstrap.servers': kafka_bootstrap_servers,
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
        try:
            logger.info("Initializing Databricks connection...")
            server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
            http_path = os.getenv("DATABRICKS_HTTP_PATH")
            access_token = os.getenv("DATABRICKS_TOKEN")

            if not all([server_hostname, http_path, access_token]):
                logger.error("Databricks credentials not found in environment variables.")
                self.use_databricks = False
                return

            self.databricks_connection = sql.connect(
                server_hostname=server_hostname,
                http_path=http_path,
                access_token=access_token
            )
            self.databricks_cursor = self.databricks_connection.cursor()
            logger.info("Databricks connection established successfully")
        except Exception as e:
            logger.error(f"Error connecting to Databricks: {str(e)}")
            self.use_databricks = False
            
    def close_databricks_connection(self):
        if self.databricks_cursor:
            self.databricks_cursor.close()
        if self.databricks_connection:
            self.databricks_connection.close()
        logger.info("Databricks connection closed")

    def initialize_models(self):
        logger.info("Initializing models...")
        self.sentiment_processor.initialize_model()
        self.topic_processor.initialize_models()
        self.keyword_processor.initialize_model()
        logger.info("Models initialized successfully")

    def load_historical_data(self, historical_dir: str):
        logger.info(f"Loading historical data from {historical_dir}")
        
        historical_records = []
        
        historical_path = Path(historical_dir)
        if not historical_path.exists():
            logger.warning(f"Historical directory not found: {historical_dir}")
            return

        for json_file in glob(os.path.join(historical_dir, "*.json")):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for record in data:
                        processed_record = {
                            'id': record.get('id'),
                            'title': '',
                            'author': record.get('author'),
                            'score': record.get('score'),
                            'created_utc': record.get('created_utc'),
                            'url': record.get('permalink'),
                            'num_comments': record.get('num_comments'),
                            'selftext': record.get('post_text'),
                            'subreddit': record.get('subreddit'),
                            'timestamp': record.get('created_utc')
                        }
                        historical_records.append(processed_record)
                logger.info(f"Loaded {len(historical_records)} records from {json_file}")
            except Exception as e:
                logger.error(f"Error loading historical data from {json_file}: {str(e)}")
                
        if historical_records:
            self.historical_data = pd.DataFrame(historical_records)
            logger.info(f"Total historical records loaded: {len(self.historical_data)}")
            
            self.process_dataframe(self.historical_data)
        else:
            logger.warning("No historical data was loaded")

    def process_dataframe(self, df: pd.DataFrame):
        try:
            batch_id = int(time.time())
            logger.info(f"Processing batch {batch_id} with {len(df)} records")
            
            df['analysis_text'] = df.apply(
                lambda x: f"{x['title']} {x['selftext']}" if x['selftext'] else x['title'], 
                axis=1
            )
            
            texts = df['analysis_text'].fillna('').tolist()
            
            logger.info(f"Performing sentiment analysis for batch {batch_id}")
            sentiment_results = self.sentiment_processor.analyze_batch(texts)
            df['sentiment'] = [r['sentiment'] for r in sentiment_results]
            df['sentiment_score'] = [r['score'] for r in sentiment_results]
            
            logger.info(f"Performing topic modeling for batch {batch_id}")
            embeddings = self.topic_processor.generate_embeddings(texts)
            topics, probs = self.topic_processor.fit_transform(texts, embeddings=embeddings)
            df['topic'] = topics
            df['topic_probability'] = [prob if isinstance(prob, (float, int)) else max(prob) if hasattr(prob, '__iter__') else 0.0 for prob in probs]
            
            logger.info(f"Extracting keywords for batch {batch_id}")
            keywords_list = self.keyword_processor.extract_batch(texts)
            
            df['keywords'] = [','.join(kw_list) for kw_list in keywords_list]
            
            df = df.drop('analysis_text', axis=1)
            
            topic_info = self.topic_processor.get_topic_info()
            topic_keywords = self.topic_processor.get_topic_keywords()
            
            topic_df = pd.DataFrame(topic_info)
            
            topic_keywords_data = []
            if topic_keywords:
                for topic_id, keywords in topic_keywords.items():
                    if isinstance(keywords, (list, tuple)):
                        for keyword_item in keywords:
                            if isinstance(keyword_item, (list, tuple)):
                                keyword = keyword_item[0]
                                score = keyword_item[-1] if len(keyword_item) > 1 else 1.0
                            else:
                                keyword = str(keyword_item)
                                score = 1.0
                            
                            topic_keywords_data.append({
                                'topic_id': topic_id,
                                'keyword': keyword,
                                'score': float(score)
                            })
                    elif isinstance(keywords, dict):
                        for keyword, score in keywords.items():
                            topic_keywords_data.append({
                                'topic_id': topic_id,
                                'keyword': str(keyword),
                                'score': float(score)
                            })
                    else:
                        topic_keywords_data.append({
                            'topic_id': topic_id,
                            'keyword': str(keywords),
                            'score': 1.0
                        })
            
            topic_keywords_df = pd.DataFrame(topic_keywords_data)
            
            posts_file = self.processed_dir / "posts.csv"
            df.to_csv(posts_file, index=False)
            
            topics_file = self.processed_dir / "topics.csv"
            topic_df.to_csv(topics_file, index=False)
            
            topic_keywords_file = self.processed_dir / "topic_keywords.csv"
            topic_keywords_df.to_csv(topic_keywords_file, index=False)
            
            if self.use_databricks and self.databricks_connection:
                try:
                    logger.info("Saving data to Databricks...")
                    self._save_to_databricks_table(df, "posts")
                    self._save_to_databricks_table(topic_df, "topics")
                    self._save_to_databricks_table(topic_keywords_df, "topic_keywords")
                    logger.info("Data successfully saved to Databricks")
                except Exception as e:
                    logger.error(f"Error saving to Databricks: {str(e)}")
                    self.initialize_databricks_connection()
            
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
            
            self.historical_data = df
            
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
        if df.empty:
            logger.warning(f"Empty DataFrame, not saving to {table_name}")
            return
            
        try:
            df_copy = df.copy()
            
            if table_name == "posts":
                if 'timestamp' in df_copy.columns and df_copy['timestamp'].dtype == 'object':
                    try:
                        df_copy['timestamp'] = df_copy['timestamp'].astype(float)
                    except (ValueError, TypeError):
                        df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'], errors='coerce').astype('int64') // 10**9
                
                numeric_cols = ['score', 'num_comments', 'sentiment_score', 'topic_probability']
                for col in numeric_cols:
                    if col in df_copy.columns:
                        df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
            
            records = df_copy.to_dict('records')
            
            batch_size = 100
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                
                for record in batch:
                    try:
                        columns = ', '.join(f"`{col}`" for col in df_copy.columns)
                        placeholders = []
                        
                        for col in df_copy.columns:
                            value = record[col]
                            
                            if pd.isna(value):
                                placeholders.append("NULL")
                            elif isinstance(value, (int, float)):
                                placeholders.append(str(value))
                            elif isinstance(value, bool):
                                placeholders.append("TRUE" if value else "FALSE")
                            else:
                                str_val = str(value).replace("'", "''")
                                placeholders.append(f"'{str_val}'")
                        
                        values_str = ', '.join(placeholders)
                        query = f"INSERT INTO data_engineering_projects.reddit_comm_radar.{table_name} ({columns}) VALUES ({values_str})"
                        
                        if self.databricks_cursor:
                            self.databricks_cursor.execute(query)
                    except Exception as e:
                        logger.error(f"Error inserting record in {table_name}: {record} - {str(e)}")
                        continue
                
            logger.info(f"Saved {len(records)} records to table {table_name}")
            
        except Exception as e:
            logger.error(f"Error saving to table {table_name}: {str(e)}")
            raise

    def process_messages(self, messages):
        if not messages:
            return
        
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
            new_df = pd.DataFrame(records)
            
            if not self.historical_data.empty:
                combined_df = pd.concat([self.historical_data, new_df])
                combined_df = combined_df.drop_duplicates(subset=['id'], keep='last')
            else:
                combined_df = new_df
            
            self.process_dataframe(combined_df)
            
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            for msg in messages:
                self.message_buffer.add(msg)

    def consumer_loop(self):
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
            
            buffer_size = self.message_buffer.add(msg)
            if buffer_size % 100 == 0:
                logger.info(f"Buffer size: {buffer_size}")
                
    def start_consumer(self):
        self.running = True
        self.consumer_thread = Thread(target=self.consumer_loop)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        
    def stop_consumer(self):
        self.running = False
        if self.consumer_thread:
            self.consumer_thread.join()
        self.consumer.close()
        
        if self.use_databricks:
            self.close_databricks_connection()
            
        logger.info("Consumer stopped and closed")
        
    def start_streaming(self, historical_dir: str = "data/historical"):
        try:
            self.initialize_models()
            
            if historical_dir:
                self.load_historical_data(historical_dir)
            
            self.consumer.subscribe(['reddit-morocco'])
            
            self.start_consumer()
            
            logger.info("Starting Reddit streaming pipeline...")
            logger.info(f"Using minimum batch size of {self.min_batch_size}")
            logger.info("Press Ctrl+C to stop the pipeline")
            
            while True:
                while self.message_buffer.size() < self.min_batch_size:
                    time.sleep(0.1)
                
                messages = self.message_buffer.get_batch(self.min_batch_size)
                
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
    processor = RedditStreamProcessor(min_batch_size=10, use_databricks=True)
    processor.start_streaming(historical_dir="data/historical")

if __name__ == "__main__":
    main() 