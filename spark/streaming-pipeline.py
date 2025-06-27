import os
import json
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sentence_transformers import SentenceTransformer
import numpy as np
import torch
import pandas as pd
from transformers import AutoModelForSequenceClassification, AutoTokenizer

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set correct Python path for executors (inside Docker)
os.environ["PYSPARK_PYTHON"] = "python3"

class RedditStreamProcessor:
    """
    A streaming processor that consumes Reddit data from Kafka and applies 
    sentiment analysis and topic modeling in real-time.
    """
    
    def __init__(self):
        self.spark = None
        self.sentiment_model = None
        self.sentiment_tokenizer = None
        self.embedding_model = None
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.sentiment_labels = {0: "negative", 1: "neutral", 2: "positive"}
        
        # Output directory for results
        self.output_dir = Path("data/streaming_results")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def initialize_spark(self):
        """Initialize Spark session with Kafka support"""
        self.spark = SparkSession.builder \
            .appName("Reddit-Streaming-Pipeline") \
            .master("spark://localhost:7077") \
            .config("spark.driver.host", "host.docker.internal") \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.ui.port", "4041") \
            .config("spark.pyspark.python", "python3") \
            .config("spark.pyspark.driver.python", "python3") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .getOrCreate()
        
        # Set log level to reduce noise
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized successfully")
    
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
    
    def create_kafka_stream(self):
        """Create Kafka streaming DataFrame"""
        # Define the schema for Reddit data
        reddit_schema = StructType([
            StructField("id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("author", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("created_utc", DoubleType(), True),
            StructField("url", StringType(), True),
            StructField("num_comments", IntegerType(), True)
        ])
        
        # Read from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "reddit-morocco") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON data
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), reddit_schema).alias("data")
        ).select("data.*")
        
        return parsed_df
    
    def process_batch(self, batch_df, batch_id):
        """
        Process each batch of streaming data
        
        Args:
            batch_df: Spark DataFrame for the current batch
            batch_id: Unique identifier for this batch
        """
        try:
            logger.info(f"Processing batch {batch_id}")
            
            # Convert to Pandas for processing
            pandas_df = batch_df.toPandas()
            
            if pandas_df.empty:
                logger.info(f"Batch {batch_id} is empty, skipping...")
                return
            
            logger.info(f"Batch {batch_id} contains {len(pandas_df)} records")
            
            # Combine title and text for analysis (using title as main text)
            texts = pandas_df['title'].fillna('').tolist()
            
            # Perform sentiment analysis
            logger.info(f"Performing sentiment analysis for batch {batch_id}")
            sentiment_results = self.analyze_sentiment_batch(texts)
            
            # Add sentiment results to DataFrame
            pandas_df['sentiment'] = [r['sentiment'] for r in sentiment_results]
            pandas_df['sentiment_score'] = [r['score'] for r in sentiment_results]
            
            # Generate embeddings for topic modeling
            logger.info(f"Generating embeddings for batch {batch_id}")
            embeddings = self.generate_embeddings_batch(texts)
            pandas_df['embeddings'] = embeddings
            
            # Add processing metadata
            pandas_df['batch_id'] = batch_id
            pandas_df['processed_timestamp'] = pd.Timestamp.now()
            
            # Save batch results
            batch_output_path = self.output_dir / f"batch_{batch_id}.json"
            
            # Prepare output format (excluding embeddings for readability)
            output_data = pandas_df.drop('embeddings', axis=1).to_dict('records')
            
            with open(batch_output_path, 'w') as f:
                json.dump(output_data, f, indent=2, default=str)
            
            logger.info(f"Batch {batch_id} results saved to {batch_output_path}")
            
            # Save embeddings separately for potential topic modeling
            embeddings_output_path = self.output_dir / f"embeddings_batch_{batch_id}.json"
            embeddings_data = {
                'batch_id': batch_id,
                'embeddings': embeddings,
                'texts': texts,
                'post_ids': pandas_df['id'].tolist()
            }
            
            with open(embeddings_output_path, 'w') as f:
                json.dump(embeddings_data, f, indent=2)
            
            # Print summary statistics
            sentiment_counts = pandas_df['sentiment'].value_counts()
            logger.info(f"Batch {batch_id} sentiment distribution: {sentiment_counts.to_dict()}")
            
        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {str(e)}")
    
    def start_streaming(self):
        """Start the streaming pipeline"""
        try:
            # Initialize components
            self.initialize_spark()
            self.initialize_models()
            
            # Create streaming DataFrame
            streaming_df = self.create_kafka_stream()
            
            logger.info("Starting Reddit streaming pipeline...")
            
            # Start the streaming query
            query = streaming_df.writeStream \
                .foreachBatch(self.process_batch) \
                .outputMode("append") \
                .trigger(processingTime='30 seconds') \
                .option("checkpointLocation", str(self.output_dir / "checkpoints")) \
                .start()
            
            logger.info("Streaming pipeline started. Waiting for data...")
            logger.info("Press Ctrl+C to stop the pipeline")
            
            # Wait for termination
            query.awaitTermination()
            
        except KeyboardInterrupt:
            logger.info("Streaming pipeline stopped by user")
        except Exception as e:
            logger.error(f"Error in streaming pipeline: {str(e)}")
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")

def main():
    """Main function to run the streaming pipeline"""
    processor = RedditStreamProcessor()
    processor.start_streaming()

if __name__ == "__main__":
    main()
