# Reddit Community Radar - Streaming Pipeline

A real-time streaming pipeline that analyzes Reddit data using Apache Spark, Kafka, and advanced NLP techniques including sentiment analysis and topic modeling.

## Architecture

The pipeline consists of several components:

1. **Reddit Producer** (`kafka/reddit-producer.py`) - Streams Reddit posts to Kafka
2. **Streaming Pipeline** (`spark/streaming-pipeline.py`) - Processes data in real-time using Spark Streaming
3. **Sentiment Analysis** - Analyzes sentiment using CardiffNLP's Twitter RoBERTa model
4. **Topic Modeling** - Generates embeddings for future topic modeling
5. **Kafka** - Message broker for real-time data streaming
6. **Apache Spark** - Distributed computing framework for data processing

## Features

- **Real-time Reddit data streaming** from any subreddit
- **Sentiment analysis** using state-of-the-art transformer models
- **Embedding generation** for topic modeling and similarity analysis
- **Scalable architecture** using Docker and Apache Spark
- **Fault-tolerant processing** with Kafka and Spark checkpointing
- **Structured data output** in JSON format for further analysis

## Prerequisites

- Docker and Docker Compose
- Python 3.12
- Reddit API credentials (client_id and client_secret)

## Quick Start

### 1. Setup

Clone the repository and navigate to the project directory:

```bash
git clone <repository-url>
cd Reddit-comm-radar
```

### 2. Start the Infrastructure

Start Kafka, Zookeeper, and Spark cluster:

```bash
docker-compose up -d
```

Wait for all services to be ready (this may take a few minutes).

### 3. Create Kafka Topic

Create the required Kafka topic:

```bash
python kafka/setup_topic.py
```

### 4. Start the Reddit Producer

In a new terminal, start streaming Reddit data:

```bash
python kafka/reddit-producer.py
```

### 5. Start the Streaming Pipeline

In another terminal, start the Spark streaming pipeline:

```bash
python spark/streaming-pipeline.py
```

## Configuration

### Reddit API Setup

Update the Reddit API credentials in `kafka/reddit-producer.py`:

```python
reddit = praw.Reddit(
    client_id="your_client_id",
    client_secret="your_client_secret",
    user_agent="Reddit Kafka Producer"
)
```

### Subreddit Configuration

Change the target subreddit in `kafka/reddit-producer.py`:

```python
producer = RedditProducer(subreddit_name="Morocco", topic_name="reddit-morocco")
```

## Output

The streaming pipeline generates several types of output:

### 1. Processed Batches
- Location: `data/streaming_results/batch_{id}.json`
- Contains: Reddit post data with sentiment analysis results
- Format:
```json
[
  {
    "id": "post_id",
    "title": "Post title",
    "author": "username",
    "score": 10,
    "sentiment": "positive",
    "sentiment_score": 0.85,
    "batch_id": 1,
    "processed_timestamp": "2024-01-01T12:00:00"
  }
]
```

### 2. Embeddings
- Location: `data/streaming_results/embeddings_batch_{id}.json`
- Contains: Text embeddings for topic modeling
- Format:
```json
{
  "batch_id": 1,
  "embeddings": [[0.1, 0.2, ...], ...],
  "texts": ["Post title 1", "Post title 2", ...],
  "post_ids": ["id1", "id2", ...]
}
```

## Services and Ports

- **Kafka**: `localhost:9092`
- **Zookeeper**: `localhost:2181`
- **Spark Master UI**: `localhost:8080`
- **Spark Application UI**: `localhost:4040`

## Monitoring

### Check Kafka Topics
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Monitor Kafka Messages
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic reddit-morocco --from-beginning
```

### View Spark Jobs
Visit `http://localhost:8080` for Spark Master UI and `http://localhost:4040` for application UI.

## Processing Details

### Sentiment Analysis
- **Model**: cardiffnlp/twitter-roberta-base-sentiment
- **Labels**: negative, neutral, positive
- **Output**: Sentiment label and confidence score

### Embedding Generation
- **Model**: all-MiniLM-L6-v2 (SentenceTransformers)
- **Dimension**: 384
- **Purpose**: Future topic modeling and similarity analysis

### Batch Processing
- **Trigger**: Every 30 seconds
- **Processing**: Convert Spark DataFrame to Pandas for ML operations
- **Checkpointing**: Automatic fault tolerance with Spark checkpoints

## Scaling

### Horizontal Scaling
Add more Spark workers by modifying `docker-compose.yml`:

```yaml
spark-worker-2:
  build: .
  container_name: spark-worker-2
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077
    - SPARK_WORKER_MEMORY=2G
    - SPARK_WORKER_CORES=4
```

### Kafka Partitioning
Increase partitions for better parallelism:

```python
# In kafka/setup_topic.py
topics = [
    {
        'name': 'reddit-morocco',
        'partitions': 6,  # Increase partitions
        'replication_factor': 1
    }
]
```

## Troubleshooting

### Common Issues

1. **Kafka Connection Failed**
   - Ensure Kafka is running: `docker-compose ps`
   - Check Kafka logs: `docker-compose logs kafka`

2. **Spark Connection Failed**
   - Verify Spark master is running: `docker-compose ps`
   - Check master UI at `localhost:8080`

3. **Reddit API Rate Limiting**
   - Increase delay in producer: `time.sleep(2)`
   - Use multiple Reddit API accounts

4. **Memory Issues**
   - Increase Spark worker memory in `docker-compose.yml`
   - Reduce batch size in streaming pipeline

### Logs

View logs for different components:

```bash
# Kafka logs
docker-compose logs kafka

# Spark master logs
docker-compose logs spark-master

# Spark worker logs
docker-compose logs spark-worker
```

## Extending the Pipeline

### Adding New Subreddits

1. Update producer configuration
2. Create new Kafka topics
3. Modify streaming pipeline to handle multiple topics

### Adding Custom NLP Models

1. Add model initialization in `initialize_models()`
2. Create processing functions
3. Update batch processing logic

### Custom Output Formats

Modify the `process_batch()` function in `streaming-pipeline.py` to change output format or add new destinations.

## Dependencies

See `requirements.txt` for complete list of Python dependencies.

Key libraries:
- **pyspark**: Apache Spark for Python
- **confluent-kafka**: Kafka client
- **praw**: Reddit API wrapper
- **transformers**: Hugging Face transformers
- **sentence-transformers**: Sentence embeddings
- **torch**: PyTorch for deep learning

## License

This project is licensed under the MIT License.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request 