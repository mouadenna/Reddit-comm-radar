#!/usr/bin/env python3
"""
Script to create Kafka topics for the Reddit streaming pipeline
"""

import logging
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_topic(topic_name, num_partitions=1, replication_factor=1):
    """
    Create a Kafka topic if it doesn't exist
    
    Args:
        topic_name: Name of the topic to create
        num_partitions: Number of partitions for the topic
        replication_factor: Replication factor for the topic
    """
    
    # Configure the admin client
    admin_client = AdminClient({
        'bootstrap.servers': 'localhost:9092'
    })
    
    # Check if topic already exists
    try:
        metadata = admin_client.list_topics(timeout=10)
        if topic_name in metadata.topics:
            logger.info(f"Topic '{topic_name}' already exists")
            return True
    except KafkaException as e:
        logger.error(f"Failed to list topics: {str(e)}")
        return False
    
    # Create the topic
    topic = NewTopic(
        topic=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )
    
    try:
        # Create topic
        fs = admin_client.create_topics([topic])
        
        # Wait for the result
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                logger.info(f"Topic '{topic}' created successfully")
                return True
            except Exception as e:
                logger.error(f"Failed to create topic '{topic}': {str(e)}")
                return False
                
    except Exception as e:
        logger.error(f"Error creating topic: {str(e)}")
        return False

def wait_for_kafka():
    """Wait for Kafka to be ready"""
    max_retries = 30
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            admin_client = AdminClient({
                'bootstrap.servers': 'localhost:9092'
            })
            
            # Try to list topics to check if Kafka is ready
            metadata = admin_client.list_topics(timeout=5)
            logger.info("Kafka is ready!")
            return True
            
        except Exception as e:
            retry_count += 1
            logger.info(f"Waiting for Kafka... (attempt {retry_count}/{max_retries})")
            time.sleep(2)
    
    logger.error("Kafka is not ready after maximum retries")
    return False

def main():
    """Main function to set up Kafka topics"""
    logger.info("Setting up Kafka topics for Reddit streaming pipeline...")
    
    # Wait for Kafka to be ready
    if not wait_for_kafka():
        logger.error("Failed to connect to Kafka")
        return
    
    # Topics to create
    topics = [
        {
            'name': 'reddit-morocco',
            'partitions': 3,
            'replication_factor': 1
        }
    ]
    
    # Create topics
    success = True
    for topic_config in topics:
        result = create_topic(
            topic_config['name'],
            topic_config['partitions'],
            topic_config['replication_factor']
        )
        if not result:
            success = False
    
    if success:
        logger.info("All topics created successfully!")
    else:
        logger.error("Some topics failed to create")

if __name__ == "__main__":
    main() 