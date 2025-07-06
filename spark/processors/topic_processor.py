"""
Topic modeling processor for Reddit data.
"""

import logging
import torch
import numpy as np
from pathlib import Path
from sentence_transformers import SentenceTransformer
from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer
from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)

class TopicProcessor(BaseProcessor):
    """Processor for topic modeling."""
    
    def __init__(self, output_dir: Path = None):
        """
        Initialize the topic processor.
        
        Args:
            output_dir: Directory for output files
        """
        super().__init__(output_dir)
        self.model = None
        self.embedding_model = None
        self.device = torch.device("cpu")  # Force CPU usage
        
    def initialize_models(self):
        """Initialize topic modeling and embedding models."""
        try:
            # Initialize embedding model
            self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
            self.embedding_model.to(self.device)
            logger.info("Embedding model initialized")
            
            # Initialize BERTopic model
            vectorizer_model = CountVectorizer(
                stop_words="english",
                min_df=2,
                max_df=0.85,
                ngram_range=(2,5)
            )
            
            self.model = BERTopic(
                embedding_model=self.embedding_model,
                vectorizer_model=vectorizer_model,
                min_topic_size=10,
                nr_topics=None,
                verbose=True
            )
            logger.info("Topic model initialized")
            
        except Exception as e:
            logger.error(f"Error initializing models: {str(e)}")
            raise
            
    def generate_embeddings(self, texts):
        """
        Generate embeddings for a list of texts.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            numpy.ndarray of embeddings
        """
        try:
            # Filter out empty texts
            valid_texts = [text if text and isinstance(text, str) else "" for text in texts]
            embeddings = self.embedding_model.encode(valid_texts)
            # Ensure embeddings are numpy array
            if isinstance(embeddings, list):
                embeddings = np.array(embeddings)
            elif isinstance(embeddings, torch.Tensor):
                embeddings = embeddings.cpu().numpy()
            return embeddings
        except Exception as e:
            logger.warning(f"Error generating embeddings: {str(e)}")
            # Return zero embeddings as numpy array
            return np.zeros((len(texts), 384))
            
    def fit_transform(self, texts, embeddings=None):
        """
        Fit the topic model and transform texts.
        
        Args:
            texts: List of texts to model
            embeddings: Pre-computed embeddings (optional)
            
        Returns:
            Tuple of (topics, probabilities)
        """
        if self.model is None:
            self.initialize_models()
            
        try:
            if embeddings is None:
                embeddings = self.generate_embeddings(texts)
            elif not isinstance(embeddings, np.ndarray):
                # Convert embeddings to numpy array if needed
                embeddings = np.array(embeddings)
                
            topics, probs = self.model.fit_transform(texts, embeddings=embeddings)
            logger.info(f"Successfully fit topic model on {len(texts)} documents")
            return topics, probs
            
        except Exception as e:
            logger.error(f"Error during topic modeling: {str(e)}")
            raise
            
    def get_topic_info(self):
        """
        Get information about the generated topics.
        
        Returns:
            DataFrame with topic information
        """
        if self.model is None:
            raise ValueError("Model not initialized or fit")
            
        return self.model.get_topic_info()
        
    def get_topic_keywords(self, topic_ids=None):
        """
        Get keywords for specified topics.
        
        Args:
            topic_ids: List of topic IDs (None for all topics)
            
        Returns:
            Dictionary mapping topic IDs to keywords
        """
        if self.model is None:
            raise ValueError("Model not initialized or fit")
            
        if topic_ids is None:
            topic_ids = sorted(set(self.model.topics_) - {-1})
            
        topic_keywords = {}
        for topic_id in topic_ids:
            topic_keywords[str(topic_id)] = [word for word, _ in self.model.get_topic(topic_id)]
            
        return topic_keywords 