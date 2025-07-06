"""
Sentiment analysis processor for Reddit data.
"""

import logging
import torch
import numpy as np
from pathlib import Path
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)

class SentimentProcessor(BaseProcessor):
    """Processor for sentiment analysis."""
    
    def __init__(self, output_dir: Path = None):
        """
        Initialize the sentiment processor.
        
        Args:
            output_dir: Directory for output files
        """
        super().__init__(output_dir)
        self.model = None
        self.tokenizer = None
        self.device = torch.device("cpu")  # Force CPU usage
        self.sentiment_labels = {0: "negative", 1: "neutral", 2: "positive"}
        
    def initialize_model(self):
        """Initialize the sentiment analysis model."""
        try:
            model_name = "cardiffnlp/twitter-roberta-base-sentiment"
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
            self.model.to(self.device)
            logger.info(f"Sentiment model initialized on {self.device}")
        except Exception as e:
            logger.error(f"Error initializing sentiment model: {str(e)}")
            raise
            
    def analyze_sentiment(self, text):
        """
        Analyze sentiment for a single text.
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with sentiment and score
        """
        if not text or not isinstance(text, str):
            return {"sentiment": "neutral", "score": 0.5}
        
        try:
            # Truncate text if too long
            max_length = 512
            if len(text) > max_length * 4:
                text = text[:max_length * 4]
            
            # Tokenize and get sentiment
            inputs = self.tokenizer(text, return_tensors="pt", truncation=True, max_length=max_length).to(self.device)
            with torch.no_grad():
                outputs = self.model(**inputs)
            
            # Get probabilities
            probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
            probs = probs.cpu().numpy()[0]
            
            # Get predicted sentiment
            sentiment_idx = np.argmax(probs)
            sentiment = self.sentiment_labels[sentiment_idx]
            score = float(probs[sentiment_idx])
            
            return {"sentiment": sentiment, "score": score}
        
        except Exception as e:
            logger.warning(f"Error analyzing sentiment: {str(e)}")
            return {"sentiment": "neutral", "score": 0.5}
            
    def analyze_batch(self, texts):
        """
        Analyze sentiment for a batch of texts.
        
        Args:
            texts: List of texts to analyze
            
        Returns:
            List of sentiment results
        """
        if self.model is None:
            self.initialize_model()
            
        results = []
        for text in texts:
            result = self.analyze_sentiment(text)
            results.append(result)
            
        return results 