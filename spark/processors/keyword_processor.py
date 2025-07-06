"""
Keyword extraction processor for Reddit data.
"""

import logging
from pathlib import Path
from collections import Counter
from keybert import KeyBERT
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)

class KeywordProcessor(BaseProcessor):
    """Processor for keyword extraction."""
    
    def __init__(self, output_dir: Path = None):
        """
        Initialize the keyword processor.
        
        Args:
            output_dir: Directory for output files
        """
        super().__init__(output_dir)
        self.model = None
        self.setup_directories('wordclouds')
        
    def initialize_model(self):
        """Initialize the KeyBERT model."""
        try:
            self.model = KeyBERT(model="all-MiniLM-L6-v2")
            logger.info("KeyBERT model initialized")
        except Exception as e:
            logger.error(f"Error initializing KeyBERT model: {str(e)}")
            raise
            
    def extract_keywords(self, text, top_n=5, min_df=1, ngram_range=(1, 2)):
        """
        Extract keywords from a text.
        
        Args:
            text: Text to extract keywords from
            top_n: Number of top keywords to extract
            min_df: Minimum document frequency for words
            ngram_range: Range of n-grams to consider
            
        Returns:
            List of extracted keywords
        """
        if not text or not isinstance(text, str):
            return []
            
        if self.model is None:
            self.initialize_model()
            
        try:
            keywords = self.model.extract_keywords(
                text,
                keyphrase_ngram_range=ngram_range,
                stop_words='english',
                top_n=top_n,
                min_df=min_df
            )
            
            return [keyword for keyword, _ in keywords]
            
        except Exception as e:
            logger.warning(f"Error extracting keywords from text: {str(e)}")
            return []
            
    def extract_batch(self, texts, top_n=5):
        """
        Extract keywords for a batch of texts.
        
        Args:
            texts: List of texts to analyze
            top_n: Number of top keywords to extract per text
            
        Returns:
            List of keyword lists
        """
        if self.model is None:
            self.initialize_model()
            
        keywords_list = []
        
        for i, text in enumerate(texts):
            try:
                keywords = self.extract_keywords(text, top_n=top_n)
                keywords_list.append(keywords)
                
                if (i + 1) % 100 == 0 or (i + 1) == len(texts):
                    logger.info(f"Extracted keywords for {i + 1}/{len(texts)} texts")
                    
            except Exception as e:
                logger.warning(f"Error extracting keywords for text {i}: {str(e)}")
                keywords_list.append([])
                
        return keywords_list
        
    def create_word_cloud(self, keywords_list, batch_id, max_words=100):
        """
        Create a word cloud visualization from extracted keywords.
        
        Args:
            keywords_list: List of keyword lists
            batch_id: ID of the current batch
            max_words: Maximum number of words in the cloud
            
        Returns:
            Tuple of (image_path, plot_path)
        """
        try:
            # Flatten the list of keywords
            all_keywords = []
            for keywords in keywords_list:
                all_keywords.extend(keywords)
                
            # Count frequencies
            word_freq = Counter(all_keywords)
            
            # Create the WordCloud object
            wc = WordCloud(
                background_color="white",
                max_words=max_words,
                width=800,
                height=400,
                colormap="viridis",
                contour_width=1,
                contour_color="steelblue"
            )
            
            # Generate the word cloud
            wc.generate_from_frequencies(word_freq)
            
            # Save the word cloud image
            img_path = self.output_dir / "wordclouds" / f"wordcloud_{batch_id}.png"
            wc.to_file(str(img_path))
            
            # Create a plotly figure with the word cloud image
            plt.figure(figsize=(10, 5))
            plt.imshow(wc, interpolation="bilinear")
            plt.axis("off")
            
            # Save interactive plot
            plot_path = self.output_dir / "plots" / f"wordcloud_{batch_id}.html"
            plt.savefig(plot_path, format='html', bbox_inches='tight', pad_inches=0)
            plt.close()
            
            logger.info(f"Created word cloud visualization for batch {batch_id}")
            return img_path, plot_path
            
        except Exception as e:
            logger.error(f"Error creating word cloud visualization: {str(e)}")
            return None, None 