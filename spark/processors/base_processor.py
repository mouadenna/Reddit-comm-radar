"""
Base processor class for Reddit data analysis.
"""

import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BaseProcessor:
    """Base class for all processors."""
    
    def __init__(self, output_dir: Path = None):
        """
        Initialize the base processor.
        
        Args:
            output_dir: Base directory for output files
        """
        self.output_dir = output_dir or Path("data/streaming_results")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def setup_directories(self, *subdirs):
        """
        Set up output directories.
        
        Args:
            *subdirs: Subdirectories to create
        """
        for subdir in subdirs:
            dir_path = self.output_dir / subdir
            dir_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory: {dir_path}")
            
    def save_json(self, data, filepath: Path, indent: int = 2):
        """
        Save data as JSON.
        
        Args:
            data: Data to save
            filepath: Path to save to
            indent: JSON indentation
        """
        import json
        
        try:
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=indent, default=str)
            logger.info(f"Saved data to: {filepath}")
        except Exception as e:
            logger.error(f"Error saving data to {filepath}: {str(e)}")
            raise 