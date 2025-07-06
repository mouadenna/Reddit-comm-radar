"""
Thread-safe message buffer for Kafka messages.
"""

import logging
from collections import deque
from threading import Lock

logger = logging.getLogger(__name__)

class MessageBuffer:
    """Thread-safe message buffer with size limit."""
    
    def __init__(self, max_size=1000):
        """
        Initialize the buffer.
        
        Args:
            max_size: Maximum number of messages to store
        """
        self.buffer = deque(maxlen=max_size)
        self.lock = Lock()
        self.max_size = max_size
    
    def add(self, message):
        """
        Add a message to the buffer.
        
        Args:
            message: Message to add
            
        Returns:
            Current buffer size
        """
        with self.lock:
            self.buffer.append(message)
            return len(self.buffer)
    
    def get_batch(self, batch_size):
        """
        Get a batch of messages from the buffer.
        
        Args:
            batch_size: Number of messages to get
            
        Returns:
            List of messages
        """
        messages = []
        with self.lock:
            while len(messages) < batch_size and self.buffer:
                messages.append(self.buffer.popleft())
        return messages
    
    def size(self):
        """
        Get current buffer size.
        
        Returns:
            Number of messages in buffer
        """
        with self.lock:
            return len(self.buffer)
    
    def clear(self):
        """Clear the buffer."""
        with self.lock:
            self.buffer.clear() 