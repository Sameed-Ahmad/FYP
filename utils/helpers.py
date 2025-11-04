"""
Helper Utilities
Common utility functions and classes.
"""

from typing import Dict, Any, Optional, List
from collections import OrderedDict
import logging
import re

logger = logging.getLogger(__name__)


class QueryCache:
    """LRU Cache for query results"""
    
    def __init__(self, maxsize: int = 50):
        """
        Initialize cache with max size.
        
        Args:
            maxsize: Maximum number of items to cache
        """
        self.cache: OrderedDict = OrderedDict()
        self.maxsize = maxsize
    
    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve item from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None
        """
        if key in self.cache:
            # Move to end (most recently used)
            self.cache.move_to_end(key)
            return self.cache[key]
        return None
    
    def set(self, key: str, value: Any) -> None:
        """
        Store item in cache.
        
        Args:
            key: Cache key
            value: Value to cache
        """
        if key in self.cache:
            # Update existing
            self.cache.move_to_end(key)
        else:
            # Add new
            if len(self.cache) >= self.maxsize:
                # Remove least recently used
                self.cache.popitem(last=False)
        
        self.cache[key] = value
    
    def clear(self) -> None:
        """Clear all cached items"""
        self.cache.clear()
    
    def __len__(self) -> int:
        """Get cache size"""
        return len(self.cache)


def log_query(query: str, user_id: str = None, success: bool = True, 
              row_count: int = 0, execution_time: float = 0) -> None:
    """
    Log query execution for analytics.
    
    Args:
        query: SQL query
        user_id: User identifier
        success: Whether query succeeded
        row_count: Number of rows returned
        execution_time: Execution time in seconds
    """
    logger.debug(
        f"Query: {query[:100]}... | User: {user_id} | "
        f"Success: {success} | Rows: {row_count} | Time: {execution_time:.2f}s"
    )


def sanitize_input(user_input: str) -> str:
    """
    Sanitize user input.
    
    Args:
        user_input: Raw user input
        
    Returns:
        Sanitized input
    """
    if not user_input:
        return ""
    
    # Remove excess whitespace
    cleaned = " ".join(user_input.split())
    
    # Remove potentially dangerous characters
    cleaned = re.sub(r'[;<>]', '', cleaned)
    
    return cleaned.strip()


def truncate_text(text: str, max_length: int = 100, suffix: str = "...") -> str:
    """
    Truncate text to max length.
    
    Args:
        text: Text to truncate
        max_length: Maximum length
        suffix: Suffix to add when truncated
        
    Returns:
        Truncated text
    """
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix


def format_error_message(error: Exception) -> str:
    """
    Format error message for user display.
    
    Args:
        error: Exception object
        
    Returns:
        User-friendly error message
    """
    error_str = str(error).lower()
    
    if 'connection' in error_str:
        return "Database connection error. Please check if the database is running."
    elif 'timeout' in error_str:
        return "Query timed out. Please try a simpler query."
    elif 'syntax' in error_str:
        return "SQL syntax error. Please rephrase your question."
    elif 'permission' in error_str:
        return "Permission denied. You don't have access to this data."
    else:
        return f"An error occurred: {str(error)}"


def parse_natural_language_intent(query: str) -> Dict[str, Any]:
    """
    Parse natural language query to detect intent.
    
    Args:
        query: Natural language query
        
    Returns:
        Dictionary with intent information
    """
    query_lower = query.lower()
    
    intent = {
        'action': 'retrieve',
        'aggregation': False,
        'filtering': False,
        'sorting': False,
        'limit': None,
        'comparison': False
    }
    
    # Detect aggregation
    if any(word in query_lower for word in ['count', 'sum', 'average', 'avg', 'total', 'min', 'max']):
        intent['aggregation'] = True
        intent['action'] = 'aggregate'
    
    # Detect filtering
    if any(word in query_lower for word in ['where', 'filter', 'from', 'in', 'with']):
        intent['filtering'] = True
    
    # Detect sorting
    if any(word in query_lower for word in ['top', 'bottom', 'best', 'worst', 'highest', 'lowest', 'latest', 'oldest']):
        intent['sorting'] = True
        intent['action'] = 'top_n'
        
        # Extract limit
        match = re.search(r'(top|first|last)\s+(\d+)', query_lower)
        if match:
            intent['limit'] = int(match.group(2))
    
    # Detect comparison
    if any(word in query_lower for word in ['compare', 'vs', 'versus', 'between', 'difference']):
        intent['comparison'] = True
        intent['action'] = 'compare'
    
    # Detect count
    if 'count' in query_lower or 'how many' in query_lower:
        intent['action'] = 'count'
    
    return intent