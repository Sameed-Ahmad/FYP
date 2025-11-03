import logging
from typing import Any, Dict
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def log_query(query: str, results: Dict[str, Any], user_id: str = "anonymous"):
    """
    Log query execution for monitoring and analytics
    
    Args:
        query: Natural language query
        results: Query execution results
        user_id: User identifier
    """
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "user_id": user_id,
        "query": query,
        "success": results.get("success", False),
        "row_count": results.get("metadata", {}).get("row_count", 0),
        "execution_time": results.get("metadata", {}).get("execution_time", "N/A")
    }
    
    logger.info(f"Query Log: {json.dumps(log_entry)}")


def sanitize_input(text: str) -> str:
    """
    Sanitize user input
    
    Args:
        text: User input text
        
    Returns:
        Sanitized text
    """
    if not text:
        return ""
    
    # Remove excessive whitespace
    text = " ".join(text.split())
    
    # Remove any potentially dangerous characters
    dangerous_chars = ['<', '>', '{', '}']
    for char in dangerous_chars:
        text = text.replace(char, '')
    
    return text.strip()


def truncate_text(text: str, max_length: int = 100) -> str:
    """
    Truncate text to specified length
    
    Args:
        text: Input text
        max_length: Maximum length
        
    Returns:
        Truncated text
    """
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."


def format_error_message(error: Exception) -> str:
    """
    Format error message for user display
    
    Args:
        error: Exception object
        
    Returns:
        User-friendly error message
    """
    error_str = str(error).lower()
    
    if "connection" in error_str:
        return "Unable to connect to the database. Please check your connection settings."
    elif "timeout" in error_str:
        return "Query took too long to execute. Try simplifying your query."
    elif "syntax" in error_str:
        return "The generated SQL query has a syntax error. Please rephrase your question."
    elif "permission" in error_str or "denied" in error_str:
        return "You don't have permission to access this data."
    else:
        return f"An error occurred: {str(error)}"


def parse_natural_language_intent(query: str) -> Dict[str, Any]:
    """
    Parse natural language query to understand intent
    
    Args:
        query: Natural language query
        
    Returns:
        Dictionary with intent information
    """
    query_lower = query.lower()
    
    intent = {
        "type": "unknown",
        "aggregate": False,
        "filter": False,
        "sort": False,
        "limit": False
    }
    
    # Check for aggregation
    aggregate_keywords = ["count", "sum", "average", "total", "mean", "max", "min"]
    if any(keyword in query_lower for keyword in aggregate_keywords):
        intent["aggregate"] = True
        intent["type"] = "aggregation"
    
    # Check for filtering
    filter_keywords = ["where", "with", "that have", "which", "when", "during"]
    if any(keyword in query_lower for keyword in filter_keywords):
        intent["filter"] = True
    
    # Check for sorting
    sort_keywords = ["sort", "order", "arrange", "top", "bottom", "highest", "lowest"]
    if any(keyword in query_lower for keyword in sort_keywords):
        intent["sort"] = True
    
    # Check for limit
    limit_keywords = ["first", "last", "top", "bottom", "limit"]
    if any(keyword in query_lower for keyword in limit_keywords):
        intent["limit"] = True
    
    # Determine query type if still unknown
    if intent["type"] == "unknown":
        if "show" in query_lower or "list" in query_lower or "get" in query_lower:
            intent["type"] = "retrieval"
        elif "how many" in query_lower:
            intent["type"] = "count"
        elif "compare" in query_lower or "difference" in query_lower:
            intent["type"] = "comparison"
    
    return intent


class QueryCache:
    """Simple in-memory cache for query results"""
    
    def __init__(self, max_size: int = 100):
        self.cache = {}
        self.max_size = max_size
        self.access_order = []
    
    def get(self, key: str) -> Any:
        """Get cached result"""
        if key in self.cache:
            # Update access order
            if key in self.access_order:
                self.access_order.remove(key)
            self.access_order.append(key)
            
            logger.info(f"Cache hit for query: {truncate_text(key)}")
            return self.cache[key]
        return None
    
    def set(self, key: str, value: Any):
        """Set cache entry"""
        if len(self.cache) >= self.max_size:
            # Remove least recently used
            lru_key = self.access_order.pop(0)
            del self.cache[lru_key]
        
        self.cache[key] = value
        self.access_order.append(key)
        logger.info(f"Cached result for query: {truncate_text(key)}")
    
    def clear(self):
        """Clear cache"""
        self.cache.clear()
        self.access_order.clear()
        logger.info("Cache cleared")