import re
import logging
from typing import Tuple, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QueryValidator:
    """Validates SQL queries for safety and correctness"""
    
    # Dangerous SQL keywords that should be blocked
    DANGEROUS_KEYWORDS = [
        'DROP', 'DELETE', 'TRUNCATE', 'INSERT', 'UPDATE',
        'ALTER', 'CREATE', 'REPLACE', 'GRANT', 'REVOKE',
        'EXEC', 'EXECUTE', 'UNION', 'DECLARE', 'SHUTDOWN'
    ]
    
    # Allowed keywords for read-only queries
    ALLOWED_KEYWORDS = [
        'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT',
        'RIGHT', 'OUTER', 'ON', 'AS', 'ORDER', 'BY', 'GROUP',
        'HAVING', 'LIMIT', 'OFFSET', 'DISTINCT', 'COUNT',
        'SUM', 'AVG', 'MIN', 'MAX', 'AND', 'OR', 'NOT', 'IN',
        'LIKE', 'BETWEEN', 'IS', 'NULL', 'CASE', 'WHEN', 'THEN',
        'ELSE', 'END', 'WITH'
    ]
    
    def validate(self, sql_query: str) -> Tuple[bool, str]:
        """
        Validate SQL query for safety
        
        Args:
            sql_query: SQL query to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not sql_query or not sql_query.strip():
            return False, "Query is empty"
        
        # Check for dangerous keywords
        is_safe, message = self._check_dangerous_keywords(sql_query)
        if not is_safe:
            return False, message
        
        # Check if query starts with SELECT
        if not self._is_select_query(sql_query):
            return False, "Only SELECT queries are allowed"
        
        # Check for SQL injection patterns
        is_safe, message = self._check_sql_injection(sql_query)
        if not is_safe:
            return False, message
        
        # Check for multiple statements
        if self._has_multiple_statements(sql_query):
            return False, "Multiple SQL statements are not allowed"
        
        logger.info("Query validation passed")
        return True, "Query is valid"
    
    def _check_dangerous_keywords(self, query: str) -> Tuple[bool, str]:
        """Check for dangerous SQL keywords"""
        query_upper = query.upper()
        
        for keyword in self.DANGEROUS_KEYWORDS:
            # Use word boundaries to avoid false positives
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, query_upper):
                return False, f"Dangerous keyword detected: {keyword}"
        
        return True, ""
    
    def _is_select_query(self, query: str) -> bool:
        """Check if query starts with SELECT"""
        query_stripped = query.strip().upper()
        # Allow WITH clause before SELECT (for CTEs)
        return query_stripped.startswith('SELECT') or query_stripped.startswith('WITH')
    
    def _check_sql_injection(self, query: str) -> Tuple[bool, str]:
        """Check for common SQL injection patterns"""
        injection_patterns = [
            r"('|\")\s*(OR|AND)\s*('|\")\s*(=|<|>)",  # ' OR '1'='1
            r";\s*(DROP|DELETE|TRUNCATE|INSERT|UPDATE)",  # ; DROP TABLE
            r"--",  # SQL comments
            r"/\*.*?\*/",  # Multi-line comments
            r"\bxp_cmdshell\b",  # Command execution
            r"\bsp_executesql\b",  # Dynamic SQL execution
        ]
        
        for pattern in injection_patterns:
            try:
                if re.search(pattern, query, re.IGNORECASE):
                    return False, "Potential SQL injection pattern detected"
            except re.error as e:
                # If regex fails, log but continue
                logger.warning(f"Regex pattern error: {pattern}, {str(e)}")
                continue
        
        return True, ""
    
    def _has_multiple_statements(self, query: str) -> bool:
        """Check if query contains multiple SQL statements"""
        # Remove string literals to avoid false positives
        query_clean = re.sub(r"'[^']*'", "", query)
        
        # Count semicolons (excluding those in comments)
        semicolon_count = query_clean.count(';')
        
        # Allow one trailing semicolon
        if semicolon_count > 1:
            return True
        if semicolon_count == 1 and not query_clean.strip().endswith(';'):
            return True
        
        return False
    
    def get_query_complexity(self, query: str) -> str:
        """
        Estimate query complexity
        
        Returns:
            String indicating complexity level
        """
        query_upper = query.upper()
        
        join_count = query_upper.count('JOIN')
        subquery_count = query_upper.count('SELECT') - 1  # Subtract main SELECT
        aggregate_count = sum([
            query_upper.count('COUNT'),
            query_upper.count('SUM'),
            query_upper.count('AVG'),
            query_upper.count('MIN'),
            query_upper.count('MAX')
        ])
        
        complexity_score = join_count * 2 + subquery_count * 3 + aggregate_count
        
        if complexity_score == 0:
            return "Simple"
        elif complexity_score <= 5:
            return "Moderate"
        else:
            return "Complex"