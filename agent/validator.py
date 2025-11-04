"""
SQL Query Validator
Validates SQL queries for security and correctness.
"""

import re
from typing import List, Tuple
import logging

logger = logging.getLogger(__name__)


class QueryValidator:
    """Validates SQL queries for security and correctness"""
    
    # Dangerous SQL keywords that should be blocked
    DANGEROUS_KEYWORDS = [
        'DROP', 'DELETE', 'TRUNCATE', 'INSERT', 'UPDATE', 
        'ALTER', 'CREATE', 'REPLACE', 'GRANT', 'REVOKE',
        'EXEC', 'EXECUTE', 'DECLARE', 'SHUTDOWN',
        'xp_cmdshell', 'sp_executesql'
    ]
    
    # Allowed SQL keywords for SELECT queries
    ALLOWED_KEYWORDS = [
        'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 
        'OUTER', 'ON', 'AS', 'ORDER', 'BY', 'GROUP', 'HAVING',
        'LIMIT', 'OFFSET', 'DISTINCT', 'COUNT', 'SUM', 'AVG', 
        'MIN', 'MAX', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN',
        'IS', 'NULL', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'WITH',
        'ASC', 'DESC', 'CAST', 'EXTRACT', 'COALESCE', 'NULLIF'
    ]
    
    def validate(self, query: str) -> bool:
        """
        Validate SQL query for security and correctness.
        
        Args:
            query: SQL query string to validate
            
        Returns:
            True if valid, raises Exception if invalid
            
        Raises:
            Exception: If query is invalid with reason
        """
        if not query or not query.strip():
            raise Exception("Query is empty")
        
        query = query.strip()
        
        # Check for dangerous keywords
        self._check_dangerous_keywords(query)
        
        # Check if it's a SELECT query
        if not self._is_select_query(query):
            raise Exception("Only SELECT queries are allowed")
        
        # Check for SQL injection patterns
        self._check_sql_injection(query)
        
        # Check for multiple statements
        if self._has_multiple_statements(query):
            raise Exception("Multiple SQL statements are not allowed")
        
        logger.debug("Query validation passed")
        return True
    
    def _check_dangerous_keywords(self, query: str) -> None:
        """
        Check for dangerous SQL keywords.
        
        Args:
            query: SQL query string
            
        Raises:
            Exception: If dangerous keywords found
        """
        query_upper = query.upper()
        
        # Check for exact word matches (not substrings)
        for keyword in self.DANGEROUS_KEYWORDS:
            # Use word boundaries to match whole words only
            pattern = r'\b' + keyword + r'\b'
            if re.search(pattern, query_upper):
                raise Exception(
                    f"Dangerous keyword '{keyword}' is not allowed. "
                    f"Only SELECT queries are permitted."
                )
    
    def _is_select_query(self, query: str) -> bool:
        """
        Check if query is a SELECT query.
        
        Args:
            query: SQL query string
            
        Returns:
            True if SELECT query, False otherwise
        """
        query_upper = query.strip().upper()
        
        # Allow WITH (CTE) followed by SELECT
        if query_upper.startswith('WITH'):
            # Check if there's a SELECT after the CTE
            return 'SELECT' in query_upper
        
        # Standard SELECT query
        return query_upper.startswith('SELECT')
    
    def _check_sql_injection(self, query: str) -> None:
        """
        Check for common SQL injection patterns.
        
        Args:
            query: SQL query string
            
        Raises:
            Exception: If injection patterns detected
        """
        injection_patterns = [
            r"'\s*OR\s*'1'\s*=\s*'1",  # OR '1'='1'
            r"'\s*OR\s*1\s*=\s*1",      # OR 1=1
            r"--\s*$",                   # Comment at end
            r"/\*.*\*/",                 # Multi-line comment
            r";\s*DROP",                 # Statement chaining
            r";\s*DELETE",               # Statement chaining
            r"xp_cmdshell",              # Command execution
            r"sp_executesql",            # Dynamic SQL
        ]
        
        for pattern in injection_patterns:
            if re.search(pattern, query, re.IGNORECASE):
                raise Exception(
                    "Query contains potentially malicious patterns. "
                    "Please rephrase your query."
                )
    
    def _has_multiple_statements(self, query: str) -> bool:
        """
        Check if query contains multiple SQL statements.
        
        Args:
            query: SQL query string
            
        Returns:
            True if multiple statements detected
        """
        # Remove string literals to avoid false positives
        cleaned_query = re.sub(r"'[^']*'", "", query)
        
        # Check for semicolons (statement separators)
        # Allow trailing semicolon but not in the middle
        semicolons = cleaned_query.count(';')
        
        if semicolons > 1:
            return True
        
        if semicolons == 1 and not cleaned_query.strip().endswith(';'):
            return True
        
        return False
    
    def get_query_complexity(self, query: str) -> str:
        """
        Estimate query complexity.
        
        Args:
            query: SQL query string
            
        Returns:
            Complexity level: 'simple', 'moderate', or 'complex'
        """
        query_upper = query.upper()
        
        # Count complexity indicators
        complexity_score = 0
        
        # JOINs
        join_count = len(re.findall(r'\bJOIN\b', query_upper))
        complexity_score += join_count * 2
        
        # Subqueries
        subquery_count = query_upper.count('SELECT') - 1  # Subtract main SELECT
        complexity_score += subquery_count * 3
        
        # Aggregations
        aggregates = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']
        for agg in aggregates:
            complexity_score += query_upper.count(agg)
        
        # GROUP BY
        if 'GROUP BY' in query_upper:
            complexity_score += 2
        
        # HAVING
        if 'HAVING' in query_upper:
            complexity_score += 2
        
        # WITH (CTE)
        if query_upper.startswith('WITH'):
            complexity_score += 3
        
        # Determine complexity level
        if complexity_score == 0:
            return 'simple'
        elif complexity_score <= 5:
            return 'moderate'
        else:
            return 'complex'