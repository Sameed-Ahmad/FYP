import logging
from typing import Dict, Any, List
from database.connection import DatabaseConnection
from agent.validator import QueryValidator
import json
from datetime import datetime, date

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QueryExecutor:
    """Executes validated SQL queries and formats results"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.validator = QueryValidator()
    
    def execute(self, sql_query: str) -> Dict[str, Any]:
        """
        Execute SQL query with validation
        
        Args:
            sql_query: SQL query to execute
            
        Returns:
            Dictionary containing results and metadata
        """
        # Validate query
        is_valid, error_message = self.validator.validate(sql_query)
        if not is_valid:
            logger.error(f"Query validation failed: {error_message}")
            return {
                "success": False,
                "error": error_message,
                "query": sql_query
            }
        
        # Get query complexity
        complexity = self.validator.get_query_complexity(sql_query)
        
        try:
            # Execute query
            start_time = datetime.now()
            results = self.db.execute_query(sql_query)
            end_time = datetime.now()
            
            execution_time = (end_time - start_time).total_seconds()
            
            return {
                "success": True,
                "results": results,
                "metadata": {
                    "row_count": len(results),
                    "execution_time": f"{execution_time:.3f}s",
                    "complexity": complexity,
                    "query": sql_query
                }
            }
            
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "query": sql_query
            }
    
    def format_results(self, results: List[Dict[str, Any]], format_type: str = "table") -> str:
        """
        Format query results for display
        
        Args:
            results: List of result dictionaries
            format_type: Output format (table, json, csv)
            
        Returns:
            Formatted string
        """
        if not results:
            return "No results found."
        
        if format_type == "json":
            return json.dumps(results, indent=2, default=str)
        
        elif format_type == "csv":
            return self._format_csv(results)
        
        else:  # table format
            return self._format_table(results)
    
    def _format_table(self, results: List[Dict[str, Any]]) -> str:
        """Format results as ASCII table"""
        if not results:
            return "No results"
        
        # Get column names
        columns = list(results[0].keys())
        
        # Calculate column widths
        col_widths = {}
        for col in columns:
            col_widths[col] = len(col)
            for row in results:
                val_len = len(str(row.get(col, "")))
                if val_len > col_widths[col]:
                    col_widths[col] = val_len
        
        # Build table
        lines = []
        
        # Header
        header = " | ".join(col.ljust(col_widths[col]) for col in columns)
        lines.append(header)
        lines.append("-" * len(header))
        
        # Rows
        for row in results:
            row_str = " | ".join(
                str(row.get(col, "")).ljust(col_widths[col]) 
                for col in columns
            )
            lines.append(row_str)
        
        return "\n".join(lines)
    
    def _format_csv(self, results: List[Dict[str, Any]]) -> str:
        """Format results as CSV"""
        if not results:
            return ""
        
        columns = list(results[0].keys())
        lines = [",".join(columns)]
        
        for row in results:
            values = [str(row.get(col, "")) for col in columns]
            # Escape values containing commas
            values = [f'"{v}"' if "," in v else v for v in values]
            lines.append(",".join(values))
        
        return "\n".join(lines)
    
    def get_summary_stats(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate summary statistics for numeric columns
        
        Args:
            results: Query results
            
        Returns:
            Dictionary with summary statistics
        """
        if not results:
            return {}
        
        stats = {}
        columns = list(results[0].keys())
        
        for col in columns:
            values = [row.get(col) for row in results]
            
            # Check if column is numeric
            numeric_values = []
            for val in values:
                if isinstance(val, (int, float)) and val is not None:
                    numeric_values.append(val)
            
            if numeric_values:
                stats[col] = {
                    "count": len(numeric_values),
                    "min": min(numeric_values),
                    "max": max(numeric_values),
                    "avg": sum(numeric_values) / len(numeric_values),
                    "sum": sum(numeric_values)
                }
        
        return stats