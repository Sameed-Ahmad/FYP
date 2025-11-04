"""
Query Executor
Executes validated SQL queries and formats results.
"""

from typing import Dict, List, Any
from database.connection import DatabaseConnection
from agent.validator import QueryValidator
import time
import logging

logger = logging.getLogger(__name__)


class QueryExecutor:
    """Executes validated SQL queries"""
    
    def __init__(self, db_connection: DatabaseConnection):
        """
        Initialize query executor.
        
        Args:
            db_connection: Database connection instance
        """
        self.db_connection = db_connection
        self.validator = QueryValidator()
    
    def execute(self, query: str, format_type: str = 'dict') -> Dict[str, Any]:
        """
        Execute SQL query after validation.
        
        Args:
            query: SQL query to execute
            format_type: Output format ('dict', 'table', 'json', 'csv')
            
        Returns:
            Dictionary with execution results
        """
        try:
            # Validate query
            self.validator.validate(query)
            
            # Get complexity
            complexity = self.validator.get_query_complexity(query)
            
            # Execute query
            start_time = time.time()
            results = self.db_connection.execute_query(query)
            execution_time = time.time() - start_time
            
            return {
                'success': True,
                'data': results,
                'row_count': len(results),
                'execution_time': execution_time,
                'complexity': complexity,
                'query': query,
                'error': None
            }
            
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            return {
                'success': False,
                'data': None,
                'row_count': 0,
                'execution_time': 0,
                'complexity': 'unknown',
                'query': query,
                'error': str(e)
            }
    
    def format_results(self, results: List[Dict], format_type: str = 'table') -> str:
        """
        Format query results for display.
        
        Args:
            results: Query results
            format_type: Output format
            
        Returns:
            Formatted string
        """
        if not results:
            return "No results found."
        
        if format_type == 'json':
            import json
            return json.dumps(results, indent=2, default=str)
        
        elif format_type == 'csv':
            if not results:
                return ""
            
            import io
            import csv
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
            return output.getvalue()
        
        else:  # table format (default)
            from rich.table import Table
            from rich.console import Console
            
            table = Table(show_header=True, header_style="bold magenta")
            
            # Add columns
            if results:
                for key in results[0].keys():
                    table.add_column(str(key), style="cyan")
                
                # Add rows
                for row in results:
                    table.add_row(*[str(v) for v in row.values()])
            
            console = Console()
            with console.capture() as capture:
                console.print(table)
            
            return capture.get()