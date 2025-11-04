"""
Database Connection Manager
Handles PostgreSQL connectivity and query execution.
"""

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.pool import NullPool
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Manages database connections and query execution"""
    
    def __init__(self, settings=None):
        """
        Initialize database connection.
        
        Args:
            settings: Settings object with database configuration (optional for backward compatibility)
        """
        if settings is None:
            # Backward compatibility - load settings here
            from config.settings import Settings
            settings = Settings()
        
        self.settings = settings
        
        # Create SQLAlchemy engine with connection pooling
        self.engine = create_engine(
            settings.database_url,
            poolclass=NullPool,  # Disable pooling for simplicity
            echo=False,  # Set to True for SQL logging
            future=True
        )
        
        logger.info("Database engine created")
    
    def test_connection(self) -> bool:
        """
        Test database connectivity.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {str(e)}")
            return False
    
    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            List of dictionaries representing rows
            
        Raises:
            Exception: If query execution fails
        """
        try:
            with self.engine.connect() as conn:
                # Execute query
                result = conn.execute(text(query), params or {})
                
                # Fetch results
                rows = result.fetchall()
                
                # Convert to list of dicts
                if rows:
                    columns = result.keys()
                    data = [dict(zip(columns, row)) for row in rows]
                    
                    # Apply result limit
                    max_results = self.settings.max_query_results
                    if len(data) > max_results:
                        logger.warning(f"Query returned {len(data)} rows, limiting to {max_results}")
                        data = data[:max_results]
                    
                    return data
                else:
                    return []
                    
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise Exception(f"Query execution error: {str(e)}")
    
    def get_schema_info(self) -> Dict[str, Dict]:
        """
        Retrieve database schema information.
        
        Returns:
            Dictionary with table names as keys and schema info as values
        """
        try:
            inspector = inspect(self.engine)
            schema_info = {}
            
            # Get all table names
            table_names = inspector.get_table_names()
            
            for table_name in table_names:
                # Get columns
                columns = []
                for col in inspector.get_columns(table_name):
                    col_info = {
                        'name': col['name'],
                        'type': str(col['type']),
                        'nullable': col['nullable'],
                        'primary_key': False,
                        'foreign_key': None
                    }
                    columns.append(col_info)
                
                # Get primary keys
                pk = inspector.get_pk_constraint(table_name)
                if pk and pk.get('constrained_columns'):
                    for col in columns:
                        if col['name'] in pk['constrained_columns']:
                            col['primary_key'] = True
                
                # Get foreign keys
                fks = inspector.get_foreign_keys(table_name)
                for fk in fks:
                    for col in columns:
                        if col['name'] in fk['constrained_columns']:
                            col['foreign_key'] = {
                                'referred_table': fk['referred_table'],
                                'referred_column': fk['referred_columns'][0] if fk['referred_columns'] else None,
                                'constrained_columns': fk['constrained_columns']
                            }
                
                schema_info[table_name] = {
                    'columns': columns,
                    'primary_key': pk.get('constrained_columns', []) if pk else []
                }
            
            logger.info(f"Schema info retrieved: {len(schema_info)} tables")
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to retrieve schema info: {str(e)}")
            raise Exception(f"Schema retrieval error: {str(e)}")
    
    def close(self):
        """Close database connections"""
        if hasattr(self, 'engine'):
            self.engine.dispose()
            logger.info("Database connections closed")