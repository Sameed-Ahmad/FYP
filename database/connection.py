from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from typing import List, Dict, Any
import logging
from config.settings import settings

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Manages database connections and query execution"""
    
    def __init__(self):
        self.engine = create_engine(
            settings.database_url,
            pool_pre_ping=True,
            echo=False  # Disable SQL echo to console
        )
        self.SessionLocal = sessionmaker(bind=self.engine)
    
    def execute_query(self, sql_query: str) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as list of dictionaries
        
        Args:
            sql_query: SQL query string to execute
            
        Returns:
            List of dictionaries containing query results
        """
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(sql_query))
                
                # Convert to list of dictionaries
                columns = result.keys()
                rows = []
                for row in result.fetchmany(settings.max_query_results):
                    rows.append(dict(zip(columns, row)))
                
                logger.info(f"Query executed successfully. Returned {len(rows)} rows.")
                return rows
                
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise Exception(f"Database query failed: {str(e)}")
    
    def get_schema_info(self) -> Dict[str, Any]:
        """
        Get database schema information
        
        Returns:
            Dictionary containing table and column information
        """
        inspector = inspect(self.engine)
        schema_info = {}
        
        for table_name in inspector.get_table_names():
            columns = []
            for column in inspector.get_columns(table_name):
                columns.append({
                    "name": column["name"],
                    "type": str(column["type"]),
                    "nullable": column["nullable"]
                })
            
            # Get primary keys
            pk_constraint = inspector.get_pk_constraint(table_name)
            primary_keys = pk_constraint.get("constrained_columns", [])
            
            # Get foreign keys
            foreign_keys = []
            for fk in inspector.get_foreign_keys(table_name):
                foreign_keys.append({
                    "columns": fk["constrained_columns"],
                    "referred_table": fk["referred_table"],
                    "referred_columns": fk["referred_columns"]
                })
            
            schema_info[table_name] = {
                "columns": columns,
                "primary_keys": primary_keys,
                "foreign_keys": foreign_keys
            }
        
        return schema_info
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            logger.info("Database connection successful")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            return False