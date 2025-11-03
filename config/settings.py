import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import logging

load_dotenv()

# Configure logging to only show warnings and errors by default
logging.basicConfig(
    level=logging.WARNING,  # Changed from INFO to WARNING
    format='%(levelname)s: %(message)s'
)

# Silence SQLAlchemy engine logs
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)


class Settings(BaseSettings):
    """Application configuration settings"""
    
    # Google Gemini (FREE tier available!)
    google_api_key: str = os.getenv("GOOGLE_API_KEY", "")
    
    # Database
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", "5432"))
    db_name: str = os.getenv("DB_NAME", "northwind")
    db_user: str = os.getenv("DB_USER", "postgres")
    db_password: str = os.getenv("DB_PASSWORD", "")
    
    # Application
    max_query_results: int = int(os.getenv("MAX_QUERY_RESULTS", "100"))
    enable_query_logging: bool = os.getenv("ENABLE_QUERY_LOGGING", "true").lower() == "true"
    
    @property
    def database_url(self) -> str:
        """Construct database connection URL"""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
    
    class Config:
        env_file = ".env"


settings = Settings()