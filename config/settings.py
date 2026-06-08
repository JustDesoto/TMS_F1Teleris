from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    """Application settings"""
    
    mongo_host: str = Field(default="localhost", alias="MONGO_HOST")
    mongo_port: int = Field(default=27017, alias="MONGO_PORT")
    mongo_username: str = Field(default="admin", alias="MONGO_USERNAME")
    mongo_password: str = Field(default="f1password123", alias="MONGO_PASSWORD")
    mongo_database: str = Field(default="f1_pipeline", alias="MONGO_DATABASE")
    mongo_auth_source: str = Field(default="admin", alias="MONGO_AUTH_SOURCE")
    
    mongodb_connection_string: str = Field(
        default="mongodb://admin:f1password123@localhost:27017/?authSource=admin",
        alias="MONGODB_CONNECTION_STRING"
    )
    
    openf1_base_url: str = Field(
        default="https://api.openf1.org/v1",
        alias="OPENF1_BASE_URL",
        description="OpenF1 API base URL"
    )
    openf1_verify_ssl: bool = Field(
        default=True,
        alias="OPENF1_VERIFY_SSL",
        description="Verify SSL certificates"
    )
    openf1_timeout: int = Field(
        default=30,
        alias="OPENF1_TIMEOUT",
        description="Request timeout in seconds"
    )
    openf1_max_retries: int = Field(
        default=3,
        alias="OPENF1_MAX_RETRIES",
        description="Maximum number of retries"
    )
    
    etl_batch_size: int = Field(default=10000, alias="ETL_BATCH_SIZE")
    etl_max_retries: int = Field(default=3, alias="ETL_MAX_RETRIES")
    etl_request_timeout: int = Field(default=30, alias="ETL_REQUEST_TIMEOUT")
    
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        populate_by_name = True
        extra = "ignore"

settings = Settings()