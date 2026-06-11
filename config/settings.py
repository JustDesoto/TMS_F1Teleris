from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional

class Settings(BaseSettings):
    """Настройки приложения"""
    
    # MongoDB настройки
    mongo_host: str = Field(default="localhost", alias="MONGO_HOST")
    mongo_port: int = Field(default=27017, alias="MONGO_PORT")
    mongo_username: str = Field(default="admin", alias="MONGO_USERNAME")
    mongo_password: str = Field(default="f1password123", alias="MONGO_PASSWORD")
    mongo_database: str = Field(default="f1_pipeline", alias="MONGO_DATABASE")
    mongo_auth_source: str = Field(default="admin", alias="MONGO_AUTH_SOURCE")
    
    @property
    def mongodb_connection_string(self) -> str:
        """Формирует строку подключения к MongoDB"""
        return f"mongodb://{self.mongo_username}:{self.mongo_password}@{self.mongo_host}:{self.mongo_port}/?authSource={self.mongo_auth_source}"
    
    # OpenF1 API настройки
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
    
    # PostgreSQL настройки
    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_user: str = Field(default="f1_user", alias="POSTGRES_USER")
    postgres_password: str = Field(default="f1_password", alias="POSTGRES_PASSWORD")
    postgres_database: str = Field(default="f1_dds", alias="POSTGRES_DATABASE")
    
    @property
    def postgres_connection_string(self) -> str:
        """Формирует строку подключения к PostgreSQL"""
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_database}"
    
    # ClickHouse настройки
    clickhouse_host: str = Field(default="localhost", alias="CLICKHOUSE_HOST")
    clickhouse_port: int = Field(default=9000, alias="CLICKHOUSE_PORT")
    clickhouse_user: str = Field(default="default", alias="CLICKHOUSE_USER")
    clickhouse_password: str = Field(default="", alias="CLICKHOUSE_PASSWORD")
    clickhouse_database: str = Field(default="f1_analytics", alias="CLICKHOUSE_DATABASE")
    
    @property
    def clickhouse_connection_params(self) -> dict:
        """Возвращает параметры подключения к ClickHouse для клиента"""
        return {
            "host": self.clickhouse_host,
            "port": self.clickhouse_port,
            "user": self.clickhouse_user,
            "password": self.clickhouse_password,
            "database": self.clickhouse_database,
        }
    
    # ETL настройки
    etl_batch_size: int = Field(default=10000, alias="ETL_BATCH_SIZE")
    etl_max_retries: int = Field(default=3, alias="ETL_MAX_RETRIES")
    etl_request_timeout: int = Field(default=30, alias="ETL_REQUEST_TIMEOUT")
    
    # Логирование
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        populate_by_name = True
        extra = "ignore"


settings = Settings()