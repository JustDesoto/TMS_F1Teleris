"""
DAG ручного запуска ETL с параметром
"""

from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List
import requests
import logging

from config.settings import settings
from repositories.mongo_repository import MongoRepository
from repositories.pg_repository import PostgresRepository
from repositories.ch_repository import ClickHouseRepository
from extractors.orchestrator import ExtractOrchestrator
from transformers.orchestrator import DDSTransformOrchestrator

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'f1_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['alerts@yourcompany.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=4),
}

# Тестовый DAG для проверки подключений
@dag(
    dag_id='f1_etl_test',
    default_args=default_args,
    description='Test F1 ETL - проверка подключений',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['f1', 'etl', 'test'],
)
def f1_etl_test():
    """Тестовый DAG для проверки подключений"""
    
    @task
    def test_connections() -> Dict:
        """Проверяет подключения ко всем базам данных"""
        logger.info("Testing database connections...")
        
        results = {}
        
        # Test MongoDB
        try:
            mongo = MongoRepository(settings.mongodb_connection_string, settings.mongo_database)
            collections = mongo.db.list_collection_names()
            results['mongodb'] = f"OK ({len(collections)} collections)"
            mongo.close()
        except Exception as e:
            results['mongodb'] = f"ERROR: {e}"
        
        # Test PostgreSQL
        try:
            pg = PostgresRepository(settings.postgres_connection_string)
            pg.execute_query("SELECT 1 as test")
            results['postgresql'] = "OK"
            pg.close()
        except Exception as e:
            results['postgresql'] = f"ERROR: {e}"
        
        # Test ClickHouse
        try:
            ch = ClickHouseRepository(**settings.clickhouse_connection_params)
            ch.client.execute("SELECT 1")
            results['clickhouse'] = "OK"
            ch.close()
        except Exception as e:
            results['clickhouse'] = f"ERROR: {e}"
        
        logger.info(f"Connection test results: {results}")
        return results
    
    test_connections()
    
test_dag = f1_etl_test()
