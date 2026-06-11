"""
DAG ручного запуска ETL с параметром
"""

from airflow import DAG
from airflow.decorators import task, dag
from airflow.models.param import Param
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

SESSION_TYPES = ['Race', 'Qualifying', 'Sprint', 'Sprint Qualifying']


def fetch_all_sessions(year: int) -> List[Dict]:
    """Получает все сессии за год из OpenF1 API"""
    url = f"{settings.openf1_base_url}/sessions"
    response = requests.get(url, params={"year": year}, timeout=30, verify=False)
    response.raise_for_status()
    sessions = response.json()
    
    filtered = [s for s in sessions if s.get('session_type') in SESSION_TYPES]
    logger.info(f"Found {len(filtered)} sessions for year {year}")
    return filtered


def utc_now():
    """Возвращает текущее время в UTC с часовым поясом"""
    return datetime.now(timezone.utc)

@dag(
    dag_id='f1_etl_manual',
    default_args=default_args,
    description='Ручная перезаливка конкретной сессии',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['f1', 'etl', 'manual'],
    max_active_runs=1,
    concurrency=1,
    max_active_tasks=1,
    params={
        "session_key": Param(
            type="integer",
            description="Ключ сессии F1 (например 11280)"
        )
    }
)
def f1_etl_manual():
    """
    Ручной запуск для перезаливки конкретной сессии.
    
    Пример запуска:
    airflow dags trigger -c '{"session_key": 11280}' f1_etl_manual
    """
    
    @task
    def get_session_from_config(**context) -> Dict[str, Any]:
        """Получает session_key из конфига запуска"""
        conf = context['dag_run'].conf or {}
        session_key = conf.get('session_key')
        
        if not session_key:
            raise ValueError("❌ Не указан session_key! Используйте: --conf '{\"session_key\": 11280}'")
        
        logger.info(f"📅 Получен session_key: {session_key}")
        return {"session_key": session_key}
    
    @task
    def run_extract(session_config: Dict) -> Dict:
        """Извлекает данные из API в MongoDB"""
        session_key = session_config['session_key']
        logger.info(f"Starting extract for session {session_key}")
        
        mongo = MongoRepository(settings.mongodb_connection_string, settings.mongo_database)
        
        try:
            orchestrator = ExtractOrchestrator(mongo)
            results = orchestrator.process_session(
                session_key=session_key,
                fetch_car_data=False,
                fetch_positions=True,
                fetch_laps=True,
                fetch_pit_stops=True,
                fetch_intervals=True,
                fetch_weather=True,
                fetch_race_control=True,
                fetch_overtakes=True,
                fetch_stints=True,
                fetch_location=False
            )
            logger.info(f"Extract completed: {results}")
            return {"session_key": session_key, "extract_results": results}
            
        except Exception as e:
            logger.error(f"Extract failed: {e}")
            return {"session_key": session_key, "error": str(e)}
        finally:
            mongo.close()
    
    @task
    def run_transform(extract_result: Dict) -> Dict:
        """Трансформирует данные в DDS"""
        if extract_result.get("error"):
            logger.error(f"Skipping transform: {extract_result.get('error')}")
            return extract_result
        
        session_key = extract_result['session_key']
        logger.info(f"Starting transform for session {session_key}")
        
        mongo = MongoRepository(settings.mongodb_connection_string, settings.mongo_database)
        pg = PostgresRepository(settings.postgres_connection_string)
        ch = ClickHouseRepository(**settings.clickhouse_connection_params)
        
        try:
            orchestrator = DDSTransformOrchestrator(mongo, pg, ch)
            results = orchestrator.process_session(session_key)
            logger.info(f"Transform completed: {results}")
            return {"session_key": session_key, "transform_results": results}
            
        except Exception as e:
            logger.error(f"Transform failed: {e}")
            return {"session_key": session_key, "error": str(e)}
        finally:
            mongo.close()
            pg.close()
            ch.close()
    
    @task
    def refresh_views(transform_result: Dict) -> bool:
        """Обновляет материализованные представления"""
        if transform_result.get("error"):
            return False
        
        session_key = transform_result['session_key']
        logger.info(f"Refreshing gold views for session {session_key}")
        
        pg = PostgresRepository(settings.postgres_connection_string)
        
        try:
            pg.execute("SELECT refresh_all_gold_views_simple();")
            return True
        except Exception as e:
            logger.error(f"Refresh failed: {e}")
            return False
        finally:
            pg.close()
    
    # Пайплайн
    session_config = get_session_from_config()
    extracted = run_extract(session_config)
    transformed = run_transform(extracted)
    refresh_views(transformed)
    
    
manual_dag = f1_etl_manual()
