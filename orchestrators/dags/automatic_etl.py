"""
DAG автоматического запуска по будущим сессиям
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
    dag_id='f1_etl_future',
    default_args=default_args,
    description='F1 ETL - только будущие сессии',
    schedule_interval='0 */6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['f1', 'etl', 'future'],
    max_active_runs=2,
    concurrency=3,
)
def f1_etl_future():
    """
    Автоматический ETL для будущих сессий.
    Запускается каждые 6 часов, находит НЕОБРАБОТАННЫЕ сессии
    и запускает ETL через 4 часа после окончания.
    """
    
    @task
    def get_unprocessed_future_sessions() -> List[Dict]:
        """Получает ТОЛЬКО НЕОБРАБОТАННЫЕ будущие сессии.
        
        Проверяет два источника уже известных сессий:
        - dim_session       — полностью обработанные (DDS)
        - etl_session_queue — уже поставленные в очередь (pending/processing/done)
        
        Новые сессии атомарно вставляются в очередь через ON CONFLICT DO NOTHING,
        что защищает от дублей при параллельных триггерах DAG.
        """
        current_year = datetime.now().year
        now_utc = utc_now()
        future_cutoff = now_utc + timedelta(days=15)

        pg = PostgresRepository(settings.postgres_connection_string)
        try:
            # Сессии уже в DDS
            try:
                processed = pg.execute_query("SELECT DISTINCT session_key FROM dim_session")
                processed_keys = {row[0] for row in processed} if processed else set()
                logger.info(f"Already in DDS: {len(processed_keys)}")
            except Exception as e:
                logger.warning(f"Could not query dim_session: {e}")
                processed_keys = set()

            # Сессии уже в очереди (любой статус, кроме 'failed')
            try:
                queued = pg.execute_query(
                    "SELECT session_key FROM etl_session_queue WHERE status != 'failed'"
                )
                queued_keys = {row[0] for row in queued} if queued else set()
                logger.info(f"Already queued: {len(queued_keys)}")
            except Exception as e:
                logger.warning(f"Could not query etl_session_queue: {e}")
                queued_keys = set()

            skip_keys = processed_keys | queued_keys

            # Собираем кандидатов
            candidates = []
            for year in [current_year, current_year + 1]:
                try:
                    year_sessions = fetch_all_sessions(year)
                    for s in year_sessions:
                        date_end = datetime.fromisoformat(s['date_end'].replace('Z', '+00:00'))
                        session_key = s['session_key']

                        if s.get('session_type') not in SESSION_TYPES:
                            continue
                        if session_key in skip_keys:
                            continue
                        if not (now_utc - timedelta(days=1) < date_end < future_cutoff):
                            continue

                        candidates.append(s)
                        logger.info(f"New candidate: {session_key} - {s.get('session_name')}")
                except Exception as e:
                    logger.error(f"Failed to fetch sessions for {year}: {e}")

            # Дедупликация (на случай пересечения годов)
            seen = set()
            unique_sessions = []
            for s in candidates:
                if s['session_key'] not in seen:
                    seen.add(s['session_key'])
                    unique_sessions.append(s)

            # Атомарно резервируем слоты в очереди
            for s in unique_sessions:
                try:
                    pg.execute(
                        """
                        INSERT INTO etl_session_queue (session_key, status, session_name, date_end)
                        VALUES (%s, 'pending', %s, %s)
                        ON CONFLICT (session_key) DO NOTHING
                        """,
                        (
                            s['session_key'],
                            s.get('session_name'),
                            s.get('date_end'),
                        )
                    )
                except Exception as e:
                    logger.error(f"Failed to queue session {s['session_key']}: {e}")

            logger.info(f"Queued {len(unique_sessions)} new sessions")
            return unique_sessions[:5]

        finally:
            pg.close()
    
    @task
    def create_wait_and_extract(session_info: Dict) -> Dict:
        from time import sleep

        session_key = session_info['session_key']
        date_end = datetime.fromisoformat(session_info['date_end'].replace('Z', '+00:00'))
        target_time = date_end + timedelta(hours=4)

        pg = PostgresRepository(settings.postgres_connection_string)
        try:
            pg.execute(
                """UPDATE etl_session_queue 
                SET status = 'processing', updated_at = NOW()
                WHERE session_key = %s""",
                (session_key,)
            )
        except Exception as e:
            logger.warning(f"Could not update queue status to processing: {e}")
        finally:
            pg.close()

        # Ждём target_time
        now_utc = utc_now()
        if now_utc < target_time:
            logger.info(f"Session {session_key}: waiting until {target_time}")
            while utc_now() < target_time:
                remaining = (target_time - utc_now()).total_seconds()
                if remaining > 0:
                    sleep(min(60, remaining))

        mongo = MongoRepository(settings.mongodb_connection_string, settings.mongo_database)
        pg = PostgresRepository(settings.postgres_connection_string)

        try:
            processed = pg.execute_query(
                "SELECT 1 FROM dim_session WHERE session_key = %s", (session_key,)
            )
            if processed:
                logger.info(f"Session {session_key} already in DDS, skipping")
                pg.execute(
                    "UPDATE etl_session_queue SET status='done', updated_at=NOW() WHERE session_key=%s",
                    (session_key,)
                )
                return {"session_key": session_key, "skipped": True}

            orchestrator = ExtractOrchestrator(mongo)
            if orchestrator.session_exists(session_key):
                logger.info(f"Session {session_key} already in MongoDB, skipping")
                return {"session_key": session_key, "skipped": True}

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
            try:
                pg.execute(
                    "UPDATE etl_session_queue SET status='failed', updated_at=NOW() WHERE session_key=%s",
                    (session_key,)
                )
            except Exception:
                pass
            return {"session_key": session_key, "error": str(e)}
        finally:
            mongo.close()
            pg.close()
    
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
            logger.info(f"Gold views refreshed for session {session_key}")
            return True
        except Exception as e:
            logger.error(f"Refresh failed: {e}")
            return False
        finally:
            pg.close()
    
    # Получаем список сессий и создаем динамические задачи
    sessions = get_unprocessed_future_sessions()
    
    extracted = create_wait_and_extract.expand(session_info=sessions)
    transformed = run_transform.expand(extract_result=extracted)
    refresh_views.expand(transform_result=transformed)
    
future_dag = f1_etl_future()
