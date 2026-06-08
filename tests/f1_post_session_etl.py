# dags/f1_post_session_etl.py
"""
DAG для загрузки данных F1 после каждой сессии
Запускается автоматически после окончания гонки/квалификации/практики
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.time_sensor import TimeSensor
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import requests
import logging

# Настройка логгера
logger = logging.getLogger(__name__)

# Конфигурация по умолчанию
default_args = {
    'owner': 'f1_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['alerts@yourcompany.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=4),
}

def get_all_sessions_for_year(year: int, session_types: List[str] = None) -> List[Dict]:
    """
    Получает все сессии за год из OpenF1 API
    
    Args:
        year: год (2024, 2025)
        session_types: типы сессий ('Race', 'Qualifying', 'Practice 1', etc.)
                       Если None - возвращает все типы
    
    Returns:
        Список сессий с метаданными
    """
    if session_types is None:
        session_types = ['Race', 'Qualifying', 'Sprint', 'Sprint Qualifying', 
                        'Practice 1', 'Practice 2', 'Practice 3']
    
    url = f"https://api.openf1.org/v1/sessions?year={year}"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        all_sessions = response.json()
        
        # Фильтруем по типам сессий
        filtered = [s for s in all_sessions if s.get('session_type') in session_types]
        
        logger.info(f"Found {len(filtered)} sessions for year {year}")
        return filtered
        
    except Exception as e:
        logger.error(f"Failed to fetch sessions: {e}")
        return []

def create_etl_for_session(session: Dict, dag: DAG) -> PythonOperator:
    """
    Создаёт набор задач для одной сессии
    
    Args:
        session: словарь с данными сессии
        dag: родительский DAG
    
    Returns:
        PythonOperator для ETL
    """
    session_key = session['session_key']
    session_type = session['session_type']
    session_name = session['session_name']
    meeting_name = session.get('meeting_name', 'Unknown')
    date_end = datetime.fromisoformat(session['date_end'].replace('Z', '+00:00'))
    
    def run_etl(**context):
        """Функция ETL для сессии"""
        from your_etl_pipeline import F1ETLOrchestrator
        
        logger.info(f"Starting ETL for {meeting_name} - {session_name} (session: {session_key})")
        
        orchestrator = F1ETLOrchestrator()
        
        # Загружаем все данные по сессии
        orchestrator.process_session(
            session_key=session_key,
            session_type=session_type,
            meeting_name=meeting_name
        )
        
        logger.info(f"ETL completed for {meeting_name} - {session_name}")
        return f"Successfully processed {session_key}"
    
    # Создаём задачу с уникальным ID
    task_id = f"etl_{meeting_name.lower().replace(' ', '_')}_{session_type.lower().replace(' ', '_')}_{session_key}"
    
    return PythonOperator(
        task_id=task_id,
        python_callable=run_etl,
        dag=dag,
        # Не ретраить слишком долго, если данные ещё не готовы
        retries=2,
        retry_delay=timedelta(minutes=10),
    )

def create_wait_sensor(session: Dict, dag: DAG) -> TimeSensor:
    """
    Создаёт сенсор, который ждёт окончания сессии + buffer
    
    Args:
        session: словарь с данными сессии
        dag: родительский DAG
    
    Returns:
        TimeSensor, который просыпается в нужное время
    """
    session_key = session['session_key']
    session_type = session['session_type']
    meeting_name = session.get('meeting_name', 'Unknown')
    
    # Время окончания сессии
    date_end = datetime.fromisoformat(session['date_end'].replace('Z', '+00:00'))
    
    # Ждём окончания + 1 час (время на публикацию результатов)
    # Для гонки ждём 2 часа (больше данных для обработки)
    buffer_hours = 2 if session_type == 'Race' else 1
    target_time = date_end + timedelta(hours=buffer_hours)
    
    logger.info(f"Session {meeting_name} - {session_type} ends at {date_end}")
    logger.info(f"ETL will start at {target_time}")
    
    sensor_task_id = f"wait_{meeting_name.lower().replace(' ', '_')}_{session_type.lower().replace(' ', '_')}_{session_key}"
    
    # Сенсор, который ждёт до указанного времени
    return TimeSensor(
        task_id=sensor_task_id,
        target_time=target_time,
        dag=dag,
        # Если опоздали - всё равно запускаем
        # Если DAG запустился позже - не ждём
        mode='reschedule',  # Освобождает worker'а пока ждёт
    )

# Создаём DAG динамически
def create_post_session_dag():
    """
    Создаёт DAG, который динамически генерирует задачи для всех сессий
    """
    dag_id = 'f1_post_session_etl'
    schedule_interval = '@once'  # Запускаем один раз, а он сам разберётся
    # Или schedule_interval='0 0 * * *' - раз в день для обновления расписания
    
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        catchup=False,
        tags=['f1', 'formula1', 'etl'],
        max_active_runs=1,  # Только один запуск DAG одновременно
        description='ETL for F1 sessions after they finish',
    ) as dag:
        
        # Шаг 1: Получаем расписание сессий на 2024-2025 годы
        def fetch_and_create_tasks(**context):
            """Получает сессии и динамически создаёт задачи"""
            
            # Получаем сессии
            sessions_2024 = get_all_sessions_for_year(2024)
            sessions_2025 = get_all_sessions_for_year(2025)
            all_sessions = sessions_2024 + sessions_2025
            
            if not all_sessions:
                logger.error("No sessions found!")
                return
            
            logger.info(f"Creating tasks for {len(all_sessions)} sessions")
            
            # Сортируем по времени
            all_sessions.sort(key=lambda x: x['date_start'])
            
            # Создаём задачи
            tasks = []
            previous_task = None
            
            for session in all_sessions:
                # Создаём сенсор ожидания
                wait_sensor = create_wait_sensor(session, dag)
                
                # Создаём ETL задачу
                etl_task = create_etl_for_session(session, dag)
                
                # Связываем: ждём → ETL
                wait_sensor >> etl_task
                
                # Если есть предыдущая задача - связываем (опционально)
                # if previous_task:
                #     previous_task >> wait_sensor
                
                tasks.append(etl_task)
                previous_task = etl_task
            
            logger.info(f"Created {len(tasks)} ETL tasks")
            return f"Created {len(tasks)} tasks"
        
        # Главная задача, которая создаёт все остальные
        create_tasks = PythonOperator(
            task_id='create_session_tasks',
            python_callable=fetch_and_create_tasks,
            dag=dag,
        )
        
        # Задача для мониторинга (опционально)
        def monitor_progress(**context):
            """Логирует прогресс выполнения"""
            dag_run = context['dag_run']
            completed = dag_run.get_task_instances(state='success')
            total = len(dag_run.get_task_instances())
            logger.info(f"Progress: {len(completed)}/{total} tasks completed")
        
        monitor = PythonOperator(
            task_id='monitor_progress',
            python_callable=monitor_progress,
            dag=dag,
            trigger_rule='all_done'  # Запускаем в конце
        )
        
        create_tasks >> monitor
    
    return dag

# Регистрируем DAG
dag = create_post_session_dag()