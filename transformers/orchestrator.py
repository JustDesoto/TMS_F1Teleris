# transformers/orchestrator.py
"""
Оркестратор - координирует работу Transformer и Loader для трансформации данных
"""

from typing import Dict, Any, List
from datetime import datetime
import logging

from repositories.mongo_repository import MongoRepository
from repositories.pg_repository import PostgresRepository
from repositories.ch_repository import ClickHouseRepository

from .dds_transformer import DDSTransformer
from loaders.dds_loader import DDSLoader

logger = logging.getLogger(__name__)


class DDSTransformOrchestrator:
    """
    Оркестратор процесса трансформации данных из MongoDB в DDS.
    
    Этапы работы:
    1. Загружает raw данные из MongoDB
    2. Получает контексты (session_context, drivers_context)
    3. Создает DDSTransformer для преобразования данных
    4. Загружает трансформированные данные через DDSLoader
    """
    
    def __init__(
        self, 
        mongo_repo: MongoRepository, 
        pg_repo: PostgresRepository, 
        ch_repo: ClickHouseRepository
    ):
        """
        Args:
            mongo_repo: Репозиторий для чтения raw данных из MongoDB
            pg_repo: Репозиторий для записи в PostgreSQL
            ch_repo: Репозиторий для записи в ClickHouse
        """
        self.mongo = mongo_repo
        self.pg = pg_repo
        self.ch = ch_repo
        self.loader = DDSLoader(pg_repo, ch_repo)
        self._session_context_cache = {}
        self._drivers_context_cache = {}
    
    def process_session(self, session_key: int) -> Dict[str, int]:
        """
        Полная обработка сессии: Transform → Load
        
        Args:
            session_key: Ключ сессии в MongoDB
        
        Returns:
            Словарь с количеством обработанных записей по каждому типу
        """
        logger.info(f"Starting DDS transformation for session {session_key}")
        start_time = datetime.utcnow()
        
        # 1. Получаем контексты для денормализации
        session_context = self._get_session_context(session_key)
        drivers_context = self._get_drivers_context(session_key)
        
        # 2. Создаем трансформер с контекстами
        transformer = DDSTransformer(session_context, drivers_context)
        
        results = {}
        
        # 3. Трансформируем и загружаем данные в PostgreSQL
        results.update(self._process_postgres(session_key, transformer))
        
        # 4. Трансформируем и загружаем данные в ClickHouse
        results.update(self._process_clickhouse(session_key, transformer))
        
        elapsed = (datetime.utcnow() - start_time).total_seconds()
        total_records = sum(results.values())
        
        logger.info(f"DDS transformation completed for session {session_key}. "
                   f"Total records: {total_records}, Time: {elapsed:.2f}s")
        
        return results
    
    def _process_postgres(self, session_key: int, transformer: DDSTransformer) -> Dict[str, int]:
        """
        Обрабатывает данные для PostgreSQL: трансформация → загрузка
        
        Args:
            session_key: Ключ сессии
            transformer: Экземпляр DDSTransformer
        
        Returns:
            Словарь с количеством загруженных записей
        """
        results = {
            "meetings": 0,
            "sessions": 0,
            "drivers": 0,
            "starting_grid": 0,
            "session_results": 0,
        }
        
        # Meetings
        meetings_raw = self.mongo.find_by_filter("meetings", {"session_key": session_key})
        if meetings_raw:
            transformed = transformer.transform_meetings(meetings_raw)
            results["meetings"] = self.loader.load_meetings(transformed)
            logger.info(f"Loaded {results['meetings']} meetings to PostgreSQL")
        
        # Sessions
        sessions_raw = self.mongo.find_by_filter("sessions", {"session_key": session_key})
        if sessions_raw:
            transformed = transformer.transform_sessions(sessions_raw)
            results["sessions"] = self.loader.load_sessions(transformed)
            logger.info(f"Loaded {results['sessions']} sessions to PostgreSQL")
        
        # Drivers
        drivers_raw = self.mongo.find_by_filter("drivers", {"session_key": session_key})
        if drivers_raw:
            transformed = transformer.transform_drivers(drivers_raw)
            results["drivers"] = self.loader.load_drivers(transformed)
            logger.info(f"Loaded {results['drivers']} drivers to PostgreSQL")
        
        # Starting Grid
        grid_raw = self.mongo.find_by_filter("starting_grid", {"session_key": session_key})
        if grid_raw:
            transformed = transformer.transform_starting_grid(grid_raw)
            results["starting_grid"] = self.loader.load_starting_grid(transformed)
            logger.info(f"Loaded {results['starting_grid']} starting grid records to PostgreSQL")
        
        # Session Results
        results_raw = self.mongo.find_by_filter("session_result", {"session_key": session_key})
        if results_raw:
            transformed = transformer.transform_session_results(results_raw)
            results["session_results"] = self.loader.load_session_results(transformed)
            logger.info(f"Loaded {results['session_results']} session results to PostgreSQL")
        
        return results
    
    def _process_clickhouse(self, session_key: int, transformer: DDSTransformer) -> Dict[str, int]:
        """
        Обрабатывает данные для ClickHouse: трансформация → загрузка
        
        Args:
            session_key: Ключ сессии
            transformer: Экземпляр DDSTransformer
        
        Returns:
            Словарь с количеством загруженных записей
        """
        results = {
            "car_data": 0,
            "positions": 0,
            "laps": 0,
            "pit_stops": 0,
            "intervals": 0,
            "weather": 0,
            "race_control": 0,
            "overtakes": 0,
            "stints": 0,
            "location": 0,
        }
        
        # Car Data
        car_data_raw = self.mongo.find_by_filter("car_data", {"session_key": session_key})
        if car_data_raw:
            transformed = transformer.transform_car_data(car_data_raw)
            results["car_data"] = self.loader.load_car_data(transformed)
            logger.info(f"Loaded {results['car_data']} car_data records to ClickHouse")
        
        # Positions
        positions_raw = self.mongo.find_by_filter("position", {"session_key": session_key})
        if positions_raw:
            transformed = transformer.transform_positions(positions_raw)
            results["positions"] = self.loader.load_positions(transformed)
            logger.info(f"Loaded {results['positions']} position records to ClickHouse")
        
        # Laps
        laps_raw = self.mongo.find_by_filter("laps", {"session_key": session_key})
        if laps_raw:
            transformed = transformer.transform_laps(laps_raw)
            results["laps"] = self.loader.load_laps(transformed)
            logger.info(f"Loaded {results['laps']} lap records to ClickHouse")
        
        # Pit Stops
        pit_raw = self.mongo.find_by_filter("pit", {"session_key": session_key})
        if pit_raw:
            transformed = transformer.transform_pit_stops(pit_raw)
            results["pit_stops"] = self.loader.load_pit_stops(transformed)
            logger.info(f"Loaded {results['pit_stops']} pit stop records to ClickHouse")
        
        # Intervals
        intervals_raw = self.mongo.find_by_filter("intervals", {"session_key": session_key})
        if intervals_raw:
            transformed = transformer.transform_intervals(intervals_raw)
            results["intervals"] = self.loader.load_intervals(transformed)
            logger.info(f"Loaded {results['intervals']} interval records to ClickHouse")
        
        # Weather
        weather_raw = self.mongo.find_by_filter("weather", {"session_key": session_key})
        if weather_raw:
            transformed = transformer.transform_weather(weather_raw)
            results["weather"] = self.loader.load_weather(transformed)
            logger.info(f"Loaded {results['weather']} weather records to ClickHouse")
        
        # Race Control
        race_control_raw = self.mongo.find_by_filter("race_control", {"session_key": session_key})
        if race_control_raw:
            transformed = transformer.transform_race_control(race_control_raw)
            results["race_control"] = self.loader.load_race_control(transformed)
            logger.info(f"Loaded {results['race_control']} race control records to ClickHouse")
        
        # Overtakes
        overtakes_raw = self.mongo.find_by_filter("overtakes", {"session_key": session_key})
        if overtakes_raw:
            transformed = transformer.transform_overtakes(overtakes_raw)
            results["overtakes"] = self.loader.load_overtakes(transformed)
            logger.info(f"Loaded {results['overtakes']} overtake records to ClickHouse")
        
        # Stints
        stints_raw = self.mongo.find_by_filter("stints", {"session_key": session_key})
        if stints_raw:
            transformed = transformer.transform_stints(stints_raw)
            results["stints"] = self.loader.load_stints(transformed)
            logger.info(f"Loaded {results['stints']} stint records to ClickHouse")
        
        # Location
        location_raw = self.mongo.find_by_filter("location", {"session_key": session_key})
        if location_raw:
            transformed = transformer.transform_location(location_raw)
            results["location"] = self.loader.load_location(transformed)
            logger.info(f"Loaded {results['location']} location records to ClickHouse")
        
        return results
    
    def _get_session_context(self, session_key: int) -> Dict[str, Any]:
        """
        Получает контекст сессии из MongoDB для денормализации.
        
        Args:
            session_key: Ключ сессии
        
        Returns:
            Словарь с контекстом сессии
        """
        if session_key in self._session_context_cache:
            logger.debug(f"Using cached session context for {session_key}")
            return self._session_context_cache[session_key]
        
        # Загружаем сессию
        session = self.mongo.find_one("sessions", {"session_key": session_key})
        if not session:
            logger.warning(f"Session {session_key} not found in MongoDB")
            return {}
        
        # Загружаем meeting (этап)
        meeting_key = session.get("meeting_key")
        meeting = {}
        if meeting_key:
            meeting = self.mongo.find_one("meetings", {"meeting_key": meeting_key}) or {}
        
        # Формируем контекст
        context = {
            "session_key": session_key,
            "meeting_name": meeting.get("meeting_name"),
            "meeting_key": meeting_key,
            "circuit_name": session.get("circuit_short_name") or meeting.get("circuit_short_name"),
            "circuit_key": session.get("circuit_key") or meeting.get("circuit_key"),
            "session_type": session.get("session_type"),
            "session_name": session.get("session_name"),
            "year": session.get("year") or meeting.get("year"),
            "country": session.get("country_name") or meeting.get("country_name"),
            "location": meeting.get("location"),
            "date_start": session.get("date_start"),
            "date_end": session.get("date_end"),
            "gmt_offset": session.get("gmt_offset") or meeting.get("gmt_offset"),
        }
        
        self._session_context_cache[session_key] = context
        logger.debug(f"Built session context for {session_key}: {context.get('meeting_name')}")
        
        return context
    
    def _get_drivers_context(self, session_key: int) -> Dict[int, Dict[str, Any]]:
        """
        Получает контекст гонщиков для сессии.
        
        Args:
            session_key: Ключ сессии
        
        Returns:
            Словарь {driver_number: driver_info}
        """
        if session_key in self._drivers_context_cache:
            logger.debug(f"Using cached drivers context for {session_key}")
            return self._drivers_context_cache[session_key]
        
        # Загружаем всех гонщиков сессии
        drivers_raw = self.mongo.find_by_filter("drivers", {"session_key": session_key})
        drivers_context = {}
        
        for driver in drivers_raw:
            driver_number = driver.get("driver_number")
            if driver_number:
                drivers_context[driver_number] = {
                    "full_name": driver.get("full_name"),
                    "first_name": driver.get("first_name"),
                    "last_name": driver.get("last_name"),
                    "acronym": driver.get("name_acronym"),
                    "team_name": driver.get("team_name"),
                    "team_colour": driver.get("team_colour"),
                    "broadcast_name": driver.get("broadcast_name"),
                    "headshot_url": driver.get("headshot_url"),
                }
        
        self._drivers_context_cache[session_key] = drivers_context
        logger.debug(f"Built drivers context for session {session_key}: {len(drivers_context)} drivers")
        
        return drivers_context
    
    def process_session_light(self, session_key: int) -> Dict[str, int]:
        """
        Легкая версия - только справочные данные (без телеметрии).
        Полезно для быстрого тестирования.
        
        Args:
            session_key: Ключ сессии
        
        Returns:
            Словарь с количеством загруженных записей
        """
        logger.info(f"Starting LIGHT DDS transformation for session {session_key}")
        
        # Получаем контексты
        session_context = self._get_session_context(session_key)
        drivers_context = self._get_drivers_context(session_key)
        
        # Создаем трансформер
        transformer = DDSTransformer(session_context, drivers_context)
        
        # Только PostgreSQL данные (без телеметрии)
        results = self._process_postgres(session_key, transformer)
        
        logger.info(f"Light DDS transformation completed for session {session_key}: {results}")
        return results
    
    def is_session_transformed(self, session_key: int) -> bool:
        """
        Проверяет, была ли уже обработана сессия.
        
        Args:
            session_key: Ключ сессии
        
        Returns:
            True если сессия уже есть в PostgreSQL
        """
        session = self.pg.find_one("dim_session", {"session_key": session_key})
        return session is not None
    
    def clear_session(self, session_key: int) -> Dict[str, int]:
        """
        Удаляет все данные сессии из DDS (для перезаливки).
        
        Args:
            session_key: Ключ сессии
        
        Returns:
            Словарь с количеством удаленных записей
        """
        logger.warning(f"Clearing session {session_key} from DDS")
        
        deleted = {"postgresql": 0, "clickhouse": 0}
        
        # PostgreSQL таблицы
        pg_tables = [
            "dim_meeting_session", "dim_session", "dim_driver_session",
            "fact_starting_grid", "fact_session_result"
        ]
        
        for table in pg_tables:
            try:
                count = self.pg.delete(table, {"session_key": session_key})
                deleted["postgresql"] += count
            except Exception as e:
                logger.warning(f"Failed to delete from {table}: {e}")
        
        # ClickHouse таблицы
        ch_tables = [
            "fact_car_data", "fact_position", "fact_lap", "fact_pit_stop",
            "fact_interval", "fact_weather", "fact_race_control",
            "fact_overtake", "fact_stint", "fact_location"
        ]
        
        for table in ch_tables:
            try:
                count = self.ch.delete(table, {"session_key": session_key})
                deleted["clickhouse"] += count
            except Exception as e:
                logger.warning(f"Failed to delete from {table}: {e}")
        
        # Очищаем кеши
        if session_key in self._session_context_cache:
            del self._session_context_cache[session_key]
        if session_key in self._drivers_context_cache:
            del self._drivers_context_cache[session_key]
        
        logger.info(f"Cleared session {session_key}: {deleted}")
        return deleted