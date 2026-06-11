"""
Оркестратор для процесса извлечения данных из OpenF1 API в MongoDB
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

from extractors.openf1_extractor import OpenF1Extractor
from repositories.mongo_repository import MongoRepository

logger = logging.getLogger(__name__)


class ExtractOrchestrator:
    """
    Оркестратор для извлечения данных из OpenF1 API и загрузки в MongoDB.
    Управляет всем процессом extract для сессии.
    """
    
    def __init__(
        self,
        mongo_repo: MongoRepository,
        extractor: Optional[OpenF1Extractor] = None
    ):
        """
        Args:
            mongo_repo: Репозиторий для работы с MongoDB
            extractor: Экстрактор для OpenF1 API (создается по умолчанию если не указан)
        """
        self.mongo = mongo_repo
        
        if extractor is None:
            from config.settings import settings
            self.extractor = OpenF1Extractor(
                base_url=settings.openf1_base_url,
                verify_ssl=settings.openf1_verify_ssl,
                timeout=settings.openf1_timeout,
                max_retries=settings.openf1_max_retries,
                request_delay_min=0.5,
                request_delay_max=1.5,
                validate_models=False
            )
        else:
            self.extractor = extractor
    
    def process_session(
        self,
        session_key: int,
        fetch_car_data: bool = True,
        fetch_positions: bool = True,
        fetch_laps: bool = True,
        fetch_pit_stops: bool = True,
        fetch_intervals: bool = True,
        fetch_weather: bool = True,
        fetch_race_control: bool = True,
        fetch_overtakes: bool = True,
        fetch_stints: bool = True,
        fetch_location: bool = True,
        driver_number: Optional[int] = None
    ) -> Dict[str, int]:
        """
        Загружает все данные для сессии в MongoDB.
        
        Args:
            session_key: Ключ сессии
            fetch_car_data: Загружать телеметрию
            fetch_positions: Загружать позиции
            fetch_laps: Загружать круги
            fetch_pit_stops: Загружать пит-стопы
            fetch_intervals: Загружать интервалы
            fetch_weather: Загружать погоду
            fetch_race_control: Загружать race control
            fetch_overtakes: Загружать обгоны
            fetch_stints: Загружать стенты
            fetch_location: Загружать локацию
            driver_number: Загружать только для конкретного гонщика
        
        Returns:
            Словарь с количеством загруженных записей по каждому типу
        """
        logger.info(f"Starting extract for session {session_key}")
        
        results = {
            "meetings": 0,
            "sessions": 0,
            "drivers": 0,
            "starting_grid": 0,
            "session_results": 0,
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
        
        # 1. Загружаем справочные данные (всегда)
        results.update(self._load_reference_data(session_key))
        
        # 2. Загружаем опциональные данные
        if fetch_car_data:
            results["car_data"] = self._load_car_data(session_key, driver_number)
        
        if fetch_positions:
            results["positions"] = self._load_positions(session_key, driver_number)
        
        if fetch_laps:
            results["laps"] = self._load_laps(session_key, driver_number)
        
        if fetch_pit_stops:
            results["pit_stops"] = self._load_pit_stops(session_key, driver_number)
        
        if fetch_intervals:
            results["intervals"] = self._load_intervals(session_key, driver_number)
        
        if fetch_weather:
            results["weather"] = self._load_weather(session_key)
        
        if fetch_race_control:
            results["race_control"] = self._load_race_control(session_key)
        
        if fetch_overtakes:
            results["overtakes"] = self._load_overtakes(session_key)
        
        if fetch_stints:
            results["stints"] = self._load_stints(session_key, driver_number)
        
        if fetch_location:
            results["location"] = self._load_location(session_key, driver_number)
        
        total_records = sum(results.values())
        logger.info(f"Extract completed for session {session_key}. "
                   f"Total records: {total_records}. Details: {results}")
        
        return results
    
    def _load_reference_data(self, session_key: int) -> Dict[str, int]:
        """Загружает справочные данные для сессии"""
        results = {}
        
        # Получаем информацию о сессии, чтобы знать meeting_key
        sessions = self.extractor.fetch_sessions(session_key=session_key)
        if sessions:
            results["sessions"] = len(self.mongo.save_many("sessions", sessions))
            session = sessions[0]
            meeting_key = session.get("meeting_key")
            
            # Загружаем meeting
            if meeting_key:
                meetings = self.extractor.fetch_meetings(meeting_key=meeting_key)
                if meetings:
                    results["meetings"] = len(self.mongo.save_many("meetings", meetings))
        
        # Загружаем пилотов
        drivers = self.extractor.fetch_drivers(session_key=session_key)
        if drivers:
            results["drivers"] = len(self.mongo.save_many("drivers", drivers))
        
        # Загружаем стартовую решетку
        starting_grid = self.extractor.fetch_starting_grid(session_key=session_key)
        if starting_grid:
            results["starting_grid"] = len(self.mongo.save_many("starting_grid", starting_grid))
        
        # Загружаем результаты
        session_results = self.extractor.fetch_session_result(session_key=session_key)
        if session_results:
            results["session_results"] = len(self.mongo.save_many("session_result", session_results))
        
        return results
    
    def _load_car_data(self, session_key: int, driver_number: Optional[int] = None) -> int:
        """Загружает телеметрию"""
        logger.info(f"Loading car_data for session {session_key}")
        car_data = self.extractor.fetch_car_data(
            session_key=session_key,
            driver_number=driver_number
        )
        if car_data:
            return len(self.mongo.save_many_batched("car_data", car_data, batch_size=10000))
        return 0
    
    def _load_positions(self, session_key: int, driver_number: Optional[int] = None) -> int:
        """Загружает позиции"""
        logger.info(f"Loading positions for session {session_key}")
        positions = self.extractor.fetch_positions(
            session_key=session_key,
            driver_number=driver_number
        )
        if positions:
            return len(self.mongo.save_many_batched("position", positions, batch_size=10000))
        return 0
    
    def _load_laps(self, session_key: int, driver_number: Optional[int] = None) -> int:
        """Загружает данные о кругах"""
        logger.info(f"Loading laps for session {session_key}")
        laps = self.extractor.fetch_laps(
            session_key=session_key,
            driver_number=driver_number
        )
        if laps:
            return len(self.mongo.save_many_batched("laps", laps, batch_size=10000))
        return 0
    
    def _load_pit_stops(self, session_key: int, driver_number: Optional[int] = None) -> int:
        """Загружает данные о пит-стопах"""
        logger.info(f"Loading pit stops for session {session_key}")
        pit_stops = self.extractor.fetch_pit_stops(
            session_key=session_key,
            driver_number=driver_number
        )
        if pit_stops:
            return len(self.mongo.save_many("pit", pit_stops))
        return 0
    
    def _load_intervals(self, session_key: int, driver_number: Optional[int] = None) -> int:
        """Загружает интервалы"""
        logger.info(f"Loading intervals for session {session_key}")
        intervals = self.extractor.fetch_intervals(
            session_key=session_key,
            driver_number=driver_number
        )
        if intervals:
            return len(self.mongo.save_many_batched("intervals", intervals, batch_size=10000))
        return 0
    
    def _load_weather(self, session_key: int) -> int:
        """Загружает погодные данные"""
        logger.info(f"Loading weather for session {session_key}")
        weather = self.extractor.fetch_weather(session_key=session_key)
        if weather:
            return len(self.mongo.save_many("weather", weather))
        return 0
    
    def _load_race_control(self, session_key: int) -> int:
        """Загружает события race control"""
        logger.info(f"Loading race control for session {session_key}")
        race_control = self.extractor.fetch_race_control(session_key=session_key)
        if race_control:
            return len(self.mongo.save_many("race_control", race_control))
        return 0
    
    def _load_overtakes(self, session_key: int) -> int:
        """Загружает данные об обгонах"""
        logger.info(f"Loading overtakes for session {session_key}")
        overtakes = self.extractor.fetch_overtakes(session_key=session_key)
        if overtakes:
            return len(self.mongo.save_many("overtakes", overtakes))
        return 0
    
    def _load_stints(self, session_key: int, driver_number: Optional[int] = None) -> int:
        """Загружает данные о стентах"""
        logger.info(f"Loading stints for session {session_key}")
        stints = self.extractor.fetch_stints(
            session_key=session_key,
            driver_number=driver_number
        )
        if stints:
            return len(self.mongo.save_many("stints", stints))
        return 0
    
    def _load_location(self, session_key: int, driver_number: Optional[int] = None) -> int:
        """Загружает GPS данные"""
        logger.info(f"Loading location for session {session_key}")
        location = self.extractor.fetch_location(
            session_key=session_key,
            driver_number=driver_number
        )
        if location:
            return len(self.mongo.save_many_batched("location", location, batch_size=10000))
        return 0
    
    def session_exists(self, session_key: int) -> bool:
        """Проверяет, загружена ли уже сессия в MongoDB"""
        session = self.mongo.find_one("sessions", {"session_key": session_key})
        return session is not None
    
    def get_session_info(self, session_key: int) -> Dict[str, Any]:
        """Получает информацию о сессии из API"""
        sessions = self.extractor.fetch_sessions(session_key=session_key)
        if sessions:
            return sessions[0]
        return {}
    
    def get_all_sessions_for_year(self, year: int) -> List[Dict[str, Any]]:
        """Получает все сессии за год"""
        return self.extractor.fetch_sessions(year=year)