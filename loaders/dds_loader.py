# loaders/dds_loader.py
"""
Чистый загрузчик - только сохраняет данные в БД
"""

from typing import Dict, Any, List
from repositories.pg_repository import PostgresRepository
from repositories.ch_repository import ClickHouseRepository
import logging

logger = logging.getLogger(__name__)


class DDSLoader:
    """
    Загрузчик данных в DDS слой (PostgreSQL + ClickHouse).
    Не знает о трансформации, только вставляет готовые данные.
    """
    
    def __init__(self, pg_repo: PostgresRepository, ch_repo: ClickHouseRepository):
        self.pg = pg_repo
        self.ch = ch_repo
    
    def load_meetings(self, records: List[Dict]) -> int:
        """Загружает встречи в PostgreSQL"""
        if not records:
            return 0
        return self.pg.insert_many("dim_meeting_session", records)
    
    def load_sessions(self, records: List[Dict]) -> int:
        """Загружает сессии в PostgreSQL"""
        if not records:
            return 0
        return self.pg.insert_many("dim_session", records)
    
    def load_drivers(self, records: List[Dict]) -> int:
        """Загружает пилотов в PostgreSQL"""
        if not records:
            return 0
        return self.pg.insert_many("dim_driver_session", records)
    
    def load_starting_grid(self, records: List[Dict]) -> int:
        """Загружает стартовую решетку в PostgreSQL"""
        if not records:
            return 0
        return self.pg.insert_many("fact_starting_grid", records)
    
    def load_session_results(self, records: List[Dict]) -> int:
        """Загружает результаты в PostgreSQL"""
        if not records:
            return 0
        return self.pg.insert_many("fact_session_result", records)
    
    def load_car_data(self, records: List[Dict]) -> int:
        """Загружает телеметрию в ClickHouse"""
        if not records:
            return 0
        return self.ch.insert_many("fact_car_data", records)
    
    def load_positions(self, records: List[Dict]) -> int:
        """Загружает позиции в ClickHouse"""
        if not records:
            return 0
        return self.ch.insert_many("fact_position", records)
    
    def load_laps(self, records: List[Dict]) -> int:
        """Загружает круги в ClickHouse"""
        if not records:
            return 0
        return self.ch.insert_many("fact_lap", records)
    
    def load_pit_stops(self, records: List[Dict]) -> int:
        """Загружает пит-стопы в ClickHouse"""
        if not records:
            return 0
        return self.ch.insert_many("fact_pit_stop", records)
    
    def load_intervals(self, records: List[Dict]) -> int:
        """Загружает интервалы в ClickHouse"""
        if not records:
            return 0
        return self.ch.insert_many("fact_interval", records)
    
    def load_weather(self, records: List[Dict]) -> int:
        """Загружает погоду в ClickHouse"""
        if not records:
            return 0
        return self.ch.insert_many("fact_weather", records)
    
    def load_race_control(self, records: List[Dict]) -> int:
        """Загружает race control в ClickHouse"""
        if not records:
            return 0
        return self.ch.insert_many("fact_race_control", records)
    
    def load_overtakes(self, records: List[Dict]) -> int:
        """Загружает обгоны в ClickHouse"""
        if not records:
            return 0
        return self.ch.insert_many("fact_overtake", records)
    
    def load_stints(self, records: List[Dict]) -> int:
        """Загружает стенты в ClickHouse"""
        if not records:
            return 0
        return self.ch.insert_many("fact_stint", records)
    
    def load_location(self, records: List[Dict]) -> int:
        """Загружает GPS данные в ClickHouse"""
        if not records:
            return 0
        return self.ch.insert_many("fact_location", records)