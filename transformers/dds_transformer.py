# transformers/dds_transformer.py
"""
Чистый трансформер - только преобразует данные, не знает о БД
"""

from typing import Dict, Any, List, Optional
from .dds.meeting_transformer import MeetingTransformer
from .dds.session_transformer import SessionTransformer
from .dds.driver_transformer import DriverTransformer
from .dds.starting_grid_transformer import StartingGridTransformer
from .dds.session_result_transformer import SessionResultTransformer
from .fact.car_data_transformer import CarDataTransformer
from .fact.position_transformer import PositionTransformer
from .fact.lap_transformer import LapTransformer
from .fact.pit_stop_transformer import PitStopTransformer
from .fact.interval_transformer import IntervalTransformer
from .fact.weather_transformer import WeatherTransformer
from .fact.race_control_transformer import RaceControlTransformer
from .fact.overtake_transformer import OvertakeTransformer
from .fact.stint_transformer import StintTransformer
from .fact.location_transformer import LocationTransformer

import logging

logger = logging.getLogger(__name__)


class DDSTransformer:
    """
    Чистый трансформер - только преобразует данные из raw в DDS структуру.
    Не знает о базах данных, только возвращает готовые словари.
    """
    
    def __init__(self, session_context: Dict[str, Any], drivers_context: Dict[int, Dict[str, Any]]):
        """
        Args:
            session_context: Контекст сессии для денормализации
            drivers_context: Контекст гонщиков для денормализации
        """
        self.session_context = session_context
        self.drivers_context = drivers_context
        self.session_key = session_context.get("session_key")
    
    def transform_meetings(self, raw_documents: List[Dict]) -> List[Dict]:
        """Трансформирует встречи"""
        transformer = MeetingTransformer(self.session_key)
        return transformer.transform(raw_documents)
    
    def transform_sessions(self, raw_documents: List[Dict]) -> List[Dict]:
        """Трансформирует сессии"""
        transformer = SessionTransformer(self.session_key)
        return transformer.transform(raw_documents)
    
    def transform_drivers(self, raw_documents: List[Dict]) -> List[Dict]:
        """Трансформирует пилотов"""
        transformer = DriverTransformer(self.session_key)
        return transformer.transform(raw_documents)
    
    def transform_starting_grid(self, raw_documents: List[Dict]) -> List[Dict]:
        """Трансформирует стартовую решетку"""
        transformer = StartingGridTransformer(self.session_key)
        return transformer.transform(raw_documents)
    
    def transform_session_results(self, raw_documents: List[Dict]) -> List[Dict]:
        """Трансформирует результаты"""
        transformer = SessionResultTransformer(self.session_key)
        return transformer.transform(raw_documents)
    
    def transform_car_data(self, raw_documents: List[Dict]) -> List[Dict]:
        """Трансформирует телеметрию"""
        transformer = CarDataTransformer(self.session_key, self.session_context, self.drivers_context)
        return transformer.transform(raw_documents)
    
    def transform_positions(self, raw_documents: List[Dict]) -> List[Dict]:
        """Трансформирует позиции"""
        transformer = PositionTransformer(self.session_key, self.session_context, self.drivers_context)
        return transformer.transform(raw_documents)
    
    def transform_laps(self, raw_documents: List[Dict]) -> List[Dict]:
        """Трансформирует круги"""
        transformer = LapTransformer(self.session_key, self.session_context, self.drivers_context)
        return transformer.transform(raw_documents)
    
    def transform_pit_stops(self, raw_documents: List[Dict]) -> List[Dict]:
        """Трансформирует пит-стопы"""
        transformer = PitStopTransformer(self.session_key, self.session_context, self.drivers_context)
        return transformer.transform(raw_documents)
    
    def transform_intervals(self, raw_documents: List[Dict]) -> List[Dict]:
        """Трансформирует интервалы"""
        transformer = IntervalTransformer(self.session_key, self.session_context, self.drivers_context)
        return transformer.transform(raw_documents)
    
    def transform_weather(self, raw_documents: List[Dict]) -> List[Dict]:
        """Трансформирует погоду"""
        transformer = WeatherTransformer(self.session_key, self.session_context, self.drivers_context)
        return transformer.transform(raw_documents)
    
    def transform_race_control(self, raw_documents: List[Dict]) -> List[Dict]:
        """Трансформирует race control"""
        transformer = RaceControlTransformer(self.session_key, self.session_context, self.drivers_context)
        return transformer.transform(raw_documents)
    
    def transform_overtakes(self, raw_documents: List[Dict]) -> List[Dict]:
        """Трансформирует обгоны"""
        transformer = OvertakeTransformer(self.session_key, self.session_context, self.drivers_context)
        return transformer.transform(raw_documents)
    
    def transform_stints(self, raw_documents: List[Dict]) -> List[Dict]:
        """Трансформирует стенты"""
        transformer = StintTransformer(self.session_key, self.session_context, self.drivers_context)
        return transformer.transform(raw_documents)
    
    def transform_location(self, raw_documents: List[Dict]) -> List[Dict]:
        """Трансформирует GPS данные"""
        transformer = LocationTransformer(self.session_key, self.session_context, self.drivers_context)
        return transformer.transform(raw_documents)