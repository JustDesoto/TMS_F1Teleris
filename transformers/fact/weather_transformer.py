# transformers/fact/weather_transformer.py
from typing import Dict, Any, List
from ..base_transformer import BaseTransformer
import logging

logger = logging.getLogger(__name__)


class WeatherTransformer(BaseTransformer):
    """
    Трансформирует погодные данные с денормализацией.
    Target: ClickHouse
    """
    
    def transform(self, raw_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        transformed = []
        
        for doc in raw_documents:
            timestamp = self._parse_timestamp(self._safe_get(doc, "date"))
            
            dds_record = {
                # Keys
                "session_key": self.session_key,
                "timestamp": timestamp,
                "date": timestamp.date() if timestamp else None,
                
                # Weather data
                "air_temperature": self._safe_get(doc, "air_temperature"),
                "track_temperature": self._safe_get(doc, "track_temperature"),
                "humidity": self._safe_get(doc, "humidity"),
                "pressure": self._safe_get(doc, "pressure"),
                "rainfall": bool(self._safe_get(doc, "rainfall", 0)),
                "wind_speed": self._safe_get(doc, "wind_speed"),
                "wind_direction": self._safe_get(doc, "wind_direction"),
                
                # Денормализация SESSION
                **self._denormalize_session(),
                
                # Вычисляемые поля
                "track_temp_air_diff": self._safe_get(doc, "track_temperature", 0) - self._safe_get(doc, "air_temperature", 0),
                "is_wet": bool(self._safe_get(doc, "rainfall", 0)) or self._safe_get(doc, "humidity", 0) > 80,
                
                # ETL
                "etl_hash": self._safe_get(doc, "etl_hash"),
                "transformed_at": self.transformed_at
            }
            transformed.append(dds_record)
        
        logger.info(f"Transformed {len(transformed)} weather records for session {self.session_key}")
        return transformed
    
    def get_target_database(self) -> str:
        return "clickhouse"
    
    def get_table_name(self) -> str:
        return "fact_weather"