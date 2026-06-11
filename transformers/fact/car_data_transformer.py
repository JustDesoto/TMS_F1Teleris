# transformers/fact/car_data_transformer.py
from typing import Dict, Any, List
from ..base_transformer import BaseTransformer
import logging

logger = logging.getLogger(__name__)


class CarDataTransformer(BaseTransformer):
    """
    Трансформирует телеметрию для ClickHouse.
    """
    
    def transform(self, raw_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        transformed = []
        
        for doc in raw_documents:
            driver_number = self._safe_get(doc, "driver_number")
            
            # Получаем datetime объект
            dt = self._parse_timestamp(self._safe_get(doc, "date"))
            
            dds_record = {
                "session_key": self.session_key,
                "driver_number": driver_number,
                "timestamp": dt,
                "date": dt.date() if dt else None,
                "hour": dt.hour if dt else None,
                "speed_kmh": self._safe_get(doc, "speed"),
                "rpm": self._safe_get(doc, "rpm"),
                "brake_percent": self._safe_get(doc, "brake"),
                "throttle_percent": self._safe_get(doc, "throttle"),
                "drs_open": bool(self._safe_get(doc, "drs", 0)),
                "gear": self._safe_get(doc, "n_gear"),
                **self._denormalize_driver(driver_number),
                **self._denormalize_session(),
                "is_speed_high": self._safe_get(doc, "speed", 0) > 300,
                "is_full_throttle": self._safe_get(doc, "throttle", 0) > 95,
                "is_braking": self._safe_get(doc, "brake", 0) > 10,
                "etl_hash": self._safe_get(doc, "etl_hash"),
                "transformed_at": self.transformed_at
            }
            transformed.append(dds_record)
        
        logger.info(f"Transformed {len(transformed)} car_data records for session {self.session_key}")
        return transformed
    
    def get_target_database(self) -> str:
        return "clickhouse"
    
    def get_table_name(self) -> str:
        return "fact_car_data"