# transformers/fact/lap_transformer.py
from typing import Dict, Any, List
from ..base_transformer import BaseTransformer
import logging

logger = logging.getLogger(__name__)


class LapTransformer(BaseTransformer):
    """
    Трансформирует данные о кругах с денормализацией.
    Target: ClickHouse
    """
    
    def transform(self, raw_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        transformed = []
        
        for doc in raw_documents:
            driver_number = self._safe_get(doc, "driver_number")
            timestamp = self._parse_timestamp(self._safe_get(doc, "date_start"))
            lap_duration = self._safe_get(doc, "lap_duration")
            
            dds_record = {
                # Keys
                "session_key": self.session_key,
                "driver_number": driver_number,
                "lap_number": self._safe_get(doc, "lap_number"),
                "date_start": timestamp,
                
                # Lap data
                "lap_duration": self._safe_get(doc, "lap_duration"),
                "sector1_duration": self._safe_get(doc, "duration_sector_1"),
                "sector2_duration": self._safe_get(doc, "duration_sector_2"),
                "sector3_duration": self._safe_get(doc, "duration_sector_3"),
                "speed_trap": self._safe_get(doc, "st_speed"),
                "speed_i1": self._safe_get(doc, "i1_speed"),
                "speed_i2": self._safe_get(doc, "i2_speed"),
                "is_pit_out_lap": self._safe_get(doc, "is_pit_out_lap", False),
                
                # Денормализация DRIVER
                **self._denormalize_driver(driver_number),
                
                # Денормализация SESSION
                **self._denormalize_session(),
                
                # Вычисляемые поля
                "lap_time_seconds": lap_duration,
                
                # ETL
                "etl_hash": self._safe_get(doc, "etl_hash"),
                "transformed_at": self.transformed_at
            }
            transformed.append(dds_record)
        
        logger.info(f"Transformed {len(transformed)} lap records for session {self.session_key}")
        return transformed
    
    def get_target_database(self) -> str:
        return "clickhouse"
    
    def get_table_name(self) -> str:
        return "fact_lap"