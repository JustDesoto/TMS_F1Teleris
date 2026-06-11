# transformers/fact/race_control_transformer.py
from typing import Dict, Any, List
from ..base_transformer import BaseTransformer
import logging

logger = logging.getLogger(__name__)


class RaceControlTransformer(BaseTransformer):
    """
    Трансформирует события race control с денормализацией.
    Target: ClickHouse
    """
    
    def transform(self, raw_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        transformed = []
        
        for doc in raw_documents:
            timestamp = self._parse_timestamp(self._safe_get(doc, "date"))
            driver_number = self._safe_get(doc, "driver_number")
            
            # Создаем базовый словарь
            dds_record = {
                # Keys
                "session_key": self.session_key,
                "timestamp": timestamp,
                
                # Race control data
                "category": self._safe_get(doc, "category"),
                "flag": self._safe_get(doc, "flag"),
                "message": self._safe_get(doc, "message"),
                "lap_number": self._safe_get(doc, "lap_number"),
                "driver_number": driver_number,
                "sector": self._safe_get(doc, "sector"),
                "scope": self._safe_get(doc, "scope"),
                
                # Вычисляемые поля
                "is_safety_car": self._safe_get(doc, "flag") in ["SC", "SAFETY CAR", "VIRTUAL SAFETY CAR"],
                "is_red_flag": self._safe_get(doc, "flag") == "RED FLAG",
                "is_yellow_flag": self._safe_get(doc, "flag") == "YELLOW",
                
                # ETL
                "etl_hash": self._safe_get(doc, "etl_hash"),
                "transformed_at": self.transformed_at
            }
            
            # Добавляем денормализованные данные отдельно
            if driver_number:
                dds_record.update(self._denormalize_driver(driver_number))
            
            dds_record.update(self._denormalize_session())
            
            transformed.append(dds_record)
        
        logger.info(f"Transformed {len(transformed)} race control records for session {self.session_key}")
        return transformed
    
    def get_target_database(self) -> str:
        return "clickhouse"
    
    def get_table_name(self) -> str:
        return "fact_race_control"