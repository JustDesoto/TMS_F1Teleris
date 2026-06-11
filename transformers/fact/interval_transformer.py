# transformers/fact/interval_transformer.py
from typing import Dict, Any, List
from ..base_transformer import BaseTransformer
import logging

logger = logging.getLogger(__name__)


class IntervalTransformer(BaseTransformer):
    """
    Трансформирует интервалы между гонщиками с денормализацией.
    Target: ClickHouse
    """
    
    def transform(self, raw_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        transformed = []
        
        for doc in raw_documents:
            driver_number = self._safe_get(doc, "driver_number")
            timestamp = self._parse_timestamp(self._safe_get(doc, "date"))
            
            gap = self._safe_get(doc, "gap_to_leader")
            interval = self._safe_get(doc, "interval")
            
            gap_str = None
            if gap is not None:
                gap_str = str(gap) if not isinstance(gap, str) else gap
            
            interval_str = None
            if interval is not None:
                interval_str = str(interval) if not isinstance(interval, str) else interval
            
            dds_record = {
                # Keys
                "session_key": self.session_key,
                "driver_number": driver_number,
                "timestamp": timestamp,
                
                # Interval data
                "gap_to_leader": gap_str,
                "interval_to_prev": interval_str,
                
                # Денормализация DRIVER
                **self._denormalize_driver(driver_number),
                
                # Денормализация SESSION
                **self._denormalize_session(),
                
                # Вычисляемые поля
                "is_lapped": gap == "+1 LAP" or interval == "+1 LAP",
                
                # ETL
                "etl_hash": self._safe_get(doc, "etl_hash"),
                "transformed_at": self.transformed_at
            }
            transformed.append(dds_record)
        
        logger.info(f"Transformed {len(transformed)} interval records for session {self.session_key}")
        return transformed
    
    def get_target_database(self) -> str:
        return "clickhouse"
    
    def get_table_name(self) -> str:
        return "fact_interval"