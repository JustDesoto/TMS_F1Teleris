# transformers/fact/position_transformer.py
from typing import Dict, Any, List
from ..base_transformer import BaseTransformer
import logging

logger = logging.getLogger(__name__)


class PositionTransformer(BaseTransformer):
    """
    Трансформирует данные о позициях с денормализацией.
    Target: ClickHouse
    """
    
    def transform(self, raw_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        transformed = []
        
        for doc in raw_documents:
            driver_number = self._safe_get(doc, "driver_number")
            timestamp = self._parse_timestamp(self._safe_get(doc, "date"))
            dt = self._parse_timestamp(self._safe_get(doc, "date"))
            position = self._safe_get(doc, "position")
            
            dds_record = {
                # Keys
                "session_key": self.session_key,
                "driver_number": driver_number,
                "timestamp": timestamp,
                "date": dt.date(),
                
                # Position
                "position": position,
                
                # Денормализация DRIVER
                **self._denormalize_driver(driver_number),
                
                # Денормализация SESSION
                **self._denormalize_session(),
                
                # Вычисляемые поля
                "is_leader": position == 1,
                "is_podium": position in [1, 2, 3],
                "points": self._get_points(position),
                
                # ETL
                "etl_hash": self._safe_get(doc, "etl_hash"),
                "transformed_at": self.transformed_at
            }
            transformed.append(dds_record)
        
        logger.info(f"Transformed {len(transformed)} position records for session {self.session_key}")
        return transformed
    
    def _get_points(self, position: int) -> int:
        """Возвращает количество очков за позицию"""
        points_map = {1: 25, 2: 18, 3: 15, 4: 12, 5: 10, 6: 8, 7: 6, 8: 4, 9: 2, 10: 1}
        return points_map.get(position, 0)
    
    def get_target_database(self) -> str:
        return "clickhouse"
    
    def get_table_name(self) -> str:
        return "fact_position"