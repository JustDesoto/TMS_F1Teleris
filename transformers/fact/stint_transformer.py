# transformers/fact/stint_transformer.py
from typing import Dict, Any, List, Optional
from ..base_transformer import BaseTransformer
import logging

logger = logging.getLogger(__name__)


class StintTransformer(BaseTransformer):
    """
    Трансформирует данные о стентах с денормализацией.
    Target: ClickHouse
    """
    
    def transform(self, raw_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        transformed = []
        
        for doc in raw_documents:
            driver_number = self._safe_get(doc, "driver_number")
            compound = self._safe_get(doc, "compound")
            
            dds_record = {
                # Keys
                "session_key": self.session_key,
                "driver_number": driver_number,
                "stint_number": self._safe_get(doc, "stint_number"),
                
                # Stint data
                "compound": compound,
                "tyre_age_at_start": self._safe_get(doc, "tyre_age_at_start"),
                "lap_start": self._safe_get(doc, "lap_start"),
                "lap_end": self._safe_get(doc, "lap_end"),
                "total_laps": self._calculate_total_laps(
                    self._safe_get(doc, "lap_start"),
                    self._safe_get(doc, "lap_end")
                ),
                
                # Денормализация DRIVER
                **self._denormalize_driver(driver_number),
                
                # Денормализация SESSION
                **self._denormalize_session(),
                
                # Вычисляемые поля
                "is_wet_tyre": compound in ["INTERMEDIATE", "WET"],
                "is_soft": compound == "SOFT",
                "is_medium": compound == "MEDIUM",
                "is_hard": compound == "HARD",
                
                # ETL
                "etl_hash": self._safe_get(doc, "etl_hash"),
                "transformed_at": self.transformed_at
            }
            transformed.append(dds_record)
        
        logger.info(f"Transformed {len(transformed)} stint records for session {self.session_key}")
        return transformed
    
    def _calculate_total_laps(self, lap_start: Any, lap_end: Any) -> Optional[int]:
        """Вычисляет количество кругов в стенте"""
        if lap_start is None or lap_end is None:
            return None
        try:
            total = int(lap_end) - int(lap_start)
            # Проверяем, что значение не отрицательное и не превышает лимиты
            if total < 0:
                logger.warning(f"Negative total_laps: {total}, setting to 0")
                return 0
            return total
        except (ValueError, TypeError):
            return None
    
    
    def get_target_database(self) -> str:
        return "clickhouse"
    
    def get_table_name(self) -> str:
        return "fact_stint"