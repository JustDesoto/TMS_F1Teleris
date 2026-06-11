# transformers/dds/starting_grid_transformer.py
from typing import Dict, Any, List
from ..base_transformer import BaseTransformer
import logging

logger = logging.getLogger(__name__)


class StartingGridTransformer(BaseTransformer):
    """
    Трансформирует данные стартовой решетки.
    SCD Type 0: (session_key, driver_number) - составной ключ.
    Target: PostgreSQL
    """
    
    def transform(self, raw_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        transformed = []
        
        for doc in raw_documents:
            dds_record = {
                # Составной ключ
                "session_key": self.session_key,
                "driver_number": self._safe_get(doc, "driver_number"),
                
                # Стартовая позиция
                "position": self._safe_get(doc, "position"),
                "lap_duration": self._safe_get(doc, "lap_duration"),
                
                # ETL
                "etl_hash": self._safe_get(doc, "etl_hash"),
                "loaded_at": self.transformed_at
            }
            transformed.append(dds_record)
        
        logger.info(f"Transformed {len(transformed)} starting grid records for session {self.session_key}")
        return transformed
    
    def get_target_database(self) -> str:
        return "postgresql"
    
    def get_table_name(self) -> str:
        return "fact_starting_grid"