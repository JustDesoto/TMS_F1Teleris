# transformers/dds/driver_transformer.py
from typing import Dict, Any, List
from ..base_transformer import BaseTransformer
import logging

logger = logging.getLogger(__name__)


class DriverTransformer(BaseTransformer):
    """
    Трансформирует данные пилотов для PostgreSQL.
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
                
                # Атрибуты пилота
                "full_name": self._safe_get(doc, "full_name"),
                "first_name": self._safe_get(doc, "first_name"),
                "last_name": self._safe_get(doc, "last_name"),
                "team_name": self._safe_get(doc, "team_name"),
                "team_colour": self._safe_get(doc, "team_colour"),
                "acronym": self._safe_get(doc, "name_acronym"),
                "broadcast_name": self._safe_get(doc, "broadcast_name"),
                "headshot_url": self._safe_get(doc, "headshot_url"),
                
                # Связи
                "meeting_key": self._safe_get(doc, "meeting_key"),
                
                # ETL
                "etl_hash": self._safe_get(doc, "etl_hash"),
                "loaded_at": self.transformed_at
            }
            transformed.append(dds_record)
        
        logger.info(f"Transformed {len(transformed)} driver records for session {self.session_key}")
        return transformed
    
    def get_target_database(self) -> str:
        return "postgresql"
    
    def get_table_name(self) -> str:
        return "dim_driver_session"