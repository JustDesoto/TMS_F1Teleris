# transformers/dds/session_transformer.py
from typing import Dict, Any, List
from ..base_transformer import BaseTransformer
import logging

logger = logging.getLogger(__name__)


class SessionTransformer(BaseTransformer):
    """
    Трансформирует данные о сессиях.
    SCD Type 0: session_key - PRIMARY KEY.
    Target: PostgreSQL
    """
    
    def transform(self, raw_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        transformed = []
        
        for doc in raw_documents:
            dds_record = {
                # Primary Key
                "session_key": self._safe_get(doc, "session_key"),
                
                # Связи
                "meeting_key": self._safe_get(doc, "meeting_key"),
                
                # Информация о сессии
                "session_name": self._safe_get(doc, "session_name"),
                "session_type": self._safe_get(doc, "session_type"),
                
                # Временные метки
                "date_start": self._format_datetime(self._safe_get(doc, "date_start")),
                "date_end": self._format_datetime(self._safe_get(doc, "date_end")),
                
                # Трек
                "circuit_key": self._safe_get(doc, "circuit_key"),
                "circuit_name": self._safe_get(doc, "circuit_short_name"),
                "country_name": self._safe_get(doc, "country_name"),
                "country_code": self._safe_get(doc, "country_code"),
                "location": self._safe_get(doc, "location"),
                
                # Год
                "year": self._safe_get(doc, "year"),
                
                # Статус
                "is_cancelled": self._safe_get(doc, "is_cancelled", False),
                "gmt_offset": self._safe_get(doc, "gmt_offset"),
                
                # ETL
                "etl_hash": self._safe_get(doc, "etl_hash"),
                "loaded_at": self.transformed_at
            }
            transformed.append(dds_record)
        
        logger.info(f"Transformed {len(transformed)} session records for session {self.session_key}")
        return transformed
    
    def get_target_database(self) -> str:
        return "postgresql"
    
    def get_table_name(self) -> str:
        return "dim_session"