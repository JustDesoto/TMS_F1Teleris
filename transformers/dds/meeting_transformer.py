# transformers/dds/meeting_transformer.py
from typing import Dict, Any, List
from ..base_transformer import BaseTransformer
import logging

logger = logging.getLogger(__name__)


class MeetingTransformer(BaseTransformer):
    """
    Трансформирует данные о встречах (этапах чемпионата).
    SCD Type 0: (meeting_key, session_key) - составной ключ.
    Target: PostgreSQL
    """
    
    def transform(self, raw_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        transformed = []
        
        for doc in raw_documents:
            dds_record = {
                # Составной ключ
                "meeting_key": self._safe_get(doc, "meeting_key"),
                "session_key": self.session_key,
                
                # Основная информация
                "meeting_name": self._safe_get(doc, "meeting_name"),
                "meeting_official_name": self._safe_get(doc, "meeting_official_name"),
                "location": self._safe_get(doc, "location"),
                "country_name": self._safe_get(doc, "country_name"),
                "country_code": self._safe_get(doc, "country_code"),
                
                # Даты
                "date_start": self._format_datetime(self._safe_get(doc, "date_start")),
                "date_end": self._format_datetime(self._safe_get(doc, "date_end")),
                "year": self._safe_get(doc, "year"),
                
                # Трек
                "circuit_key": self._safe_get(doc, "circuit_key"),
                "circuit_short_name": self._safe_get(doc, "circuit_short_name"),
                "circuit_type": self._safe_get(doc, "circuit_type"),
                
                # Дополнительно
                "gmt_offset": self._safe_get(doc, "gmt_offset"),
                "is_cancelled": self._safe_get(doc, "is_cancelled", False),
                
                # ETL
                "etl_hash": self._safe_get(doc, "etl_hash"),
                "loaded_at": self.transformed_at
            }
            transformed.append(dds_record)
        
        logger.info(f"Transformed {len(transformed)} meeting records for session {self.session_key}")
        return transformed
    
    def get_target_database(self) -> str:
        return "postgresql"
    
    def get_table_name(self) -> str:
        return "dim_meeting_session"