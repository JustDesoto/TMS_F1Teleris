# transformers/fact/overtake_transformer.py
from typing import Dict, Any, List
from ..base_transformer import BaseTransformer
import logging

logger = logging.getLogger(__name__)


class OvertakeTransformer(BaseTransformer):
    """
    Трансформирует данные об обгонах с денормализацией.
    Target: ClickHouse
    """
    
    def transform(self, raw_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        transformed = []
        
        for doc in raw_documents:
            dt = self._parse_timestamp(self._safe_get(doc, "date"))
            overtaking_driver = self._safe_get(doc, "overtaking_driver_number")
            overtaken_driver = self._safe_get(doc, "overtaken_driver_number")
            
            overtaking_info = self._get_driver_info(overtaking_driver)
            overtaken_info = self._get_driver_info(overtaken_driver)
            
            dds_record = {
                "session_key": self.session_key,
                "timestamp": dt,
                "date": dt.date() if dt else None,
                "overtaking_driver": overtaking_driver,
                "overtaken_driver": overtaken_driver,
                "position": self._safe_get(doc, "position"),
                "overtaking_driver_full_name": overtaking_info.get("full_name"),
                "overtaking_driver_acronym": overtaking_info.get("acronym"),
                "overtaking_driver_team": overtaking_info.get("team_name"),
                "overtaking_driver_team_colour": overtaking_info.get("team_colour"),
                "overtaken_driver_full_name": overtaken_info.get("full_name"),
                "overtaken_driver_acronym": overtaken_info.get("acronym"),
                "overtaken_driver_team": overtaken_info.get("team_name"),
                "overtaken_driver_team_colour": overtaken_info.get("team_colour"),
                **self._denormalize_session(),
                "etl_hash": self._safe_get(doc, "etl_hash"),
                "transformed_at": self.transformed_at
            }
            transformed.append(dds_record)
        
        logger.info(f"Transformed {len(transformed)} overtake records for session {self.session_key}")
        return transformed
    
    def get_target_database(self) -> str:
        return "clickhouse"
    
    def get_table_name(self) -> str:
        return "fact_overtake"