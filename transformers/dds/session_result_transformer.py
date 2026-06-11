# transformers/dds/session_result_transformer.py
from typing import Dict, Any, List
from ..base_transformer import BaseTransformer
import logging

logger = logging.getLogger(__name__)


class SessionResultTransformer(BaseTransformer):
    """
    Трансформирует результаты сессии.
    SCD Type 0: (session_key, driver_number) - составной ключ.
    Target: PostgreSQL
    """
    
    def _format_gap(self, value) -> str:
        """Форматирует gap_to_leader в строку"""
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, (int, float)):
            return str(value)
        return str(value)
    
    def transform(self, raw_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        transformed = []
        
        for doc in raw_documents:
            # Парсим duration (массив для квалификации, число для гонки)
            duration = self._safe_get(doc, "duration")
            
            duration_q1 = None
            duration_q2 = None
            duration_q3 = None
            duration_race = None
            
            if isinstance(duration, (list, tuple)):
                # Квалификация - массив [Q1, Q2, Q3]
                if len(duration) > 0 and duration[0] is not None:
                    duration_q1 = float(duration[0])
                if len(duration) > 1 and duration[1] is not None:
                    duration_q2 = float(duration[1])
                if len(duration) > 2 and duration[2] is not None:
                    duration_q3 = float(duration[2])
            elif duration is not None:
                # Гонка или практика - одно число
                duration_race = float(duration)
            
            # Парсим gap_to_leader (тоже может быть массивом для квалификации)
            gap = self._safe_get(doc, "gap_to_leader")
            
            gap_q1 = None
            gap_q2 = None
            gap_q3 = None
            gap_race = None
            
            if isinstance(gap, (list, tuple)):
                # Квалификация - массив [Q1, Q2, Q3]
                if len(gap) > 0 and gap[0] is not None:
                    gap_q1 = self._format_gap(gap[0])
                if len(gap) > 1 and gap[1] is not None:
                    gap_q2 = self._format_gap(gap[1])
                if len(gap) > 2 and gap[2] is not None:
                    gap_q3 = self._format_gap(gap[2])
            elif gap is not None:
                # Гонка - одно значение
                gap_race = self._format_gap(gap)
            
            dds_record = {
                # Составной ключ
                "session_key": self.session_key,
                "driver_number": self._safe_get(doc, "driver_number"),
                
                # Позиция
                "position": self._safe_get(doc, "position"),
                
                # Квалификация
                "gap_to_leader_q1": gap_q1,
                "gap_to_leader_q2": gap_q2,
                "gap_to_leader_q3": gap_q3,
                "duration_q1": duration_q1,
                "duration_q2": duration_q2,
                "duration_q3": duration_q3,
                
                # Гонка
                "gap_to_leader_race": gap_race,
                "duration_race": duration_race,
                
                # Общие поля
                "number_of_laps": self._safe_get(doc, "number_of_laps"),
                "dnf": self._safe_get(doc, "dnf", False),
                "dns": self._safe_get(doc, "dns", False),
                "dsq": self._safe_get(doc, "dsq", False),
                
                # ETL
                "etl_hash": self._safe_get(doc, "etl_hash"),
                "loaded_at": self.transformed_at
            }
            transformed.append(dds_record)
        
        logger.info(f"Transformed {len(transformed)} session result records for session {self.session_key}")
        return transformed
    
    def get_target_database(self) -> str:
        return "postgresql"
    
    def get_table_name(self) -> str:
        return "fact_session_result"