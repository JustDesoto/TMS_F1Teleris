# transformers/base_transformer.py
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class BaseTransformer(ABC):
    """
    Базовый класс для всех трансформеров.
    """
    
    def __init__(
        self, 
        session_key: int, 
        session_context: Optional[Dict[str, Any]] = None,
        drivers_context: Optional[Dict[int, Dict[str, Any]]] = None
    ):
        self.session_key = session_key
        self.session_context = session_context or {}
        self.drivers_context = drivers_context or {}
        self.transformed_at = datetime.utcnow()
    
    def _get_driver_info(self, driver_number: int) -> Dict[str, Any]:
        return self.drivers_context.get(driver_number, {})
    
    def _denormalize_driver(self, driver_number: int, prefix: str = "driver_") -> Dict[str, Any]:
        driver_info = self._get_driver_info(driver_number)
        
        return {
            f"{prefix}full_name": driver_info.get("full_name"),
            f"{prefix}first_name": driver_info.get("first_name"),
            f"{prefix}last_name": driver_info.get("last_name"),
            f"{prefix}acronym": driver_info.get("acronym"),
            f"{prefix}team": driver_info.get("team_name"),
            f"{prefix}team_colour": driver_info.get("team_colour"),
            f"{prefix}broadcast_name": driver_info.get("broadcast_name"),
            f"{prefix}number": driver_number,
        }
    
    def _denormalize_session(self, prefix: str = "") -> Dict[str, Any]:
        result = {}
        
        session_fields = [
            "meeting_name", "meeting_key", "circuit_name", "circuit_key",
            "session_type", "session_name", "year", "country"
        ]
        
        for field in session_fields:
            key = f"{prefix}{field}" if prefix else field
            result[key] = self.session_context.get(field)
        
        date_start = self.session_context.get("date_start")
        date_end = self.session_context.get("date_end")
        
        result[f"{prefix}date_start" if prefix else "date_start"] = self._format_datetime_for_ch(date_start)
        result[f"{prefix}date_end" if prefix else "date_end"] = self._format_datetime_for_ch(date_end)
        
        return result
    
    def _safe_get(self, doc: Dict[str, Any], key: str, default: Any = None) -> Any:
        return doc.get(key, default)
    
    def _format_datetime_for_ch(self, value: Any) -> Optional[datetime]:
        """
        Преобразует значение в datetime объект для ClickHouse.
        """
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                clean_value = value.replace('Z', '+00:00')
                return datetime.fromisoformat(clean_value)
            except (ValueError, TypeError):
                return None
        return None
    
    def _format_datetime(self, value: Any, include_ms: bool = False) -> Optional[str]:
        """
        Форматирует дату/время в строку.
        
        Args:
            value: datetime объект, строка или None
            include_ms: включать миллисекунды
        """
        if value is None:
            return None
        
        # Если уже datetime объект
        if isinstance(value, datetime):
            if include_ms:
                return value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            else:
                return value.strftime('%Y-%m-%d %H:%M:%S')
        
        # Если строка
        if isinstance(value, str):
            try:
                clean_value = value.replace('Z', '+00:00')
                dt = datetime.fromisoformat(clean_value)
                if include_ms:
                    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                else:
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
            except (ValueError, TypeError):
                if len(value) >= 23:
                    return value[:19].replace('T', ' ') + value[19:23]
                elif len(value) >= 19:
                    return value[:19].replace('T', ' ')
                return value
        
        return None
    
    def _format_timestamp(self, value: Any) -> Optional[str]:
        """
        Форматирует timestamp С миллисекундами для DateTime64(3).
        Возвращает строку: 'YYYY-MM-DD HH:MM:SS.sss'
        """
        return self._format_datetime(value, include_ms=True)
    
    def _parse_timestamp(self, value: Any) -> Optional[datetime]:
        """
        Парсит timestamp в datetime объект для ClickHouse.
        """
        return self._format_datetime_for_ch(value)
    
    def _ensure_datetime(self, value: Any) -> Optional[datetime]:
        """Преобразует значение в datetime объект"""
        return self._format_datetime_for_ch(value)
    
    @abstractmethod
    def transform(self, raw_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pass
    
    @abstractmethod
    def get_target_database(self) -> str:
        pass
    
    @abstractmethod
    def get_table_name(self) -> str:
        pass