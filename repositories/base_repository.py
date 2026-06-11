from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

class BaseRepository(ABC):
    """Абстрактный базовый класс для всех репозиториев"""
    
    @abstractmethod
    def save_one(self, collection: str, document: Dict[str, Any]) -> str:
        """Сохранить один документ"""
        pass
    
    @abstractmethod
    def save_many(self, collection: str, documents: List[Dict[str, Any]]) -> List[str]:
        """Сохранить много документов"""
        pass
    
    def find_by_filter(self, collection: str, filter_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Найти документы по фильтру"""
        pass
    
    def find_one(self, collection: str, filter_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Найти один документ по фильтру"""
        pass
    
    def upsert(self, collection: str, filter_dict: Dict[str, Any], document: Dict[str, Any]) -> str:
        """Обновить или вставить новый документ"""
        pass
    
    def get_last_watermark(self, collection: str, session_key: int) -> Optional[str]:
        """Получить последний watermark для коллекции и сессии"""
        pass