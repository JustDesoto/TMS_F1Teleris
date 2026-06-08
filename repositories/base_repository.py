from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

class BaseRepository(ABC):
    """Abstract class for all repositories"""
    
    @abstractmethod
    def save_one(self, collection: str, document: Dict[str, Any]) -> str:
        """Save one doc"""
        pass
    
    @abstractmethod
    def save_many(self, collection: str, documents: List[Dict[str, Any]]) -> List[str]:
        """Save many docs"""
        pass
    
    @abstractmethod
    def find_by_filter(self, collection: str, filter_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find docs by filter"""
        pass
    
    @abstractmethod
    def find_one(self, collection: str, filter_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find one doc"""
        pass
    
    @abstractmethod
    def upsert(self, collection: str, filter_dict: Dict[str, Any], document: Dict[str, Any]) -> str:
        """Update or insert new doc"""
        pass
    
    @abstractmethod
    def get_last_watermark(self, collection: str, session_key: int) -> Optional[str]:
        """Get last watermark for collection and session"""
        pass