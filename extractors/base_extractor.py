from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Abstract class for extractor"""
    
    @abstractmethod
    def fetch_endpoint(self, endpoint: str, params: Dict[str, Any]) -> List[Dict]:
        """Base method for API requests"""
        pass
    
    def save_to_dead_letter(self, data: Dict, endpoint: str, error: str):
        """
        Save problems data
        """
        logger.error(f"Dead letter - {endpoint}: {error}")
        # TODO: можно сохранять в отдельную коллекцию MongoDB или в файл