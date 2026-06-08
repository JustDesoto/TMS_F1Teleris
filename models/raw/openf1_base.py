# models/raw/openf1_base.py
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

import hashlib
import json

class OpenF1BaseModel(BaseModel):
    """
    Base model
    """
    
    etl_loaded_at: datetime = Field(default_factory=datetime.utcnow)
    etl_source: str = "openf1"
    etl_endpoint: str
    etl_watermark: Optional[str] = None
    etl_hash: Optional[str] = None
    
    def generate_hash(self) -> str:
        """Generate hash for deduplication"""
        
        data_dict = self.model_dump(exclude={'etl_loaded_at', 'etl_source', 'etl_endpoint', 'etl_watermark', 'etl_hash'})
        json_str = json.dumps(data_dict, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()
    
    def model_post_init(self, __context):
        if not self.etl_hash:
            self.etl_hash = self.generate_hash()