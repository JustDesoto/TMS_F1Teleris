from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional

class OpenF1Meeting(BaseModel):
    """Information about meetings"""
    
    circuit_key: int
    circuit_image: Optional[str] = None
    circuit_info_url: str
    circuit_short_name: str
    circuit_type: str
    country_code: str
    country_flag: str
    country_key: int
    country_name:str
    date_end: datetime
    date_start: datetime
    gmt_offset: str
    is_cancelled: bool
    location: str
    meeting_key: int
    meeting_name: str
    meeting_official_name: str
    year: int
        
    etl_loaded_at: datetime = Field(default_factory=datetime.utcnow)
    etl_source: str = "openf1"
    etl_endpoint: str = "meetings"