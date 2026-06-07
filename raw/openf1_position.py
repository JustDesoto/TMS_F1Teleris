from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional

class OpenF1Position(BaseModel):
    """Driver position throughout a session,
    including initial placement and subsequent changes"""
    
    date: datetime
    driver_number: int
    meeting_key: int
    position: int
    session_key: int
    
        
    etl_loaded_at: datetime = Field(default_factory=datetime.utcnow)
    etl_source: str = "openf1"
    etl_endpoint: str = "position"