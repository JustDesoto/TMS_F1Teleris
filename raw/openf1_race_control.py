from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional

class OpenF1RaceControl(BaseModel):
    """Information about race control (session status, racing incidents, flags etc...)"""
    
    category: str
    date: datetime
    driver_number: int
    flag: str
    lap_number: int
    meeting_key: int
    message: str
    qualifying_phase: str
    scope: str
    sector: Optional[int] = None
    session_key: int
    
        
    etl_loaded_at: datetime = Field(default_factory=datetime.utcnow)
    etl_source: str = "openf1"
    etl_endpoint: str = "race_control"