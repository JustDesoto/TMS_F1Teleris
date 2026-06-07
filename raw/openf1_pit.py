from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional

class OpenF1Pit(BaseModel):
    """Information about cars going through the pit lane"""
    
    date: datetime
    driver_number: int
    lane_duration: float
    lap_number: int
    meeting_key: int
    pit_duration: float
    session_key: int
    stop_duration: float
    
        
    etl_loaded_at: datetime = Field(default_factory=datetime.utcnow)
    etl_source: str = "openf1"
    etl_endpoint: str = "pit"