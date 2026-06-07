from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional

class OpenF1Location(BaseModel):
    """The approximate location of the cars on the circuit"""
    
    date: datetime
    driver_number: int
    meeting_key: int
    session_key: int
    x: int
    y: int
    z: int
    
    etl_loaded_at: datetime = Field(default_factory=datetime.utcnow)
    etl_source: str = "openf1"
    etl_endpoint: str = "location"