from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional


class OpenF1Driver(BaseModel):
    """Information about the drivers perticipating in a specific session"""
    
    broadcast_name: str
    driver_number: int
    first_name: str
    full_name: str
    headshot_url: Optional[str] = None
    last_name: str
    meeting_key: int
    name_acronym: str
    session_key: int
    team_colour: str                     # HEX цвет команды
    team_name: str
    
    etl_loaded_at: datetime = Field(default_factory=datetime.utcnow)
    etl_source: str = "openf1"
    etl_endpoint: str = "drivers"
    
    
