from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional
from .openf1_base import OpenF1BaseModel

class OpenF1Driver(OpenF1BaseModel):
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
    
    etl_endpoint: str = "drivers"
    
    
