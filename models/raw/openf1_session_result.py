from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional, Union
from .openf1_base import OpenF1BaseModel

class OpenF1SessionResult(OpenF1BaseModel):
    """Standings after a session"""
    
    dnf: bool
    dns: bool
    dsq: bool
    driver_number: int
    duration: float
    gap_to_leader: Optional[Union[float, str]] = None
    number_of_laps: int
    meeting_key: int
    position: int
    session_key: int
    
    etl_endpoint: str = "session_result"
    
    @field_validator('gap_to_leader', mode='before')
    @classmethod
    def parse_gap_value(cls, value):
        if value == "+1 LAP":
            return value
        return value