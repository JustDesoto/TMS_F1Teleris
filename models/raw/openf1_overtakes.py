from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional
from .openf1_base import OpenF1BaseModel

class OpenF1Overtake(OpenF1BaseModel):
    """An overtake refers to one driver (the overtaking driver) exchanging positions with another driver (the overtaken driver).
    This includes both on-track passes and position changes resulting from pit stops or post-race penalties."""
    
    date: datetime
    meeting_key: int
    overtaken_driver_number: int
    overtaking_driver_number: int
    position: int
    session_key: int
    
    etl_endpoint: str = "overtakes"