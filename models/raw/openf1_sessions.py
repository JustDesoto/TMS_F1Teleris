from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional
from .openf1_base import OpenF1BaseModel

class OpenF1Session(OpenF1BaseModel):
    """Information about session. A session prefers to a distinct period of track activity during a Grand Prix or testing weekend"""
    
    circuit_key: int
    circuit_short_name: str
    country_code: str
    country_key: int
    country_name: str
    date_end: datetime
    date_start: datetime
    gmt_offset: str
    is_cancelled: bool
    location: str
    meeting_key: int
    session_key: int
    session_name: str
    session_type: str
    year: int

    etl_endpoint: str = "sessions"