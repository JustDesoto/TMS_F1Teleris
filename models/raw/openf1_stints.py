from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional
from .openf1_base import OpenF1BaseModel

class OpenF1Stints(OpenF1BaseModel):
    """Information about individual stints"""
    
    compound: str
    driver_number: int
    lap_end: Optional[int] = None
    lap_start: Optional[int] = None
    meeting_key: int
    session_key: int
    stint_number: int
    tyre_age_at_start: int

    etl_endpoint: str = "stints"