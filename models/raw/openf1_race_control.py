from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional
from .openf1_base import OpenF1BaseModel

class OpenF1RaceControl(OpenF1BaseModel):
    """Information about race control (session status, racing incidents, flags etc...)"""
    
    category: str
    date: datetime
    driver_number: Optional[int] = None
    flag: Optional[str] = None
    lap_number: int
    meeting_key: int
    message: str
    qualifying_phase: Optional[str] = None
    scope: Optional[str] = None
    sector: Optional[int] = None
    session_key: int
    
    etl_endpoint: str = "race_control"