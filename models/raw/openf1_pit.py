from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional
from .openf1_base import OpenF1BaseModel

class OpenF1Pit(OpenF1BaseModel):
    """Information about cars going through the pit lane"""
    
    date: datetime
    driver_number: int
    lane_duration: float
    lap_number: int
    meeting_key: int
    pit_duration: float
    session_key: int
    stop_duration: float
    
    etl_endpoint: str = "pit"