from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional
from .openf1_base import OpenF1BaseModel

class OpenF1StartingGrid(OpenF1BaseModel):
    """Starting grid for the upcoming race"""
    
    driver_number: int
    lap_duration: float
    meeting_key: int
    position: int
    session_key: int
    
    etl_endpoint: str = "starting_grid"