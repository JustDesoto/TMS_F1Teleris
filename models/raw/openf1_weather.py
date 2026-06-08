from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional
from .openf1_base import OpenF1BaseModel

class OpenF1Weather(OpenF1BaseModel):
    """The weather over the track"""
    
    air_temperature: float
    date: datetime
    humidity: int
    meeting_key: int
    pressure: float
    rainfall: int # like bool
    session_key: int
    track_temperature: float
    wind_direction: int
    wind_speed: float
    
    etl_endpoint: str = "weather"