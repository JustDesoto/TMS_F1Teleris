from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional
from .openf1_base import OpenF1BaseModel

class OpenF1Location(OpenF1BaseModel):
    """The approximate location of the cars on the circuit"""
    
    date: datetime
    driver_number: int
    meeting_key: int
    session_key: int
    x: int
    y: int
    z: int
    
    etl_endpoint: str = "location"