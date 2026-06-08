from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional
from .openf1_base import OpenF1BaseModel

class OpenF1Position(OpenF1BaseModel):
    """Driver position throughout a session,
    including initial placement and subsequent changes"""
    
    date: datetime
    driver_number: int
    meeting_key: int
    position: int
    session_key: int
    
    etl_endpoint: str = "position"