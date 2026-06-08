from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional
from .openf1_base import OpenF1BaseModel

class OpenF1CarData(OpenF1BaseModel):
    """Some data about each car"""
    
    brake: int
    date: datetime
    driver_number: int
    drs: int
    meeting_key: int
    n_gear: int
    rpm: int
    session_key: int
    speed: int
    throttle: int
    
    etl_endpoint: str = "car_data"
    
    @field_validator('brake','throttle')
    @classmethod
    def validate_percentage(cls, v: int) -> int:
        if v < 0 or v > 100:
            raise ValueError(f"Value must be 0-100, got {v}")
        return v

    @field_validator('n_gear')
    @classmethod
    def validate_gear(cls, v: int) -> int:
        """Проверяем передачу"""
        if v < 0 or v > 8:
            raise ValueError(f"Gear must be 0-8, got {v}")
        return v