from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional

class OpenF1CarData(BaseModel):
    """Some data about each car"""
    
    brake: int
    date: datetime
    driver_number: int
    drs: int
    meeting_key: int
    n_gear: int
    npm: int
    session_key: int
    spead: int
    throttle: int
    
    etl_loaded_at: datetime = Field(default_factory=datetime.utcnow)
    etl_source: str = "openf1"
    etl_endpoint: str = "car_data"
    
    @field_validator
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