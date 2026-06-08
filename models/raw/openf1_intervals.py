from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional, Union
from .openf1_base import OpenF1BaseModel

class OpenF1Interval(OpenF1BaseModel):
    """Interval data between drivers and their gap to the race leader"""
    
    date: datetime
    driver_number: int
    gap_to_leader: Optional[Union[float, str]] = None
    interval: Optional[Union[float, str]] = None
    meeting_key: int
    session_key: int
        
    etl_endpoint: str = "intervals"
    
    @field_validator('gap_to_leader', 'interval', mode='before')
    @classmethod
    def parse_gap_value(cls, value):
        if value == "+1 LAP":
            return value
        return value
    
    def is_lapped(self) -> bool:
        """Проверяет, отстает ли гонщик на круг"""
        return self.gap_to_leader == "+1 LAP" or self.interval == "+1 LAP"
    
    
