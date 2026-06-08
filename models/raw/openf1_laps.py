from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional
from .openf1_base import OpenF1BaseModel

class OpenF1Lap(OpenF1BaseModel):
    """Provides detailed information about individual laps"""
    
    date_start: datetime # Время начала круга
    driver_number: int
    duration_sector_1: Optional[float] = None
    duration_sector_2: Optional[float] = None
    duration_sector_3: Optional[float] = None
    i1_speed: Optional[int] = None
    i2_speed: Optional[int] = None
    is_pit_out_lap: bool
    lap_duration: float
    lap_number: int
    meeting_key: int
    segments_sector_1: Optional[list[Optional[int]]] = None
    segments_sector_2: Optional[list[Optional[int]]] = None
    segments_sector_3: Optional[list[Optional[int]]] = None
    session_key: int
    st_speed:Optional[int] = None # Скорость на speed trap, где обычно замеряют максимальную скорость
    
    etl_endpoint: str = "laps"