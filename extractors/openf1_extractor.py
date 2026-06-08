# extractors/openf1_extractor.py
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import ValidationError
import logging
import time
import random

from .base_extractor import BaseExtractor
from models.raw.openf1_car_data import OpenF1CarData
from models.raw.openf1_laps import OpenF1Lap
from models.raw.openf1_pit import OpenF1Pit
from models.raw.openf1_position import OpenF1Position
from models.raw.openf1_weather import OpenF1Weather
from models.raw.openf1_intervals import OpenF1Interval
from models.raw.openf1_drivers import OpenF1Driver
from models.raw.openf1_meetings import OpenF1Meeting
from models.raw.openf1_sessions import OpenF1Session
from models.raw.openf1_race_control import OpenF1RaceControl
from models.raw.openf1_overtakes import OpenF1Overtake
from models.raw.openf1_stints import OpenF1Stints
from models.raw.openf1_starting_grid import OpenF1StartingGrid
from models.raw.openf1_session_result import OpenF1SessionResult
from models.raw.openf1_location import OpenF1Location
from config.settings import settings

logger = logging.getLogger(__name__)


class OpenF1Extractor(BaseExtractor):
    
    def __init__(
        self, 
        base_url: Optional[str] = None,
        verify_ssl: Optional[bool] = None,
        timeout: Optional[int] = None,
        max_retries: Optional[int] = None,
        request_delay_min: float = 0.5,
        request_delay_max: float = 1.5
    ):
        from config.settings import settings
        
        self.base_url = base_url if base_url is not None else settings.openf1_base_url
        self.verify_ssl = verify_ssl if verify_ssl is not None else settings.openf1_verify_ssl
        self.timeout = timeout if timeout is not None else settings.openf1_timeout
        self.max_retries = max_retries if max_retries is not None else settings.openf1_max_retries
        self.dead_letter_collection = "dead_letter"
        self.request_delay_min = request_delay_min
        self.request_delay_max = request_delay_max
        self.last_request_time = 0
        self.min_request_interval = 0.2
        
        logger.info(f"OpenF1 Extractor initialized: base_url={self.base_url}, verify_ssl={self.verify_ssl}, timeout={self.timeout}, max_retries={self.max_retries}")
    
    def _rate_limit_wait(self):
        """Ограничивает частоту запросов"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def fetch_endpoint(self, endpoint: str, params: Dict[str, Any]) -> List[Dict]:
        return self._fetch_endpoint(endpoint, params)
    
    def _fetch_endpoint(self, endpoint: str, params: Dict[str, Any]) -> List[Dict]:
        url = f"{self.base_url}/{endpoint}"
        params = {k: v for k, v in params.items() if v is not None}
        
        self._rate_limit_wait()
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, params=params, timeout=self.timeout, verify=self.verify_ssl)
                
                if response.status_code == 429:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"Rate limited (429). Waiting {wait_time:.1f}s before retry {attempt + 1}/{self.max_retries}")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                data = response.json()
                if not isinstance(data, list):
                    data = [data] if data else []
                logger.debug(f"Fetched {len(data)} records from {endpoint}")
                return data
                
            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries - 1:
                    raise
                wait_time = (2 ** attempt) + random.uniform(0, 0.5)
                logger.warning(f"Attempt {attempt + 1}/{self.max_retries} failed for {endpoint}: {e}. Waiting {wait_time:.1f}s")
                time.sleep(wait_time)
        
        return []
    
    def _validate_and_model(self, data_list: List[Dict], model_class, endpoint: str, watermark: str = None) -> List:
        validated = []
        for item in data_list:
            try:
                obj = model_class(**item)
                if watermark:
                    obj.etl_watermark = watermark
                validated.append(obj)
            except ValidationError as e:
                logger.error(f"Validation error for {endpoint}: {e}")
                self.save_to_dead_letter(item, endpoint, str(e))
        logger.info(f"Validated {len(validated)}/{len(data_list)} records for {endpoint}")
        return validated
    
    def fetch_car_data(
        self,
        session_key: int,
        driver_number: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        watermark: str = None
    ) -> List[OpenF1CarData]:
        all_data = []
        
        if driver_number:
            params = {
                "session_key": session_key,
                "driver_number": driver_number,
            }
            if start_date:
                params["date>="] = start_date.isoformat()
            if end_date:
                params["date<="] = end_date.isoformat()
            raw_data = self._fetch_endpoint("car_data", params)
            return self._validate_and_model(raw_data, OpenF1CarData, "car_data", watermark)
        
        logger.info(f"Fetching car_data for session {session_key} - getting drivers list first...")
        
        drivers = self.fetch_drivers(session_key=session_key)
        if not drivers:
            logger.warning(f"No drivers found for session {session_key}")
            return []
        
        logger.info(f"Found {len(drivers)} drivers, fetching car_data for each...")
        
        for idx, driver in enumerate(drivers):
            try:
                params = {
                    "session_key": session_key,
                    "driver_number": driver.driver_number,
                }
                if start_date:
                    params["date>="] = start_date.isoformat()
                if end_date:
                    params["date<="] = end_date.isoformat()
                
                raw_data = self._fetch_endpoint("car_data", params)
                
                if raw_data:
                    validated = self._validate_and_model(raw_data, OpenF1CarData, "car_data", watermark)
                    all_data.extend(validated)
                    logger.info(f"  Driver {driver.driver_number}: {len(validated)} records ({idx + 1}/{len(drivers)})")
                else:
                    logger.debug(f"  Driver {driver.driver_number}: no data")
                
                delay = random.uniform(self.request_delay_min, self.request_delay_max)
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"Error fetching data for driver {driver.driver_number}: {e}")
                continue
        
        logger.info(f"Total car_data records fetched for session {session_key}: {len(all_data)}")
        return all_data
    
    def fetch_laps(
        self,
        session_key: int,
        driver_number: Optional[int] = None,
        lap_number: Optional[int] = None,
        watermark: str = None
    ) -> List[OpenF1Lap]:
        all_data = []
        
        if driver_number:
            params = {
                "session_key": session_key,
                "driver_number": driver_number,
                "lap_number": lap_number,
            }
            raw_data = self._fetch_endpoint("laps", params)
            return self._validate_and_model(raw_data, OpenF1Lap, "laps", watermark)
        
        drivers = self.fetch_drivers(session_key=session_key)
        for idx, driver in enumerate(drivers):
            params = {
                "session_key": session_key,
                "driver_number": driver.driver_number,
            }
            raw_data = self._fetch_endpoint("laps", params)
            if raw_data:
                validated = self._validate_and_model(raw_data, OpenF1Lap, "laps", watermark)
                all_data.extend(validated)
                logger.info(f"  Driver {driver.driver_number}: {len(validated)} records ({idx + 1}/{len(drivers)})")
            delay = random.uniform(self.request_delay_min, self.request_delay_max)
            time.sleep(delay)
        
        return all_data
    
    def fetch_pit_stops(
        self,
        session_key: int,
        driver_number: Optional[int] = None,
        watermark: str = None
    ) -> List[OpenF1Pit]:
        params = {
            "session_key": session_key,
            "driver_number": driver_number
        }
        raw_data = self._fetch_endpoint("pit", params)
        return self._validate_and_model(raw_data, OpenF1Pit, "pit", watermark)
    
    def fetch_location(
    self,
    session_key: int,
    driver_number: Optional[int] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    watermark: str = None
) -> List[OpenF1Location]:
        all_data = []
        
        if driver_number:
            params = {
                "session_key": session_key,
                "driver_number": driver_number,
            }
            if start_date:
                params["date>="] = start_date.isoformat()
            if end_date:
                params["date<="] = end_date.isoformat()
            raw_data = self._fetch_endpoint("location", params)
            return self._validate_and_model(raw_data, OpenF1Location, "location", watermark)
        
        logger.info(f"Fetching location for session {session_key} - getting drivers list first...")
        
        drivers = self.fetch_drivers(session_key=session_key)
        if not drivers:
            logger.warning(f"No drivers found for session {session_key}")
            return []
        
        logger.info(f"Found {len(drivers)} drivers, fetching location for each...")
        
        for idx, driver in enumerate(drivers):
            try:
                params = {
                    "session_key": session_key,
                    "driver_number": driver.driver_number,
                }
                if start_date:
                    params["date>="] = start_date.isoformat()
                if end_date:
                    params["date<="] = end_date.isoformat()
                
                raw_data = self._fetch_endpoint("location", params)
                
                if raw_data:
                    validated = self._validate_and_model(raw_data, OpenF1Location, "location", watermark)
                    all_data.extend(validated)
                    logger.info(f"  Driver {driver.driver_number}: {len(validated)} records ({idx + 1}/{len(drivers)})")
                else:
                    logger.debug(f"  Driver {driver.driver_number}: no data")
                
                delay = random.uniform(self.request_delay_min, self.request_delay_max)
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"Error fetching location for driver {driver.driver_number}: {e}")
                continue
        
        logger.info(f"Total location records fetched for session {session_key}: {len(all_data)}")
        return all_data
    
    def fetch_weather(
        self,
        session_key: int,
        watermark: str = None
    ) -> List[OpenF1Weather]:
        params = {"session_key": session_key}
        raw_data = self._fetch_endpoint("weather", params)
        return self._validate_and_model(raw_data, OpenF1Weather, "weather", watermark)
    
    def fetch_intervals(
        self,
        session_key: int,
        driver_number: Optional[int] = None,
        watermark: str = None
    ) -> List[OpenF1Interval]:
        all_data = []
        
        if driver_number:
            params = {
                "session_key": session_key,
                "driver_number": driver_number,
            }
            raw_data = self._fetch_endpoint("intervals", params)
            return self._validate_and_model(raw_data, OpenF1Interval, "intervals", watermark)
        
        drivers = self.fetch_drivers(session_key=session_key)
        for idx, driver in enumerate(drivers):
            try:
                params = {
                    "session_key": session_key,
                    "driver_number": driver.driver_number,
                }
                raw_data = self._fetch_endpoint("intervals", params)
                
                if raw_data:
                    validated = self._validate_and_model(raw_data, OpenF1Interval, "intervals", watermark)
                    all_data.extend(validated)
                    logger.info(f"  Driver {driver.driver_number}: {len(validated)} records ({idx + 1}/{len(drivers)})")
                else:
                    logger.debug(f"  Driver {driver.driver_number}: no data")
                
                delay = random.uniform(self.request_delay_min, self.request_delay_max)
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"Error fetching data for driver {driver.driver_number}: {e}")
                continue
        
        logger.info(f"Total intervals records fetched for session {session_key}: {len(all_data)}")
        return all_data
    
    def fetch_race_control(
        self,
        session_key: int,
        watermark: str = None
    ) -> List[OpenF1RaceControl]:
        params = {"session_key": session_key}
        raw_data = self._fetch_endpoint("race_control", params)
        return self._validate_and_model(raw_data, OpenF1RaceControl, "race_control", watermark)
    
    def fetch_overtakes(
        self,
        session_key: int,
        watermark: str = None
    ) -> List[OpenF1Overtake]:
        params = {"session_key": session_key}
        raw_data = self._fetch_endpoint("overtakes", params)
        return self._validate_and_model(raw_data, OpenF1Overtake, "overtakes", watermark)
    
    def fetch_stints(
        self,
        session_key: int,
        driver_number: Optional[int] = None,
        watermark: str = None
    ) -> List[OpenF1Stints]:
        params = {
            "session_key": session_key,
            "driver_number": driver_number
        }
        raw_data = self._fetch_endpoint("stints", params)
        return self._validate_and_model(raw_data, OpenF1Stints, "stints", watermark)
    
    def fetch_drivers(
        self,
        session_key: Optional[int] = None,
        driver_number: Optional[int] = None,
        watermark: str = None
    ) -> List[OpenF1Driver]:
        params = {
            "session_key": session_key,
            "driver_number": driver_number
        }
        raw_data = self._fetch_endpoint("drivers", params)
        return self._validate_and_model(raw_data, OpenF1Driver, "drivers", watermark)
    
    def fetch_meetings(
        self,
        year: Optional[int] = None,
        meeting_key: Optional[int] = None,
        watermark: str = None
    ) -> List[OpenF1Meeting]:
        params = {
            "year": year,
            "meeting_key": meeting_key
        }
        raw_data = self._fetch_endpoint("meetings", params)
        return self._validate_and_model(raw_data, OpenF1Meeting, "meetings", watermark)
    
    def fetch_sessions(
        self,
        meeting_key: Optional[int] = None,
        session_key: Optional[int] = None,
        session_type: Optional[str] = None,
        year: Optional[int] = None,
        watermark: str = None
    ) -> List[OpenF1Session]:
        params = {
            "meeting_key": meeting_key,
            "session_key": session_key,
            "session_type": session_type,
            "year": year
        }
        raw_data = self._fetch_endpoint("sessions", params)
        return self._validate_and_model(raw_data, OpenF1Session, "sessions", watermark)
    
    def fetch_starting_grid(
        self,
        session_key: int,
        watermark: str = None
    ) -> List[OpenF1StartingGrid]:
        params = {"session_key": session_key}
        raw_data = self._fetch_endpoint("starting_grid", params)
        return self._validate_and_model(raw_data, OpenF1StartingGrid, "starting_grid", watermark)
    
    def fetch_session_result(
        self,
        session_key: int,
        watermark: str = None
    ) -> List[OpenF1SessionResult]:
        params = {"session_key": session_key}
        raw_data = self._fetch_endpoint("session_result", params)
        return self._validate_and_model(raw_data, OpenF1SessionResult, "session_result", watermark)
    
    def fetch_positions(
        self,
        session_key: int,
        driver_number: Optional[int] = None,
        watermark: str = None
    ) -> List[OpenF1Position]:
        params = {
            "session_key": session_key,
            "driver_number": driver_number
        }
        raw_data = self._fetch_endpoint("position", params)
        return self._validate_and_model(raw_data, OpenF1Position, "position", watermark)