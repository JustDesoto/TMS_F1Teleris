# extractors/openf1_extractor.py
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
import time
import random
import hashlib
import json

from .base_extractor import BaseExtractor
from config.settings import settings

logger = logging.getLogger(__name__)


class OpenF1Extractor(BaseExtractor):
    """
    Экстрактор для OpenF1 API.
    Извлекает данные из API и добавляет ETL метаданные без Pydantic валидации.
    """
    
    def __init__(
        self, 
        base_url: Optional[str] = None,
        verify_ssl: Optional[bool] = None,
        timeout: Optional[int] = None,
        max_retries: Optional[int] = None,
        request_delay_min: float = 0.5,
        request_delay_max: float = 1.5,
        validate_models: bool = False
    ):
        """
        Инициализация экстрактора OpenF1.
        
        Args:
            base_url: Базовый URL OpenF1 API
            verify_ssl: Проверять SSL сертификаты
            timeout: Таймаут запроса в секундах
            max_retries: Максимальное количество повторных попыток
            request_delay_min: Минимальная задержка между запросами (секунды)
            request_delay_max: Максимальная задержка между запросами (секунды)
            validate_models: Если True, валидировать через Pydantic модели (для тестирования)
        """
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
        self.validate_models = validate_models
        
        # Ленивая загрузка моделей только если нужно
        self._models = None
        
        logger.info(f"OpenF1 Extractor инициализирован: base_url={self.base_url}, "
                   f"verify_ssl={self.verify_ssl}, timeout={self.timeout}, "
                   f"max_retries={self.max_retries}, validate_models={self.validate_models}")
    
    def _get_models(self):
        """Ленивая загрузка Pydantic моделей (только если включена валидация)"""
        if self._models is None and self.validate_models:
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
            
            self._models = {
                "car_data": OpenF1CarData,
                "laps": OpenF1Lap,
                "pit": OpenF1Pit,
                "position": OpenF1Position,
                "weather": OpenF1Weather,
                "intervals": OpenF1Interval,
                "drivers": OpenF1Driver,
                "meetings": OpenF1Meeting,
                "sessions": OpenF1Session,
                "race_control": OpenF1RaceControl,
                "overtakes": OpenF1Overtake,
                "stints": OpenF1Stints,
                "starting_grid": OpenF1StartingGrid,
                "session_result": OpenF1SessionResult,
                "location": OpenF1Location,
            }
        return self._models
    
    def _generate_hash(self, data: Dict[str, Any]) -> str:
        """
        Генерирует хэш для дедупликации.
        
        Args:
            data: Словарь с данными
        
        Returns:
            SHA256 хэш в виде строки
        """
        # Исключаем метаданные из хэша
        data_for_hash = {k: v for k, v in data.items() 
                        if not k.startswith('etl_')}
        json_str = json.dumps(data_for_hash, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()
    
    def _rate_limit_wait(self):
        """Ограничивает частоту запросов к API"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def fetch_endpoint(self, endpoint: str, params: Dict[str, Any]) -> List[Dict]:
        """Публичный метод для запроса к эндпоинту API"""
        return self._fetch_endpoint(endpoint, params)
    
    def _is_no_data_response(self, response_data: Any) -> bool:
        """
        Проверяет, является ли ответ сообщением об отсутствии данных.
        
        OpenF1 API возвращает {"detail":"No results found."} когда нет данных.
        Это не ошибка, а просто пустой результат.
        """
        if isinstance(response_data, dict) and "detail" in response_data:
            detail = response_data.get("detail", "")
            if "No results found" in detail or "not found" in detail.lower():
                return True
        return False
    
    def _fetch_endpoint(self, endpoint: str, params: Dict[str, Any]) -> List[Dict]:
        """
        Получает данные из API с повторными попытками и ограничением частоты.
        
        Args:
            endpoint: Название эндпоинта API
            params: Параметры запроса
        
        Returns:
            Список словарей с данными (пустой список если данных нет)
        """
        url = f"{self.base_url}/{endpoint}"
        params = {k: v for k, v in params.items() if v is not None}
        
        self._rate_limit_wait()
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    url, 
                    params=params, 
                    timeout=self.timeout, 
                    verify=self.verify_ssl
                )
                
                # Обработка ограничения частоты запросов
                if response.status_code == 429:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"Лимит запросов (429) для {endpoint}. Ожидание {wait_time:.1f}с "
                                 f"перед попыткой {attempt + 1}/{self.max_retries}")
                    time.sleep(wait_time)
                    continue
                
                # Проверка на отсутствие данных (200 OK но нет результатов)
                if response.status_code == 200:
                    data = response.json()
                    
                    # Проверяем, не является ли ответ сообщением об отсутствии данных
                    if self._is_no_data_response(data):
                        logger.info(f"Нет данных для {endpoint} с параметрами {params}")
                        return []
                    
                    # Обычный ответ с данными
                    if not isinstance(data, list):
                        data = [data] if data else []
                    logger.debug(f"Получено {len(data)} записей из {endpoint}")
                    return data
                
                # Обработка других HTTP ошибок
                response.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                # Не повторяем при 404 - скорее всего это постоянная ошибка
                if hasattr(e, 'response') and e.response is not None:
                    if e.response.status_code == 404:
                        logger.warning(f"Эндпоинт {endpoint} не найден (404): {e}")
                        return []
                    
                    # Проверка на 400 с сообщением об отсутствии результатов
                    if e.response.status_code == 400:
                        try:
                            error_data = e.response.json()
                            if self._is_no_data_response(error_data):
                                logger.info(f"Нет данных для {endpoint} (400 ответ с сообщением об отсутствии результатов)")
                                return []
                        except:
                            pass
                
                # Повторяем при других ошибках
                if attempt == self.max_retries - 1:
                    logger.error(f"Не удалось получить данные из {endpoint} после {self.max_retries} попыток: {e}")
                    raise
                
                wait_time = (2 ** attempt) + random.uniform(0, 0.5)
                logger.warning(f"Попытка {attempt + 1}/{self.max_retries} не удалась "
                             f"для {endpoint}: {e}. Ожидание {wait_time:.1f}с")
                time.sleep(wait_time)
        
        return []
    
    def _process_records(
        self, 
        data_list: List[Dict], 
        endpoint: str, 
        watermark: str = None,
        extra_fields: Dict[str, Any] = None
    ) -> List[Dict]:
        """
        Обрабатывает записи: либо валидирует через Pydantic, либо добавляет метаданные напрямую.
        
        Args:
            data_list: Сырые данные из API
            endpoint: Название эндпоинта
            watermark: Watermark для инкрементальной загрузки
            extra_fields: Дополнительные поля для добавления к каждой записи
        
        Returns:
            Список словарей, готовых для сохранения в MongoDB
        """
        extra_fields = extra_fields or {}
        
        # Режим без валидации - просто добавляем метаданные
        if not self.validate_models:
            processed = []
            for item in data_list:
                doc = {
                    **item,
                    "etl_loaded_at": datetime.utcnow(),
                    "etl_source": "openf1",
                    "etl_endpoint": endpoint,
                    **extra_fields
                }
                if watermark:
                    doc["etl_watermark"] = watermark
                
                # Добавляем хэш для дедупликации
                doc["etl_hash"] = self._generate_hash(doc)
                
                processed.append(doc)
            
            logger.debug(f"Обработано {len(processed)} записей для {endpoint} (без валидации)")
            return processed
        
        # Режим с валидацией - используем Pydantic модели
        models = self._get_models()
        model_class = models.get(endpoint)
        
        if not model_class:
            logger.warning(f"Нет модели для эндпоинта {endpoint}, возвращаем сырые данные")
            return data_list
        
        validated = []
        for item in data_list:
            try:
                obj = model_class(**item)
                if watermark:
                    obj.etl_watermark = watermark
                # Добавляем дополнительные поля
                for key, value in extra_fields.items():
                    setattr(obj, key, value)
                validated.append(obj.model_dump())
            except Exception as e:
                logger.error(f"Ошибка валидации для {endpoint}: {e}")
                self.save_to_dead_letter(item, endpoint, str(e))
        
        logger.info(f"Валидировано {len(validated)}/{len(data_list)} записей для {endpoint}")
        return validated
    
    def fetch_car_data(
        self,
        session_key: int,
        driver_number: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        watermark: str = None
    ) -> List[Dict]:
        """
        Получает телеметрию машин.
        
        Args:
            session_key: Идентификатор сессии
            driver_number: Номер гонщика (опционально)
            start_date: Начальная дата (опционально)
            end_date: Конечная дата (опционально)
            watermark: Watermark для инкрементальной загрузки
        
        Returns:
            Список словарей с телеметрией
        """
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
            return self._process_records(raw_data, "car_data", watermark)
        
        logger.info(f"Загрузка car_data для сессии {session_key} - сначала получаем список гонщиков...")
        
        drivers = self.fetch_drivers(session_key=session_key)
        if not drivers:
            logger.warning(f"Не найдено гонщиков для сессии {session_key}")
            return []
        
        logger.info(f"Найдено {len(drivers)} гонщиков, загружаем car_data для каждого...")
        
        for idx, driver in enumerate(drivers):
            try:
                driver_number_val = driver.get("driver_number") if isinstance(driver, dict) else driver.driver_number
                
                params = {
                    "session_key": session_key,
                    "driver_number": driver_number_val,
                }
                if start_date:
                    params["date>="] = start_date.isoformat()
                if end_date:
                    params["date<="] = end_date.isoformat()
                
                raw_data = self._fetch_endpoint("car_data", params)
                
                if raw_data:
                    processed = self._process_records(raw_data, "car_data", watermark)
                    all_data.extend(processed)
                    logger.info(f"  Гонщик {driver_number_val}: {len(processed)} записей ({idx + 1}/{len(drivers)})")
                else:
                    logger.debug(f"  Гонщик {driver_number_val}: нет данных")
                
                # Задержка между запросами
                delay = random.uniform(self.request_delay_min, self.request_delay_max)
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"Ошибка при загрузке данных для гонщика {driver_number_val}: {e}")
                continue
        
        logger.info(f"Всего загружено car_data записей для сессии {session_key}: {len(all_data)}")
        return all_data
    
    def fetch_laps(
        self,
        session_key: int,
        driver_number: Optional[int] = None,
        lap_number: Optional[int] = None,
        watermark: str = None
    ) -> List[Dict]:
        """Получает данные о кругах"""
        all_data = []
        
        if driver_number:
            params = {
                "session_key": session_key,
                "driver_number": driver_number,
                "lap_number": lap_number,
            }
            raw_data = self._fetch_endpoint("laps", params)
            return self._process_records(raw_data, "laps", watermark)
        
        drivers = self.fetch_drivers(session_key=session_key)
        for idx, driver in enumerate(drivers):
            driver_number_val = driver.get("driver_number") if isinstance(driver, dict) else driver.driver_number
            
            params = {
                "session_key": session_key,
                "driver_number": driver_number_val,
            }
            raw_data = self._fetch_endpoint("laps", params)
            if raw_data:
                processed = self._process_records(raw_data, "laps", watermark)
                all_data.extend(processed)
                logger.info(f"  Гонщик {driver_number_val}: {len(processed)} записей ({idx + 1}/{len(drivers)})")
            
            delay = random.uniform(self.request_delay_min, self.request_delay_max)
            time.sleep(delay)
        
        return all_data
    
    def fetch_pit_stops(
        self,
        session_key: int,
        driver_number: Optional[int] = None,
        watermark: str = None
    ) -> List[Dict]:
        """Получает данные о пит-стопах"""
        params = {
            "session_key": session_key,
            "driver_number": driver_number
        }
        raw_data = self._fetch_endpoint("pit", params)
        return self._process_records(raw_data, "pit", watermark)
    
    def fetch_location(
        self,
        session_key: int,
        driver_number: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        watermark: str = None
    ) -> List[Dict]:
        """Получает GPS данные о местоположении машин"""
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
            return self._process_records(raw_data, "location", watermark)
        
        logger.info(f"Загрузка location для сессии {session_key} - сначала получаем список гонщиков...")
        
        drivers = self.fetch_drivers(session_key=session_key)
        if not drivers:
            logger.warning(f"Не найдено гонщиков для сессии {session_key}")
            return []
        
        logger.info(f"Найдено {len(drivers)} гонщиков, загружаем location для каждого...")
        
        for idx, driver in enumerate(drivers):
            try:
                driver_number_val = driver.get("driver_number") if isinstance(driver, dict) else driver.driver_number
                
                params = {
                    "session_key": session_key,
                    "driver_number": driver_number_val,
                }
                if start_date:
                    params["date>="] = start_date.isoformat()
                if end_date:
                    params["date<="] = end_date.isoformat()
                
                raw_data = self._fetch_endpoint("location", params)
                
                if raw_data:
                    processed = self._process_records(raw_data, "location", watermark)
                    all_data.extend(processed)
                    logger.info(f"  Гонщик {driver_number_val}: {len(processed)} записей ({idx + 1}/{len(drivers)})")
                else:
                    logger.debug(f"  Гонщик {driver_number_val}: нет данных")
                
                delay = random.uniform(self.request_delay_min, self.request_delay_max)
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"Ошибка при загрузке location для гонщика {driver_number_val}: {e}")
                continue
        
        logger.info(f"Всего загружено location записей для сессии {session_key}: {len(all_data)}")
        return all_data
    
    def fetch_weather(
        self,
        session_key: int,
        watermark: str = None
    ) -> List[Dict]:
        """Получает погодные данные"""
        params = {"session_key": session_key}
        raw_data = self._fetch_endpoint("weather", params)
        return self._process_records(raw_data, "weather", watermark)
    
    def fetch_intervals(
        self,
        session_key: int,
        driver_number: Optional[int] = None,
        watermark: str = None
    ) -> List[Dict]:
        """Получает интервалы между гонщиками"""
        all_data = []
        
        if driver_number:
            params = {
                "session_key": session_key,
                "driver_number": driver_number,
            }
            raw_data = self._fetch_endpoint("intervals", params)
            return self._process_records(raw_data, "intervals", watermark)
        
        drivers = self.fetch_drivers(session_key=session_key)
        for idx, driver in enumerate(drivers):
            try:
                driver_number_val = driver.get("driver_number") if isinstance(driver, dict) else driver.driver_number
                
                params = {
                    "session_key": session_key,
                    "driver_number": driver_number_val,
                }
                raw_data = self._fetch_endpoint("intervals", params)
                
                if raw_data:
                    processed = self._process_records(raw_data, "intervals", watermark)
                    all_data.extend(processed)
                    logger.info(f"  Гонщик {driver_number_val}: {len(processed)} записей ({idx + 1}/{len(drivers)})")
                else:
                    logger.debug(f"  Гонщик {driver_number_val}: нет данных")
                
                delay = random.uniform(self.request_delay_min, self.request_delay_max)
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"Ошибка при загрузке данных для гонщика {driver_number_val}: {e}")
                continue
        
        logger.info(f"Всего загружено interval записей для сессии {session_key}: {len(all_data)}")
        return all_data
    
    def fetch_race_control(
        self,
        session_key: int,
        watermark: str = None
    ) -> List[Dict]:
        """Получает события race control (флаги, инциденты)"""
        params = {"session_key": session_key}
        raw_data = self._fetch_endpoint("race_control", params)
        return self._process_records(raw_data, "race_control", watermark)
    
    def fetch_overtakes(
        self,
        session_key: int,
        watermark: str = None
    ) -> List[Dict]:
        """Получает данные об обгонах"""
        params = {"session_key": session_key}
        raw_data = self._fetch_endpoint("overtakes", params)
        return self._process_records(raw_data, "overtakes", watermark)
    
    def fetch_stints(
        self,
        session_key: int,
        driver_number: Optional[int] = None,
        watermark: str = None
    ) -> List[Dict]:
        """Получает данные о стентах (периодах между пит-стопами)"""
        params = {
            "session_key": session_key,
            "driver_number": driver_number
        }
        raw_data = self._fetch_endpoint("stints", params)
        return self._process_records(raw_data, "stints", watermark)
    
    def fetch_drivers(
        self,
        session_key: Optional[int] = None,
        driver_number: Optional[int] = None,
        watermark: str = None
    ) -> List[Dict]:
        """Получает информацию о гонщиках"""
        params = {
            "session_key": session_key,
            "driver_number": driver_number
        }
        raw_data = self._fetch_endpoint("drivers", params)
        return self._process_records(raw_data, "drivers", watermark)
    
    def fetch_meetings(
        self,
        year: Optional[int] = None,
        meeting_key: Optional[int] = None,
        watermark: str = None
    ) -> List[Dict]:
        """Получает информацию о встречах (этапах чемпионата)"""
        params = {
            "year": year,
            "meeting_key": meeting_key
        }
        raw_data = self._fetch_endpoint("meetings", params)
        return self._process_records(raw_data, "meetings", watermark)
    
    def fetch_sessions(
        self,
        meeting_key: Optional[int] = None,
        session_key: Optional[int] = None,
        session_type: Optional[str] = None,
        year: Optional[int] = None,
        watermark: str = None
    ) -> List[Dict]:
        """Получает информацию о сессиях"""
        params = {
            "meeting_key": meeting_key,
            "session_key": session_key,
            "session_type": session_type,
            "year": year
        }
        raw_data = self._fetch_endpoint("sessions", params)
        return self._process_records(raw_data, "sessions", watermark)
    
    def fetch_starting_grid(
        self,
        session_key: int,
        watermark: str = None
    ) -> List[Dict]:
        """Получает данные о стартовой решетке"""
        params = {"session_key": session_key}
        raw_data = self._fetch_endpoint("starting_grid", params)
        return self._process_records(raw_data, "starting_grid", watermark)
    
    def fetch_session_result(
        self,
        session_key: int,
        watermark: str = None
    ) -> List[Dict]:
        """Получает результаты сессии"""
        params = {"session_key": session_key}
        raw_data = self._fetch_endpoint("session_result", params)
        return self._process_records(raw_data, "session_result", watermark)
    
    def fetch_positions(
        self,
        session_key: int,
        driver_number: Optional[int] = None,
        watermark: str = None
    ) -> List[Dict]:
        """Получает данные о позициях гонщиков"""
        params = {
            "session_key": session_key,
            "driver_number": driver_number
        }
        raw_data = self._fetch_endpoint("position", params)
        return self._process_records(raw_data, "position", watermark)