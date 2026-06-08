# orchestrators/dags/test_etl.py
import sys
from pathlib import Path

# Добавляем корень проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import logging
from datetime import datetime

from config.settings import settings
from repositories.mongo_repository import MongoRepository
from extractors.openf1_extractor import OpenF1Extractor

# ============================================
# НАСТРОЙКИ ТЕСТОВ (меняй здесь!)
# ============================================
TEST_YEAR = 2024
TEST_SESSION_KEY = 9693  # Если None - берется первая гонка года
TEST_DRIVER_NUMBER = None  # Если None - тестируем всех
# ============================================

# Настройка логирования
logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)


def get_test_session(extractor, year: int, session_key: int = None):
    """Получает тестовую сессию"""
    if session_key:
        sessions = extractor.fetch_sessions(session_key=session_key)
        if sessions:
            return sessions[0]
    
    sessions = extractor.fetch_sessions(year=year, session_type="Race")
    if sessions:
        return sessions[0]
    
    raise Exception(f"No races found for year {year}")


def get_test_meeting(extractor, year: int):
    """Получает тестовый meeting"""
    meetings = extractor.fetch_meetings(year=year)
    if meetings:
        return meetings[0]
    raise Exception(f"No meetings found for year {year}")


def test_mongo_connection():
    """Тест 1: Подключение к MongoDB"""
    print("\n" + "="*60)
    print("ТЕСТ 1: Подключение к MongoDB")
    print("="*60)
    
    try:
        repo = MongoRepository(
            connection_string=settings.mongodb_connection_string,
            database_name=settings.mongo_database
        )
        
        collections = repo.db.list_collection_names()
        print(f"✅ MongoDB подключен успешно!")
        print(f"   Хост: {settings.mongo_host}:{settings.mongo_port}")
        print(f"   База: {settings.mongo_database}")
        print(f"   Коллекции: {collections}")
        
        repo.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return False


def test_openf1_api():
    """Тест 2: Запрос к OpenF1 API"""
    print("\n" + "="*60)
    print("ТЕСТ 2: Запрос к OpenF1 API")
    print("="*60)
    
    try:
        extractor = OpenF1Extractor(
            base_url=settings.openf1_base_url,
            verify_ssl=settings.openf1_verify_ssl,
            timeout=settings.openf1_timeout,
            max_retries=settings.openf1_max_retries
        )
        
        print("Тестирование эндпоинтов API...")
        
        meetings = extractor.fetch_meetings(year=TEST_YEAR)
        print(f"   ✅ meetings: {len(meetings)} записей")
        
        sessions = extractor.fetch_sessions(year=TEST_YEAR, session_type="Race")
        print(f"   ✅ sessions: {len(sessions)} записей")
        
        if sessions:
            test_session = sessions[0]
            session_key = test_session.session_key
            
            drivers = extractor.fetch_drivers(session_key=session_key)
            print(f"   ✅ drivers: {len(drivers)} записей")
            
            starting_grid = extractor.fetch_starting_grid(session_key=session_key)
            print(f"   ✅ starting_grid: {len(starting_grid)} записей")
            
            session_results = extractor.fetch_session_result(session_key=session_key)
            print(f"   ✅ session_result: {len(session_results)} записей")
            
            # Дополнительные эндпоинты
            weather = extractor.fetch_weather(session_key=session_key)
            print(f"   ✅ weather: {len(weather)} записей")
            
            race_control = extractor.fetch_race_control(session_key=session_key)
            print(f"   ✅ race_control: {len(race_control)} записей")
            
            overtakes = extractor.fetch_overtakes(session_key=session_key)
            print(f"   ✅ overtakes: {len(overtakes)} записей")
            
            pit_stops = extractor.fetch_pit_stops(session_key=session_key)
            print(f"   ✅ pit: {len(pit_stops)} записей")
        
        print(f"\n✅ API работает!")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка API: {e}")
        return False


def test_load_meetings():
    """Тест 2a: Загрузка встреч (meetings) в MongoDB"""
    print("\n" + "="*60)
    print("ТЕСТ 2a: Загрузка встреч (meetings)")
    print("="*60)
    
    try:
        extractor = OpenF1Extractor(
            base_url=settings.openf1_base_url,
            verify_ssl=settings.openf1_verify_ssl
        )
        
        repo = MongoRepository(
            connection_string=settings.mongodb_connection_string,
            database_name=settings.mongo_database
        )
        
        meetings = extractor.fetch_meetings(year=TEST_YEAR)
        
        if meetings:
            docs = [m.model_dump() for m in meetings]
            result = repo.save_many("meetings", docs)
            print(f"✅ Загружено {len(result)} встреч")
        else:
            print("⚠️ Нет данных о встречах")
        
        repo.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_load_sessions():
    """Тест 2b: Загрузка сессий в MongoDB"""
    print("\n" + "="*60)
    print("ТЕСТ 2b: Загрузка сессий")
    print("="*60)
    
    try:
        extractor = OpenF1Extractor(
            base_url=settings.openf1_base_url,
            verify_ssl=settings.openf1_verify_ssl
        )
        
        repo = MongoRepository(
            connection_string=settings.mongodb_connection_string,
            database_name=settings.mongo_database
        )
        
        test_meeting = get_test_meeting(extractor, TEST_YEAR)
        print(f"Тестовый meeting: {test_meeting.meeting_key} - {test_meeting.meeting_name}")
        
        sessions = extractor.fetch_sessions(meeting_key=test_meeting.meeting_key)
        
        if sessions:
            docs = [s.model_dump() for s in sessions]
            result = repo.save_many("sessions", docs)
            print(f"✅ Загружено {len(result)} сессий")
        else:
            print("⚠️ Нет данных о сессиях")
        
        repo.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_load_drivers():
    """Тест 3: Загрузка пилотов в MongoDB"""
    print("\n" + "="*60)
    print("ТЕСТ 3: Загрузка пилотов в MongoDB")
    print("="*60)
    
    try:
        extractor = OpenF1Extractor(
            base_url=settings.openf1_base_url,
            verify_ssl=settings.openf1_verify_ssl
        )
        
        repo = MongoRepository(
            connection_string=settings.mongodb_connection_string,
            database_name=settings.mongo_database
        )
        
        test_session = get_test_session(extractor, TEST_YEAR, TEST_SESSION_KEY)
        print(f"Тестовая сессия: {test_session.session_key} - {test_session.session_name}")
        
        drivers = extractor.fetch_drivers(session_key=test_session.session_key)
        
        if drivers:
            docs = [d.model_dump() for d in drivers]
            result = repo.save_many("drivers", docs)
            print(f"✅ Загружено {len(result)} пилотов")
        else:
            print("⚠️ Нет данных о пилотах")
        
        repo.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_load_starting_grid():
    """Тест 3a: Загрузка стартовой решетки"""
    print("\n" + "="*60)
    print("ТЕСТ 3a: Загрузка стартовой решетки")
    print("="*60)
    
    try:
        extractor = OpenF1Extractor(
            base_url=settings.openf1_base_url,
            verify_ssl=settings.openf1_verify_ssl
        )
        
        repo = MongoRepository(
            connection_string=settings.mongodb_connection_string,
            database_name=settings.mongo_database
        )
        
        test_session = get_test_session(extractor, TEST_YEAR, TEST_SESSION_KEY)
        print(f"Тестовая сессия: {test_session.session_key}")
        
        starting_grid = extractor.fetch_starting_grid(session_key=test_session.session_key)
        
        if starting_grid:
            docs = [sg.model_dump() for sg in starting_grid]
            result = repo.save_many("starting_grid", docs)
            print(f"✅ Загружено {len(result)} записей стартовой решетки")
        else:
            print("⚠️ Нет данных стартовой решетки")
        
        repo.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False


def test_load_session_results():
    """Тест 3b: Загрузка результатов сессии"""
    print("\n" + "="*60)
    print("ТЕСТ 3b: Загрузка результатов сессии")
    print("="*60)
    
    try:
        extractor = OpenF1Extractor(
            base_url=settings.openf1_base_url,
            verify_ssl=settings.openf1_verify_ssl
        )
        
        repo = MongoRepository(
            connection_string=settings.mongodb_connection_string,
            database_name=settings.mongo_database
        )
        
        test_session = get_test_session(extractor, TEST_YEAR, TEST_SESSION_KEY)
        print(f"Тестовая сессия: {test_session.session_key}")
        
        session_results = extractor.fetch_session_result(session_key=test_session.session_key)
        
        if session_results:
            docs = [sr.model_dump() for sr in session_results]
            result = repo.save_many("session_result", docs)
            print(f"✅ Загружено {len(result)} записей результатов")
        else:
            print("⚠️ Нет данных результатов")
        
        repo.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False


def test_load_car_data():
    """Тест 4: Загрузка телеметрии в MongoDB"""
    print("\n" + "="*60)
    print("ТЕСТ 4: Загрузка телеметрии (car_data)")
    print("="*60)
    
    try:
        extractor = OpenF1Extractor(
            base_url=settings.openf1_base_url,
            verify_ssl=settings.openf1_verify_ssl,
            request_delay_min=0.3,
            request_delay_max=0.8
        )
        
        repo = MongoRepository(
            connection_string=settings.mongodb_connection_string,
            database_name=settings.mongo_database
        )
        
        test_session = get_test_session(extractor, TEST_YEAR, TEST_SESSION_KEY)
        print(f"Тестовая сессия: {test_session.session_key} - {test_session.session_name}")
        
        if TEST_DRIVER_NUMBER:
            print(f"Загрузка для гонщика #{TEST_DRIVER_NUMBER}")
        
        car_data = extractor.fetch_car_data(
            session_key=test_session.session_key,
            driver_number=TEST_DRIVER_NUMBER
        )
        
        if car_data:
            docs = [d.model_dump() for d in car_data]
            result = repo.save_many_batched("car_data", docs, batch_size=10000)
            print(f"✅ Загружено {len(result)} записей телеметрии")
        else:
            print("⚠️ Нет данных телеметрии для этой сессии")
        
        repo.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_load_positions():
    """Тест 5a: Загрузка позиций в MongoDB"""
    print("\n" + "="*60)
    print("ТЕСТ 5a: Загрузка позиций")
    print("="*60)
    
    try:
        extractor = OpenF1Extractor(
            base_url=settings.openf1_base_url,
            verify_ssl=settings.openf1_verify_ssl
        )
        
        repo = MongoRepository(
            connection_string=settings.mongodb_connection_string,
            database_name=settings.mongo_database
        )
        
        test_session = get_test_session(extractor, TEST_YEAR, TEST_SESSION_KEY)
        print(f"Тестовая сессия: {test_session.session_key}")
        
        positions = extractor.fetch_positions(
            session_key=test_session.session_key,
            driver_number=TEST_DRIVER_NUMBER
        )
        
        if positions:
            docs = [d.model_dump() for d in positions]
            result = repo.save_many_batched("position", docs, batch_size=10000)
            print(f"✅ Загружено {len(result)} записей позиций")
        else:
            print("⚠️ Нет данных позиций")
        
        repo.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False


def test_load_laps():
    """Тест 5b: Загрузка данных о кругах"""
    print("\n" + "="*60)
    print("ТЕСТ 5b: Загрузка данных о кругах")
    print("="*60)
    
    try:
        extractor = OpenF1Extractor(
            base_url=settings.openf1_base_url,
            verify_ssl=settings.openf1_verify_ssl
        )
        
        repo = MongoRepository(
            connection_string=settings.mongodb_connection_string,
            database_name=settings.mongo_database
        )
        
        test_session = get_test_session(extractor, TEST_YEAR, TEST_SESSION_KEY)
        print(f"Тестовая сессия: {test_session.session_key}")
        
        laps = extractor.fetch_laps(
            session_key=test_session.session_key,
            driver_number=TEST_DRIVER_NUMBER
        )
        
        if laps:
            docs = [d.model_dump() for d in laps]
            result = repo.save_many_batched("laps", docs, batch_size=10000)
            print(f"✅ Загружено {len(result)} записей кругов")
        else:
            print("⚠️ Нет данных о кругах")
        
        repo.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False


def test_load_intervals():
    """Тест 5c: Загрузка интервалов"""
    print("\n" + "="*60)
    print("ТЕСТ 5c: Загрузка интервалов")
    print("="*60)
    
    try:
        extractor = OpenF1Extractor(
            base_url=settings.openf1_base_url,
            verify_ssl=settings.openf1_verify_ssl
        )
        
        repo = MongoRepository(
            connection_string=settings.mongodb_connection_string,
            database_name=settings.mongo_database
        )
        
        test_session = get_test_session(extractor, TEST_YEAR, TEST_SESSION_KEY)
        print(f"Тестовая сессия: {test_session.session_key}")
        
        intervals = extractor.fetch_intervals(
            session_key=test_session.session_key,
            driver_number=TEST_DRIVER_NUMBER
        )
        
        if intervals:
            docs = [d.model_dump() for d in intervals]
            result = repo.save_many_batched("intervals", docs, batch_size=10000)
            print(f"✅ Загружено {len(result)} записей интервалов")
        else:
            print("⚠️ Нет данных интервалов")
        
        repo.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False


def test_load_weather():
    """Тест 5d: Загрузка погодных данных"""
    print("\n" + "="*60)
    print("ТЕСТ 5d: Загрузка погодных данных")
    print("="*60)
    
    try:
        extractor = OpenF1Extractor(
            base_url=settings.openf1_base_url,
            verify_ssl=settings.openf1_verify_ssl
        )
        
        repo = MongoRepository(
            connection_string=settings.mongodb_connection_string,
            database_name=settings.mongo_database
        )
        
        test_session = get_test_session(extractor, TEST_YEAR, TEST_SESSION_KEY)
        print(f"Тестовая сессия: {test_session.session_key}")
        
        weather = extractor.fetch_weather(session_key=test_session.session_key)
        
        if weather:
            docs = [d.model_dump() for d in weather]
            result = repo.save_many("weather", docs)
            print(f"✅ Загружено {len(result)} записей погоды")
        else:
            print("⚠️ Нет данных погоды")
        
        repo.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False


def test_load_pit_stops():
    """Тест 5e: Загрузка данных о пит-стопах"""
    print("\n" + "="*60)
    print("ТЕСТ 5e: Загрузка данных о пит-стопах")
    print("="*60)
    
    try:
        extractor = OpenF1Extractor(
            base_url=settings.openf1_base_url,
            verify_ssl=settings.openf1_verify_ssl
        )
        
        repo = MongoRepository(
            connection_string=settings.mongodb_connection_string,
            database_name=settings.mongo_database
        )
        
        test_session = get_test_session(extractor, TEST_YEAR, TEST_SESSION_KEY)
        print(f"Тестовая сессия: {test_session.session_key}")
        
        pit_stops = extractor.fetch_pit_stops(
            session_key=test_session.session_key,
            driver_number=TEST_DRIVER_NUMBER
        )
        
        if pit_stops:
            docs = [d.model_dump() for d in pit_stops]
            result = repo.save_many("pit", docs)
            print(f"✅ Загружено {len(result)} записей пит-стопов")
        else:
            print("⚠️ Нет данных о пит-стопах")
        
        repo.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False


def test_load_race_control():
    """Тест 5f: Загрузка событий race control"""
    print("\n" + "="*60)
    print("ТЕСТ 5f: Загрузка событий race control")
    print("="*60)
    
    try:
        extractor = OpenF1Extractor(
            base_url=settings.openf1_base_url,
            verify_ssl=settings.openf1_verify_ssl
        )
        
        repo = MongoRepository(
            connection_string=settings.mongodb_connection_string,
            database_name=settings.mongo_database
        )
        
        test_session = get_test_session(extractor, TEST_YEAR, TEST_SESSION_KEY)
        print(f"Тестовая сессия: {test_session.session_key}")
        
        race_control = extractor.fetch_race_control(session_key=test_session.session_key)
        
        if race_control:
            docs = [d.model_dump() for d in race_control]
            result = repo.save_many("race_control", docs)
            print(f"✅ Загружено {len(result)} записей race control")
        else:
            print("⚠️ Нет данных race control")
        
        repo.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False


def test_load_overtakes():
    """Тест 5g: Загрузка данных об обгонах"""
    print("\n" + "="*60)
    print("ТЕСТ 5g: Загрузка данных об обгонах")
    print("="*60)
    
    try:
        extractor = OpenF1Extractor(
            base_url=settings.openf1_base_url,
            verify_ssl=settings.openf1_verify_ssl
        )
        
        repo = MongoRepository(
            connection_string=settings.mongodb_connection_string,
            database_name=settings.mongo_database
        )
        
        test_session = get_test_session(extractor, TEST_YEAR, TEST_SESSION_KEY)
        print(f"Тестовая сессия: {test_session.session_key}")
        
        overtakes = extractor.fetch_overtakes(session_key=test_session.session_key)
        
        if overtakes:
            docs = [d.model_dump() for d in overtakes]
            result = repo.save_many("overtakes", docs)
            print(f"✅ Загружено {len(result)} записей об обгонах")
        else:
            print("⚠️ Нет данных об обгонах")
        
        repo.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False


def test_load_stints():
    """Тест 5h: Загрузка данных о стентах"""
    print("\n" + "="*60)
    print("ТЕСТ 5h: Загрузка данных о стентах")
    print("="*60)
    
    try:
        extractor = OpenF1Extractor(
            base_url=settings.openf1_base_url,
            verify_ssl=settings.openf1_verify_ssl
        )
        
        repo = MongoRepository(
            connection_string=settings.mongodb_connection_string,
            database_name=settings.mongo_database
        )
        
        test_session = get_test_session(extractor, TEST_YEAR, TEST_SESSION_KEY)
        print(f"Тестовая сессия: {test_session.session_key}")
        
        stints = extractor.fetch_stints(
            session_key=test_session.session_key,
            driver_number=TEST_DRIVER_NUMBER
        )
        
        if stints:
            docs = [d.model_dump() for d in stints]
            result = repo.save_many("stints", docs)
            print(f"✅ Загружено {len(result)} записей о стентах")
        else:
            print("⚠️ Нет данных о стентах")
        
        repo.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False


def test_load_location():
    """Тест 5i: Загрузка данных о местоположении"""
    print("\n" + "="*60)
    print("ТЕСТ 5i: Загрузка данных о местоположении")
    print("="*60)
    
    try:
        extractor = OpenF1Extractor(
            base_url=settings.openf1_base_url,
            verify_ssl=settings.openf1_verify_ssl,
            request_delay_min=0.3,
            request_delay_max=0.8
        )
        
        repo = MongoRepository(
            connection_string=settings.mongodb_connection_string,
            database_name=settings.mongo_database
        )
        
        test_session = get_test_session(extractor, TEST_YEAR, TEST_SESSION_KEY)
        print(f"Тестовая сессия: {test_session.session_key}")
        
        location = extractor.fetch_location(
            session_key=test_session.session_key,
            driver_number=TEST_DRIVER_NUMBER
        )
        
        if location:
            docs = [d.model_dump() for d in location]
            result = repo.save_many_batched("location", docs, batch_size=10000)
            print(f"✅ Загружено {len(result)} записей о местоположении")
        else:
            print("⚠️ Нет данных о местоположении")
        
        repo.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False


def test_deduplication():
    """Тест 6: Защита от дубликатов (etl_hash)"""
    print("\n" + "="*60)
    print("ТЕСТ 6: Защита от дубликатов (etl_hash)")
    print("="*60)
    
    try:
        repo = MongoRepository(
            connection_string=settings.mongodb_connection_string,
            database_name=settings.mongo_database
        )
        
        test_doc = {
            "test_id": "duplicate_test_1",
            "data": "test",
            "etl_hash": "test_hash_123"
        }
        
        id1 = repo.save_one("test_collection", test_doc)
        print(f"Первая вставка: {id1}")
        
        id2 = repo.save_one("test_collection", test_doc)
        print(f"Вторая вставка: {id2}")
        
        docs = repo.find_by_filter("test_collection", {"etl_hash": "test_hash_123"})
        
        if len(docs) == 1:
            print("✅ Дедупликация работает! Дубликат не создан")
        else:
            print(f"⚠️ Найдено {len(docs)} документов, ожидался 1")
        
        repo.db.test_collection.delete_many({"etl_hash": "test_hash_123"})
        repo.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False


def run_all_tests():
    """Запускает все тесты"""
    print("\n" + "="*60)
    print("🧪 ЗАПУСК ТЕСТОВ F1 ETL")
    print("="*60)
    print(f"📅 Год: {TEST_YEAR}")
    print(f"🔑 Сессия: {TEST_SESSION_KEY if TEST_SESSION_KEY else 'auto (первая гонка)'}")
    print(f"🏎️  Гонщик: {TEST_DRIVER_NUMBER if TEST_DRIVER_NUMBER else 'все'}")
    print("="*60)
    
    results = {
        # Базовые тесты
        # "MongoDB": test_mongo_connection(),
        # "OpenF1 API": test_openf1_api(),
        
        # # Загрузка справочных данных
        # "Загрузка встреч (meetings)": test_load_meetings(),
        # "Загрузка сессий (sessions)": test_load_sessions(),
        # "Загрузка пилотов (drivers)": test_load_drivers(),
        # "Загрузка стартовой решетки": test_load_starting_grid(),
        # "Загрузка результатов сессии": test_load_session_results(),
        
        # # Загрузка телеметрии и позиций
        # "Загрузка телеметрии (car_data)": test_load_car_data(),
        "Загрузка позиций (position)": test_load_positions(),
        # "Загрузка местоположения (location)": test_load_location(),
        
        # # Загрузка данных о кругах и интервалах
        # "Загрузка кругов (laps)": test_load_laps(),
        # "Загрузка интервалов (intervals)": test_load_intervals(),
        
        # # Загрузка событий и пит-стопов
        # "Загрузка пит-стопов (pit)": test_load_pit_stops(),
        # "Загрузка race control": test_load_race_control(),
        # "Загрузка обгонов (overtakes)": test_load_overtakes(),
        # "Загрузка стентов (stints)": test_load_stints(),
        
        # # Загрузка погоды
        # "Загрузка погоды (weather)": test_load_weather(),
        
        # # Системные тесты
        # "Защита от дубликатов": test_deduplication(),
    }
    
    print("\n" + "="*60)
    print("РЕЗУЛЬТАТЫ ТЕСТОВ")
    print("="*60)
    
    passed = 0
    for name, result in results.items():
        status = "✅" if result else "❌"
        print(f"{status} {name}")
        if result:
            passed += 1
    
    print(f"\n📊 Итого: {passed}/{len(results)} тестов пройдено")
    print("="*60)
    
    if passed < len(results):
        print("⚠️ Некоторые тесты не пройдены. Проверьте логи выше.")
    
    return passed == len(results)


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)