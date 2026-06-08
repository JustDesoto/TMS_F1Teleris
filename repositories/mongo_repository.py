from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError, BulkWriteError
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import logging

from .base_repository import BaseRepository

logger = logging.getLogger(__name__)


class MongoRepository(BaseRepository):
    """
    Репозиторий для работы с MongoDB
    """
    
    def __init__(self, connection_string: str, database_name: str):
        """
        Args:
            connection_string: строка подключения MongoDB
            database_name: имя базы данных
        """
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]
        self._ensure_indexes()
        logger.info(f"MongoDB connected: {database_name}")
    
    def _ensure_indexes(self):
        """Создаёт индексы для всех коллекций (пропускает существующие)"""
        
        indexes_config = {
            'car_data': [
                IndexModel([("session_key", ASCENDING), ("driver_number", ASCENDING), ("date", ASCENDING)]),
                IndexModel([("etl_watermark", ASCENDING)]),
                IndexModel([("etl_hash", ASCENDING)], unique=True, sparse=True),
                IndexModel([("date", DESCENDING)]),
            ],
            'laps': [
                IndexModel([("session_key", ASCENDING), ("driver_number", ASCENDING), ("lap_number", ASCENDING)]),
                IndexModel([("etl_watermark", ASCENDING)]),
                IndexModel([("etl_hash", ASCENDING)], unique=True, sparse=True),
            ],
            'pit': [
                IndexModel([("session_key", ASCENDING), ("driver_number", ASCENDING), ("lap_number", ASCENDING)]),
                IndexModel([("etl_watermark", ASCENDING)]),
                IndexModel([("etl_hash", ASCENDING)], unique=True, sparse=True),
            ],
            'position': [
                IndexModel([("session_key", ASCENDING), ("driver_number", ASCENDING), ("date", ASCENDING)]),
                IndexModel([("etl_watermark", ASCENDING)]),
                IndexModel([("etl_hash", ASCENDING)], unique=True, sparse=True),
            ],
            'weather': [
                IndexModel([("session_key", ASCENDING), ("date", ASCENDING)]),
                IndexModel([("etl_watermark", ASCENDING)]),
                IndexModel([("etl_hash", ASCENDING)], unique=True, sparse=True),
            ],
            'intervals': [
                IndexModel([("session_key", ASCENDING), ("driver_number", ASCENDING), ("date", ASCENDING)]),
                IndexModel([("etl_watermark", ASCENDING)]),
                IndexModel([("etl_hash", ASCENDING)], unique=True, sparse=True),
            ],
            'drivers': [
                IndexModel([("driver_number", ASCENDING), ("session_key", ASCENDING)], unique=True),
                IndexModel([("meeting_key", ASCENDING)]),
                IndexModel([("etl_hash", ASCENDING)], unique=True, sparse=True),
            ],
            'meetings': [
                IndexModel([("meeting_key", ASCENDING)], unique=True),
                IndexModel([("year", ASCENDING)]),
                IndexModel([("etl_hash", ASCENDING)], unique=True, sparse=True),
            ],
            'sessions': [
                IndexModel([("session_key", ASCENDING)], unique=True),
                IndexModel([("meeting_key", ASCENDING)]),
                IndexModel([("date_end", ASCENDING)]),
                IndexModel([("etl_hash", ASCENDING)], unique=True, sparse=True),
            ],
            'overtakes': [
                IndexModel([("session_key", ASCENDING), ("overtaking_driver_number", ASCENDING)]),
                IndexModel([("etl_watermark", ASCENDING)]),
                IndexModel([("etl_hash", ASCENDING)], unique=True, sparse=True),
            ],
            'race_control': [
                IndexModel([("session_key", ASCENDING), ("date", ASCENDING)]),
                IndexModel([("flag", ASCENDING)]),
                IndexModel([("etl_hash", ASCENDING)], unique=True, sparse=True),
            ],
            'stints': [
                IndexModel([("session_key", ASCENDING), ("driver_number", ASCENDING), ("stint_number", ASCENDING)]),
                IndexModel([("etl_hash", ASCENDING)], unique=True, sparse=True),
            ],
            'starting_grid': [
                IndexModel([("session_key", ASCENDING), ("position", ASCENDING)]),
                IndexModel([("etl_hash", ASCENDING)], unique=True, sparse=True),
            ],
            'session_result': [
                IndexModel([("session_key", ASCENDING), ("driver_number", ASCENDING)], unique=True),
                IndexModel([("etl_hash", ASCENDING)], unique=True, sparse=True),
            ],
            'location': [
                IndexModel([("session_key", ASCENDING), ("driver_number", ASCENDING), ("date", ASCENDING)]),
                IndexModel([("etl_watermark", ASCENDING)]),
                IndexModel([("etl_hash", ASCENDING)], unique=True, sparse=True),
            ],
        }
        
        for collection, indexes in indexes_config.items():
            try:
                self.db[collection].create_indexes(indexes)
                logger.debug(f"Indexes ensured for {collection}")
            except Exception as e:
                logger.warning(f"Error creating indexes for {collection}: {e}")
        
        logger.info("All MongoDB indexes ensured")
    
    def save_one(self, collection: str, document: Dict[str, Any]) -> str:
        """Сохраняет один документ"""
        try:
            result = self.db[collection].insert_one(document)
            logger.debug(f"Saved document to {collection}: {result.inserted_id}")
            return str(result.inserted_id)
        except DuplicateKeyError as e:
            logger.warning(f"Duplicate key in {collection}: {e}")
            existing = self.db[collection].find_one({"etl_hash": document.get("etl_hash")})
            return str(existing['_id']) if existing else None
        except Exception as e:
            logger.error(f"Error saving to {collection}: {e}")
            raise
    
    def save_many(self, collection: str, documents: List[Dict[str, Any]]) -> List[str]:
        """Сохраняет много документов батчем"""
        if not documents:
            return []
        
        try:
            result = self.db[collection].insert_many(documents, ordered=False)
            logger.info(f"Saved {len(result.inserted_ids)} documents to {collection}")
            return [str(id) for id in result.inserted_ids]
        except BulkWriteError as e:
            inserted = e.details.get('nInserted', 0)
            logger.warning(f"Partial save to {collection}: {inserted} inserted, {len(documents) - inserted} duplicates")
            
            # Находим ID сохранённых документов
            hashes = [d.get("etl_hash") for d in documents if d.get("etl_hash")]
            if hashes:
                result = self.db[collection].find({"etl_hash": {"$in": hashes}})
                return [str(doc['_id']) for doc in result]
            return []
        except Exception as e:
            logger.error(f"Error saving batch to {collection}: {e}")
            raise
    
    def save_many_batched(self, collection: str, documents: List[Dict[str, Any]], batch_size: int = 1000) -> List[str]:
        """Сохраняет документы батчами для обхода лимита BSON"""
        all_ids = []
        
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            try:
                result = self.db[collection].insert_many(batch, ordered=False)
                all_ids.extend([str(id) for id in result.inserted_ids])
                logger.info(f"Saved batch {i//batch_size + 1}: {len(result.inserted_ids)} documents")
            except BulkWriteError as e:
                inserted = e.details.get('nInserted', 0)
                logger.warning(f"Batch partial save: {inserted} inserted, {len(batch) - inserted} duplicates")
                for doc in batch:
                    if doc.get("etl_hash"):
                        existing = self.db[collection].find_one({"etl_hash": doc["etl_hash"]})
                        if existing:
                            all_ids.append(str(existing['_id']))
            except Exception as e:
                logger.error(f"Error saving batch: {e}")
                raise
        
        return all_ids
    
    def find_by_filter(self, collection: str, filter_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Находит документы по фильтру"""
        cursor = self.db[collection].find(filter_dict)
        return list(cursor)
    
    def find_one(self, collection: str, filter_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Находит один документ по фильтру"""
        return self.db[collection].find_one(filter_dict)
    
    def upsert(self, collection: str, filter_dict: Dict[str, Any], document: Dict[str, Any]) -> str:
        """Обновляет или вставляет документ"""
        result = self.db[collection].update_one(
            filter_dict,
            {"$set": document},
            upsert=True
        )
        
        if result.upserted_id:
            return str(result.upserted_id)
        else:
            existing = self.db[collection].find_one(filter_dict)
            return str(existing['_id']) if existing else None
    
    def get_last_watermark(self, collection: str, session_key: int) -> Optional[str]:
        """Получает последний watermark для коллекции и сессии"""
        result = self.db[collection].find_one(
            {"session_key": session_key, "etl_watermark": {"$exists": True, "$ne": None}},
            sort=[("etl_watermark", DESCENDING)]
        )
        return result.get("etl_watermark") if result else None
    
    def delete_old_data(self, collection: str, days_old: int = 30) -> int:
        """Удаляет старые данные (для телеметрии)"""
        cutoff_date = datetime.utcnow() - timedelta(days=days_old)
        result = self.db[collection].delete_many({"date": {"$lt": cutoff_date}})
        logger.info(f"Deleted {result.deleted_count} old documents from {collection}")
        return result.deleted_count
    
    def get_session_keys(self) -> List[int]:
        """Получает все уникальные session_key из коллекции sessions"""
        result = self.db.sessions.distinct("session_key")
        return result
    
    def close(self):
        """Закрывает соединение с MongoDB"""
        self.client.close()
        logger.info("MongoDB connection closed")