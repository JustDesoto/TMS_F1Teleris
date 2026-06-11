# repositories/ch_repository.py
"""
Репозиторий для работы с ClickHouse (DDS слой - факты)
"""

from typing import List, Dict, Any, Optional
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ClickHouseRepository:
    """
    Репозиторий для работы с ClickHouse.
    """
    
    def __init__(self, host: str, port: int = 9000, user: str = 'default', 
                 password: str = '', database: str = 'default', 
                 send_receive_timeout: int = 300):
        self.client = Client(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            send_receive_timeout=send_receive_timeout
        )
        self.database = database
        logger.info(f"ClickHouse repository initialized: {host}:{port}/{database}")
    
    def save_one(self, collection: str, document: Dict[str, Any]) -> str:
        """Сохраняет один документ"""
        return self.save_many(collection, [document])
    
    def save_many(self, collection: str, documents: List[Dict[str, Any]]) -> int:
        """
        Сохраняет много документов батчем.
        """
        if not documents:
            return 0
        
        columns = list(documents[0].keys())
        data = []
        
        for doc in documents:
            row = []
            for col in columns:
                value = doc.get(col)
                row.append(value)
            data.append(row)
        
        columns_str = ', '.join(columns)
        
        try:
            self.client.execute(f"INSERT INTO {collection} ({columns_str}) VALUES", data)
            logger.debug(f"Inserted {len(data)} rows into {collection}")
            return len(data)
        except ClickHouseError as e:
            logger.error(f"Error inserting into {collection}: {e}")
            raise
    
    def save_many_batched(self, collection: str, documents: List[Dict[str, Any]], 
                          batch_size: int = 10000) -> int:
        """Сохраняет документы батчами"""
        if not documents:
            return 0
        
        total_inserted = 0
        total_batches = (len(documents) + batch_size - 1) // batch_size
        
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            inserted = self.save_many(collection, batch)
            total_inserted += inserted
            logger.debug(f"Batch {i//batch_size + 1}/{total_batches}: inserted {inserted} rows")
        
        logger.info(f"Inserted {total_inserted} rows into {collection} in {total_batches} batches")
        return total_inserted
    
    def insert_many(self, collection: str, documents: List[Dict[str, Any]]) -> int:
        """Алиас для save_many_batched"""
        return self.save_many_batched(collection, documents)
    
    def find_by_filter(self, collection: str, filter_dict: Dict[str, Any], limit: int = None) -> List[Dict[str, Any]]:
        """Находит документы по фильтру"""
        if not filter_dict:
            sql = f"SELECT * FROM {collection}"
            params = {}
        else:
            conditions = ' AND '.join([f"{k} = %({k})s" for k in filter_dict.keys()])
            sql = f"SELECT * FROM {collection} WHERE {conditions}"
            params = filter_dict
        
        if limit:
            sql += f" LIMIT {limit}"
        
        try:
            result = self.client.execute(sql, params, with_column_types=True)
            rows, columns = result
            
            column_names = [col[0] for col in columns]
            documents = []
            
            for row in rows:
                doc = dict(zip(column_names, row))
                documents.append(doc)
            
            return documents
        except ClickHouseError as e:
            logger.error(f"Error finding in {collection}: {e}")
            raise
    
    def find_one(self, collection: str, filter_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Находит один документ"""
        results = self.find_by_filter(collection, filter_dict, limit=1)
        return results[0] if results else None
    
    def upsert(self, collection: str, filter_dict: Dict[str, Any], document: Dict[str, Any]) -> str:
        """Для ClickHouse - удаляем и вставляем"""
        self.delete(collection, filter_dict)
        self.save_one(collection, document)
        return document.get('etl_hash', '')
    
    def get_last_watermark(self, collection: str, session_key: int) -> Optional[str]:
        """Получает последний watermark"""
        sql = f"""
            SELECT etl_watermark 
            FROM {collection} 
            WHERE session_key = %(session_key)s 
              AND etl_watermark IS NOT NULL
            ORDER BY etl_watermark DESC 
            LIMIT 1
        """
        
        try:
            result = self.client.execute(sql, {'session_key': session_key})
            if result:
                return result[0][0]
            return None
        except ClickHouseError as e:
            logger.error(f"Error getting watermark: {e}")
            return None
    
    def delete(self, collection: str, filter_dict: Dict[str, Any]) -> int:
        """Удаляет документы по фильтру"""
        if not filter_dict:
            logger.warning("Delete without filter is not allowed")
            return 0
        
        conditions = ' AND '.join([f"{k} = %({k})s" for k in filter_dict.keys()])
        sql = f"ALTER TABLE {collection} DELETE WHERE {conditions}"
        
        try:
            self.client.execute(sql, filter_dict)
            logger.debug(f"Deleted from {collection}")
            return -1
        except ClickHouseError as e:
            logger.error(f"Error deleting from {collection}: {e}")
            raise
    
    def get_table_count(self, table_name: str, session_key: Optional[int] = None) -> int:
        """Получает количество записей"""
        if session_key:
            sql = f"SELECT count() FROM {table_name} WHERE session_key = %(session_key)s"
            params = {'session_key': session_key}
        else:
            sql = f"SELECT count() FROM {table_name}"
            params = {}
        
        try:
            result = self.client.execute(sql, params)
            return result[0][0] if result else 0
        except ClickHouseError as e:
            logger.error(f"Error getting count: {e}")
            return 0
    
    def table_exists(self, table_name: str) -> bool:
        """Проверяет существование таблицы"""
        sql = f"""
            SELECT 1 
            FROM system.tables 
            WHERE database = %(database)s AND name = %(table)s
        """
        
        try:
            result = self.client.execute(sql, {
                'database': self.database,
                'table': table_name
            })
            return len(result) > 0
        except ClickHouseError as e:
            logger.error(f"Error checking table: {e}")
            return False
    
    def close(self):
        """Закрывает соединение"""
        self.client.disconnect()
        logger.info("ClickHouse connection closed")