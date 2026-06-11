# repositories/pg_repository.py
"""
Репозиторий для работы с PostgreSQL (DDS слой - измерения)
"""

from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
import logging
from datetime import datetime

from .base_repository import BaseRepository

logger = logging.getLogger(__name__)


class PostgresRepository(BaseRepository):
    """
    Репозиторий для работы с PostgreSQL.
    Используется для хранения измерений (dimensions) в DDS слое.
    SCD Type 0 - только INSERT, без обновлений.
    """
    
    def __init__(self, connection_string: str, min_connections: int = 1, max_connections: int = 10):
        """
        Args:
            connection_string: Строка подключения PostgreSQL
            min_connections: Минимальное количество соединений в пуле
            max_connections: Максимальное количество соединений в пуле
        """
        self.connection_string = connection_string
        self.pool = SimpleConnectionPool(
            min_connections,
            max_connections,
            connection_string
        )
        logger.info(f"PostgreSQL repository initialized with connection pool (min={min_connections}, max={max_connections})")
    
    @contextmanager
    def _get_connection(self):
        """Получает соединение из пула (контекстный менеджер)"""
        conn = self.pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            self.pool.putconn(conn)
    
    @contextmanager
    def _get_cursor(self, conn, cursor_factory=None):
        """Получает курсор (контекстный менеджер)"""
        cursor = conn.cursor(cursor_factory=cursor_factory)
        try:
            yield cursor
        finally:
            cursor.close()
    
    def save_one(self, collection: str, document: Dict[str, Any]) -> str:
        """
        Сохраняет один документ в PostgreSQL.
        
        Args:
            collection: Имя таблицы
            document: Словарь с данными
        
        Returns:
            ID вставленной записи
        """
        if not document:
            return None
        
        columns = list(document.keys())
        values = [document[col] for col in columns]
        
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        sql = f"""
            INSERT INTO {collection} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING
            RETURNING id
        """
        
        try:
            with self._get_connection() as conn:
                with self._get_cursor(conn) as cur:
                    cur.execute(sql, values)
                    result = cur.fetchone()
                    if result:
                        return str(result[0])
                    return None
        except Exception as e:
            logger.error(f"Error saving to {collection}: {e}")
            raise
    
    def save_many(self, collection: str, documents: List[Dict[str, Any]]) -> int:
        """
        Сохраняет много документов батчем.
        
        Args:
            collection: Имя таблицы
            documents: Список словарей с данными
        
        Returns:
            Количество вставленных записей
        """
        if not documents:
            return 0
        
        columns = list(documents[0].keys())
        values = [[doc[col] for col in columns] for doc in documents]
        
        columns_str = ', '.join(columns)
        
        sql = f"""
            INSERT INTO {collection} ({columns_str})
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        
        try:
            with self._get_connection() as conn:
                with self._get_cursor(conn) as cur:
                    execute_values(cur, sql, values, page_size=1000)
                    return cur.rowcount
        except Exception as e:
            logger.error(f"Error saving batch to {collection}: {e}")
            raise
    
    

    def insert_many(self, table_name: str, records: List[Dict[str, Any]]) -> int:
        """
        Алиас для save_many - вставляет много записей.
        
        Args:
            table_name: Имя таблицы
            records: Список словарей с данными
        
        Returns:
            Количество вставленных записей
        """
        return self.save_many(table_name, records)
        
    def find_by_filter(self, collection: str, filter_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Находит документы по фильтру.
        
        Args:
            collection: Имя таблицы
            filter_dict: Словарь с фильтрами
        
        Returns:
            Список найденных документов
        """
        if not filter_dict:
            sql = f"SELECT * FROM {collection}"
            params = []
        else:
            conditions = ' AND '.join([f"{k} = %s" for k in filter_dict.keys()])
            sql = f"SELECT * FROM {collection} WHERE {conditions}"
            params = list(filter_dict.values())
        
        try:
            with self._get_connection() as conn:
                with self._get_cursor(conn, cursor_factory=RealDictCursor) as cur:
                    cur.execute(sql, params)
                    return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error finding in {collection}: {e}")
            raise

    def find_one(self, collection: str, filter_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Находит один документ по фильтру.
        
        Args:
            collection: Имя таблицы
            filter_dict: Словарь с фильтрами
        
        Returns:
            Найденный документ или None
        """
        results = self.find_by_filter(collection, filter_dict)
        return results[0] if results else None

    def upsert(self, collection: str, filter_dict: Dict[str, Any], document: Dict[str, Any]) -> str:
        """
        Обновляет или вставляет документ.
        Для PostgreSQL использует INSERT ... ON CONFLICT DO UPDATE.
        
        Args:
            collection: Имя таблицы
            filter_dict: Словарь с условием для поиска
            document: Словарь с данными для обновления/вставки
        
        Returns:
            ID документа
        """
        # Объединяем фильтр и документ
        all_fields = {**filter_dict, **document}
        columns = list(all_fields.keys())
        values = [all_fields[col] for col in columns]
        
        # Определяем конфликтующие колонки (первые из filter_dict)
        conflict_columns = list(filter_dict.keys())
        conflict_str = ', '.join(conflict_columns)
        
        # Формируем UPDATE часть
        update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in document.keys()])
        
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        sql = f"""
            INSERT INTO {collection} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_str})
            DO UPDATE SET {update_set}
            RETURNING id
        """
        
        try:
            with self._get_connection() as conn:
                with self._get_cursor(conn) as cur:
                    cur.execute(sql, values)
                    result = cur.fetchone()
                    return str(result[0]) if result else None
        except Exception as e:
            logger.error(f"Error upserting to {collection}: {e}")
            raise

    def get_last_watermark(self, collection: str, session_key: int) -> Optional[str]:
        """
        Получает последний watermark для коллекции и сессии.
        
        Args:
            collection: Имя таблицы
            session_key: Ключ сессии
        
        Returns:
            Значение watermark или None
        """
        sql = f"""
            SELECT etl_watermark 
            FROM {collection} 
            WHERE session_key = %s 
                AND etl_watermark IS NOT NULL
            ORDER BY etl_watermark DESC 
            LIMIT 1
        """
        
        try:
            with self._get_connection() as conn:
                with self._get_cursor(conn, cursor_factory=RealDictCursor) as cur:
                    cur.execute(sql, [session_key])
                    result = cur.fetchone()
                    return result['etl_watermark'] if result else None
        except Exception as e:
            logger.error(f"Error getting watermark from {collection}: {e}")
            return None

    def delete(self, collection: str, filter_dict: Dict[str, Any]) -> int:
        """
        Удаляет документы по фильтру.
        
        Args:
            collection: Имя таблицы
            filter_dict: Словарь с фильтрами
        
        Returns:
            Количество удаленных записей
        """
        if not filter_dict:
            logger.warning(f"Delete without filter on {collection} is not allowed")
            return 0
        
        conditions = ' AND '.join([f"{k} = %s" for k in filter_dict.keys()])
        sql = f"DELETE FROM {collection} WHERE {conditions}"
        params = list(filter_dict.values())
        
        try:
            with self._get_connection() as conn:
                with self._get_cursor(conn) as cur:
                    cur.execute(sql, params)
                    return cur.rowcount
        except Exception as e:
            logger.error(f"Error deleting from {collection}: {e}")
            raise

    def record_exists(self, collection: str, filter_dict: Dict[str, Any]) -> bool:
        """
        Проверяет существует ли запись с указанными ключами.
        
        Args:
            collection: Имя таблицы
            filter_dict: Словарь с фильтрами
        
        Returns:
            True если запись существует
        """
        conditions = ' AND '.join([f"{k} = %s" for k in filter_dict.keys()])
        sql = f"SELECT 1 FROM {collection} WHERE {conditions} LIMIT 1"
        params = list(filter_dict.values())
        
        try:
            with self._get_connection() as conn:
                with self._get_cursor(conn) as cur:
                    cur.execute(sql, params)
                    return cur.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking existence in {collection}: {e}")
            return False

    def create_table(self, table_name: str, schema: Dict[str, str], primary_keys: List[str]) -> None:
        """
        Создает таблицу если она не существует.
        
        Args:
            table_name: Имя таблицы
            schema: Словарь {column_name: sql_type}
            primary_keys: Список колонок для первичного ключа
        """
        columns = ', '.join([f"{col} {typ}" for col, typ in schema.items()])
        pk_str = ', '.join(primary_keys)
        
        sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns},
                PRIMARY KEY ({pk_str})
            )
        """
        
        try:
            with self._get_connection() as conn:
                with self._get_cursor(conn) as cur:
                    cur.execute(sql)
                    logger.info(f"Table {table_name} created/verified")
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            raise

    def close(self):
        """Закрывает пул соединений"""
        self.pool.closeall()
        logger.info("PostgreSQL connection pool closed")
        
    def execute(self, query: str, params: tuple = None) -> None:
        """
        Выполняет SQL запрос без возврата результата (для UPDATE, DELETE, DDL).
        
        Args:
            query: SQL запрос
            params: Параметры для запроса
        """
        try:
            with self._get_connection() as conn:
                with self._get_cursor(conn) as cur:
                    cur.execute(query, params)
                    logger.debug(f"Executed query: {query[:100]}...")
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    def execute_query(self, query: str, params: tuple = None) -> List[tuple]:
        """
        Выполняет SQL запрос и возвращает результат.
        
        Args:
            query: SQL запрос
            params: Параметры для запроса
        
        Returns:
            Список кортежей с результатами
        """
        try:
            with self._get_connection() as conn:
                with self._get_cursor(conn) as cur:
                    cur.execute(query, params)
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise