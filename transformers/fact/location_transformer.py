# transformers/fact/location_transformer.py
from typing import Dict, Any, List
from ..base_transformer import BaseTransformer
import logging

logger = logging.getLogger(__name__)


class LocationTransformer(BaseTransformer):
    """
    Трансформирует GPS данные о местоположении машин с денормализацией.
    Target: ClickHouse
    """

    def transform(self, raw_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        transformed = []

        for doc in raw_documents:
            driver_number = self._safe_get(doc, "driver_number")
            timestamp = self._parse_timestamp(self._safe_get(doc, "date"))

            dds_record = {
                # Keys
                "session_key": self.session_key,
                "driver_number": driver_number,
                "timestamp": timestamp,
                # GPS coordinates
                "x": self._safe_get(doc, "x"),
                "y": self._safe_get(doc, "y"),
                "z": self._safe_get(doc, "z"),
                # Денормализация DRIVER
                **self._denormalize_driver(driver_number),
                # Денормализация SESSION
                **self._denormalize_session(),
                # ETL
                "etl_hash": self._safe_get(doc, "etl_hash"),
                "transformed_at": self.transformed_at,
            }
            transformed.append(dds_record)

        logger.info(
            f"Transformed {len(transformed)} location records for session {self.session_key}"
        )
        return transformed

    def get_target_database(self) -> str:
        return "clickhouse"

    def get_table_name(self) -> str:
        return "fact_location"
