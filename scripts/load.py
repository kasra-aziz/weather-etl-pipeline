"""
Load Module - Weather Data Loading to PostgreSQL

This module handles loading transformed weather data into PostgreSQL
with upsert logic and batch processing for efficiency.
"""

import logging
from typing import List, Dict, Any
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_weather_data(postgres_conn_id: str, **context) -> Dict[str, Any]:
    """
    Load transformed weather data into PostgreSQL.
    
    Uses batch inserts with ON CONFLICT for upsert behavior.
    
    Args:
        postgres_conn_id: Airflow connection ID for PostgreSQL
        context: Airflow context for XCom pull
        
    Returns:
        Load metadata
    """
    ti = context["ti"]
    transformed_data = ti.xcom_pull(
        key="transformed_weather_data", 
        task_ids="transform_weather_data"
    )
    
    if not transformed_data:
        raise ValueError("No transformed data found in XCom")
    
    logger.info(f"Loading {len(transformed_data)} records to PostgreSQL")
    
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    insert_query = """
        INSERT INTO weather.weather_observations (
            city_id, city_name, country_code, latitude, longitude,
            weather_condition, weather_description, weather_icon,
            temperature, feels_like, temp_min, temp_max, feels_like_category,
            pressure, humidity, visibility,
            wind_speed, wind_deg, wind_gust, wind_direction,
            cloudiness, data_timestamp, sunrise, sunset,
            extraction_timestamp, timezone_offset
        ) VALUES (
            %(city_id)s, %(city_name)s, %(country_code)s, %(latitude)s, %(longitude)s,
            %(weather_condition)s, %(weather_description)s, %(weather_icon)s,
            %(temperature)s, %(feels_like)s, %(temp_min)s, %(temp_max)s, %(feels_like_category)s,
            %(pressure)s, %(humidity)s, %(visibility)s,
            %(wind_speed)s, %(wind_deg)s, %(wind_gust)s, %(wind_direction)s,
            %(cloudiness)s, %(data_timestamp)s, %(sunrise)s, %(sunset)s,
            %(extraction_timestamp)s, %(timezone_offset)s
        )
        ON CONFLICT (city_id, data_timestamp) 
        DO UPDATE SET
            temperature = EXCLUDED.temperature,
            feels_like = EXCLUDED.feels_like,
            humidity = EXCLUDED.humidity,
            extraction_timestamp = EXCLUDED.extraction_timestamp;
    """
    
    records_inserted = 0
    records_failed = 0
    
    for record in transformed_data:
        try:
            cursor.execute(insert_query, record)
            records_inserted += 1
        except Exception as e:
            logger.error(f"Failed to insert record for {record.get(\"city_name\")}: {str(e)}")
            records_failed += 1
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info(f"Successfully loaded {records_inserted} records")
    
    return {
        "status": "success",
        "records_inserted": records_inserted,
        "records_failed": records_failed
    }


def bulk_load_weather_data(
    data: List[Dict], 
    postgres_conn_id: str,
    batch_size: int = 1000
) -> Dict[str, Any]:
    """
    Bulk load weather data using COPY for better performance.
    
    Args:
        data: List of weather records
        postgres_conn_id: Airflow connection ID
        batch_size: Records per batch
        
    Returns:
        Load metadata
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    # Implementation for bulk loading using COPY
    # Useful for historical data backfills
    pass

