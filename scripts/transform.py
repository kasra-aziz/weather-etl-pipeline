"""
Transform Module - Weather Data Transformation

This module handles the transformation of raw weather API responses
into a clean, structured format suitable for database loading.
"""

import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_weather_data(**context) -> Dict[str, Any]:
    """
    Transform raw weather data into structured records.
    
    Transformations applied:
    - Flatten nested JSON structure
    - Convert timestamps to datetime
    - Standardize field names
    - Handle missing values
    - Add derived fields (feels_like_category, wind_direction_cardinal)
    
    Args:
        context: Airflow context for XCom pull/push
        
    Returns:
        Transformation metadata
    """
    ti = context["ti"]
    raw_data = ti.xcom_pull(key="raw_weather_data", task_ids="extract_weather_data")
    
    if not raw_data:
        raise ValueError("No raw data found in XCom")
    
    logger.info(f"Transforming {len(raw_data)} weather records")
    
    transformed_records = []
    
    for record in raw_data:
        try:
            transformed = _transform_record(record)
            transformed_records.append(transformed)
        except Exception as e:
            logger.error(f"Failed to transform record: {str(e)}")
            continue
    
    # Push transformed data to XCom
    ti.xcom_push(key="transformed_weather_data", value=transformed_records)
    
    return {
        "status": "success",
        "records_transformed": len(transformed_records),
        "records_failed": len(raw_data) - len(transformed_records)
    }


def _transform_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a single weather record."""
    
    # Extract nested fields
    main = record.get("main", {})
    wind = record.get("wind", {})
    weather = record.get("weather", [{}])[0]
    coord = record.get("coord", {})
    sys = record.get("sys", {})
    
    transformed = {
        # Location info
        "city_id": record.get("id"),
        "city_name": record.get("name"),
        "country_code": sys.get("country"),
        "latitude": coord.get("lat"),
        "longitude": coord.get("lon"),
        
        # Weather conditions
        "weather_condition": weather.get("main"),
        "weather_description": weather.get("description"),
        "weather_icon": weather.get("icon"),
        
        # Temperature (Celsius)
        "temperature": main.get("temp"),
        "feels_like": main.get("feels_like"),
        "temp_min": main.get("temp_min"),
        "temp_max": main.get("temp_max"),
        "feels_like_category": _categorize_feels_like(main.get("feels_like")),
        
        # Atmospheric conditions
        "pressure": main.get("pressure"),
        "humidity": main.get("humidity"),
        "visibility": record.get("visibility"),
        
        # Wind
        "wind_speed": wind.get("speed"),
        "wind_deg": wind.get("deg"),
        "wind_gust": wind.get("gust"),
        "wind_direction": _degrees_to_cardinal(wind.get("deg")),
        
        # Cloud cover
        "cloudiness": record.get("clouds", {}).get("all"),
        
        # Timestamps
        "data_timestamp": datetime.utcfromtimestamp(record.get("dt", 0)).isoformat(),
        "sunrise": datetime.utcfromtimestamp(sys.get("sunrise", 0)).isoformat() if sys.get("sunrise") else None,
        "sunset": datetime.utcfromtimestamp(sys.get("sunset", 0)).isoformat() if sys.get("sunset") else None,
        "extraction_timestamp": record.get("extraction_timestamp"),
        
        # Metadata
        "timezone_offset": record.get("timezone"),
        "api_response_code": record.get("cod"),
    }
    
    return transformed


def _categorize_feels_like(temp: Optional[float]) -> str:
    """Categorize feels-like temperature."""
    if temp is None:
        return "unknown"
    if temp < -10:
        return "extreme_cold"
    elif temp < 0:
        return "cold"
    elif temp < 10:
        return "cool"
    elif temp < 20:
        return "mild"
    elif temp < 30:
        return "warm"
    else:
        return "hot"


def _degrees_to_cardinal(degrees: Optional[float]) -> str:
    """Convert wind degrees to cardinal direction."""
    if degrees is None:
        return "unknown"
    
    directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                  "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    
    index = round(degrees / 22.5) % 16
    return directions[index]

