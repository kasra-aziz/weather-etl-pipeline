"""
Extract Module - Weather Data Extraction from OpenWeatherMap API

This module handles the extraction of weather data from the OpenWeatherMap API
for multiple cities. Data is cached temporarily for downstream processing.
"""

import json
import logging
import requests
from datetime import datetime
from typing import List, Dict, Any
from airflow.models import Variable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"


def extract_weather_data(cities: List[str], api_key: str, **context) -> Dict[str, Any]:
    """
    Extract current weather data for specified cities.
    
    Args:
        cities: List of city names to fetch weather for
        api_key: OpenWeatherMap API key
        context: Airflow context for XCom push
        
    Returns:
        Dictionary containing extraction metadata
    """
    logger.info(f"Starting weather data extraction for {len(cities)} cities")
    
    extracted_data = []
    extraction_timestamp = datetime.utcnow().isoformat()
    
    for city in cities:
        try:
            response = requests.get(
                BASE_URL,
                params={
                    "q": city,
                    "appid": api_key,
                    "units": "metric"
                },
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            data["extraction_timestamp"] = extraction_timestamp
            data["city_query"] = city
            extracted_data.append(data)
            
            logger.info(f"Successfully extracted data for {city}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to extract data for {city}: {str(e)}")
            continue
    
    # Push to XCom for downstream tasks
    context["ti"].xcom_push(key="raw_weather_data", value=extracted_data)
    context["ti"].xcom_push(key="extraction_timestamp", value=extraction_timestamp)
    
    return {
        "status": "success",
        "cities_processed": len(extracted_data),
        "cities_failed": len(cities) - len(extracted_data),
        "timestamp": extraction_timestamp
    }


def extract_historical_data(city: str, api_key: str, days: int = 7) -> List[Dict]:
    """
    Extract historical weather data (requires paid API plan).
    
    Args:
        city: City name
        api_key: OpenWeatherMap API key  
        days: Number of days of historical data
        
    Returns:
        List of historical weather records
    """
    # Implementation for historical data extraction
    # Note: Requires OpenWeatherMap paid plan
    pass

