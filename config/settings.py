"""
Pipeline Configuration

Centralized configuration management for the Weather ETL Pipeline.
"""

from dataclasses import dataclass
from typing import List
import os


@dataclass
class PipelineConfig:
    """Configuration for the weather ETL pipeline."""
    
    # API Settings
    api_base_url: str = "https://api.openweathermap.org/data/2.5/weather"
    api_timeout: int = 30
    
    # Cities to fetch weather for
    cities: List[str] = None
    
    # Database settings
    db_schema: str = "weather"
    batch_size: int = 1000
    
    # Quality check thresholds
    min_temp_celsius: float = -60.0
    max_temp_celsius: float = 60.0
    max_data_age_hours: int = 25
    
    def __post_init__(self):
        if self.cities is None:
            self.cities = ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"]
    
    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Load configuration from environment variables."""
        return cls(
            api_timeout=int(os.getenv("API_TIMEOUT", 30)),
            batch_size=int(os.getenv("BATCH_SIZE", 1000)),
        )


# Default configuration instance
default_config = PipelineConfig()

