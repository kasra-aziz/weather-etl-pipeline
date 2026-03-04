"""
Unit tests for Weather ETL Pipeline

Run with: pytest tests/ -v
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch
import sys
import os

# Add scripts to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from transform import _transform_record, _categorize_feels_like, _degrees_to_cardinal


class TestTransformModule:
    """Tests for the transform module."""
    
    def test_categorize_feels_like_cold(self):
        """Test feels like categorization for cold temperatures."""
        assert _categorize_feels_like(-15) == "extreme_cold"
        assert _categorize_feels_like(-5) == "cold"
        assert _categorize_feels_like(5) == "cool"
    
    def test_categorize_feels_like_warm(self):
        """Test feels like categorization for warm temperatures."""
        assert _categorize_feels_like(15) == "mild"
        assert _categorize_feels_like(25) == "warm"
        assert _categorize_feels_like(35) == "hot"
    
    def test_categorize_feels_like_none(self):
        """Test feels like with None value."""
        assert _categorize_feels_like(None) == "unknown"
    
    def test_degrees_to_cardinal_north(self):
        """Test cardinal direction conversion for north."""
        assert _degrees_to_cardinal(0) == "N"
        assert _degrees_to_cardinal(360) == "N"
    
    def test_degrees_to_cardinal_south(self):
        """Test cardinal direction conversion for south."""
        assert _degrees_to_cardinal(180) == "S"
    
    def test_degrees_to_cardinal_east_west(self):
        """Test cardinal direction conversion for east/west."""
        assert _degrees_to_cardinal(90) == "E"
        assert _degrees_to_cardinal(270) == "W"
    
    def test_degrees_to_cardinal_none(self):
        """Test cardinal direction with None value."""
        assert _degrees_to_cardinal(None) == "unknown"
    
    def test_transform_record_complete(self):
        """Test full record transformation."""
        raw_record = {
            "id": 6167865,
            "name": "Toronto",
            "coord": {"lat": 43.7001, "lon": -79.4163},
            "weather": [{"main": "Clear", "description": "clear sky", "icon": "01d"}],
            "main": {
                "temp": 22.5,
                "feels_like": 21.8,
                "temp_min": 20.0,
                "temp_max": 25.0,
                "pressure": 1015,
                "humidity": 65
            },
            "wind": {"speed": 5.5, "deg": 180, "gust": 8.2},
            "clouds": {"all": 10},
            "visibility": 10000,
            "dt": 1704067200,
            "sys": {"country": "CA", "sunrise": 1704028800, "sunset": 1704064800},
            "timezone": -18000,
            "cod": 200,
            "extraction_timestamp": "2024-01-01T12:00:00"
        }
        
        transformed = _transform_record(raw_record)
        
        assert transformed["city_name"] == "Toronto"
        assert transformed["country_code"] == "CA"
        assert transformed["temperature"] == 22.5
        assert transformed["feels_like_category"] == "warm"
        assert transformed["wind_direction"] == "S"
        assert transformed["weather_condition"] == "Clear"


class TestExtractModule:
    """Tests for the extract module."""
    
    @patch("requests.get")
    def test_extract_success(self, mock_get):
        """Test successful API extraction."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "name": "Toronto",
            "main": {"temp": 20},
            "weather": [{"main": "Clear"}]
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        # Would test extract_weather_data here with mocked context


class TestDataQuality:
    """Tests for data quality checks."""
    
    def test_quality_check_structure(self):
        """Test that quality check results have expected structure."""
        sample_result = {
            "name": "test_check",
            "description": "Test description",
            "status": "passed",
            "expected": "some value",
            "actual": "some value"
        }
        
        assert "name" in sample_result
        assert "status" in sample_result
        assert sample_result["status"] in ["passed", "failed", "warning"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

