-- Create schema and tables for weather data
-- Author: Kasra Azizbaigi

-- Create schema
CREATE SCHEMA IF NOT EXISTS weather;

-- Main weather observations table
CREATE TABLE IF NOT EXISTS weather.weather_observations (
    id SERIAL PRIMARY KEY,
    
    -- Location
    city_id INTEGER NOT NULL,
    city_name VARCHAR(100) NOT NULL,
    country_code VARCHAR(5),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    
    -- Weather conditions
    weather_condition VARCHAR(50),
    weather_description VARCHAR(200),
    weather_icon VARCHAR(10),
    
    -- Temperature (Celsius)
    temperature DECIMAL(5,2),
    feels_like DECIMAL(5,2),
    temp_min DECIMAL(5,2),
    temp_max DECIMAL(5,2),
    feels_like_category VARCHAR(20),
    
    -- Atmospheric
    pressure INTEGER,
    humidity INTEGER,
    visibility INTEGER,
    
    -- Wind
    wind_speed DECIMAL(5,2),
    wind_deg INTEGER,
    wind_gust DECIMAL(5,2),
    wind_direction VARCHAR(5),
    
    -- Clouds
    cloudiness INTEGER,
    
    -- Timestamps
    data_timestamp TIMESTAMP NOT NULL,
    sunrise TIMESTAMP,
    sunset TIMESTAMP,
    extraction_timestamp TIMESTAMP NOT NULL,
    timezone_offset INTEGER,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint for upsert
    CONSTRAINT unique_city_timestamp UNIQUE (city_id, data_timestamp)
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_weather_city ON weather.weather_observations(city_name);
CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather.weather_observations(data_timestamp);
CREATE INDEX IF NOT EXISTS idx_weather_extraction ON weather.weather_observations(extraction_timestamp);

-- Daily aggregations table
CREATE TABLE IF NOT EXISTS weather.daily_aggregations (
    id SERIAL PRIMARY KEY,
    city_id INTEGER NOT NULL,
    city_name VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    
    -- Temperature stats
    avg_temperature DECIMAL(5,2),
    min_temperature DECIMAL(5,2),
    max_temperature DECIMAL(5,2),
    
    -- Other aggregations
    avg_humidity DECIMAL(5,2),
    avg_pressure DECIMAL(7,2),
    avg_wind_speed DECIMAL(5,2),
    
    -- Record count
    observation_count INTEGER,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_city_date UNIQUE (city_id, date)
);

-- City metadata table
CREATE TABLE IF NOT EXISTS weather.cities (
    city_id INTEGER PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    country_code VARCHAR(5),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    timezone VARCHAR(50),
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index
CREATE INDEX IF NOT EXISTS idx_cities_name ON weather.cities(city_name);

