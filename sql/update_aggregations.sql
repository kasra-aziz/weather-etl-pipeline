-- Update daily aggregations
-- Run after each ETL load

INSERT INTO weather.daily_aggregations (
    city_id,
    city_name,
    date,
    avg_temperature,
    min_temperature,
    max_temperature,
    avg_humidity,
    avg_pressure,
    avg_wind_speed,
    observation_count
)
SELECT 
    city_id,
    city_name,
    DATE(data_timestamp) as date,
    ROUND(AVG(temperature)::numeric, 2) as avg_temperature,
    MIN(temperature) as min_temperature,
    MAX(temperature) as max_temperature,
    ROUND(AVG(humidity)::numeric, 2) as avg_humidity,
    ROUND(AVG(pressure)::numeric, 2) as avg_pressure,
    ROUND(AVG(wind_speed)::numeric, 2) as avg_wind_speed,
    COUNT(*) as observation_count
FROM weather.weather_observations
WHERE DATE(data_timestamp) = CURRENT_DATE - INTERVAL '1 day'
GROUP BY city_id, city_name, DATE(data_timestamp)
ON CONFLICT (city_id, date) 
DO UPDATE SET
    avg_temperature = EXCLUDED.avg_temperature,
    min_temperature = EXCLUDED.min_temperature,
    max_temperature = EXCLUDED.max_temperature,
    avg_humidity = EXCLUDED.avg_humidity,
    avg_pressure = EXCLUDED.avg_pressure,
    avg_wind_speed = EXCLUDED.avg_wind_speed,
    observation_count = EXCLUDED.observation_count,
    updated_at = CURRENT_TIMESTAMP;

-- Update city metadata
INSERT INTO weather.cities (city_id, city_name, country_code, latitude, longitude, last_updated)
SELECT DISTINCT 
    city_id,
    city_name,
    country_code,
    latitude,
    longitude,
    CURRENT_TIMESTAMP
FROM weather.weather_observations
WHERE extraction_timestamp >= NOW() - INTERVAL '24 hours'
ON CONFLICT (city_id) 
DO UPDATE SET
    last_updated = CURRENT_TIMESTAMP;

