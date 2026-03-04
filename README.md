# Weather ETL Pipeline

[\![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[\![Apache Airflow 2.8](https://img.shields.io/badge/airflow-2.8-green.svg)](https://airflow.apache.org/)
[\![PostgreSQL 15](https://img.shields.io/badge/postgresql-15-blue.svg)](https://www.postgresql.org/)
[\![Docker](https://img.shields.io/badge/docker-ready-blue.svg)](https://www.docker.com/)

A production-ready ETL (Extract, Transform, Load) pipeline built with Apache Airflow that ingests weather data from the OpenWeatherMap API, transforms it, and loads it into PostgreSQL for analytics.

\![Pipeline Architecture](https://mermaid.ink/img/pako:eNp1kU1qwzAQha9izLoO9gVcCLgQugmUrnYaTTRjW8SS0I9DQ8jdK9tJnBa6Evq-9_RmtGelrSLF6k4P8LQJCJ2H6xhg9gkOF4OHZj_CYoGwXCLM5_Cw2cD9PcLdHTw-Idw-w80N-A0-bOH5BdZreHuH1y94-YC3d_hSLZpO?)

## Features

- **Automated Data Ingestion**: Daily extraction of weather data for multiple Canadian cities
- **Data Transformation**: Flattening nested JSON, data enrichment, and standardization
- **Data Quality Checks**: Automated validation including null checks, range validation, and freshness monitoring
- **Daily Aggregations**: Pre-computed statistics for analytics dashboards
- **Dockerized Deployment**: One-command setup with Docker Compose
- **Production-Ready**: Retry logic, error handling, logging, and monitoring

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  OpenWeatherMap │────▶│    Airflow      │────▶│   PostgreSQL    │
│      API        │     │   (Scheduler)   │     │   (Analytics)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │
                               ▼
                        ┌─────────────┐
                        │  Data       │
                        │  Quality    │
                        │  Checks     │
                        └─────────────┘
```

### Pipeline Tasks

1. **create_tables** - Initialize database schema
2. **extract_weather_data** - Fetch data from OpenWeatherMap API
3. **transform_weather_data** - Clean and structure the data
4. **load_weather_data** - Insert into PostgreSQL with upsert logic
5. **data_quality_checks** - Validate data integrity
6. **update_aggregations** - Compute daily statistics

## Tech Stack

| Component | Technology |
|-----------|------------|
| Orchestration | Apache Airflow 2.8 |
| Database | PostgreSQL 15 |
| Language | Python 3.11 |
| Containerization | Docker & Docker Compose |
| Data Source | OpenWeatherMap API |

## Project Structure

```
weather-etl-pipeline/
├── dags/
│   └── weather_etl_dag.py      # Main Airflow DAG
├── scripts/
│   ├── __init__.py
│   ├── extract.py              # API data extraction
│   ├── transform.py            # Data transformation
│   ├── load.py                 # Database loading
│   └── data_quality.py         # Quality checks
├── sql/
│   ├── create_tables.sql       # Database schema
│   └── update_aggregations.sql # Aggregation queries
├── tests/
│   └── test_etl.py             # Unit tests
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
├── .env.example
└── README.md
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- OpenWeatherMap API key ([Get free key](https://openweathermap.org/api))

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/kasra-aziz/weather-etl-pipeline.git
   cd weather-etl-pipeline
   ```

2. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env and add your OpenWeatherMap API key
   ```

3. **Start the services**
   ```bash
   docker-compose up -d
   ```

4. **Access Airflow UI**
   - URL: http://localhost:8080
   - Username: `admin`
   - Password: `admin`

5. **Enable the DAG**
   - Navigate to the DAGs page
   - Toggle on `weather_etl_pipeline`

### Running Manually

```bash
# Trigger DAG run
docker exec weather_etl_scheduler airflow dags trigger weather_etl_pipeline

# Check DAG status
docker exec weather_etl_scheduler airflow dags list-runs -d weather_etl_pipeline
```

## Configuration

### Cities

Edit the cities list in `dags/weather_etl_dag.py`:

```python
op_kwargs={
    "cities": ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"],
    "api_key": "{{ var.value.openweathermap_api_key }}",
}
```

### Schedule

Default: Daily at 6:00 AM UTC

```python
schedule_interval="0 6 * * *"
```

## Data Quality Checks

The pipeline includes automated quality checks:

| Check | Description |
|-------|-------------|
| Row Count | Ensures minimum records exist |
| Null Values | Validates critical columns |
| Temperature Range | Flags values outside -60°C to 60°C |
| Data Freshness | Alerts if data is stale |
| Duplicates | Detects duplicate records |

## Database Schema

### weather.weather_observations
Main fact table storing raw weather observations.

### weather.daily_aggregations  
Pre-computed daily statistics per city.

### weather.cities
Dimension table with city metadata.

## Testing

```bash
# Run unit tests
docker exec weather_etl_scheduler pytest tests/ -v

# Run with coverage
docker exec weather_etl_scheduler pytest tests/ --cov=scripts --cov-report=html
```

## Monitoring

### Airflow UI
- DAG runs and task status
- Logs and error traces
- Execution history

### Database Queries
```sql
-- Recent observations
SELECT * FROM weather.weather_observations 
ORDER BY extraction_timestamp DESC LIMIT 10;

-- Daily averages
SELECT * FROM weather.daily_aggregations 
ORDER BY date DESC;

-- Data quality summary
SELECT city_name, COUNT(*), MAX(extraction_timestamp)
FROM weather.weather_observations
GROUP BY city_name;
```

## Scaling Considerations

For production deployments:

- **Celery Executor**: Replace LocalExecutor for distributed task execution
- **Connection Pooling**: Use PgBouncer for database connections
- **Kubernetes**: Deploy on K8s with KubernetesExecutor
- **Monitoring**: Add Prometheus/Grafana for metrics
- **Alerting**: Configure email/Slack notifications

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/improvement`)
3. Commit changes (`git commit -am "Add new feature"`)
4. Push to branch (`git push origin feature/improvement`)
5. Open a Pull Request

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contact

**Kasra Azizbaigi**  
Email: kasra.azizbaigi@gmail.com  
GitHub: [@kasra-aziz](https://github.com/kasra-aziz)

---

*Built with Apache Airflow, PostgreSQL, and Docker*

