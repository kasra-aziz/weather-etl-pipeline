"""
Weather ETL Pipeline DAG

This DAG orchestrates the extraction, transformation, and loading of weather data
from the OpenWeatherMap API into a PostgreSQL database for analytics.

Schedule: Daily at 6:00 AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

from scripts.extract import extract_weather_data
from scripts.transform import transform_weather_data
from scripts.load import load_weather_data
from scripts.data_quality import run_data_quality_checks


default_args = {
    "owner": "kasra-azizbaigi",
    "depends_on_past": False,
    "email": ["kasra.azizbaigi@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for weather data ingestion and analytics",
    schedule_interval="0 6 * * *",  # Daily at 6 AM UTC
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "weather", "production"],
) as dag:

    # Task 1: Create tables if not exists
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="postgres_default",
        sql="sql/create_tables.sql",
    )

    # Task 2: Extract weather data from API
    extract_task = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_weather_data,
        op_kwargs={
            "cities": ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"],
            "api_key": "{{ var.value.openweathermap_api_key }}",
        },
    )

    # Task 3: Transform raw data
    transform_task = PythonOperator(
        task_id="transform_weather_data",
        python_callable=transform_weather_data,
    )

    # Task 4: Load data into PostgreSQL
    load_task = PythonOperator(
        task_id="load_weather_data",
        python_callable=load_weather_data,
        op_kwargs={"postgres_conn_id": "postgres_default"},
    )

    # Task 5: Run data quality checks
    quality_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_data_quality_checks,
        op_kwargs={"postgres_conn_id": "postgres_default"},
    )

    # Task 6: Update aggregation tables
    update_aggregations = PostgresOperator(
        task_id="update_aggregations",
        postgres_conn_id="postgres_default",
        sql="sql/update_aggregations.sql",
    )

    # Define task dependencies
    create_tables >> extract_task >> transform_task >> load_task >> quality_checks >> update_aggregations

