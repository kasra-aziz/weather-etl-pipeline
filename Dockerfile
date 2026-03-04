FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy scripts
COPY scripts/ /opt/airflow/scripts/
COPY sql/ /opt/airflow/sql/

# Set Python path
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

