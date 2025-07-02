FROM apache/airflow:2.11.0

# Install netcat for database readiness check
USER root
RUN apt-get update && apt-get install -y netcat-traditional && rm -rf /var/lib/apt/lists/*

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Switch to airflow user
USER airflow

# Install additional providers
RUN pip install --no-cache-dir \
    "apache-airflow-providers-amazon>=9.9.0" \
    apache-airflow-providers-postgres \
    requests
