# as of 2025-07-02, Python 3.11 is the only language
# version supported by the datahub airflow integration
FROM apache/airflow:2.11.0-python3.11

# Install netcat for database readiness check
USER root
RUN apt-get update && apt-get install -y netcat-traditional && rm -rf /var/lib/apt/lists/*

# Install additional providers
USER airflow
RUN pip install \
    "apache-airflow-providers-amazon>=9.9.0" \
    apache-airflow-providers-postgres \
    apache-airflow-providers-openlineage \
    acryl-datahub-airflow-plugin[plugin-v2] \
    typing-extensions==4.8.0 \
    requests
    
# Copy entrypoint script
USER root
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Switch to airflow user
USER airflow

