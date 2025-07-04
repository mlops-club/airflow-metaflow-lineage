#!/bin/bash
set -ex

# Wait for postgres to be ready
echo "Waiting for PostgreSQL to be ready..."
while ! nc -z postgres 5432; do
  sleep 1
done
echo "PostgreSQL is ready!"

# Initialize the database if it hasn't been done yet
# if ! airflow users list 2>/dev/null | grep -q airflow; then
  echo "Initializing Airflow database..."
  airflow db init
  
  echo "Setting up Airflow variables..."
  airflow variables set datalake-aws-region "${AWS_REGION}"
  airflow variables set datalake-s3-bucket "${S3_DATA_LAKE_BUCKET_NAME}"
  airflow variables set datalake-glue-database "${GLUE_DATABASE}"

  echo "Creating datahub connection..."

  # datahub says to use this command here: https://docs.datahub.com/docs/lineage/airflow#configuration
  airflow connections add --conn-type 'datahub_rest' 'datahub_rest_default' --conn-host "http://host.docker.internal:${DATAHUB_MAPPED_GMS_PORT}" || echo "Connection already exists"

  echo "Creating admin user..."
  airflow users create \
    --role Admin \
    --username airflow \
    --password airflow \
    --email airflow@airflow.com \
    --firstname airflow \
    --lastname airflow
  
#   echo "Airflow initialization complete!"
# else
#   echo "Airflow already initialized, skipping..."
# fi

# Execute the provided command
exec "$@"
