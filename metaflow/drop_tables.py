#!/usr/bin/env python3
"""
Script to drop all tables managed by the TrainForecastModelFlow.
This will clean up the Iceberg tables created by the Metaflow pipeline.
"""

import yaml
from pathlib import Path
from helpers.athena import execute_query

THIS_DIR = Path(__file__).parent
CONFIG_FILE = THIS_DIR / "config.yaml"

def load_config():
    """Load configuration from config.yaml"""
    with open(CONFIG_FILE, 'r') as f:
        return yaml.safe_load(f)

def drop_tables():
    """Drop all tables managed by the Metaflow flow"""
    
    # Load configuration
    config = load_config()
    glue_database = config['glue_database']
    datalake_s3_bucket = config['datalake_s3_bucket']
    
    print(f"Dropping tables in database: {glue_database}")
    
    # List of tables to drop
    tables_to_drop = [
        "yellow_rides_hourly_actuals",
        "yellow_rides_hourly_forecast"
    ]
    
    for table_name in tables_to_drop:
        print(f"Dropping table: {table_name}")
        
        drop_sql = f"""\
            DROP TABLE IF EXISTS {glue_database}.{table_name};
        """
        
        try:
            query_id = execute_query(
                sql_query=drop_sql,
                glue_database=glue_database,
                datalake_s3_bucket=datalake_s3_bucket,
                ctx={
                    "glue_database": glue_database,
                    "datalake_s3_bucket": datalake_s3_bucket,
                }
            )
            print(f"✓ Successfully dropped table {table_name} (Query ID: {query_id})")
        except Exception as e:
            print(f"✗ Failed to drop table {table_name}: {str(e)}")
    
    print("\nTable cleanup completed!")
    print(f"Note: S3 data files in s3://{datalake_s3_bucket}/iceberg/ may still exist.")
    print("You may want to manually clean up S3 files if needed.")

if __name__ == "__main__":
    drop_tables()
