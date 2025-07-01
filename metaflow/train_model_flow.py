from metaflow import FlowSpec, step, Config
from pydantic import BaseModel, Field
from pathlib import Path
import yaml
import io
import os

THIS_DIR = Path(__file__).parent
SQL_DIR = THIS_DIR / "sql"


class DatasetConfig(BaseModel):
    as_of_datetime: str
    lookback_days: int = Field(gt=0, default=30)
    predict_horizon_hours: int = Field(gt=0, default=24)


class AWSConfig(BaseModel):
    glue_database: str
    s3_bucket: str
    region: str = "us-east-1"


class FlowConfig(BaseModel):
    dataset: DatasetConfig
    aws: AWSConfig


def config_parser(txt: str):
    """Parse and validate configuration using Pydantic."""
    cfg = yaml.safe_load(io.BytesIO(txt.encode("utf-8")))
    validated_config = FlowConfig.model_validate(cfg)
    return validated_config.model_dump()


class TrainForecastModelFlow(FlowSpec):
    config: FlowConfig = Config(
        "config",
        default=THIS_DIR / "config.yaml",
        parser=config_parser,
    )  # type: ignore

    @step
    def start(self):
        self.next(self.compute_actuals)

    @step
    def compute_actuals(self):
        """Step 1: Compute the actuals using the SQL query."""
        from helpers.data_preparation import compute_actuals
        from helpers.athena import execute_query

        # Create table if not exist
        execute_query(
            sql_query="""\
                -- Create the actuals table if it doesn't exist
            CREATE TABLE IF NOT EXISTS {{ glue_database }}.yellow_rides_hourly_actuals (
                year INT,
                month INT, 
                day INT,
                hour INT,
                pulocationid INT,
                total_rides INT,
                created_at TIMESTAMP
            )
            LOCATION 's3://{{ s3_bucket }}/iceberg/yellow_rides_hourly_actuals/'
            TBLPROPERTIES (
                'table_type' = 'ICEBERG',
                'format' = 'parquet',
                'write_compression' = 'snappy'
            );""",
            glue_database=self.config.aws.glue_database,
            s3_bucket=self.config.aws.s3_bucket,
            region=self.config.aws.region,
            ctx={
                "glue_database": self.config.aws.glue_database,
                "s3_bucket": self.config.aws.s3_bucket,
            },
        )

        sql_path = SQL_DIR / "compute_actuals.sql"
        self.actuals_query_id = compute_actuals(
            sql_file_path=sql_path,
            glue_database=self.config.aws.glue_database,
            s3_bucket=self.config.aws.s3_bucket,
            region=self.config.aws.region,
            as_of_datetime=self.config.dataset.as_of_datetime,
            lookback_days=self.config.dataset.lookback_days,
        )
        print(f"Actuals computation completed. Query ID: {self.actuals_query_id}")
        self.next(self.create_forecast_table)

    @step
    def create_forecast_table(self):
        """Step 2: Create the forecast output table using SQL query."""
        from helpers.athena import execute_query

        sql_path = SQL_DIR / "create_hourly_forecast_table.sql"

        # Read SQL file using pathlib
        sql_content = sql_path.read_text()

        # Prepare context for Jinja2 templating
        ctx = {
            "glue_database": self.config.aws.glue_database,
            "s3_bucket": self.config.aws.s3_bucket,
        }

        self.forecast_table_query_id = execute_query(
            sql_query=sql_content,
            glue_database=self.config.aws.glue_database,
            s3_bucket=self.config.aws.s3_bucket,
            region=self.config.aws.region,
            ctx=ctx,
        )
        print(
            f"Forecast table creation completed. Query ID: {self.forecast_table_query_id}"
        )
        self.next(self.prepare_training_data)

    @step
    def prepare_training_data(self):
        """Step 3: Prepare training set from actuals table."""
        from helpers.data_preparation import prepare_training_data

        sql_path = SQL_DIR / "prepare_training_data.sql"
        self.training_data = prepare_training_data(
            sql_file_path=sql_path,
            glue_database=self.config.aws.glue_database,
            s3_bucket=self.config.aws.s3_bucket,
            region=self.config.aws.region,
            as_of_datetime=self.config.dataset.as_of_datetime,
            lookback_days=self.config.dataset.lookback_days,
        )
        print(
            f"Training data preparation completed. Dataset shape: {self.training_data.shape}"
        )
        self.next(self.generate_seasonal_naive_forecast)

    @step
    def generate_seasonal_naive_forecast(self):
        """Step 4: Calculate seasonal naive baseline forecast."""
        from helpers.forecasting import generate_seasonal_naive_forecast

        seasonal_sql_path = SQL_DIR / "seasonal_naive_forecast.sql"
        self.seasonal_forecast = generate_seasonal_naive_forecast(
            sql_file_path=seasonal_sql_path,
            glue_database=self.config.aws.glue_database,
            s3_bucket=self.config.aws.s3_bucket,
            region=self.config.aws.region,
            as_of_datetime=self.config.dataset.as_of_datetime,
            lookback_days=self.config.dataset.lookback_days,
            predict_horizon_hours=self.config.dataset.predict_horizon_hours,
        )
        print(
            f"Seasonal naive forecast completed. Forecast shape: {self.seasonal_forecast.shape}"
        )
        self.next(self.write_forecasts_to_table)

    @step
    def write_forecasts_to_table(self):
        """Step 5: Write forecast results to output table in an idempotent manner."""
        from helpers.forecasting import write_forecasts_to_table

        write_sql_path = SQL_DIR / "write_forecasts_to_table.sql"
        seasonal_sql_path = SQL_DIR / "seasonal_naive_forecast.sql"

        self.write_forecast_query_id = write_forecasts_to_table(
            write_sql_path=write_sql_path,
            seasonal_sql_path=seasonal_sql_path,
            glue_database=self.config.aws.glue_database,
            s3_bucket=self.config.aws.s3_bucket,
            region=self.config.aws.region,
            as_of_datetime=self.config.dataset.as_of_datetime,
            lookback_days=self.config.dataset.lookback_days,
            predict_horizon_hours=self.config.dataset.predict_horizon_hours,
        )
        print(f"Write forecasts completed. Query ID: {self.write_forecast_query_id}")
        self.next(self.end)

    @step
    def end(self):
        print("Forecast pipeline completed!")
        print(f"Training data preparation: {self.training_data.shape} training records")
        print(
            f"Seasonal naive forecast: {self.seasonal_forecast.shape} forecasts generated"
        )

        # Show sample forecast data if available
        if not self.seasonal_forecast.empty:
            print("\nSample forecasts:")
            print(self.seasonal_forecast.head())

        print("\nAll seasonal naive forecasts have been written to the forecast table.")


if __name__ == "__main__":
    os.environ["AWS_PROFILE"] = "sandbox"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    TrainForecastModelFlow()
