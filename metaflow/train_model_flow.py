from metaflow import FlowSpec, step, Config
from pydantic import BaseModel, Field
from pathlib import Path
from datetime import datetime, timedelta
import yaml
import io
import os

THIS_DIR = Path(__file__).parent
SQL_DIR = THIS_DIR / "sql"


class FlowConfig(BaseModel):
    as_of_datetime: str
    lookback_days: int = Field(gt=0, default=30)
    predict_horizon_hours: int = Field(gt=0, default=24)
    glue_database: str
    s3_bucket: str
    region: str = "us-east-1"


def config_parser(txt: str):
    """Parse and validate configuration using Pydantic."""
    cfg = yaml.safe_load(io.BytesIO(txt.encode("utf-8")))
    validated_config = FlowConfig.model_validate(cfg)
    return validated_config.model_dump()


class TrainForecastModelFlow(FlowSpec):
    cfg: FlowConfig = Config(
        "config",
        default=str(THIS_DIR / "config.yaml"),
        parser=config_parser,
    )  # type: ignore

    @step
    def start(self):
        self.next(self.compute_actuals, self.create_forecast_table)

    @step
    def compute_actuals(self):
        from helpers.athena import execute_query

        # create a table to contain actuals
        create_actuals_table_sql = """\
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
            );"""

        execute_query(
            sql_query=create_actuals_table_sql,
            glue_database=self.cfg.glue_database,
            s3_bucket=self.cfg.s3_bucket,
            ctx={
                "glue_database": self.cfg.glue_database,
                "s3_bucket": self.cfg.s3_bucket,
            },
        )

        # compute and merge actuals into the actuals table
        as_of_dt = datetime.fromisoformat(
            self.cfg.as_of_datetime.replace("Z", "+00:00")
        )
        execute_query(
            sql_query=(SQL_DIR / "compute_actuals.sql").read_text(),
            glue_database=self.cfg.glue_database,
            s3_bucket=self.cfg.s3_bucket,
            ctx={
                "glue_database": self.cfg.glue_database,
                "s3_bucket": self.cfg.s3_bucket,
                "start_datetime": (
                    as_of_dt - timedelta(days=self.cfg.lookback_days * 2)
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "end_datetime": (
                    as_of_dt + timedelta(days=self.cfg.lookback_days * 2)
                ).strftime("%Y-%m-%d %H:%M:%S"),
            },
        )

        self.next(self.prepare_training_data)

    @step
    def create_forecast_table(self):
        from helpers.athena import execute_query

        query = """\
        CREATE TABLE IF NOT EXISTS {{ glue_database }}.yellow_rides_hourly_forecast (
            forecast_created_at TIMESTAMP,
            year INT,
            month INT,
            day INT,
            hour INT,
            pulocationid INT,
            forecast_value INT
        )
        LOCATION 's3://{{ s3_bucket }}/iceberg/yellow_rides_hourly_forecast/'
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'format' = 'parquet',
            'write_compression' = 'snappy'
        );
        """

        execute_query(
            sql_query=query,
            glue_database=self.cfg.glue_database,
            s3_bucket=self.cfg.s3_bucket,
            ctx={
                "glue_database": self.cfg.glue_database,
                "s3_bucket": self.cfg.s3_bucket,
            },
        )

        self.next(self.prepare_training_data)

    @step
    def prepare_training_data(self, inputs):
        from helpers.athena import query_pandas_from_athena

        self.training_data_df = query_pandas_from_athena(
            sql_query=(SQL_DIR / "prepare_training_data.sql").read_text(),
            glue_database=self.cfg.glue_database,
            s3_bucket=self.cfg.s3_bucket,
            ctx={
                "glue_database": self.cfg.glue_database,
                "s3_bucket": self.cfg.s3_bucket,
                "as_of_datetime": self.cfg.as_of_datetime,
                "lookback_days": self.cfg.lookback_days,
            },
        )

        self.next(self.train_forecasting_model)

    @step
    def train_forecasting_model(self):
        # this is a no-op until we implement a model beyond the "seasonal naive" baseline
        self.next(self.predict_forecast)

    @step
    def predict_forecast(self):
        """
        Ideally, this would be an evaluation step.

        And predictions would be written to the lakehouse in a separate flow.
        """
        from helpers.forecasting import generate_seasonal_naive_forecast

        ## LOGIC SHOULD BE WRITTEN IN PANDAS

        seasonal_sql_path = SQL_DIR / "seasonal_naive_forecast.sql"
        self.seasonal_forecast = generate_seasonal_naive_forecast(
            sql_file_path=seasonal_sql_path,
            glue_database=self.cfg.glue_database,
            s3_bucket=self.cfg.s3_bucket,
            as_of_datetime=self.cfg.as_of_datetime,
            lookback_days=self.cfg.lookback_days,
            predict_horizon_hours=self.cfg.predict_horizon_hours,
        )
        self.next(self.write_forecasts_to_table)

    @step
    def write_forecasts_to_table(self):
        from helpers.forecasting import write_forecasts_to_table

        ## DATA WRANGLER Library should do an upsert to add the predictions to forecast table

        write_sql_path = SQL_DIR / "write_forecasts_to_table.sql"

        self.write_forecast_query_id = write_forecasts_to_table(
            write_sql_path=write_sql_path,
            glue_database=self.cfg.glue_database,
            s3_bucket=self.cfg.s3_bucket,
            as_of_datetime=self.cfg.as_of_datetime,
            lookback_days=self.cfg.lookback_days,
            predict_horizon_hours=self.cfg.predict_horizon_hours,
        )
        self.next(self.end)

    @step
    def end(self):
        print("Forecast pipeline completed!")
        print(
            f"Training data preparation: {self.training_data_df.shape} training records"
        )
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
