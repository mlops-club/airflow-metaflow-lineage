from metaflow import FlowSpec, step, Config, current
from pydantic import BaseModel, Field
from pathlib import Path
from datetime import datetime, timedelta
import yaml
import io


THIS_DIR = Path(__file__).parent
SQL_DIR = THIS_DIR / "sql"


class FlowConfig(BaseModel):
    as_of_datetime: str
    lookback_days: int = Field(gt=0, default=30)
    predict_horizon_hours: int = Field(gt=0, default=24)
    glue_database: str
    datalake_s3_bucket: str
    region: str


def config_parser(txt: str):
    """Parse and validate configuration using Pydantic."""
    cfg = yaml.safe_load(io.BytesIO(txt.encode("utf-8")))
    validated_config = FlowConfig.model_validate(cfg)
    return validated_config.model_dump()


class ForecastNumberOfYellowTaxiRides(FlowSpec):
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
                year BIGINT,
                month BIGINT, 
                day BIGINT,
                hour BIGINT,
                pulocationid BIGINT,
                total_rides BIGINT,
                created_at TIMESTAMP
            )
            LOCATION 's3://{{ datalake_s3_bucket }}/iceberg/yellow_rides_hourly_actuals/'
            TBLPROPERTIES (
                'table_type' = 'ICEBERG',
                'format' = 'parquet',
                'write_compression' = 'snappy'
            );"""

        execute_query(
            sql_query=create_actuals_table_sql,
            glue_database=self.cfg.glue_database,
            datalake_s3_bucket=self.cfg.datalake_s3_bucket,
            job_name="create_actuals_table",
            ctx={
                "glue_database": self.cfg.glue_database,
                "datalake_s3_bucket": self.cfg.datalake_s3_bucket,
            },
        )

        # compute and merge actuals into the actuals table
        as_of_dt = datetime.fromisoformat(
            self.cfg.as_of_datetime.replace("Z", "+00:00")
        )
        execute_query(
            sql_query=(SQL_DIR / "compute_actuals.sql").read_text(),
            glue_database=self.cfg.glue_database,
            datalake_s3_bucket=self.cfg.datalake_s3_bucket,
            job_name="compute_actuals",
            ctx={
                "glue_database": self.cfg.glue_database,
                "datalake_s3_bucket": self.cfg.datalake_s3_bucket,
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
            -- created_at TIMESTAMP, -- for the life of me, I can't set this value via pandas and awswrangler
            year BIGINT, -- these needed to be BIGINT's rather than INT's due to 
                         -- wr.athena.to_iceberg() type casting pandas integers to BIGINTs
                         -- It is not ideal that we changed our DDL to something more expensive
                         -- just because this one insert function is so picky, but we can
                         -- revisit this issue later.
            month BIGINT,
            day BIGINT,
            hour BIGINT,
            pulocationid BIGINT,
            forecasted_total_rides BIGINT,
            metaflow_run_id STRING
        )
        LOCATION 's3://{{ datalake_s3_bucket }}/iceberg/yellow_rides_hourly_forecast/'
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'format' = 'parquet',
            'write_compression' = 'snappy'
        );
        """

        execute_query(
            sql_query=query,
            glue_database=self.cfg.glue_database,
            datalake_s3_bucket=self.cfg.datalake_s3_bucket,
            job_name="create_forecast_table",
            ctx={
                "glue_database": self.cfg.glue_database,
                "datalake_s3_bucket": self.cfg.datalake_s3_bucket,
            },
        )

        self.next(self.prepare_training_data)

    @step
    def prepare_training_data(self, inputs):
        from helpers.athena import query_pandas_from_athena

        self.training_data_df = query_pandas_from_athena(
            sql_query=(SQL_DIR / "prepare_training_data.sql").read_text(),
            glue_database=self.cfg.glue_database,
            datalake_s3_bucket=self.cfg.datalake_s3_bucket,
            job_name="prepare_training_data",
            ctx={
                "glue_database": self.cfg.glue_database,
                "datalake_s3_bucket": self.cfg.datalake_s3_bucket,
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
        import pandas as pd

        # Populate up_to_as_of_datetime with training data up to the as_of_datetime
        as_of_dt = datetime.fromisoformat(
            self.cfg.as_of_datetime.replace("Z", "+00:00")
        )
        
        up_to_as_of_datetime = self.training_data_df[
            pd.to_datetime(self.training_data_df[['year', 'month', 'day', 'hour']]) <= as_of_dt
        ].copy()
        
        self.seasonal_forecast: pd.DataFrame = generate_seasonal_naive_forecast(
            predict_horizon_hours=self.cfg.predict_horizon_hours,
            up_to_as_of_datetime=up_to_as_of_datetime,
        )
        self.next(self.write_forecasts_to_table)

    @step
    def write_forecasts_to_table(self):
        # TODO: move this step to a separate inference flow that does more extensive testing and/or runs inference
        import awswrangler as wr

        # Add created_at column with proper datetime format
        # These lines DO NOT WORK with awswrangler.to_iceberg... tried for hours, ran into type casting errors
        # self.seasonal_forecast["created_at"] = pd.Timestamp.now()
        # self.seasonal_forecast["created_at"] = self.seasonal_forecast["created_at"].astype('datetime64[ns]')
        self.seasonal_forecast["metaflow_run_id"] = current.run_id

        wr.athena.to_iceberg(
            df=self.seasonal_forecast,
            database=self.cfg.glue_database,
            table="yellow_rides_hourly_forecast",
            mode="append",
            keep_files=False, # CLEAN UP duplicate files or you'll regret it!
            merge_condition="update",
            merge_cols=["pulocationid", "year", "month", "day", "hour"],
            temp_path= f"s3://{self.cfg.datalake_s3_bucket}/athena-results/temp/",
            schema_evolution=False,
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
    ForecastNumberOfYellowTaxiRides()
