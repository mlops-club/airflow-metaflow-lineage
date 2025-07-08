import io
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from helpers.openlineage import openlineage
from pydantic import BaseModel, Field

from metaflow import Config, FlowSpec, current, step

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

    @openlineage
    @step
    def start(self):
        self.next(self.compute_actuals, self.create_forecast_table)

    @openlineage
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
        as_of_dt = datetime.fromisoformat(self.cfg.as_of_datetime.replace("Z", "+00:00"))
        execute_query(
            sql_query=(SQL_DIR / "compute_actuals.sql").read_text(),
            glue_database=self.cfg.glue_database,
            datalake_s3_bucket=self.cfg.datalake_s3_bucket,
            job_name="compute_actuals",
            ctx={
                "glue_database": self.cfg.glue_database,
                "datalake_s3_bucket": self.cfg.datalake_s3_bucket,
                "start_datetime": (as_of_dt - timedelta(days=self.cfg.lookback_days * 2)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "end_datetime": (as_of_dt + timedelta(days=self.cfg.lookback_days * 2)).strftime("%Y-%m-%d %H:%M:%S"),
            },
        )

        self.next(self.prepare_training_data)

    @openlineage
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

    @openlineage
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

    @openlineage
    @step
    def train_forecasting_model(self):
        # Create the filtered "model" dataframe - this is our seasonal naive model
        import pandas as pd
        from helpers.openlineage import (
            create_dataframe_facets,
            create_datasource_facet,
            create_version_facet,
            emit_model_lineage_event,
        )

        as_of_dt = datetime.fromisoformat(self.cfg.as_of_datetime.replace("Z", "+00:00"))

        # The "model" is the training data filtered up to the as_of_datetime
        self.seasonal_naive_model = self.training_data_df[
            pd.to_datetime(self.training_data_df[["year", "month", "day", "hour"]]) <= as_of_dt
        ].copy()

        # Create model facets combining conceptual and physical aspects
        model_facets = create_dataframe_facets(
            self.seasonal_naive_model,
            metadata={
                "modelType": {
                    "algorithm": "seasonal_naive",
                    "seasonality_period": "7_days",
                    "model_category": "time_series",
                    "description": "Seasonal naive forecasting model using 7-day lookback",
                },
                "trainingParameters": {
                    "as_of_datetime": self.cfg.as_of_datetime,
                    "lookback_days": self.cfg.lookback_days,
                    "predict_horizon_hours": self.cfg.predict_horizon_hours,
                    "filter_condition": "datetime <= as_of_datetime",
                },
            },
        )

        # Add version facet
        model_facets.update(create_version_facet(current.run_id))

        # Add temporal coverage specific to our data
        if not self.seasonal_naive_model.empty:
            # Get date range from the actual data
            datetime_series = pd.to_datetime(self.seasonal_naive_model[["year", "month", "day", "hour"]])
            model_facets["temporalCoverage"] = {
                "start": datetime_series.min().isoformat(),
                "end": datetime_series.max().isoformat(),
                "total_hours": len(datetime_series),
                "unique_locations": self.seasonal_naive_model["pulocationid"].nunique(),
            }

        # Emit training lineage event
        emit_model_lineage_event(
            inputs=[
                {
                    "name": f"AwsDataCatalog.{self.cfg.glue_database}.yellow_rides_hourly_actuals",
                    "namespace": "awsathena://athena.us-east-1.amazonaws.com",
                    "facets": create_datasource_facet(
                        name=f"{self.cfg.glue_database}.yellow_rides_hourly_actuals",
                        uri=f"athena://{self.cfg.glue_database}/yellow_rides_hourly_actuals",
                    ),
                }
            ],
            outputs=[{"name": "seasonal_naive_model", "facets": model_facets}],
            job_name="train_seasonal_naive_model",
        )

        print(f"Seasonal naive model created with {len(self.seasonal_naive_model)} training records")
        print(f"Model covers {self.seasonal_naive_model['pulocationid'].nunique()} unique locations")

        self.next(self.predict_forecast)

    @openlineage
    @step
    def predict_forecast(self):
        """
        Ideally, this would be an evaluation step.

        And predictions would be written to the lakehouse in a separate flow.
        """
        import pandas as pd
        from helpers.forecasting import generate_seasonal_naive_forecast
        from helpers.openlineage import create_dataframe_facets, create_version_facet, emit_model_lineage_event

        # Use the stored model from the training step
        model_data = self.seasonal_naive_model

        # Generate the forecast using our "model" (the filtered training data)
        self.seasonal_forecast = generate_seasonal_naive_forecast(
            predict_horizon_hours=self.cfg.predict_horizon_hours,
            up_to_as_of_datetime=model_data,
        )

        # Create facets for the forecast output
        forecast_facets = create_dataframe_facets(
            self.seasonal_forecast,
            metadata={
                "predictionMetadata": {
                    "model_run_id": current.run_id,
                    "algorithm": "seasonal_naive",
                    "prediction_timestamp": datetime.now().isoformat(),
                    "forecast_horizon_hours": self.cfg.predict_horizon_hours,
                    "locations_forecasted": self.seasonal_forecast["pulocationid"].nunique()
                    if not self.seasonal_forecast.empty
                    else 0,
                }
            },
        )

        # Add version facet
        forecast_facets.update(create_version_facet(current.run_id))

        # Calculate model usage statistics
        total_predictions = len(self.seasonal_forecast)
        successful_predictions = len(self.seasonal_forecast[self.seasonal_forecast["forecasted_total_rides"] > 0])

        # Emit prediction lineage event
        emit_model_lineage_event(
            inputs=[
                {
                    "name": "seasonal_naive_model",
                    "facets": {
                        **create_version_facet(current.run_id),
                        "modelUsage": {
                            "lookup_operations": total_predictions,
                            "successful_lookups": successful_predictions,
                            "fallback_to_zero": total_predictions - successful_predictions,
                            "lookup_success_rate": round(successful_predictions / total_predictions, 3)
                            if total_predictions > 0
                            else 0,
                        },
                    },
                }
            ],
            outputs=[{"name": "forecast_predictions", "facets": forecast_facets}],
            job_name="predict_seasonal_naive",
        )

        print(f"Generated {len(self.seasonal_forecast)} predictions using seasonal naive model")
        print(
            f"Successful predictions: {successful_predictions}/{total_predictions} ({successful_predictions / total_predictions * 100:.1f}%)"
        )

        self.next(self.write_forecasts_to_table)

    @openlineage
    @step
    def write_forecasts_to_table(self):
        # TODO: move this step to a separate inference flow that does more extensive testing and/or runs inference
        import awswrangler as wr
        from helpers.openlineage import (
            create_datasource_facet,
            create_version_facet,
            emit_model_lineage_event,
        )

        # Add created_at column with proper datetime format
        # These lines DO NOT WORK with awswrangler.to_iceberg... tried for hours, ran into type casting errors
        # self.seasonal_forecast["created_at"] = pd.Timestamp.now()
        # self.seasonal_forecast["created_at"] = self.seasonal_forecast["created_at"].astype('datetime64[ns]')
        self.seasonal_forecast["metaflow_run_id"] = current.run_id

        # Emit lineage event for writing predictions to the forecast table
        emit_model_lineage_event(
            inputs=[
                {
                    "name": "forecast_predictions",
                    "facets": {
                        **create_version_facet(current.run_id),
                        "writeOperation": {
                            "operation": "write_to_iceberg",
                            "mode": "append",
                            "merge_condition": "update",
                            "merge_cols": ["pulocationid", "year", "month", "day", "hour"],
                        },
                    },
                }
            ],
            outputs=[
                {
                    # "name": f"{self.cfg.glue_database}.yellow_rides_hourly_forecast",
                    "name": f"AwsDataCatalog.{self.cfg.glue_database}.yellow_rides_hourly_forecast",
                    "namespace": "awsathena://athena.us-east-1.amazonaws.com",
                    "facets": create_datasource_facet(
                        name=f"{self.cfg.glue_database}.yellow_rides_hourly_forecast",
                        uri=f"iceberg://{self.cfg.glue_database}/yellow_rides_hourly_forecast",
                    ),
                }
            ],
            job_name="write_forecasts",
        )

        wr.athena.to_iceberg(
            df=self.seasonal_forecast,
            database=self.cfg.glue_database,
            table="yellow_rides_hourly_forecast",
            mode="append",
            keep_files=False,  # CLEAN UP duplicate files or you'll regret it!
            merge_condition="update",
            merge_cols=["pulocationid", "year", "month", "day", "hour"],
            temp_path=f"s3://{self.cfg.datalake_s3_bucket}/athena-results/temp/",
            schema_evolution=False,
        )

        print(f"Successfully wrote {len(self.seasonal_forecast)} forecast records to the forecast table")
        self.next(self.end)

    @openlineage
    @step
    def end(self):
        print("Forecast pipeline completed!")
        print(f"Training data preparation: {self.training_data_df.shape} training records")
        print(f"Seasonal naive model: {self.seasonal_naive_model.shape} model records")
        print(f"Seasonal naive forecast: {self.seasonal_forecast.shape} forecasts generated")

        # Show sample forecast data if available
        if not self.seasonal_forecast.empty:
            print("\nSample forecasts:")
            print(self.seasonal_forecast.head())

        print("\nAll seasonal naive forecasts have been written to the forecast table.")
        print("\nLineage Summary:")
        print(
            f"1. Training: {self.cfg.glue_database}.yellow_rides_hourly_actuals → seasonal_naive_model (v{current.run_id})"
        )
        print(f"2. Prediction: seasonal_naive_model (v{current.run_id}) → forecast_predictions (v{current.run_id})")
        print(
            f"3. Storage: forecast_predictions (v{current.run_id}) → {self.cfg.glue_database}.yellow_rides_hourly_forecast"
        )


if __name__ == "__main__":
    ForecastNumberOfYellowTaxiRides()
