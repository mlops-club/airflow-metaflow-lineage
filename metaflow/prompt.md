This is the best way to manage config files are managed in Metaflow.

Here is an example file

```yaml
# config.yaml
nested:
  type: "xgboost"
  forecast_horizon: 24
  train_months: 1
```

And here is the Python code to parse it. Notice we can access
the nested Pydantic model attributes usinge dot notation. This is 
the best way to access attributes because it results in better autocompletion.

```python
from metaflow import FlowSpec, step, current, Config
from pydantic import BaseModel, Field
from typing import Literal
import yaml
import io
from pathlib import Path

THIS_DIR = Path(__file__).parent


class Nested(BaseModel):
    type: Literal["xgboost", "random_forest", "linear"] = "xgboost"
    forecast_horizon: int = Field(gt=0, default=24)
    train_months: int = Field(gt=0, default=6)


class FlowConfig(BaseModel):
    nested: Nested


def config_parser(txt: str):
    """Parse and validate configuration using Pydantic."""
    cfg = yaml.safe_load(io.BytesIO(txt.encode('utf-8')))
    validated_config = FlowConfig.model_validate(cfg)
    return validated_config.model_dump()


class Flow(FlowSpec):
    config: FlowConfig = Config("config", default=THIS_DIR / "config.yaml", parser=config_parser) # type: ignore
    
    @step
    def start(self):
        print(self.config.nested)
        print(self.config.nested.forecast_horizon)
        print(self.config.nested.type)
        print(self.config.nested.train_months)

        self.next(self.end)

    @step
    def end(self):
        ...


if __name__ == '__main__':
    Flow()
```

The `train_forecast_model_flow.py` file should 

- be as short and simple as possible
  - all business logic should be extracted to pure functions in `helpers/*.py`
    - use absolute imports at all times when using these
  - all SQL queries (even multi-statement files) should be in `sql/*.sql`
    - placeholders may use Jinja, but please try to keep them simple, e.g. minimize or eliminate the use of conditional blocks
  - Reference SQL query file paths from the `_flow.py` file and pass them to helper functions as arguments
- push as much logic to SQL as possible. Assume the data is fairly large, so we
  want the data lakehouse to handle as much of the processing as possible

The config

```yaml
dataset:
  as_of_datetime: "2023-10-01T00:00:00Z"
  lookback_days: 30
  predict_horizon_hours: 24
aws:
  glue_database: "nyc_taxi"
  s3_bucket: "nyc-taxi-forecasting"
  region: "us-east-1"
```

In an earlier Airflow DAG, this was the DDL for the `raw_yellow` table

```sql
CREATE TABLE IF NOT EXISTS {{ var.value.get("datalake-glue-database") }}.raw_yellow (
    unique_row_id         STRING,
    filename              STRING,
    ingest_timestamp      TIMESTAMP,
    vendorid              INT,
    tpep_pickup_datetime  TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count       BIGINT,
    trip_distance         DOUBLE,
    ratecodeid            BIGINT,
    store_and_fwd_flag    STRING,
    pulocationid          INT,
    dolocationid          INT,
    payment_type          BIGINT,
    fare_amount           DOUBLE,
    extra                 DOUBLE,
    mta_tax               DOUBLE,
    tip_amount            DOUBLE,
    tolls_amount          DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount          DOUBLE,
    congestion_surcharge  DOUBLE,
    cbd_congestion_fee    DOUBLE,
    airport_fee           DOUBLE
)
```

From these columns, we can calculate a variety of time series features. Keep in mind that there are NULL values for some rows, but if we were to drop 

The target should be: number of rides per pickup location ID in a given hour.

For operations against athena, use aws data wrangler.

Don't save the model to S3. Save it as bytes using self.model_bytes: bytes = ...
It can be read from later flows accordingly.

The flow can be executed using

```bash
AWS_PROFILE=sandbox uv run ./metaflow/train_model_flow.py {run|resume}
```

The flow steps should be

1. `start`
2. `prepare_data`
3. `train_model`
4. `evaluate_model`
5. `end`

Cols:

1. Timestamp, e.g. date (for day-level forcasting)
2. PickupZoneId
3. Count of Taxi pickups in that time range

```sql
CREATE TABLE features AS SELECT                                -- these require a join
   day, hour, pickupzoneid, count(*), max_temp, min_temp, avg_temp, prcp_mm
FROM ...
GROUP BY day, hour, pickupzoneid
WHERE (date range);

CREATE TABLE forecast
    day  int,
    hour int,
    pickupzoneid varchar,
    count_rides int,
    model_id varchar
```

In SKU-level forecasting, the "naive forecast" is a common baseline.
Take the previous day, and you make that the forecast for the next day.
The "seasonal naive" is using the value of 7 days ago (assuming 7-day seasonality).

With the `nixtla` package, you can define a custom class that
does the seasonal naive.

```yaml
- as_of_datetime
- lookback_n_periods
- prediction_horizon
```

Redo this flow.

1. compute the actuals using the sql query
2. create the forecast output table using another sql query file
3. prepare a training set by taking as_of_datetime - lookback_n_days from the actuals table
4. calculate the "seasonal naive" baseline forecast with a function whose prediction is whatever the forecast was for that same year, month, day, hour, pulocationid 7 days ago--do this with a sql query against the actuals table
5. write those results to the output table in an idempotent manner