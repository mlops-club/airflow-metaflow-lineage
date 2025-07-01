from metaflow import FlowSpec, step, Config
from pydantic import BaseModel, Field
from pathlib import Path
import yaml
import io

# Use absolute imports as requested
import helpers.data_preparation
import helpers.model_training_simple
import helpers.model_evaluation_simple

THIS_DIR = Path(__file__).parent


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
    cfg = yaml.safe_load(io.BytesIO(txt.encode('utf-8')))
    validated_config = FlowConfig.model_validate(cfg)
    return validated_config.model_dump()


class TrainForecastModelFlow(FlowSpec):
    config: FlowConfig = Config("config", default=THIS_DIR / "config.yaml", parser=config_parser)  # type: ignore
    
    @step
    def start(self):
        self.next(self.prepare_data)

    @step
    def prepare_data(self):
        sql_path = str(THIS_DIR / "sql" / "prepare_features_simple.sql")
        self.training_data_path = helpers.data_preparation.prepare_training_data(
            sql_path=sql_path,
            glue_database=self.config.aws.glue_database,
            s3_bucket=self.config.aws.s3_bucket,
            as_of_datetime=self.config.dataset.as_of_datetime,
            lookback_days=self.config.dataset.lookback_days,
            region=self.config.aws.region
        )
        self.next(self.train_model)

    @step
    def train_model(self):
        self.model_artifacts = helpers.model_training_simple.train_forecasting_model_simple(
            training_data_path=self.training_data_path,
            region=self.config.aws.region
        )
        # Save model bytes for use in later flows
        self.model_bytes: bytes = self.model_artifacts['model_bytes']
        self.next(self.evaluate_model)

    @step
    def evaluate_model(self):
        self.evaluation_metrics = helpers.model_evaluation_simple.evaluate_model_simple(
            model_artifacts=self.model_artifacts,
            training_data_path=self.training_data_path,
            region=self.config.aws.region
        )
        self.next(self.end)

    @step
    def end(self):
        print("Model training completed!")
        print(f"Model size: {len(self.model_bytes)} bytes")
        print(f"RMSE: {self.evaluation_metrics['rmse']:.3f}")
        print(f"R²: {self.evaluation_metrics['r2_score']:.3f}")


if __name__ == '__main__':
    TrainForecastModelFlow()
