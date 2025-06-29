# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "aws-cdk-lib>=2.201.0",
#     "constructs>=10.4.2",
# ]
# ///

import aws_cdk as cdk
from aws_cdk import (
    Stack,
    CfnOutput,
    aws_s3 as s3,
    aws_glue as glue,
    RemovalPolicy,
)
from constructs import Construct
import os


#################
# --- Stack --- #
#################

class GlueStack(Stack):
    """Stack for data ingestion infrastructure"""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        self.datalake_bucket = s3.Bucket(
            self,
            "DataBucket",
            bucket_name=f"nyc-taxi-datalake-{construct_id}",
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        
        self.glue_database = glue.CfnDatabase(
            self,
            # this ends up being the physical ID
            "NycTaxiDatabase",
            catalog_id=self.account,            
            database_input={
                # valid values here: https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-properties-glue-database-databaseinput.html
                "description": "NYC taxi data, more enriched than ever before.",
                "name": "nyc_taxi",
                # TODO: find out what this does and why / why not we should set it
                "location_uri": f"s3://{self.datalake_bucket.bucket_name}/nyc_taxi/",
            },
            # this property does not specify the physical database name for some reason
            # instead that is taken care of by database_input.name
            database_name="nyc_taxi"
        )

        CfnOutput(
            self,
            "S3BucketName",
            value=self.datalake_bucket.bucket_name,
            description="Name of the S3 bucket for data storage"
        )
        
        CfnOutput(
            self,
            "S3BucketConsoleLink",
            value=f"https://s3.console.aws.amazon.com/s3/buckets/{self.datalake_bucket.bucket_name}",
            description="AWS Console link to the S3 bucket"
        )
        
        CfnOutput(
            self,
            "GlueDatabaseName",
            value="nyc_taxi",
            description="Name of the Glue database"
        )
        
        CfnOutput(
            self,
            "GlueDatabaseConsoleLink",
            value=f"https://{self.region}.console.aws.amazon.com/glue/home?region={self.region}#/v2/data-catalog/databases/view/nyc_taxi",
            description="AWS Console link to the Glue database"
        )
        

###############
# --- App --- #
###############

# CDK App
app = cdk.App()

# Create the data ingestion stack
GlueStack(
    app,
    "glue-nyc-taxi",
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=os.environ.get("CDK_DEFAULT_REGION"),
    ),
    description="Data ingestion infrastructure for SageMaker exploration"
)

# Synthesize the app
app.synth()
