# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pulumi-aws>=6.81.0",
# ]
# ///

import pulumi
import pulumi_aws as aws
from pulumi import automation as auto
import os

from pathlib import Path

THIS_DIR = Path(__file__).parent

# disable encryption for Pulumi since the state and secrets will only
# ever exist locally and not be committed; aka lax encryption is okay because
# this is not meant for collaboration between multiple team members
os.environ["PULUMI_CONFIG_PASSPHRASE"] = ""

PULUMI_STATE_DIR = THIS_DIR / ".pulumi"
os.environ["PULUMI_BACKEND_URL"] = f"file://{PULUMI_STATE_DIR}"
PULUMI_STATE_DIR.mkdir(exist_ok=True)

S3_BUCKET_NAME = os.environ["S3_DATA_LAKE_BUCKET_NAME"]
GLUE_DATABASE_NAME = os.environ["GLUE_DATABASE"]

PULUMI_PROJECT_NAME = os.environ["PULUMI_PROJECT_NAME"]
PULUMI_STACK_NAME = os.environ["PULUMI_STACK_NAME"]

# Check environment variable to determine whether to create or destroy the stack
STACK_ACTION = os.environ.get("STACK_ACTION", "up").lower()


def pulumi_program():
    # The first argument is the Pulumi resource name, the second is the bucket name property
    bucket = aws.s3.Bucket(resource_name="datalake-bucket", bucket=S3_BUCKET_NAME, force_destroy=True)
    glue_db = aws.glue.CatalogDatabase(resource_name="glue-db", name=GLUE_DATABASE_NAME)
    pulumi.export("bucket_name", bucket.id)
    pulumi.export("glue_database_name", glue_db.name)


stack = auto.create_or_select_stack(
    stack_name=PULUMI_STACK_NAME,
    project_name=PULUMI_PROJECT_NAME,
    program=pulumi_program,
)

stack.set_config("aws:region", auto.ConfigValue(value="us-east-1"))
stack.workspace.install_plugin("aws", "v5.0.0")


if STACK_ACTION == "destroy":
    print("Destroying stack...")
    stack.destroy(on_output=print)
    print("Stack destroyed successfully!")
else:
    print("Creating/updating stack...")
    up_res = stack.up(on_output=print)
    up_res = stack.up()

    print(f"Bucket name: {up_res.outputs['bucket_name'].value}")
    bucket_url = f"https://s3.console.aws.amazon.com/s3/buckets/{up_res.outputs['bucket_name'].value}?region=us-east-1"
    print(f"S3 Console URL: {bucket_url}")
    print(f"Glue Database: {up_res.outputs['glue_database_name'].value}")
    glue_db_url = f"https://us-east-1.console.aws.amazon.com/glue/home?region=us-east-1#database:name={up_res.outputs['glue_database_name'].value}"
    print(f"Glue Database Console URL: {glue_db_url}")
