import json

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DataContractPropertiesClass,
    DataQualityContractClass,
    FreshnessContractClass,
    FreshnessCronScheduleClass,
    GenericAspectClass,
    MetadataChangeProposalClass,
)

# Initialize the DataHub REST emitter
emitter = DatahubRestEmitter("http://localhost:8091")

# Define the dataset URN
dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,/iceberg/yellow_rides_hourly_forecast,PROD)"

# Define the Freshness contract with CRON schedule
freshness_cron = FreshnessCronScheduleClass(
    cron="4 8 * * 1-5",  # 8:04 AM, Monday to Friday
    timezone="UTC",  # Adjust timezone as needed
)

freshness_contract = FreshnessContractClass(assertion="urn:li:assertion:e343b48e7faddf20c394f4f2df36ed5f")

# Define the Data Quality contract for uniqueness on 'metaflow_run_id'
# data_quality_contract = DataQualityContractClass(type="unique", column="metaflow_run_id")
data_quality_contract = DataQualityContractClass(assertion="urn:li:assertion:e343b48e7faddf20c394f4f2df36ed5f")

# Define the DataContractProperties
data_contract_properties = DataContractPropertiesClass(
    entity=dataset_urn, freshness=[freshness_contract], dataQuality=[data_quality_contract]
)

# Create the MetadataChangeProposal
mcp = MetadataChangeProposalClass(
    entityType="dataset",
    entityUrn=dataset_urn,
    changeType=ChangeTypeClass.UPSERT,
    aspectName="dataContractProperties",
    aspect=GenericAspectClass(
        contentType="application/json",
        value=json.dumps(data_contract_properties.to_obj()).encode("utf-8"),
    ),
)

# Emit the MetadataChangeProposal
emitter.emit(mcp)
