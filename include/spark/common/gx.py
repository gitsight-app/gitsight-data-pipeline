import great_expectations as gx
from great_expectations.data_context import AbstractDataContext
from great_expectations.data_context.types.base import DataContextConfig

GX_PROJECT_CONFIG_DICT = {
    "config_version": 3.0,
    "stores": {
        "expectations_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": "gitsight",
                "prefix": "gx/expectations/",
                "boto3_options": {"endpoint_url": "http://10.0.3.200:30061"},
            },
        },
        "validations_store": {
            "class_name": "ValidationsStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": "gitsight",
                "prefix": "gx/validations/",
                "boto3_options": {"endpoint_url": "http://10.0.3.200:30061"},
            },
        },
        "checkpoint_store": {
            "class_name": "CheckpointStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": "gitsight",
                "prefix": "gx/checkpoints/",
                "boto3_options": {"endpoint_url": "http://10.0.3.200:30061"},
            },
        },
        "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
    },
    "expectations_store_name": "expectations_store",
    "validations_store_name": "validations_store",
    "checkpoint_store_name": "checkpoint_store",
    "evaluation_parameter_store_name": "evaluation_parameter_store",
    "data_docs_sites": {
        "s3_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": "gitsight",
                "prefix": "gx/data_docs/",
                "boto3_options": {"endpoint_url": "http://10.0.3.200:30061"},
            },
            "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
        }
    },
    "datasources": {
        "gitsight_datalake": {
            "class_name": "Datasource",
            "execution_engine": {"class_name": "SparkDFExecutionEngine"},
            "data_connectors": {
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                }
            },
        }
    },
    "anonymous_usage_statistics": {"enabled": False},
}


def get_gx_context(config: dict | None = None) -> AbstractDataContext:
    project_config = DataContextConfig(**(config or GX_PROJECT_CONFIG_DICT))
    return gx.get_context(project_config=project_config)


def is_exists_checkpoint(context: AbstractDataContext, checkpoint) -> bool:

    return checkpoint in context.list_checkpoints()
