import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from include.spark.common.gx.context import get_gx_context


def _create_datasource(
    *,
    datasource_name,
    engine_class: str = "SparkDFExecutionEngine",
    connector_class: str = "RuntimeDataConnector",
    **context,
):
    gx_context = get_gx_context()

    datasource_config = {
        "name": datasource_name,
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": engine_class,
        },
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": connector_class,
                "batch_identifiers": ["default_identifier_name"],
            }
        },
    }

    gx_context.add_or_update_datasource(**datasource_config)

    return datasource_name


with DAG(
    dag_id="init_gx_context",
    schedule="@once",
    start_date=pendulum.datetime(2026, 1, 1),
    catchup=False,
    doc_md="""
    init_gx_context:
    - Datasource named gitsight_datalake
    """,
) as dag:
    create_gitsight_datalake_datasource = PythonOperator(
        task_id="create_gitsight_datalake_datasource",
        python_callable=_create_datasource,
        op_kwargs={
            "datasource_name": "gitsight_datalake",
        },
    )
