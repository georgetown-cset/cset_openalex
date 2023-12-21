import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLCreateInstanceDatabaseOperator,
)
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from dataloader.airflow_utils.defaults import (
    DAGS_DIR,
    DATA_BUCKET,
    GCP_ZONE,
    PROJECT_ID,
    get_default_args,
    get_post_success,
)
from dataloader.scripts.populate_documentation import update_table_descriptions

args = get_default_args()
args["retries"] = 1
args["on_failure_callback"] = None


with DAG(
    "cset_openalex_updater",
    default_args=args,
    description="Updates CSET-OpenAlex data dumps",
    schedule_interval=None,
) as dag:
    production_dataset = "cset_openalex"
    staging_dataset = f"staging_{production_dataset}"
    backups_dataset = f"{production_dataset}_backups"
    tmp_dir = f"{production_dataset}/tmp"
    sql_dir = f"sql/{production_dataset}"
    schema_dir = f"schemas/{production_dataset}"
    curr_date = datetime.now().strftime("%Y%m%d")
    run_dir = "current_run"
    public_bucket = "mos-static"
    table = "metadata"

    """
    - clear tmp dir
    - run query
    - run checks
    - push to gcs
    - push to prod
    - push to backups
    - add docs
    - put on zenodo
    """

    clear_tmp_dir = GCSDeleteObjectsOperator(
        task_id="clear_tmp_dir", bucket_name=DATA_BUCKET, prefix=tmp_dir
    )

    run_metadata = BigQueryInsertJobOperator(
        task_id="run_metadata",
        configuration={
            "query": {
                "query": "{% include '" + f"{sql_dir}/{table}.sql" + "' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": staging_dataset,
                    "tableId": "metadata"
                },
                "allowLargeResults": True,
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE"
            }
        },
    )

    checks = [
        BigQueryCheckOperator(
            task_id="check_id_unique",
            sql=(f"select count(distinct(id)) = count(id) from {staging_dataset}.metadata"),
            use_legacy_sql=False
        )
    ]
    with open(f"{os.environ.get('DAGS_FOLDER')}/{schema_dir}/{table}.json") as f:
        schema = json.loads(f.read())
    # Check that numbers of non-null values in each column don't change by more than 5% per run
    for column in schema:
        column_name = column["name"]
        checks.append(
            BigQueryCheckOperator(
                task_id=f"check_no_huge_change_in_{column_name}",
                sql=f"select ((select count({column_name}) from {staging_dataset}.{table}) > "
                    f"(select 0.95*count({column_name}) from {production_dataset}.{table})) and "
                    f"((select count({column_name}) from {staging_dataset}.{table}) < "
                    f"(select 1.05*count({column_name}) from {production_dataset}.{table}))",
                use_legacy_sql=False
            )
        )
        # Check that counts of positive preditions don't change by more than 5% per run
        if column_name.startswith("is_"):
            checks.append(
                BigQueryCheckOperator(
                    task_id=f"check_no_huge_change_in_{column_name}_predictions",
                    sql=f"select ((select countif({column_name}) from {staging_dataset}.{table}) > "
                        f"(select 0.95*countif({column_name}) from {production_dataset}.{table})) and "
                        f"((select countif({column_name}) from {staging_dataset}.{table}) < "
                        f"(select 1.05*countif({column_name}) from {production_dataset}.{table}))",
                    use_legacy_sql=False
                )
            )

    push_to_production = BigQueryToBigQueryOperator(
        task_id=f"copy_{table}",
        source_project_dataset_tables=[f"{staging_dataset}.{table}"],
        destination_project_dataset_table=f"{production_dataset}.{table}",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE"
    )

    snapshot = BigQueryToBigQueryOperator(
        task_id=f"snapshot_{table}",
        source_project_dataset_tables=[f"{production_dataset}.{table}"],
        destination_project_dataset_table=f"{backups_dataset}.{table}_{curr_date}",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE"
    )

    pop_descriptions = PythonOperator(
        task_id="populate_column_documentation_for_" + table,
        op_kwargs={
            "input_schema": f"{os.environ.get('DAGS_FOLDER')}/{schema_dir}/{table}.json",
            "table": f"{production_dataset}.{table}",
            "table_description": "Metadata containing CSET data augmentation applied to OpenAlex"
        },
        python_callable=update_table_descriptions
    )

    msg_success = get_post_success(
        "OpenAlex-CSET data updated!", dag
    )

    (clear_tmp_dir >> run_metadata >> checks >> push_to_production >> snapshot >> pop_descriptions >> msg_success)


