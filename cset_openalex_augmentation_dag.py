import json
import os
from datetime import datetime

import semver
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
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
from dataloader.airflow_utils.defaults import (
    DAGS_DIR,
    DATA_BUCKET,
    GCP_ZONE,
    PROJECT_ID,
    get_default_args,
    get_post_success,
)
from dataloader.scripts.populate_documentation import update_table_descriptions

args = get_default_args(pocs=["Jennifer"])
args["retries"] = 1


def get_updated_version() -> str:
    """
    Get the latest semver string for the delivery, and update the corresponding airflow Variable. By default,
    we bump the minor version with each delivery, but this function allows us to manually set a different
    part of the semver string to bump in the variable
    :return: Updated semver string
    """
    oa_variable = "cset_openalex_version"
    # This is a dict mapping "current_version" to the current semver version and "increment" to the part
    # of the version string that should be updated ("major", "minor", or "patch")
    version_config = Variable.get(oa_variable, deserialize_json=True)
    version_to_updater = {
        "major": semver.bump_major,
        "minor": semver.bump_minor,
        "patch": semver.bump_patch,
    }
    new_version = version_to_updater[version_config["increment"]](
        version_config["current_version"]
    )
    Variable.set(
        oa_variable, json.dumps({"current_version": new_version, "increment": "minor"})
    )
    return new_version


with DAG(
    "cset_openalex_updater",
    default_args=args,
    description="Updates CSET-OpenAlex data dumps",
    schedule_interval="0 0 25 * *",
    catchup=False,
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
    gce_resource_id = "cset-openalex-updater"

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
                    "tableId": "metadata",
                },
                "allowLargeResults": True,
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    checks = [
        BigQueryCheckOperator(
            task_id="check_id_unique",
            sql=(
                f"select count(distinct(id)) = count(id) from {staging_dataset}.metadata"
            ),
            use_legacy_sql=False,
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
                use_legacy_sql=False,
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
                    use_legacy_sql=False,
                )
            )

    push_to_production = BigQueryToBigQueryOperator(
        task_id=f"copy_{table}",
        source_project_dataset_tables=[f"{staging_dataset}.{table}"],
        destination_project_dataset_table=f"{production_dataset}.{table}",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    snapshot = BigQueryToBigQueryOperator(
        task_id=f"snapshot_{table}",
        source_project_dataset_tables=[f"{production_dataset}.{table}"],
        destination_project_dataset_table=f"{backups_dataset}.{table}_{curr_date}",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )

    pop_descriptions = PythonOperator(
        task_id="populate_column_documentation_for_" + table,
        op_kwargs={
            "input_schema": f"{os.environ.get('DAGS_FOLDER')}/{schema_dir}/{table}.json",
            "table_name": f"{production_dataset}.{table}",
            "table_description": "Metadata containing CSET data augmentation applied to OpenAlex",
        },
        python_callable=update_table_descriptions,
    )

    export_metadata = BigQueryToGCSOperator(
        task_id="export_metadata",
        source_project_dataset_table=f"{staging_dataset}.metadata",
        destination_cloud_storage_uris=f"gs://{DATA_BUCKET}/{tmp_dir}/{production_dataset}/data*",
        export_format="NEWLINE_DELIMITED_JSON",
        force_rerun=True,
    )

    update_version = PythonOperator(
        task_id="update_version",
        python_callable=get_updated_version,
    )

    gce_instance_start = ComputeEngineStartInstanceOperator(
        task_id=f"start-{gce_resource_id}",
        project_id=PROJECT_ID,
        zone=GCP_ZONE,
        resource_id=gce_resource_id,
    )

    update_zenodo_sequence = [
        "sudo apt-get -y update",
        "sudo apt-get install -y zip curl",
        f"rm -r {production_dataset} || true",
        f"gsutil -m cp -r gs://{DATA_BUCKET}/{tmp_dir}/{production_dataset} .",
        f"gsutil -m cp -r gs://{DATA_BUCKET}/{production_dataset}/upload.py .",
        f"zip -r {production_dataset}.zip {production_dataset}",
        "python3 upload.py --version {{ task_instance.xcom_pull('update_version', key='return_value') }}",
    ]
    update_zenodo_script = " && ".join(update_zenodo_sequence)

    update_zenodo = BashOperator(
        task_id="update_zenodo",
        bash_command=f'gcloud compute ssh jm3312@{gce_resource_id} --zone {GCP_ZONE} --command "{update_zenodo_script}"',
    )

    gce_instance_stop = ComputeEngineStopInstanceOperator(
        task_id=f"stop-{gce_resource_id}",
        project_id=PROJECT_ID,
        zone=GCP_ZONE,
        resource_id=gce_resource_id,
    )

    msg_success = get_post_success("OpenAlex-CSET data updated!", dag)

    (
        clear_tmp_dir
        >> run_metadata
        >> checks
        >> push_to_production
        >> snapshot
        >> pop_descriptions
        >> export_metadata
        >> update_version
        >> gce_instance_start
        >> update_zenodo
        >> gce_instance_stop
        >> msg_success
    )
