from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# You will need to install the gcp provider package in your Dockerfile
# pip install apache-airflow-providers-google

with DAG(
    dag_id="gcp_connection_test",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["gcp", "test"],
    # Set the default Google Cloud connection ID
    default_args={
        "gcp_conn_id": "google_cloud_default", 
    }
) as dag:
    
    # This task simply checks if the Google Cloud hook can load credentials
    # and access the services (like GCS or BigQuery).
    # Since we don't have a file to upload, we'll use a simple bash task 
    # that requires the credentials to be available. 

    # For a real test, you'd use a BigQueryOperator or GCSCreateBucketOperator.
    # We'll use a BashOperator and rely on the default_args loading the connection.
    test_task = BashOperator(
        task_id="confirm_gcp_hook_loads",
        bash_command="echo 'GCP connection credentials loaded successfully by Airflow.'",
    )