
import pendulum
from airflow.decorators import dag, task
from simulate_data import generate_multi_user_data_and_upload_raw_data
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
#from google.cloud.bigquery.schema import SchemaField
from simulate_data import generate_multi_user_data_and_upload_raw_data
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import PythonOperator
from clv_models import calculate_clv_score
from validate_features import validate_features_logic
from datetime import timedelta

# --- CONFIGURATION ---
GCP_CONN_ID = "gc-to-clv-pipeline-con"
GCS_BUCKET = "your-graw-data-bucket"
BQ_STAGING_TABLE = "ecom_staging_and_prod.transactions_staging" # The target table for the raw data

TRANSACTION_SCHEMA = [
    {"name": "CustomerID", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "Quantity", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "UnitPrice", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "order_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "TotalPurchase", "type": "FLOAT", "mode": "REQUIRED"},
]


# --- DAG Definition ---
@dag(
    dag_id="clv_data_generator_v2",
    # Set the starting date to sometime in the past
    start_date=pendulum.datetime(2025, 10, 1, tz="UTC"),
    # Set the schedule to None for manual runs, or "0 0 * * *" for daily midnight
    schedule=None, 
    catchup=False,
    tags=["clv", "raw_data", "gcp"],
    #default_args={
    #    'retries': 2,
    #    'retry_delay': timedelta(minutes=5),
    #    'retry_exponential_backoff': True
    #}
)
def clv_data_pipeline():
    """
    DAG to generate synthetic customer transaction data and upload it to GCS.
    """
    
    # Task 1: Generate Data & Upload to GCS
    @task(task_id="generate_and_upload_raw_data")
    def generate_and_upload():        
        #generate_multi_user_data_and_upload_raw_data()
        return generate_multi_user_data_and_upload_raw_data()
    
    # Task 1: Runs Python script to generate data and returns GCS path    
    gcs_object_path = generate_and_upload()
    
    # Task 2: Loads the GCS file (returned from Task 1) into BigQuery
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq_staging",
        # Source (GCS) Configuration
        bucket=GCS_BUCKET,
        # **XCom PULL:** Uses the output (GCS path) from the previous TaskFlow task
        source_objects=[gcs_object_path], 
        # Destination (BigQuery) Configuration
        destination_project_dataset_table=BQ_STAGING_TABLE,
        schema_fields=TRANSACTION_SCHEMA,
        # Load Job Configuration
        write_disposition="WRITE_APPEND", # Append the new data to the table
        create_disposition="CREATE_IF_NEEDED", # Create table/dataset if it doesn't exist
        skip_leading_rows=1, # Skip the CSV header row
        source_format="CSV",
        field_delimiter=",",
        autodetect=False,
        gcp_conn_id=GCP_CONN_ID,
    )
    # Task 3: Transform raw transactions into Customer Features
    transform_data = BigQueryExecuteQueryOperator(
        task_id="transform_to_customer_features",
        sql="""
            CREATE OR REPLACE TABLE `clv-pipeline.ecom_staging_and_prod.customer_features` AS
            SELECT
                CustomerID AS customer_id,
                DATE_DIFF(DATE(MAX(order_timestamp)), DATE(MIN(order_timestamp)), DAY) AS recency,
                DATE_DIFF(CURRENT_DATE(), DATE(MIN(order_timestamp)), DAY) AS T,
                COUNT(DISTINCT DATE(order_timestamp)) - 1 AS frequency,
                AVG(TotalPurchase) AS monetary_value,
                MIN(order_timestamp) AS first_purchase,
                MAX(order_timestamp) AS last_purchase
            FROM
                `clv-pipeline.ecom_staging_and_prod.transactions_staging`
            GROUP BY
                CustomerID
        """,
        use_legacy_sql=False,
        gcp_conn_id=GCP_CONN_ID,
        )
        
    # gatekeep task
    @task(task_id="validate_features_step")
    def run_validation():
        return validate_features_logic(GCP_CONN_ID)

    validation_step = run_validation()
        
    # Task 4: Run CLV Prediction in a Virtual Environment
    predict_clv = PythonOperator(
        task_id="predict_clv_scores",
        python_callable=calculate_clv_score,
        # This task will now "just work" because lifetimes is in the Docker image
)
       
    
    # --- SET DEPENDENCIES ---
    # The GCS path is passed implicitly, but we set the execution dependency explicitly
    gcs_object_path >> load_gcs_to_bq >> transform_data >> validation_step >> predict_clv
    
    
# Instantiate the DAG
clv_data_pipeline()