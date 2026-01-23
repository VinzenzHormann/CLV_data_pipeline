import pandas as pd
import numpy as np
import os
import pendulum
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud.bigquery.schema import SchemaField
from airflow.exceptions import AirflowException



# --- CONFIGURATION (Must match DAG settings) ---
FILE_NAME = "customer_transactions_" + pendulum.now('UTC').strftime("%Y%m%d_%H%M%S") + ".csv"
LOCAL_PATH = f"/opt/airflow/data/{FILE_NAME}" # Needs the mounted ./data volume
GCS_BUCKET = "your-graw-data-bucket" # <-- Your bucket name
GCS_OBJECT = f"raw_data/{FILE_NAME}"
GCP_CONN_ID = "gc-to-clv-pipeline-con"
END_TIME = pendulum.now('UTC')
START_TIME = END_TIME - pendulum.duration(days=1)
NEW_USERS_DAILY= 10 # Number of brand new customers to onboard
RETURNING_USERS_DAILY = 200 # 490 returning users
BQ_METADATA_TABLE = "metadata.master_users" 



def get_max_customer_id(gcp_conn_id):
    """Fetches the maximum existing CustomerID from BigQuery."""
    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id, location="EU")
    #project_id = bq_hook.project_id or "clv-pipeline" 
    query = f"SELECT MAX(CustomerID) FROM {BQ_METADATA_TABLE}" 
    # Run query and fetch result
    #max_id = bq_hook.get_first(sql=query)
    
    results = bq_hook.run(
        sql=query, 
        handler=lambda cursor: cursor.fetchone()
    )
    # The result is a tuple, e.g., (5000,). If table is empty, max_id might be (None,)
    max_id = results[0] if results and results[0] is not None else 0
    #return max_id[0] if max_id and max_id[0] is not None else 0
    
    return max_id

def save_new_customer_ids(new_ids, gcp_conn_id):
     # 1. Initialize the Hook with the necessary location fix
    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id, location="EU") 
    
    # 2. Get the BigQuery client and project details
    client = bq_hook.get_client()
    project_id = bq_hook.project_id
    
    # BQ_METADATA_TABLE is assumed to be "metadata.master_users"
    dataset_id, table_name = BQ_METADATA_TABLE.split('.')
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    
    MASTER_USERS_SCHEMA = [
        SchemaField('CustomerID', 'INTEGER', mode='REQUIRED', description='Unique customer identifier'),
    ]
    
    try:
        # Check if the table exists (the BQ Hook handles the project_id)
        if not bq_hook.table_exists(dataset_id=dataset_id, table_id=table_name, project_id=project_id):
            print(f"Table {table_id} does not exist. Creating table...")
            
            # Create the table using the defined schema
            bq_hook.create_empty_table(
                dataset_id=dataset_id,
                table_id=table_name,
                schema_fields=MASTER_USERS_SCHEMA,
                location="EU", # Use the same location as the Hook init
                project_id=project_id,
                exists_ok=True # Ensure it doesn't fail if concurrent runs try to create it
            )
            print(f"Table {table_id} created successfully.")

    except Exception as e:
        # Catch any failure during the check or creation phase
        print(f"FATAL ERROR during table check/creation: {e}")
        raise AirflowException(f"Failed to ensure BigQuery table existence: {e}")
    
    # 3. Format the data for insert_rows_json (BigQuery Streaming API expects a list of dictionaries)
    rows_to_insert = [{'CustomerID': int(id)} for id in new_ids]
    
    # 4. Use the fully implemented Google Cloud SDK method for streaming insert
    errors = client.insert_rows_json(
        table=table_id,
        json_rows=rows_to_insert,
    )
    
    if errors:
        print(f"Encountered errors while inserting rows: {errors}")
        # Consider uncommenting this to fail the task if data insertion fails
        # raise AirflowException(f"Failed to insert new customer IDs: {errors}")
    else:
        print(f"Added {len(new_ids)} new users to the master list in BigQuery.")

def get_daily_customer_ids(current_max_id, new_users_count, returning_users_count):
    """
    Selects a random subset of existing (returning) customers and generates new ones.

    Args:
        current_max_id (int): The highest existing CustomerID from BigQuery.
        new_users_count (int): The number of brand new users to create.
        returning_users_count (int): The number of existing users to randomly select.

    Returns:
        tuple: (daily_customer_ids, new_customer_ids)
    """

    # --- 1. Select Returning Customers ---

    # If the max ID is 0, we can't select returning users yet.
    if current_max_id == 0:
        returning_ids = np.array([])
        # All users for today must be "new" until the pool is established
        users_needed = new_users_count + returning_users_count 
        #users_needed = new_users_count + returning_users_count
        returning_users_count = 0
    else:
        # Create a list of all IDs currently in BigQuery (from 1 up to the max)
        # We assume the user pool is dense (no large gaps) for simplicity.
        active_pool = np.arange(1, current_max_id + 1)

        # Determine how many returning users we *can* select (don't exceed the active pool size)
        max_possible_returns = min(returning_users_count, len(active_pool))

        # Randomly select the desired number of returning users (no duplicates: replace=False)
        returning_ids = np.random.choice(
            active_pool,
            size=max_possible_returns,
            replace=False
        )

        # Adjust new users count if we couldn't meet the returning quota (rare, only on startup)
        users_needed = new_users_count + (returning_users_count - max_possible_returns)


    # --- 2. Generate New Customers ---

    start_new_id = current_max_id + 1
    end_new_id = start_new_id + users_needed

    # Generate the sequential list of brand new Customer IDs
    new_ids = np.arange(start_new_id, end_new_id)

    # --- 3. Combine and Finalize ---

    # Combine the returning and new IDs for today's batch
    daily_ids = np.concatenate([returning_ids, new_ids])

    # Shuffle the final list so the new users aren't all at the end
    np.random.shuffle(daily_ids)

    # Return both the full list for transaction generation and the new IDs for persistence
    return daily_ids.tolist(), new_ids.tolist()


# Assuming your original generation function is named 'generate_transaction_data'
def generate_single_user_data(customer_id):
    """Generates synthetic transaction data for a single customer."""

    # 1. Simulate a random number of purchases for this user
    num_purchases = np.random.randint(1, 15)

    # 2. Generate random prices and quantities
    price = np.round(np.random.uniform(5.0, 100.0, num_purchases), 2)
    quantity = np.random.randint(1, 5, num_purchases)

    # --- REVISED TIMESTAMP GENERATION ---

    # 1. Generate a single, random 'Session Start Time' within the 24-hour window
    time_difference_seconds = (END_TIME - START_TIME).total_seconds()

    # Pick one random second offset for the FIRST purchase (the session start)
    session_start_offset = np.random.uniform(0, time_difference_seconds, 1)[0]
    session_start_ts = pd.to_datetime(START_TIME) + pd.to_timedelta(session_start_offset, unit='s')

    if num_purchases > 1:
      # Generate sequential offsets in seconds (e.g., 60s to 900s)
      small_offsets_seconds = np.cumsum(np.random.randint(20, 100, num_purchases - 1))

    # Add the initial 0 offset for the first purchase
      all_offsets_seconds = np.insert(small_offsets_seconds, 0, 0)
    else:
      all_offsets_seconds = np.array([0])

    # 3. Calculate the final timestamps
    time_deltas = pd.to_timedelta(all_offsets_seconds, unit='s')


    timestamps = session_start_ts + time_deltas


    # 4. Create the DataFrame for this user
    df = pd.DataFrame({
        'CustomerID': customer_id,
        'Quantity': quantity,
        'UnitPrice': price,
        "order_timestamp": timestamps,

    })

    # Calculate TotalPurchase for CLV
    df['TotalPurchase'] = df['Quantity'] * df['UnitPrice']

    return df


def generate_multi_user_data_and_upload_raw_data():
    """Generates synthetic customer data and uploads it to Google Cloud Storage."""

    # A list to hold the transaction data for all users
    all_users_data = []

    # --- Persistence Logic ---
    current_max_id = get_max_customer_id(GCP_CONN_ID)

    daily_ids, new_ids = get_daily_customer_ids(current_max_id, NEW_USERS_DAILY, RETURNING_USERS_DAILY )

    for i in daily_ids:
        # Generate data for user ID 'i'
        user_df = generate_single_user_data(customer_id=i)

        # Add the single-user DataFrame to the list
        all_users_data.append(user_df)


    final_df = pd.concat(all_users_data, ignore_index=True)
    final_df

    # Save the complete dataset
    os.makedirs(os.path.dirname(LOCAL_PATH), exist_ok=True)
    final_df.to_csv(LOCAL_PATH, index=False)

    # Optional verification print (to check the number of unique customers)
    print(f"Successfully generated data for {final_df['CustomerID'].nunique()} unique customers.")
    print(f"Total transactions generated: {len(final_df)}")


    # UPLOAD TO GCS (Simulated Load to Staging)
    print(f"Uploading data to gs://{GCS_BUCKET}/{GCS_OBJECT}...")
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    # Perform the upload
    gcs_hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=GCS_OBJECT,
        filename=LOCAL_PATH,
        mime_type='text/csv'
    )


    if new_ids:
      save_new_customer_ids(new_ids, GCP_CONN_ID)

    print("Upload successful.")

    # Pass the GCS object path to the next task using XCom
    return GCS_OBJECT