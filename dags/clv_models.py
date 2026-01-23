import pandas as pd
import numpy as np
from lifetimes import BetaGeoFitter, GammaGammaFitter
import logging
 
def get_data() :  
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    logging.info(f"Setting up Bigquery hook and client")
    hook = BigQueryHook(gcp_conn_id='gc-to-clv-pipeline-con')
    client = hook.get_client()
    
    logging.info(f"Loading data")
    # 1. Get Data
    df = client.query("SELECT * FROM `clv-pipeline.ecom_staging_and_prod.customer_features`").to_dataframe()
    df.columns = df.columns.str.lower()
    if 'monetary_value' in df.columns:
        df = df.rename(columns={'monetary_value': 'monetary'})       

    return df, client
    
def apply_data_quality_fixes(df):
   #DEF: Check if calculated clv is negative
    # get count of negative values, clip them to 0, print the amount of cliped negative values
    df['negatif_clv_flag'] = np.where(df['clv'] < 0, 1, 0)
    df['clv'] = df['clv'].clip(lower=0)

    fix_count = df['negatif_clv_flag'].sum()
    if fix_count > 0:
      logging.info(f"QUALITY CHECK: {fix_count} negative CLV values detected and floored to 0.")

    #DEF: Check for positif autliners
    df['outliners_flag'] = np.where(df['clv'] > 1000000, 1, 0)
    outliners_count = df['outliners_flag'].sum()
    if outliners_count > 0:
      logging.info(f"QUALITY CHECK: {outliners_count} outliers detected and flagged.")
    
    return df
 
def run_clv_logic(df):
    # DEF: check if df is not empty
    
    if df is None or df.empty:
        logging.error("The input DataFrame is empty. Cannot fit models.")
        raise ValueError("INPUT ERROR: Dataframe is empty")

    # DEF: check if df has all expected columns
    expected_columns = ['customer_id', 'recency', 't', 'frequency', 'monetary', 'first_purchase', 'last_purchase']
    if list(df.columns) != expected_columns:
      raise ValueError(f"Bad Schema! Expected {expected_columns}, got {list(df.columns)}")
    logging.info(f"data passed expected colum test, preparing data for models, fitting models, making clv caluclations") 

    # 2. Filter & Prep
    returning_customers = df[(df['frequency'] > 0) & (df['monetary'] > 0)].copy()
    
    # Force to standard float64 arrays for the "Fit" stage
    f = returning_customers['frequency'].values.astype('float64')
    r = returning_customers['recency'].values.astype('float64')
    t = returning_customers['t'].values.astype('float64')
    m = returning_customers['monetary'].values.astype('float64')

    # 3. Fit Models (Higher penalizer for stability)
    bgf = BetaGeoFitter(penalizer_coef=0.1)
    bgf.fit(f, r, t)
    
    ggf = GammaGammaFitter(penalizer_coef=0.1)
    ggf.fit(f, m)

    # 4. Predictions (Individual steps to avoid the 'exp' bug)
    # We use the internal .params instead of the helper to stay safe
    returning_customers['predicted_purchases'] = bgf.predict(30, f, r, t)
    returning_customers['predicted_avg_value'] = ggf.conditional_expected_average_profit(f, m)

    # 5. Manual CLV Calculation (Bypassing the buggy lifetimes helper)
    # CLV = Expected Purchases (12 months) * Expected Value * Discount Factor
    # We use predict(365 days) for a 1-year view
    expected_purchases_1yr = bgf.predict(365, f, r, t)
    returning_customers['clv'] = expected_purchases_1yr * returning_customers['predicted_avg_value'] * 0.99
    
    logging.info(f"CLV calculations finished. Starting data quality checks negative CLV, Outliners CLV")
    returning_customers = apply_data_quality_fixes(returning_customers)
    
    logging.info(f"SUCCESS: Processed {len(returning_customers)} customers. Average CLV: ${returning_customers['clv'].mean():.2f}")
    
    return returning_customers    

    
    
def save_data(df, client):
    from google.cloud import bigquery
    logging.info("Starting with upload")    
    
    # 6. Cleanup & Upload
    output_df = df[['customer_id', 'predicted_purchases', 'predicted_avg_value', 'clv', 'negatif_clv_flag', 'outliners_flag' ]]
    
    table_id = "clv-pipeline.ecom_staging_and_prod.predicted_clv"
    client.load_table_from_dataframe(output_df, table_id, 
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")).result()       
    
    
def calculate_clv_score():
    # A. Get Data
    raw_df, bq_client = get_data()
    
    # B. Run Logic
    processed_df = run_clv_logic(raw_df)
    
    # C. Save Data
    if not processed_df.empty:
        # Select only needed columns for upload
        
        save_data(processed_df, bq_client)
        logging.info("Pipeline Finished Successfully.")