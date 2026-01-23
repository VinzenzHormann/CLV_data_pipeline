import logging

def run_validation_checks(raw_c, feat_c, invalid_count, actual_cols):
    """
    THE BRAIN: No BigQuery here. Just logic.
    This is what we will test in Pytest.
    """
    # 1. Ratio Check
    ratio = feat_c / raw_c if raw_c > 0 else 0
    
    logging.info(f"Validation: Raw Unique Customers: {raw_c}, Feature Unique Customers: {feat_c} (Ratio: {ratio:.2f})")
    if ratio < 0.95:
        raise ValueError(f"DATA LOSS: {ratio:.2%} migrated.")

    # 2. Schema Check
    required_cols = {'customer_id', 'recency', 'T', 'frequency', 'monetary_value', 'first_purchase', 'last_purchase'}
    missing = required_cols - set(actual_cols)
    if missing:
        raise ValueError(f"SCHEMA ERROR: Missing {missing}")
    logging.info("Validation Passed: Schema and Record Count are healthy.")
    
    # 3. Sanity Check
    if invalid_count > 0:
        raise ValueError(f"SANITY ERROR: {invalid_count} negative rows found.")
    
    return True


def validate_features_logic(gcp_conn_id):
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    """
    THE ARMS & LEGS: Gets data from BQ, then calls the Brain.
    """
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    client = hook.get_client()
    
       # 1. Record Count Check (Unique Customer Ratio)
    query = """
        SELECT 
            (SELECT COUNT(DISTINCT CustomerID) FROM `clv-pipeline.ecom_staging_and_prod.transactions_staging`) as raw_count,
            (SELECT COUNT(DISTINCT customer_id) FROM `clv-pipeline.ecom_staging_and_prod.customer_features`) as feature_count
    """
    results = client.query(query).to_dataframe()
    raw_c = results['raw_count'][0]
    feat_c = results['feature_count'][0]
    
    sanity_query = """
        SELECT COUNT(*) as invalid_rows
        FROM `clv-pipeline.ecom_staging_and_prod.customer_features`
        WHERE T < 0 
           OR frequency < 0 
           OR recency < 0 
           OR monetary_value < 0
    """
    sanity_results = client.query(sanity_query).to_dataframe()
    invalid_count = sanity_results['invalid_rows'][0]
    
    table = client.get_table("clv-pipeline.ecom_staging_and_prod.customer_features")
    actual_cols = {field.name for field in table.schema}
    
    
    run_validation_checks(
        raw_c, 
        feat_c, 
        invalid_count,
        actual_cols
    )
    logging.info("Validation Passed: All model features are positive and healthy.")
    return "Validation Success"


## OLD CODE
# def validate_features_logic(gcp_conn_id):
    # hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    # client = hook.get_client()

    # # 1. Record Count Check (Unique Customer Ratio)
    # query = """
        # SELECT 
            # (SELECT COUNT(DISTINCT CustomerID) FROM `clv-pipeline.ecom_staging_and_prod.transactions_staging`) as raw_count,
            # (SELECT COUNT(DISTINCT customer_id) FROM `clv-pipeline.ecom_staging_and_prod.customer_features`) as feature_count
    # """
    # results = client.query(query).to_dataframe()
    # raw_c = results['raw_count'][0]
    # feat_c = results['feature_count'][0]
    
    # ratio = feat_c / raw_c if raw_c > 0 else 0
    # logging.info(f"Validation: Raw Unique Customers: {raw_c}, Feature Unique Customers: {feat_c} (Ratio: {ratio:.2f})")

    # if ratio < 0.95: 
        # raise ValueError(f"DATA LOSS DETECTED! Only {ratio:.2%} of customers migrated to features table.")

    # # 2. Schema Check
    # required_cols = {'customer_id', 'recency', 'T', 'frequency', 'monetary_value', 'first_purchase', 'last_purchase'}
    # table = client.get_table("clv-pipeline.ecom_staging_and_prod.customer_features")
    # actual_cols = {field.name for field in table.schema}

    # missing = required_cols - actual_cols
    # if missing:
        # raise ValueError(f"SCHEMA ERROR: Missing columns needed for model: {missing}")
        
    # logging.info("Validation Passed: Schema and Record Count are healthy.")
    
    # # 3. Numerical Sanity Check: Ensure all model inputs are positive
    # # We count how many rows violate our rules
    # sanity_query = """
        # SELECT COUNT(*) as invalid_rows
        # FROM `clv-pipeline.ecom_staging_and_prod.customer_features`
        # WHERE T < 0 
           # OR frequency < 0 
           # OR recency < 0 
           # OR monetary_value < 0
    # """
    # sanity_results = client.query(sanity_query).to_dataframe()
    # invalid_count = sanity_results['invalid_rows'][0]

    # if invalid_count > 0:
        # logging.error(f"SANITY ERROR: Found {invalid_count} rows with negative values in model columns!")
        # raise ValueError(f"CRITICAL DATA ERROR: {invalid_count} rows contain negative T, Frequency, Recency, or Monetary values.")

    # logging.info("Validation Passed: All model features are positive and healthy.")
    # return "Validation Success"
