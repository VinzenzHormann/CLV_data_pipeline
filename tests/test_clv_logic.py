import pandas as pd
import pytest
import sys
import os

# 1. MANUALLY TELL PYTHON WHERE DAGS ARE
# This gets the absolute path to your project folder
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dags_path = os.path.join(project_root, 'dags')
sys.path.insert(0, dags_path)

from clv_models import run_clv_logic, apply_data_quality_fixes
from validate_features import validate_features_logic, run_validation_checks


def test_clv_happy_path():
    """
    Test 1: Does a normal customer result in a valid CLV?
    """
    # Create 3 rows for different dummy customers (diverse data for models)
    mock_data = pd.DataFrame({
        'customer_id': [1, 2, 3],
        'recency': [100, 110, 120],
        't': [150, 160, 170],
        'frequency': [2, 3, 4],
        'monetary': [50.0, 60.0, 70.0],
        'first_purchase': ['2025-01-01', '2025-01-02', '2025-01-03'],
        'last_purchase': ['2025-03-01', '2025-03-02', '2025-03-03']
    })

    # 3. USE THE FUNCTION
    output = run_clv_logic(mock_data)

    assert not output.empty
    assert 'clv' in output.columns
    print("✅ Happy Path Test Passed!")


def test_missing_column_error():
    """
    Test 2: Does the function raise a ValueError if 'monetary' is missing?
    """
    # Create data but REMOVE the 'monetary' column
    bad_data = pd.DataFrame({
        'customer_id': [1, 2],
        'recency': [10, 20],
        't': [100, 100],
        'frequency': [1, 2]
        # 'monetary' is missing!
    })

    # We use pytest.raises to check if the code correctly "screams" when data is bad
    with pytest.raises(ValueError) as excinfo:
        run_clv_logic(bad_data)
    
    assert "Bad Schema" in str(excinfo.value)
    print("\n✅ Missing Column Test Passed!")


def test_negative_clv_clipping_authentic():
    """
    TESTS THE REAL PRODUCTION LOGIC:
    Checks if apply_data_quality_fixes handles negatives correctly.
    """
    # 1. Setup 'Dirty' Data
    dirty_data = pd.DataFrame({
        'customer_id': [1, 2],
        'clv': [-100.0, 2000000.0] # One negative, one outlier
    })

    # 2. Run the ACTUAL production function
    cleaned_data = apply_data_quality_fixes(dirty_data)

    # 3. Assertions (Does the real code work?)
    # Check Negative fix
    assert cleaned_data.loc[cleaned_data['customer_id'] == 1, 'clv'].values[0] == 0
    assert cleaned_data.loc[cleaned_data['customer_id'] == 1, 'negatif_clv_flag'].values[0] == 1
    
    # Check Outlier flag
    assert cleaned_data.loc[cleaned_data['customer_id'] == 2, 'outliners_flag'].values[0] == 1
    
    print("✅ Authentic Quality Fixes Test Passed!")


def test_empty_df_as_input():
    """
    TESTS the code for a empty df as input
    """
    empty_df = pd.DataFrame()

    with pytest.raises(ValueError) as excinfo:
        run_clv_logic(empty_df)
    
    assert "Dataframe is empty" in str(excinfo.value)
    print("\n✅ Empty Data Test Passed!")

def test_validation_fails_on_data_loss():
    # Scenario: 100 raw customers, but only 80 in features (80% ratio)
    with pytest.raises(ValueError) as excinfo:
        run_validation_checks(raw_c=100, feat_c=80, invalid_count=0, actual_cols=['customer_id', 'recency', 'T', 'frequency', 'monetary_value', 'first_purchase', 'last_purchase'])
    
    assert "DATA LOSS" in str(excinfo.value)
    print("✅ validation_checks Data Loss Passed!")

def test_validation_fails_on_negative_values():
    # Scenario: 5 rows have negative values
    with pytest.raises(ValueError) as excinfo:
        run_validation_checks(raw_c=100, feat_c=100, invalid_count=5, actual_cols=['customer_id', 'recency', 'T', 'frequency', 'monetary_value', 'first_purchase', 'last_purchase'])
    
    assert "SANITY ERROR" in str(excinfo.value)
    print("✅ validation_checks Negative Value Test Passed!")

def test_validation_fails_missing_columns():
    # Scenario: 5 rows have negative values
    with pytest.raises(ValueError) as excinfo:
        run_validation_checks(raw_c=100, feat_c=100, invalid_count=0, actual_cols=['customer_id', 'recency', 'T', 'monetary_value', 'first_purchase', 'last_purchase'])
    
    assert "SCHEMA ERROR" in str(excinfo.value)
    print("✅ validation_checks Missing Columns Passed!")

