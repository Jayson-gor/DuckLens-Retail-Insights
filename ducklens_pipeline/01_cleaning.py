"""
Stage 1: Data Cleaning
-----------------------
Clean raw data from staging.stg_sales_raw
Calculate unit_price = Total Sales / Quantity
Flag data quality issues (negatives, duplicates, missing RRP)
Standardize text (strip, title case)

Output: Cleaned Pandas DataFrame (not saved to DB yet)
"""

import pandas as pd
import psycopg2
import os
from datetime import datetime
import numpy as np


def load_from_staging():
    """Load raw data from staging.stg_sales_raw"""
    
    db_params = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5000'),
        'database': os.getenv('DB_NAME', 'ducklens_db'),
        'user': os.getenv('DB_USER', 'user'),
        'password': os.getenv('DB_PASSWORD', 'password')
    }
    
    print(f"[{datetime.now()}] Connecting to database...")
    conn = psycopg2.connect(**db_params)
    
    query = "SELECT * FROM staging.stg_sales_raw;"
    print(f"[{datetime.now()}] Loading data from staging...")
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"[{datetime.now()}] Loaded {len(df)} rows from staging")
    return df


def clean_data(df):
    """
    Clean and standardize the data
    
    Actions:
    1. Calculate unit_price = total_sales / quantity
    2. Standardize text fields (strip, title case)
    3. Flag data quality issues
    4. Remove duplicates
    """
    
    print(f"[{datetime.now()}] Starting data cleaning...")
    
    # Create a copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    # ------------------------------------------------------------------
    # 1. Calculate unit_price
    # ------------------------------------------------------------------
    print(f"[{datetime.now()}] Calculating unit_price...")
    
    # Avoid division by zero
    df['unit_price'] = np.where(
        df['quantity'] > 0,
        df['total_sales'] / df['quantity'],
        0
    )
    
    # ------------------------------------------------------------------
    # 2. Standardize text fields
    # ------------------------------------------------------------------
    print(f"[{datetime.now()}] Standardizing text fields...")
    
    text_columns = [
        'store_name', 'description', 'category', 
        'department', 'sub_department', 'section', 'supplier'
    ]
    
    for col in text_columns:
        if col in df.columns:
            # Strip whitespace and convert to title case
            df[col] = df[col].astype(str).str.strip().str.title()
    
    # Keep item_code and item_barcode as uppercase
    if 'item_code' in df.columns:
        df['item_code'] = df['item_code'].astype(str).str.strip().str.upper()
    if 'item_barcode' in df.columns:
        df['item_barcode'] = df['item_barcode'].astype(str).str.strip()
    
    # ------------------------------------------------------------------
    # 3. Flag data quality issues
    # ------------------------------------------------------------------
    print(f"[{datetime.now()}] Flagging data quality issues...")
    
    # Initialize quality flag
    df['data_quality_flag'] = 'high'
    
    # Flag: Negative quantity or total_sales
    negative_qty = df['quantity'] < 0
    negative_sales = df['total_sales'] < 0
    df.loc[negative_qty | negative_sales, 'data_quality_flag'] = 'low'
    
    print(f"  - Negative quantity: {negative_qty.sum()} rows")
    print(f"  - Negative sales: {negative_sales.sum()} rows")
    
    # Flag: Missing or zero RRP
    missing_rrp = (df['rrp'].isna()) | (df['rrp'] <= 0)
    df.loc[missing_rrp & (df['data_quality_flag'] == 'high'), 'data_quality_flag'] = 'medium'
    
    print(f"  - Missing/zero RRP: {missing_rrp.sum()} rows")
    
    # Flag: Unit price significantly different from RRP (>50% deviation)
    # This could indicate data entry errors
    rrp_deviation = np.abs(df['unit_price'] - df['rrp']) / df['rrp']
    high_deviation = (rrp_deviation > 0.5) & (df['rrp'] > 0)
    df.loc[high_deviation & (df['data_quality_flag'] == 'high'), 'data_quality_flag'] = 'medium'
    
    print(f"  - High RRP deviation (>50%): {high_deviation.sum()} rows")
    
    # Flag: Missing critical fields
    missing_store = df['store_name'].isna() | (df['store_name'] == '')
    missing_item = df['item_code'].isna() | (df['item_code'] == '')
    missing_date = df['date_of_sale'].isna()
    
    critical_missing = missing_store | missing_item | missing_date
    df.loc[critical_missing, 'data_quality_flag'] = 'low'
    
    print(f"  - Missing critical fields: {critical_missing.sum()} rows")
    
    # ------------------------------------------------------------------
    # 4. Remove duplicates
    # ------------------------------------------------------------------
    print(f"[{datetime.now()}] Checking for duplicates...")
    
    # Define duplicate keys: same store, item, date
    duplicate_cols = ['store_name', 'item_code', 'date_of_sale']
    
    initial_count = len(df)
    df = df.drop_duplicates(subset=duplicate_cols, keep='first')
    duplicates_removed = initial_count - len(df)
    
    print(f"  - Duplicates removed: {duplicates_removed}")
    
    # ------------------------------------------------------------------
    # 5. Data type conversions
    # ------------------------------------------------------------------
    print(f"[{datetime.now()}] Converting data types...")
    
    # Ensure numeric columns are float
    numeric_cols = ['quantity', 'total_sales', 'rrp', 'unit_price']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Ensure date is datetime
    df['date_of_sale'] = pd.to_datetime(df['date_of_sale'], errors='coerce')
    
    # ------------------------------------------------------------------
    # Summary statistics
    # ------------------------------------------------------------------
    print(f"\n[{datetime.now()}] Cleaning Summary:")
    print(f"  - Total rows: {len(df)}")
    print(f"  - High quality: {(df['data_quality_flag'] == 'high').sum()}")
    print(f"  - Medium quality: {(df['data_quality_flag'] == 'medium').sum()}")
    print(f"  - Low quality: {(df['data_quality_flag'] == 'low').sum()}")
    print(f"  - Date range: {df['date_of_sale'].min()} to {df['date_of_sale'].max()}")
    print(f"  - Unique stores: {df['store_name'].nunique()}")
    print(f"  - Unique items: {df['item_code'].nunique()}")
    print(f"  - Total sales value: ${df['total_sales'].sum():,.2f}")
    
    print(f"\n[{datetime.now()}] ✅ Data cleaning complete!")
    
    return df


def main():
    """Main cleaning pipeline"""
    
    print(f"\n{'='*70}")
    print(f"STAGE 1: DATA CLEANING")
    print(f"{'='*70}\n")
    
    # Load raw data
    df_raw = load_from_staging()
    
    # Clean data
    df_clean = clean_data(df_raw)
    
    # Return cleaned dataframe (will be used by next stage)
    return df_clean


if __name__ == "__main__":
    df_cleaned = main()
    print(f"\nCleaned data shape: {df_cleaned.shape}")
    print(f"Columns: {list(df_cleaned.columns)}")
