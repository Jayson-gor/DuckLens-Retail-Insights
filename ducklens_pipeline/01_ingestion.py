"""
Stage 0: Data Ingestion
-----------------------
Load raw data from Test_Data.xlsx into staging.stg_sales_raw

This script acts as the initial data loader, similar to Airbyte.
It reads the Excel file and dumps it as-is into the staging table.
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
from datetime import datetime


def load_excel_to_staging():
    """Load Test_Data.xlsx into staging.stg_sales_raw"""
    
    # Database connection parameters
    db_params = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5000'),
        'database': os.getenv('DB_NAME', 'ducklens_db'),
        'user': os.getenv('DB_USER', 'user'),
        'password': os.getenv('DB_PASSWORD', 'password')
    }
    
    print(f"[{datetime.now()}] Starting data ingestion...")
    
    # Read Excel file
    excel_path = '/opt/airflow/Test_Data.xlsx'
    print(f"[{datetime.now()}] Reading Excel file: {excel_path}")
    
    try:
        df = pd.read_excel(excel_path, engine='openpyxl')
        print(f"[{datetime.now()}] Loaded {len(df)} rows from Excel")
        print(f"[{datetime.now()}] Columns: {list(df.columns)}")
    except Exception as e:
        print(f"[{datetime.now()}] ERROR: Failed to read Excel file: {e}")
        raise
    
    # Normalize column names to match database schema
    column_mapping = {
        'Store Name': 'store_name',
        'Item_Code': 'item_code',
        'Item Barcode': 'item_barcode',
        'Description': 'description',
        'Category': 'category',
        'Department': 'department',
        'Sub-Department': 'sub_department',
        'Section': 'section',
        'Quantity': 'quantity',
        'Total Sales': 'total_sales',
        'RRP': 'rrp',
        'Supplier': 'supplier',
        'Date Of Sale': 'date_of_sale'
    }
    
    df = df.rename(columns=column_mapping)
    
    # Convert date column to datetime
    df['date_of_sale'] = pd.to_datetime(df['date_of_sale'], errors='coerce')
    
    # Connect to database
    print(f"[{datetime.now()}] Connecting to database...")
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        print(f"[{datetime.now()}] Connected successfully")
    except Exception as e:
        print(f"[{datetime.now()}] ERROR: Failed to connect to database: {e}")
        raise
    
    # Clear existing data in staging table
    print(f"[{datetime.now()}] Clearing staging table...")
    cursor.execute("TRUNCATE TABLE staging.stg_sales_raw;")
    conn.commit()
    
    # Prepare data for insertion
    columns = list(df.columns)
    values = [tuple(row) for row in df.values]
    
    # Insert data using execute_values for better performance
    print(f"[{datetime.now()}] Inserting {len(values)} rows into staging.stg_sales_raw...")
    
    insert_query = f"""
        INSERT INTO staging.stg_sales_raw ({', '.join(columns)})
        VALUES %s
    """
    
    try:
        execute_values(cursor, insert_query, values, page_size=1000)
        conn.commit()
        print(f"[{datetime.now()}] ✅ Successfully inserted {len(values)} rows")
    except Exception as e:
        conn.rollback()
        print(f"[{datetime.now()}] ERROR: Failed to insert data: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
    
    # Verify data
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM staging.stg_sales_raw;")
    count = cursor.fetchone()[0]
    print(f"[{datetime.now()}] Verification: {count} rows in staging.stg_sales_raw")
    cursor.close()
    conn.close()
    
    print(f"[{datetime.now()}] ✅ Ingestion complete!")
    return df


if __name__ == "__main__":
    load_excel_to_staging()
