"""
DuckLens Pipeline - Stage 3: Data Loading
Loads transformed data into star schema (dimension tables + fact table)
"""

import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# Database connection parameters
DB_HOST = os.getenv('DB_HOST', 'db')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'ducklens_db')
DB_USER = os.getenv('DB_USER', 'user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')


def get_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def load_dimension_stores(df, conn):
    """Load/update store dimension"""
    print("[{}] Loading dimension: dim_store...".format(datetime.now()))
    
    # Get unique stores
    stores = df[['store_name']].drop_duplicates()
    
    cursor = conn.cursor()
    
    # Upsert stores (SCD Type 1 - overwrite)
    for _, row in stores.iterrows():
        cursor.execute("""
            INSERT INTO dw.dim_store (store_name)
            VALUES (%s)
            ON CONFLICT (store_name) DO NOTHING
        """, (row['store_name'],))
    
    conn.commit()
    cursor.close()
    
    print(f"  - Loaded {len(stores)} stores")


def load_dimension_suppliers(df, conn):
    """Load/update supplier dimension"""
    print("[{}] Loading dimension: dim_supplier...".format(datetime.now()))
    
    # Get unique suppliers
    suppliers = df[['supplier']].drop_duplicates()
    
    cursor = conn.cursor()
    
    # Upsert suppliers
    for _, row in suppliers.iterrows():
        cursor.execute("""
            INSERT INTO dw.dim_supplier (supplier_name)
            VALUES (%s)
            ON CONFLICT (supplier_name) DO NOTHING
        """, (row['supplier'],))
    
    conn.commit()
    cursor.close()
    
    print(f"  - Loaded {len(suppliers)} suppliers")


def load_dimension_dates(df, conn):
    """Load/update date dimension"""
    print("[{}] Loading dimension: dim_date...".format(datetime.now()))
    
    # Get unique dates
    dates = df[['date_of_sale']].drop_duplicates()
    dates['date_of_sale'] = pd.to_datetime(dates['date_of_sale'])
    
    cursor = conn.cursor()
    
    # Upsert dates with computed fields
    for _, row in dates.iterrows():
        date_val = row['date_of_sale']
        date_id = int(date_val.strftime('%Y%m%d'))
        cursor.execute("""
            INSERT INTO dw.dim_date (
                date_id, full_date, year, month, day, weekday_name, is_weekend
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_id) DO NOTHING
        """, (
            date_id,
            date_val.date(),
            date_val.year,
            date_val.month,
            date_val.day,
            date_val.strftime('%A'),
            date_val.weekday() >= 5  # Saturday=5, Sunday=6
        ))
    
    conn.commit()
    cursor.close()
    
    print(f"  - Loaded {len(dates)} dates")


def load_dimension_items(df, conn):
    """Load/update item dimension"""
    print("[{}] Loading dimension: dim_item...".format(datetime.now()))
    
    # Get unique items with all attributes
    items = df[[
        'item_code', 'item_barcode', 'description', 'category',
        'department', 'sub_department', 'section', 'is_bidco'
    ]].drop_duplicates(subset=['item_code'])
    
    cursor = conn.cursor()
    
    # Upsert items (note: schema doesn't have UNIQUE constraint on item_code, so just insert)
    for _, row in items.iterrows():
        # Check if item_code already exists
        cursor.execute("SELECT item_id FROM dw.dim_item WHERE item_code = %s", (row['item_code'],))
        existing = cursor.fetchone()
        
        if existing is None:
            cursor.execute("""
                INSERT INTO dw.dim_item (
                    item_code, item_barcode, description, category,
                    department, sub_department, section, is_bidco
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['item_code'],
                row['item_barcode'],
                row['description'],
                row['category'],
                row['department'],
                row['sub_department'],
                row['section'],
                row['is_bidco']
            ))
    
    conn.commit()
    cursor.close()
    
    print(f"  - Loaded {len(items)} items")


def load_fact_sales(df, conn):
    """Load fact table with foreign key lookups"""
    print("[{}] Loading fact table: fact_sales_enriched...".format(datetime.now()))
    
    cursor = conn.cursor()
    
    # Get foreign key mappings
    print("  - Fetching foreign key mappings...")
    
    # Store mapping
    cursor.execute("SELECT store_id, store_name FROM dw.dim_store")
    store_map = {row[1]: row[0] for row in cursor.fetchall()}
    
    # Supplier mapping
    cursor.execute("SELECT supplier_id, supplier_name FROM dw.dim_supplier")
    supplier_map = {row[1]: row[0] for row in cursor.fetchall()}
    
    # Date mapping
    cursor.execute("SELECT date_id, TO_CHAR(full_date, 'YYYYMMDD') as date_str FROM dw.dim_date")
    date_map = {row[1]: row[0] for row in cursor.fetchall()}
    
    # Item mapping
    cursor.execute("SELECT item_id, item_code FROM dw.dim_item")
    item_map = {row[1]: row[0] for row in cursor.fetchall()}
    
    print(f"  - Store IDs: {len(store_map)}")
    print(f"  - Supplier IDs: {len(supplier_map)}")
    print(f"  - Date IDs: {len(date_map)}")
    print(f"  - Item IDs: {len(item_map)}")
    
    # Prepare fact records
    print("  - Preparing fact records...")
    df['date_of_sale'] = pd.to_datetime(df['date_of_sale'])
    df['date_id_str'] = df['date_of_sale'].dt.strftime('%Y%m%d')
    
    df['store_fk'] = df['store_name'].map(store_map)
    df['supplier_fk'] = df['supplier'].map(supplier_map)
    df['date_fk'] = df['date_id_str'].map(date_map)
    df['item_fk'] = df['item_code'].map(item_map)
    
    # Check for missing FKs
    missing_stores = df['store_fk'].isna().sum()
    missing_suppliers = df['supplier_fk'].isna().sum()
    missing_dates = df['date_fk'].isna().sum()
    missing_items = df['item_fk'].isna().sum()
    
    if missing_stores > 0:
        print(f"  ⚠️  Warning: {missing_stores} rows with missing store FK")
    if missing_suppliers > 0:
        print(f"  ⚠️  Warning: {missing_suppliers} rows with missing supplier FK")
    if missing_dates > 0:
        print(f"  ⚠️  Warning: {missing_dates} rows with missing date FK")
    if missing_items > 0:
        print(f"  ⚠️  Warning: {missing_items} rows with missing item FK")
    
    # Drop rows with missing FKs
    df_valid = df.dropna(subset=['store_fk', 'supplier_fk', 'date_fk', 'item_fk'])
    
    if len(df_valid) < len(df):
        print(f"  - Dropped {len(df) - len(df_valid)} rows with missing foreign keys")
    
    # Insert fact records
    print("  - Inserting fact records...")
    fact_records = []
    for _, row in df_valid.iterrows():
        fact_records.append((
            int(row['store_fk']),
            int(row['item_fk']),
            int(row['supplier_fk']),
            int(row['date_fk']),
            int(row['quantity']),
            float(row['total_sales']),
            float(row['rrp']),
            float(row['unit_price']),
            float(row['discount_pct']) if pd.notna(row['discount_pct']) else 0.0,
            bool(row['is_promo']),
            float(row['baseline_units']) if pd.notna(row['baseline_units']) else None,
            float(row['promo_uplift_pct']) if pd.notna(row['promo_uplift_pct']) else None,
            float(row['price_index_vs_comp']) if pd.notna(row['price_index_vs_comp']) else None,
            row['data_quality_flag']
        ))
    
    # Bulk insert (note: fact table column order is store_id, item_id, supplier_id, date_id per schema)
    execute_values(
        cursor,
        """
        INSERT INTO dw.fact_sales_enriched (
            store_id, item_id, supplier_id, date_id,
            quantity, total_sales, rrp, unit_price, discount_pct,
            is_promo, baseline_units, promo_uplift_pct,
            price_index_vs_comp, data_quality_flag
        )
        VALUES %s
        """,
        fact_records
    )
    
    conn.commit()
    cursor.close()
    
    print(f"  - Loaded {len(fact_records)} fact records")


def main(df_transformed):
    """Main loading function"""
    print("\n" + "=" * 70)
    print("STAGE 3: DATA LOADING TO STAR SCHEMA")
    print("=" * 70 + "\n")
    
    start_time = datetime.now()
    
    # Connect to database
    conn = get_connection()
    
    try:
        # Load dimensions first (in dependency order)
        load_dimension_stores(df_transformed, conn)
        load_dimension_suppliers(df_transformed, conn)
        load_dimension_dates(df_transformed, conn)
        load_dimension_items(df_transformed, conn)
        
        # Load fact table last
        load_fact_sales(df_transformed, conn)
        
        # Get final counts
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dw.dim_store")
        store_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM dw.dim_supplier")
        supplier_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM dw.dim_date")
        date_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM dw.dim_item")
        item_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM dw.fact_sales_enriched")
        fact_count = cursor.fetchone()[0]
        
        # Show sample data
        cursor.execute("""
            SELECT s.supplier_name, COUNT(*) as cnt 
            FROM dw.fact_sales_enriched f
            JOIN dw.dim_supplier s ON f.supplier_id = s.supplier_id
            GROUP BY s.supplier_name
            ORDER BY cnt DESC
            LIMIT 5
        """)
        top_suppliers = cursor.fetchall()
        
        cursor.close()
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        print(f"\n[{datetime.now()}] Loading Summary:")
        print(f"  - dim_store: {store_count} records")
        print(f"  - dim_supplier: {supplier_count} records")
        print(f"  - dim_date: {date_count} records")
        print(f"  - dim_item: {item_count} records")
        print(f"  - fact_sales_enriched: {fact_count} records")
        print(f"  - Elapsed time: {elapsed:.2f} seconds")
        
        print(f"\n  Top 5 suppliers by sales volume:")
        for supplier_name, cnt in top_suppliers:
            print(f"    - {supplier_name}: {cnt:,} transactions")
        
        print(f"\n[{datetime.now()}] ✅ Data loading complete!")
        
    except Exception as e:
        print(f"\n❌ Error during loading: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()
    
    return True


if __name__ == "__main__":
    # For standalone testing, import and run transform first
    import sys
    import os
    import importlib.util
    
    # Load transform module
    spec = importlib.util.spec_from_file_location("transform", "/opt/airflow/ducklens_pipeline/02_transform.py")
    transform_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(transform_module)
    
    # Load cleaning module
    spec2 = importlib.util.spec_from_file_location("cleaning", "/opt/airflow/ducklens_pipeline/01_cleaning.py")
    cleaning_module = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(cleaning_module)
    
    print("Running standalone loading test...")
    print("Step 1: Running cleaning...")
    df_cleaned = cleaning_module.main()
    
    print("\nStep 2: Running transformation...")
    df_transformed = transform_module.main(df_cleaned)
    
    print("\nStep 3: Running loading...")
    main(df_transformed)
    
    print("\n✅ All pipeline stages completed successfully!")
