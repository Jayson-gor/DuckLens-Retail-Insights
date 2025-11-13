"""
Stage 2: Data Transformation
-----------------------------
Apply business logic to cleaned data:
1. Detect promo SKUs (≥10% discount + ≥2 days)
2. Calculate promo uplift % (promo units vs baseline)
3. Compute Bidco Price Index vs competitors
4. Flag Bidco products

Input: Cleaned DataFrame from 01_cleaning.py
Output: Enriched DataFrame with promo flags, uplift, price index
"""

import pandas as pd
import numpy as np
from datetime import datetime


# Business rules
PROMO_MIN_DISCOUNT_PCT = 0.10  # 10% minimum discount to qualify as promo
PROMO_MIN_DAYS = 2  # Promo must run for at least 2 days


def detect_promos(df):
    """
    Detect promotional SKUs based on:
    - Discount >= 10% (unit_price vs RRP)
    - Running for >= 2 days
    """
    
    print(f"[{datetime.now()}] Detecting promotional SKUs...")
    
    df = df.copy()
    
    # Calculate discount percentage
    df['discount_pct'] = np.where(
        df['rrp'] > 0,
        (df['rrp'] - df['unit_price']) / df['rrp'],
        0
    )
    
    # Initial promo flag: discount >= 10%
    df['is_promo_candidate'] = df['discount_pct'] >= PROMO_MIN_DISCOUNT_PCT
    
    # Count days each SKU runs promo
    promo_days = df[df['is_promo_candidate']].groupby('item_code')['date_of_sale'].nunique()
    promo_days = promo_days.rename('promo_days_count')
    
    df = df.merge(promo_days, on='item_code', how='left')
    df['promo_days_count'] = df['promo_days_count'].fillna(0)
    
    # Final promo flag: discount >= 10% AND runs >= 2 days
    df['is_promo'] = (
        df['is_promo_candidate'] & 
        (df['promo_days_count'] >= PROMO_MIN_DAYS)
    )
    
    promo_count = df['is_promo'].sum()
    promo_pct = (promo_count / len(df)) * 100
    
    print(f"  - Promo rows: {promo_count:,} ({promo_pct:.2f}%)")
    print(f"  - Unique promo SKUs: {df[df['is_promo']]['item_code'].nunique()}")
    
    # Clean up temporary columns
    df = df.drop(columns=['is_promo_candidate'])
    
    return df


def calculate_uplift(df):
    """
    Calculate promo uplift percentage:
    - baseline_units = average quantity when NOT on promo
    - promo_uplift_pct = (promo_avg - baseline) / baseline * 100
    """
    
    print(f"[{datetime.now()}] Calculating promo uplift...")
    
    df = df.copy()
    
    # Calculate baseline (non-promo) average quantity per SKU
    baseline = df[~df['is_promo']].groupby('item_code')['quantity'].mean()
    baseline = baseline.rename('baseline_units')
    
    # Calculate promo average quantity per SKU
    promo_avg = df[df['is_promo']].groupby('item_code')['quantity'].mean()
    promo_avg = promo_avg.rename('promo_avg_units')
    
    # Merge back to main dataframe
    df = df.merge(baseline, on='item_code', how='left')
    df = df.merge(promo_avg, on='item_code', how='left')
    
    # Fill NaNs (SKUs with no baseline or no promo)
    df['baseline_units'] = df['baseline_units'].fillna(df['quantity'])
    df['promo_avg_units'] = df['promo_avg_units'].fillna(0)
    
    # Calculate uplift percentage
    df['promo_uplift_pct'] = np.where(
        (df['baseline_units'] > 0) & (df['is_promo']),
        ((df['promo_avg_units'] - df['baseline_units']) / df['baseline_units']) * 100,
        0
    )
    
    # Print summary
    promo_data = df[df['is_promo'] & (df['promo_uplift_pct'] > 0)]
    if len(promo_data) > 0:
        avg_uplift = promo_data['promo_uplift_pct'].mean()
        print(f"  - Average promo uplift: {avg_uplift:.2f}%")
        print(f"  - Top uplift: {promo_data['promo_uplift_pct'].max():.2f}%")
    
    return df


def calculate_price_index(df):
    """
    Calculate Bidco Price Index vs competitors:
    - Group by: Sub-Department + Section + Store
    - Compare Bidco avg price vs all brands avg price
    - price_index = bidco_price / competitor_avg_price
    - >1.0 = Bidco more expensive, <1.0 = Bidco cheaper
    """
    
    print(f"[{datetime.now()}] Calculating price index...")
    
    df = df.copy()
    
    # Identify Bidco products
    df['is_bidco'] = df['supplier'].str.contains('Bidco', case=False, na=False)
    
    print(f"  - Bidco products: {df['is_bidco'].sum():,} rows ({(df['is_bidco'].sum()/len(df)*100):.2f}%)")
    
    # Calculate average price per Sub-Dept + Section + Store
    groupby_cols = ['sub_department', 'section', 'store_name']
    
    avg_prices = df.groupby(groupby_cols)['unit_price'].mean().reset_index()
    avg_prices = avg_prices.rename(columns={'unit_price': 'group_avg_price'})
    
    # Merge back
    df = df.merge(avg_prices, on=groupby_cols, how='left')
    
    # Calculate price index
    df['price_index_vs_comp'] = np.where(
        df['group_avg_price'] > 0,
        df['unit_price'] / df['group_avg_price'],
        1.0
    )
    
    # Interpret price index for Bidco products
    bidco_df = df[df['is_bidco']]
    if len(bidco_df) > 0:
        avg_index = bidco_df['price_index_vs_comp'].mean()
        premium_count = (bidco_df['price_index_vs_comp'] > 1.1).sum()
        discount_count = (bidco_df['price_index_vs_comp'] < 0.9).sum()
        
        print(f"  - Bidco avg price index: {avg_index:.4f}")
        print(f"  - Bidco premium (>10% above avg): {premium_count} rows")
        print(f"  - Bidco discount (>10% below avg): {discount_count} rows")
    
    return df


def calculate_promo_coverage(df):
    """
    Calculate promo coverage:
    - How many stores run each promo SKU
    - promo_coverage_pct = stores_with_promo / total_stores * 100
    """
    
    print(f"[{datetime.now()}] Calculating promo coverage...")
    
    df = df.copy()
    
    # Total unique stores
    total_stores = df['store_name'].nunique()
    
    # For each promo SKU, count how many stores carry it
    promo_coverage = df[df['is_promo']].groupby('item_code')['store_name'].nunique()
    promo_coverage = promo_coverage.rename('promo_store_count')
    
    df = df.merge(promo_coverage, on='item_code', how='left')
    df['promo_store_count'] = df['promo_store_count'].fillna(0)
    
    # Calculate coverage percentage
    df['promo_coverage_pct'] = (df['promo_store_count'] / total_stores) * 100
    
    # Summary
    promo_data = df[df['is_promo']].drop_duplicates(subset='item_code')
    if len(promo_data) > 0:
        avg_coverage = promo_data['promo_coverage_pct'].mean()
        print(f"  - Average promo coverage: {avg_coverage:.2f}% of stores")
    
    return df


def main(df_cleaned):
    """
    Main transformation pipeline
    Takes cleaned DataFrame as input
    """
    
    print(f"\n{'='*70}")
    print(f"STAGE 2: DATA TRANSFORMATION")
    print(f"{'='*70}\n")
    
    # 1. Detect promos
    df = detect_promos(df_cleaned)
    
    # 2. Calculate uplift
    df = calculate_uplift(df)
    
    # 3. Calculate price index
    df = calculate_price_index(df)
    
    # 4. Calculate promo coverage
    df = calculate_promo_coverage(df)
    
    # Summary
    print(f"\n[{datetime.now()}] Transformation Summary:")
    print(f"  - Total rows: {len(df)}")
    print(f"  - Promo rows: {df['is_promo'].sum()} ({(df['is_promo'].sum()/len(df)*100):.2f}%)")
    print(f"  - Bidco rows: {df['is_bidco'].sum()} ({(df['is_bidco'].sum()/len(df)*100):.2f}%)")
    print(f"  - New columns added: discount_pct, is_promo, baseline_units, promo_uplift_pct, is_bidco, price_index_vs_comp")
    
    print(f"\n[{datetime.now()}] ✅ Data transformation complete!")
    
    return df


if __name__ == "__main__":
    # For standalone testing, import and run cleaning first
    import sys
    import os
    
    # Add the parent directory to path
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Import the cleaning module
    import importlib.util
    spec = importlib.util.spec_from_file_location("cleaning", "/opt/airflow/ducklens_pipeline/01_cleaning.py")
    cleaning_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cleaning_module)
    
    print("Running standalone transformation test...")
    print("Step 1: Running cleaning...")
    df_cleaned = cleaning_module.main()
    
    print("\nStep 2: Running transformation...")
    df_transformed = main(df_cleaned)
    
    print(f"\nTransformed data shape: {df_transformed.shape}")
    print(f"Columns: {list(df_transformed.columns)}")
