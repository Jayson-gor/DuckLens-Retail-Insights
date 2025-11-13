-- =============================================
-- STAGING DATA HEALTH: Raw Data Quality Analysis
-- Analyzes data quality BEFORE any cleaning/transformation
-- This helps identify source data issues
-- =============================================
CREATE OR REPLACE VIEW dw.v_staging_data_health AS
WITH raw_stats AS (
    SELECT
        -- Total records
        COUNT(*) AS total_raw_records,
        
        -- Missing/NULL values
        SUM(CASE WHEN store_name IS NULL OR TRIM(store_name) = '' THEN 1 ELSE 0 END) AS missing_store,
        SUM(CASE WHEN item_code IS NULL OR TRIM(item_code) = '' THEN 1 ELSE 0 END) AS missing_item_code,
        SUM(CASE WHEN supplier IS NULL OR TRIM(supplier) = '' THEN 1 ELSE 0 END) AS missing_supplier,
        SUM(CASE WHEN date_of_sale IS NULL THEN 1 ELSE 0 END) AS missing_date,
        SUM(CASE WHEN description IS NULL OR TRIM(description) = '' THEN 1 ELSE 0 END) AS missing_description,
        
        -- Data quality issues
        SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) AS null_quantity,
        SUM(CASE WHEN quantity < 0 THEN 1 ELSE 0 END) AS negative_quantity,
        SUM(CASE WHEN quantity = 0 THEN 1 ELSE 0 END) AS zero_quantity,
        
        SUM(CASE WHEN total_sales IS NULL THEN 1 ELSE 0 END) AS null_sales,
        SUM(CASE WHEN total_sales < 0 THEN 1 ELSE 0 END) AS negative_sales,
        SUM(CASE WHEN total_sales = 0 THEN 1 ELSE 0 END) AS zero_sales,
        
        SUM(CASE WHEN rrp IS NULL THEN 1 ELSE 0 END) AS null_rrp,
        SUM(CASE WHEN rrp <= 0 THEN 1 ELSE 0 END) AS zero_or_negative_rrp,
        
        -- Price consistency issues
        SUM(CASE 
            WHEN quantity > 0 AND rrp > 0 
            THEN CASE WHEN ABS((total_sales / quantity) - rrp) / rrp > 0.5 THEN 1 ELSE 0 END
            ELSE 0 
        END) AS extreme_price_deviation,
        
        -- Duplicates (exact same record on all key fields)
        COUNT(*) - COUNT(DISTINCT (store_name || '|' || item_code || '|' || date_of_sale::TEXT || '|' || quantity::TEXT)) AS exact_duplicates,
        
        -- Business logic duplicates (same store, item, date - regardless of quantity)
        COUNT(*) - COUNT(DISTINCT (store_name || '|' || item_code || '|' || date_of_sale::TEXT)) AS business_duplicates,
        
        -- Date range
        MIN(date_of_sale) AS earliest_date,
        MAX(date_of_sale) AS latest_date,
        
        -- Unique counts
        COUNT(DISTINCT store_name) AS unique_stores,
        COUNT(DISTINCT item_code) AS unique_items,
        COUNT(DISTINCT supplier) AS unique_suppliers,
        COUNT(DISTINCT date_of_sale) AS unique_dates
        
    FROM staging.stg_sales_raw
),
quality_summary AS (
    SELECT
        r.*,
        -- Calculate total issues
        (r.missing_store + r.missing_item_code + r.missing_supplier + r.missing_date +
         r.null_quantity + r.negative_quantity + r.null_sales + r.negative_sales +
         r.null_rrp + r.zero_or_negative_rrp + r.extreme_price_deviation + r.business_duplicates) AS total_issues,
        
        -- Calculate health score
        ROUND(
            100 - (
                25.0 * (r.business_duplicates::NUMERIC / NULLIF(r.total_raw_records, 0) * 100) +
                20.0 * ((r.negative_quantity + r.negative_sales)::NUMERIC / NULLIF(r.total_raw_records, 0) * 100) +
                15.0 * ((r.missing_store + r.missing_item_code + r.missing_supplier)::NUMERIC / NULLIF(r.total_raw_records, 0) * 100) +
                15.0 * (r.zero_or_negative_rrp::NUMERIC / NULLIF(r.total_raw_records, 0) * 100) +
                10.0 * (r.extreme_price_deviation::NUMERIC / NULLIF(r.total_raw_records, 0) * 100) +
                10.0 * (r.missing_date::NUMERIC / NULLIF(r.total_raw_records, 0) * 100) +
                5.0 * (r.zero_quantity::NUMERIC / NULLIF(r.total_raw_records, 0) * 100)
            ), 2
        ) AS raw_data_health_score
    FROM raw_stats r
)
SELECT
    '📊 RAW DATA OVERVIEW' AS section,
    'Total Records' AS metric,
    total_raw_records::TEXT AS value,
    '' AS percentage
FROM quality_summary
UNION ALL
SELECT '📊 RAW DATA OVERVIEW', 'Date Range', 
       earliest_date::TEXT || ' to ' || latest_date::TEXT,
       (latest_date - earliest_date + 1)::TEXT || ' days'
FROM quality_summary
UNION ALL
SELECT '📊 RAW DATA OVERVIEW', 'Unique Stores', unique_stores::TEXT, ''
FROM quality_summary
UNION ALL
SELECT '📊 RAW DATA OVERVIEW', 'Unique Items', unique_items::TEXT, ''
FROM quality_summary
UNION ALL
SELECT '📊 RAW DATA OVERVIEW', 'Unique Suppliers', unique_suppliers::TEXT, ''
FROM quality_summary
UNION ALL
SELECT '📊 RAW DATA OVERVIEW', 'Unique Dates', unique_dates::TEXT, ''
FROM quality_summary

UNION ALL SELECT '', '', '', ''  -- Separator

UNION ALL
SELECT '🔴 CRITICAL ISSUES' AS section,
       'Negative Quantities' AS metric,
       negative_quantity::TEXT AS value,
       ROUND(100.0 * negative_quantity / NULLIF(total_raw_records, 0), 2)::TEXT || '%' AS percentage
FROM quality_summary
UNION ALL
SELECT '🔴 CRITICAL ISSUES', 'Negative Sales',
       negative_sales::TEXT,
       ROUND(100.0 * negative_sales / NULLIF(total_raw_records, 0), 2)::TEXT || '%'
FROM quality_summary
UNION ALL
SELECT '🔴 CRITICAL ISSUES', 'Missing Store Names',
       missing_store::TEXT,
       ROUND(100.0 * missing_store / NULLIF(total_raw_records, 0), 2)::TEXT || '%'
FROM quality_summary
UNION ALL
SELECT '🔴 CRITICAL ISSUES', 'Missing Item Codes',
       missing_item_code::TEXT,
       ROUND(100.0 * missing_item_code / NULLIF(total_raw_records, 0), 2)::TEXT || '%'
FROM quality_summary
UNION ALL
SELECT '🔴 CRITICAL ISSUES', 'Missing Dates',
       missing_date::TEXT,
       ROUND(100.0 * missing_date / NULLIF(total_raw_records, 0), 2)::TEXT || '%'
FROM quality_summary

UNION ALL SELECT '', '', '', ''  -- Separator

UNION ALL
SELECT '🟡 DATA QUALITY WARNINGS' AS section,
       'NULL/Zero RRP' AS metric,
       (null_rrp + zero_or_negative_rrp)::TEXT AS value,
       ROUND(100.0 * (null_rrp + zero_or_negative_rrp) / NULLIF(total_raw_records, 0), 2)::TEXT || '%' AS percentage
FROM quality_summary
UNION ALL
SELECT '🟡 DATA QUALITY WARNINGS', 'Extreme Price Deviation (>50%)',
       extreme_price_deviation::TEXT,
       ROUND(100.0 * extreme_price_deviation / NULLIF(total_raw_records, 0), 2)::TEXT || '%'
FROM quality_summary
UNION ALL
SELECT '🟡 DATA QUALITY WARNINGS', 'Zero Quantities',
       zero_quantity::TEXT,
       ROUND(100.0 * zero_quantity / NULLIF(total_raw_records, 0), 2)::TEXT || '%'
FROM quality_summary
UNION ALL
SELECT '🟡 DATA QUALITY WARNINGS', 'Missing Descriptions',
       missing_description::TEXT,
       ROUND(100.0 * missing_description / NULLIF(total_raw_records, 0), 2)::TEXT || '%'
FROM quality_summary

UNION ALL SELECT '', '', '', ''  -- Separator

UNION ALL
SELECT '🔄 DUPLICATE ANALYSIS' AS section,
       'Exact Duplicates (all fields match)' AS metric,
       exact_duplicates::TEXT AS value,
       ROUND(100.0 * exact_duplicates / NULLIF(total_raw_records, 0), 2)::TEXT || '%' AS percentage
FROM quality_summary
UNION ALL
SELECT '🔄 DUPLICATE ANALYSIS', 'Business Duplicates (store+item+date)',
       business_duplicates::TEXT,
       ROUND(100.0 * business_duplicates / NULLIF(total_raw_records, 0), 2)::TEXT || '%'
FROM quality_summary
UNION ALL
SELECT '🔄 DUPLICATE ANALYSIS', 'Records After Dedup',
       (total_raw_records - business_duplicates)::TEXT,
       ROUND(100.0 * (total_raw_records - business_duplicates) / NULLIF(total_raw_records, 0), 2)::TEXT || '%'
FROM quality_summary

UNION ALL SELECT '', '', '', ''  -- Separator

UNION ALL
SELECT '📈 OVERALL ASSESSMENT' AS section,
       'Total Issues Found' AS metric,
       total_issues::TEXT AS value,
       ROUND(100.0 * total_issues / NULLIF(total_raw_records, 0), 2)::TEXT || '%' AS percentage
FROM quality_summary
UNION ALL
SELECT '📈 OVERALL ASSESSMENT', 'Clean Records (no issues)',
       (total_raw_records - total_issues)::TEXT,
       ROUND(100.0 * (total_raw_records - total_issues) / NULLIF(total_raw_records, 0), 2)::TEXT || '%'
FROM quality_summary
UNION ALL
SELECT '📈 OVERALL ASSESSMENT', '⭐ Raw Data Health Score',
       raw_data_health_score::TEXT || ' / 100',
       CASE 
           WHEN raw_data_health_score >= 95 THEN '🟢 Excellent'
           WHEN raw_data_health_score >= 85 THEN '🟡 Good'
           WHEN raw_data_health_score >= 70 THEN '🟠 Fair'
           ELSE '🔴 Poor'
       END
FROM quality_summary;
