-- =============================================
-- UNRELIABLE STORES ANALYSIS
-- Flags stores with data quality issues, suspicious patterns, and outliers
-- =============================================
CREATE OR REPLACE VIEW dw.v_unreliable_stores_analysis AS
WITH store_metrics AS (
    SELECT
        st.store_id,
        st.store_name,
        
        -- Transaction counts
        COUNT(*) AS total_transactions,
        COUNT(DISTINCT f.date_id) AS days_active,
        COUNT(DISTINCT f.item_id) AS unique_items_sold,
        COUNT(DISTINCT f.supplier_id) AS unique_suppliers,
        
        -- Revenue metrics
        SUM(f.total_sales) AS total_revenue,
        AVG(f.total_sales) AS avg_transaction_value,
        STDDEV(f.total_sales) AS stddev_transaction_value,
        
        -- Data quality issues
        SUM(CASE WHEN f.data_quality_flag = 'low' THEN 1 ELSE 0 END) AS critical_quality_issues,
        SUM(CASE WHEN f.data_quality_flag = 'medium' THEN 1 ELSE 0 END) AS medium_quality_issues,
        SUM(CASE WHEN f.quantity < 0 OR f.total_sales < 0 THEN 1 ELSE 0 END) AS negative_values,
        SUM(CASE WHEN ABS(f.unit_price - f.rrp) / NULLIF(f.rrp, 0) > 0.5 THEN 1 ELSE 0 END) AS extreme_pricing,
        
        -- Suspicious patterns
        SUM(CASE WHEN f.quantity = 0 THEN 1 ELSE 0 END) AS zero_quantity_sales,
        SUM(CASE WHEN f.total_sales = 0 THEN 1 ELSE 0 END) AS zero_value_sales,
        
        -- Promotional metrics
        SUM(CASE WHEN f.is_promo THEN 1 ELSE 0 END) AS promo_transactions,
        ROUND(AVG(f.discount_pct) * 100, 2) AS avg_discount_pct,
        MAX(f.discount_pct) * 100 AS max_discount_pct,
        
        -- Quantity metrics
        MAX(f.quantity) AS max_quantity_sold,
        AVG(f.quantity) AS avg_quantity_sold
        
    FROM dw.fact_sales_enriched f
    JOIN dw.dim_store st ON f.store_id = st.store_id
    GROUP BY st.store_id, st.store_name
),
reliability_scoring AS (
    SELECT
        store_id,
        store_name,
        total_transactions,
        days_active,
        unique_items_sold,
        unique_suppliers,
        total_revenue,
        avg_transaction_value,
        critical_quality_issues,
        medium_quality_issues,
        negative_values,
        extreme_pricing,
        zero_quantity_sales,
        zero_value_sales,
        promo_transactions,
        avg_discount_pct,
        max_discount_pct,
        max_quantity_sold,
        avg_quantity_sold,
        
        -- Calculate issue percentages
        ROUND(100.0 * critical_quality_issues / NULLIF(total_transactions, 0), 2) AS pct_critical_issues,
        ROUND(100.0 * negative_values / NULLIF(total_transactions, 0), 2) AS pct_negative_values,
        ROUND(100.0 * extreme_pricing / NULLIF(total_transactions, 0), 2) AS pct_extreme_pricing,
        
        -- Reliability Score (100 = perfect)
        ROUND(
            100 - (
                50.0 * (negative_values::NUMERIC / NULLIF(total_transactions, 0)) +
                30.0 * (extreme_pricing::NUMERIC / NULLIF(total_transactions, 0)) +
                20.0 * (critical_quality_issues::NUMERIC / NULLIF(total_transactions, 0))
            ), 2
        ) AS reliability_score,
        
        -- Risk Flags
        CASE WHEN negative_values > 0 THEN 1 ELSE 0 END AS flag_negative_values,
        CASE WHEN extreme_pricing::NUMERIC / NULLIF(total_transactions, 0) > 0.05 THEN 1 ELSE 0 END AS flag_pricing_issues,
        CASE WHEN critical_quality_issues > 0 THEN 1 ELSE 0 END AS flag_quality_issues,
        CASE WHEN zero_quantity_sales > 5 THEN 1 ELSE 0 END AS flag_suspicious_zeros
        
    FROM store_metrics
),
final_classification AS (
    SELECT
        *,
        -- Total risk flags
        (flag_negative_values + flag_pricing_issues + flag_quality_issues + flag_suspicious_zeros) AS total_risk_flags,
        
        -- Overall Status
        CASE
            WHEN flag_negative_values = 1 THEN '🔴 UNRELIABLE - Critical Issues'
            WHEN (flag_pricing_issues + flag_quality_issues) >= 2 THEN '🟠 HIGH RISK - Multiple Issues'
            WHEN (flag_pricing_issues + flag_quality_issues + flag_suspicious_zeros) >= 2 THEN '🟡 MEDIUM RISK - Monitor Closely'
            WHEN (flag_pricing_issues + flag_quality_issues + flag_suspicious_zeros) = 1 THEN '🟢 LOW RISK - Minor Issues'
            ELSE '✅ RELIABLE - No Issues'
        END AS reliability_status,
        
        -- Issue Summary
        CASE
            WHEN flag_negative_values = 1 THEN 'Negative quantities/sales detected'
            WHEN flag_pricing_issues = 1 THEN 'Extreme pricing deviations (>50% from RRP)'
            WHEN flag_quality_issues = 1 THEN 'Critical data quality issues'
            WHEN flag_suspicious_zeros = 1 THEN 'Suspicious zero-quantity sales'
            ELSE 'Clean data'
        END AS primary_issue
        
    FROM reliability_scoring
)
SELECT
    store_name,
    total_transactions,
    TO_CHAR(total_revenue, 'FM$999,999,999.00') AS total_revenue,
    days_active,
    unique_items_sold,
    reliability_score::TEXT || ' / 100' AS reliability_score,
    reliability_status,
    total_risk_flags,
    primary_issue,
    
    -- Issue Details
    critical_quality_issues,
    negative_values,
    extreme_pricing,
    zero_quantity_sales,
    
    -- Percentages
    pct_critical_issues::TEXT || '%' AS pct_critical,
    pct_negative_values::TEXT || '%' AS pct_negative,
    pct_extreme_pricing::TEXT || '%' AS pct_extreme_pricing,
    
    -- Additional context
    ROUND(avg_transaction_value, 2) AS avg_transaction_value,
    avg_discount_pct::TEXT || '%' AS avg_discount,
    max_quantity_sold
    
FROM final_classification
ORDER BY 
    total_risk_flags DESC,
    reliability_score ASC,
    total_transactions DESC;
