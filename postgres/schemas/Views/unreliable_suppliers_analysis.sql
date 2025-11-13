-- =============================================
-- UNRELIABLE SUPPLIERS ANALYSIS
-- Flags suppliers with data quality issues, suspicious patterns, and outliers
-- =============================================
CREATE OR REPLACE VIEW dw.v_unreliable_suppliers_analysis AS
WITH supplier_metrics AS (
    SELECT
        s.supplier_id,
        s.supplier_name,
        
        -- Transaction counts
        COUNT(*) AS total_transactions,
        COUNT(DISTINCT f.store_id) AS stores_supplied,
        COUNT(DISTINCT f.item_id) AS unique_items,
        COUNT(DISTINCT f.date_id) AS days_active,
        
        -- Revenue metrics
        SUM(f.total_sales) AS total_revenue,
        AVG(f.total_sales) AS avg_transaction_value,
        STDDEV(f.total_sales) AS stddev_transaction_value,
        MIN(f.total_sales) AS min_transaction,
        MAX(f.total_sales) AS max_transaction,
        
        -- Data quality issues
        SUM(CASE WHEN f.data_quality_flag = 'low' THEN 1 ELSE 0 END) AS critical_quality_issues,
        SUM(CASE WHEN f.data_quality_flag = 'medium' THEN 1 ELSE 0 END) AS medium_quality_issues,
        SUM(CASE WHEN f.quantity < 0 OR f.total_sales < 0 THEN 1 ELSE 0 END) AS negative_values,
        SUM(CASE WHEN ABS(f.unit_price - f.rrp) / NULLIF(f.rrp, 0) > 0.5 THEN 1 ELSE 0 END) AS extreme_pricing,
        
        -- Pricing consistency across stores
        COUNT(DISTINCT f.unit_price) AS unique_price_points,
        STDDEV(f.unit_price) AS price_variation,
        MAX(f.unit_price) - MIN(f.unit_price) AS price_range,
        
        -- Suspicious patterns
        SUM(CASE WHEN f.quantity = 0 THEN 1 ELSE 0 END) AS zero_quantity_sales,
        SUM(CASE WHEN f.total_sales = 0 THEN 1 ELSE 0 END) AS zero_value_sales,
        
        -- Promotional behavior
        SUM(CASE WHEN f.is_promo THEN 1 ELSE 0 END) AS promo_transactions,
        ROUND(AVG(f.discount_pct) FILTER (WHERE f.is_promo) * 100, 2) AS avg_promo_discount,
        MAX(f.discount_pct) * 100 AS max_discount_given,
        
        -- Market presence
        ROUND(100.0 * COUNT(DISTINCT f.store_id) / (SELECT COUNT(*) FROM dw.dim_store), 2) AS market_penetration_pct
        
    FROM dw.fact_sales_enriched f
    JOIN dw.dim_supplier s ON f.supplier_id = s.supplier_id
    GROUP BY s.supplier_id, s.supplier_name
),
reliability_scoring AS (
    SELECT
        supplier_id,
        supplier_name,
        total_transactions,
        stores_supplied,
        unique_items,
        days_active,
        total_revenue,
        avg_transaction_value,
        critical_quality_issues,
        medium_quality_issues,
        negative_values,
        extreme_pricing,
        zero_quantity_sales,
        zero_value_sales,
        promo_transactions,
        avg_promo_discount,
        max_discount_given,
        market_penetration_pct,
        price_variation,
        price_range,
        unique_price_points,
        
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
        CASE WHEN extreme_pricing::NUMERIC / NULLIF(total_transactions, 0) > 0.10 THEN 1 ELSE 0 END AS flag_pricing_inconsistent,
        CASE WHEN critical_quality_issues > 0 THEN 1 ELSE 0 END AS flag_quality_issues,
        CASE WHEN zero_quantity_sales > 10 THEN 1 ELSE 0 END AS flag_suspicious_zeros,
        CASE WHEN stores_supplied < 3 AND total_transactions > 100 THEN 1 ELSE 0 END AS flag_limited_distribution
        
    FROM supplier_metrics
),
final_classification AS (
    SELECT
        *,
        -- Total risk flags
        (flag_negative_values + flag_pricing_inconsistent + flag_quality_issues + 
         flag_suspicious_zeros + flag_limited_distribution) AS total_risk_flags
    FROM reliability_scoring
),
status_assignment AS (
    SELECT
        *,
        -- Overall Status
        CASE
            WHEN flag_negative_values = 1 THEN '🔴 UNRELIABLE - Critical Data Issues'
            WHEN (flag_pricing_inconsistent + flag_quality_issues) >= 2 THEN '🟠 HIGH RISK - Pricing & Quality Issues'
            WHEN total_risk_flags >= 3 THEN '🟡 MEDIUM RISK - Multiple Red Flags'
            WHEN total_risk_flags = 2 THEN '🟡 MONITOR - Some Concerns'
            WHEN total_risk_flags = 1 THEN '🟢 LOW RISK - Minor Issues'
            ELSE '✅ RELIABLE - Clean Data'
        END AS reliability_status,
        
        -- Primary Issue Identification
        ARRAY_TO_STRING(
            ARRAY_REMOVE(ARRAY[
                CASE WHEN flag_negative_values = 1 THEN 'Negative values' END,
                CASE WHEN flag_pricing_inconsistent = 1 THEN 'Inconsistent pricing' END,
                CASE WHEN flag_quality_issues = 1 THEN 'Data quality issues' END,
                CASE WHEN flag_suspicious_zeros = 1 THEN 'Suspicious zero transactions' END,
                CASE WHEN flag_limited_distribution = 1 THEN 'Limited distribution' END
            ], NULL),
            ', '
        ) AS issues_detected
        
    FROM final_classification
)
SELECT
    supplier_name,
    total_transactions,
    stores_supplied,
    unique_items,
    TO_CHAR(total_revenue, 'FM$999,999,999.00') AS total_revenue,
    market_penetration_pct::TEXT || '%' AS market_penetration,
    reliability_score::TEXT || ' / 100' AS reliability_score,
    reliability_status,
    total_risk_flags,
    issues_detected,
    
    -- Issue Details
    critical_quality_issues,
    negative_values,
    extreme_pricing,
    zero_quantity_sales,
    
    -- Percentages
    pct_critical_issues::TEXT || '%' AS pct_critical,
    pct_negative_values::TEXT || '%' AS pct_negative,
    pct_extreme_pricing::TEXT || '%' AS pct_extreme_pricing,
    
    -- Pricing metrics
    ROUND(price_variation, 2) AS price_std_dev,
    ROUND(price_range, 2) AS price_range,
    
    -- Promo metrics
    promo_transactions,
    COALESCE(avg_promo_discount, 0)::TEXT || '%' AS avg_promo_discount,
    max_discount_given::TEXT || '%' AS max_discount
    
FROM status_assignment
ORDER BY 
    total_risk_flags DESC,
    reliability_score ASC,
    total_revenue DESC;
