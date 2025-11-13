-- =============================================
-- DATA HEALTH: Per Supplier Summary
-- Identifies unreliable suppliers based on data quality
-- =============================================
CREATE OR REPLACE VIEW dw.v_data_health_by_supplier AS
WITH supplier_stats AS (
    SELECT
        s.supplier_name,
        COUNT(*) AS total_transactions,
        SUM(f.total_sales) AS total_revenue,
        COUNT(DISTINCT st.store_name) AS store_coverage,
        COUNT(DISTINCT i.item_code) AS unique_items,
        SUM(CASE WHEN f.data_quality_flag = 'low' THEN 1 ELSE 0 END) AS low_quality_count,
        SUM(CASE WHEN f.data_quality_flag = 'medium' THEN 1 ELSE 0 END) AS medium_quality_count,
        SUM(CASE WHEN f.quantity < 0 OR f.total_sales < 0 THEN 1 ELSE 0 END) AS negative_count,
        SUM(CASE WHEN ABS(f.unit_price - f.rrp) / NULLIF(f.rrp, 0) > 0.5 THEN 1 ELSE 0 END) AS extreme_price_count,
        -- Promo metrics
        SUM(CASE WHEN f.is_promo THEN 1 ELSE 0 END) AS promo_transactions,
        ROUND(100.0 * SUM(CASE WHEN f.is_promo THEN 1 ELSE 0 END) / COUNT(*), 1) AS promo_rate,
        AVG(f.promo_uplift_pct) FILTER (WHERE f.is_promo) AS avg_promo_uplift
    FROM dw.fact_sales_enriched f
    JOIN dw.dim_supplier s ON f.supplier_id = s.supplier_id
    JOIN dw.dim_store st ON f.store_id = st.store_id
    JOIN dw.dim_item i ON f.item_id = i.item_id
    GROUP BY s.supplier_name
),
health_calc AS (
    SELECT
        supplier_name,
        total_transactions,
        total_revenue,
        store_coverage,
        unique_items,
        low_quality_count,
        medium_quality_count,
        negative_count,
        extreme_price_count,
        promo_transactions,
        promo_rate,
        avg_promo_uplift,
        -- Health Score Calculation (100 = perfect)
        ROUND(
            (100::NUMERIC - (
                50 * (negative_count::NUMERIC / NULLIF(total_transactions, 0)) +
                30 * (extreme_price_count::NUMERIC / NULLIF(total_transactions, 0)) +
                20 * (low_quality_count::NUMERIC / NULLIF(total_transactions, 0))
            )), 1
        ) AS health_score,
        -- Reliability Flag
        CASE
            WHEN negative_count > 0 THEN '🔴 Unreliable'
            WHEN extreme_price_count::NUMERIC / NULLIF(total_transactions, 0) > 0.10 THEN '🟡 Review Pricing'
            WHEN low_quality_count > 0 THEN '🟡 Minor Issues'
            ELSE '🟢 Reliable'
        END AS reliability_status
    FROM supplier_stats
)
SELECT
    supplier_name,
    total_transactions,
    TO_CHAR(total_revenue, 'FM$999,999,999.00') AS total_revenue,
    store_coverage AS stores_supplied,
    unique_items,
    health_score::TEXT || ' / 100' AS health_score,
    reliability_status,
    negative_count AS critical_issues,
    extreme_price_count AS pricing_issues,
    promo_transactions,
    promo_rate::TEXT || '%' AS promo_rate,
    ROUND(avg_promo_uplift, 1)::TEXT || '%' AS avg_promo_uplift
FROM health_calc
ORDER BY health_score ASC, total_revenue DESC;
