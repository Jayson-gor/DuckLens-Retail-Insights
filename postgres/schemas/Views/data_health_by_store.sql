-- =============================================
-- DATA HEALTH: Per Store Summary
-- Shows which stores have data quality issues
-- =============================================
CREATE OR REPLACE VIEW dw.v_data_health_by_store AS
WITH store_stats AS (
    SELECT
        st.store_name,
        COUNT(*) AS total_transactions,
        SUM(f.total_sales) AS total_revenue,
        SUM(CASE WHEN f.data_quality_flag = 'low' THEN 1 ELSE 0 END) AS low_quality_count,
        SUM(CASE WHEN f.data_quality_flag = 'medium' THEN 1 ELSE 0 END) AS medium_quality_count,
        SUM(CASE WHEN f.quantity < 0 OR f.total_sales < 0 THEN 1 ELSE 0 END) AS negative_count,
        SUM(CASE WHEN ABS(f.unit_price - f.rrp) / NULLIF(f.rrp, 0) > 0.5 THEN 1 ELSE 0 END) AS extreme_price_count,
        -- Average discount
        AVG(f.discount_pct) AS avg_discount,
        -- Promo metrics
        SUM(CASE WHEN f.is_promo THEN 1 ELSE 0 END) AS promo_transactions,
        ROUND(100.0 * SUM(CASE WHEN f.is_promo THEN 1 ELSE 0 END) / COUNT(*), 1) AS promo_percentage
    FROM dw.fact_sales_enriched f
    JOIN dw.dim_store st ON f.store_id = st.store_id
    GROUP BY st.store_name
),
health_calc AS (
    SELECT
        store_name,
        total_transactions,
        total_revenue,
        low_quality_count,
        medium_quality_count,
        negative_count,
        extreme_price_count,
        avg_discount,
        promo_transactions,
        promo_percentage,
        -- Health Score Calculation (100 = perfect)
        ROUND(
            (100::NUMERIC - (
                40 * (negative_count::NUMERIC / NULLIF(total_transactions, 0)) +
                30 * (extreme_price_count::NUMERIC / NULLIF(total_transactions, 0)) +
                30 * (low_quality_count::NUMERIC / NULLIF(total_transactions, 0))
            )), 1
        ) AS health_score,
        -- Flag stores with issues
        CASE
            WHEN negative_count > 0 THEN '🔴 Critical'
            WHEN low_quality_count > 0 OR extreme_price_count::NUMERIC / NULLIF(total_transactions, 0) > 0.05 THEN '🟡 Warning'
            ELSE '🟢 Healthy'
        END AS health_status
    FROM store_stats
)
SELECT
    store_name,
    total_transactions,
    TO_CHAR(total_revenue, 'FM$999,999,999.00') AS total_revenue,
    health_score::TEXT || ' / 100' AS health_score,
    health_status,
    low_quality_count,
    negative_count,
    extreme_price_count,
    ROUND(avg_discount * 100, 2)::TEXT || '%' AS avg_discount,
    promo_transactions,
    promo_percentage::TEXT || '%' AS promo_coverage
FROM health_calc
ORDER BY health_score ASC, total_transactions DESC;
