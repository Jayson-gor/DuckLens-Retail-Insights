-- =====================================================================
-- SUPERSET CONNECTION TEST QUERIES
-- =====================================================================
-- Use these queries in Superset SQL Lab to test your connection
-- and verify all views are accessible
-- =====================================================================

-- =====================================================================
-- TEST 1: Check all promo views exist and are accessible
-- =====================================================================
SELECT 
    schemaname,
    matviewname AS view_name,
    'Materialized View' AS object_type
FROM pg_matviews
WHERE schemaname = 'dw'
    AND matviewname LIKE 'v_%promo%'
ORDER BY matviewname;

-- Expected: 6 rows showing all promo views


-- =====================================================================
-- TEST 2: Sample data from each view (Quick Preview)
-- =====================================================================

-- Promo Uplift Summary
SELECT 'v_promo_uplift_summary' AS view_name, COUNT(*) AS row_count
FROM dw.v_promo_uplift_summary
UNION ALL
SELECT 'v_promo_coverage_analysis', COUNT(*)
FROM dw.v_promo_coverage_analysis
UNION ALL
SELECT 'v_promo_price_impact', COUNT(*)
FROM dw.v_promo_price_impact
UNION ALL
SELECT 'v_baseline_vs_promo_pricing', COUNT(*)
FROM dw.v_baseline_vs_promo_pricing
UNION ALL
SELECT 'v_top_performing_skus', COUNT(*)
FROM dw.v_top_performing_skus
UNION ALL
SELECT 'v_bidco_promo_executive_summary', COUNT(*)
FROM dw.v_bidco_promo_executive_summary;

-- Expected: 6 rows with counts > 0


-- =====================================================================
-- TEST 3: Bidco Products Only (For Dashboard Filters)
-- =====================================================================
SELECT 
    COUNT(*) AS total_bidco_skus,
    SUM(promo_transactions) AS total_promo_transactions,
    ROUND(SUM(promo_revenue), 2) AS total_promo_revenue,
    ROUND(AVG(promo_uplift_pct), 2) AS avg_uplift_pct
FROM dw.v_promo_uplift_summary
WHERE is_bidco = TRUE;

-- Expected: 1 row with Bidco totals


-- =====================================================================
-- TEST 4: Category Breakdown (For Pie Chart)
-- =====================================================================
SELECT 
    category,
    COUNT(*) AS sku_count,
    SUM(promo_transactions) AS promo_transactions,
    ROUND(SUM(promo_revenue), 2) AS promo_revenue
FROM dw.v_promo_uplift_summary
WHERE is_bidco = TRUE
GROUP BY category
ORDER BY promo_revenue DESC;

-- Expected: Multiple rows showing category performance


-- =====================================================================
-- TEST 5: Performance Tier Distribution (For Filters)
-- =====================================================================
SELECT 
    performance_tier,
    COUNT(*) AS sku_count,
    ROUND(AVG(performance_score), 2) AS avg_score
FROM dw.v_top_performing_skus
WHERE is_bidco = TRUE
GROUP BY performance_tier
ORDER BY avg_score DESC;

-- Expected: 5 tiers (Elite, Top, Strong, Average, Low)


-- =====================================================================
-- TEST 6: Top 5 Products by Each Metric (Quick Validation)
-- =====================================================================

-- By Uplift %
SELECT 'Top by Uplift' AS metric, item_code, item_description, promo_uplift_pct AS value
FROM dw.v_promo_uplift_summary
WHERE is_bidco = TRUE
ORDER BY promo_uplift_pct DESC
LIMIT 5;

-- By Coverage %
SELECT 'Top by Coverage' AS metric, item_code, item_description, promo_coverage_pct AS value
FROM dw.v_promo_coverage_analysis
WHERE is_bidco = TRUE
ORDER BY promo_coverage_pct DESC
LIMIT 5;

-- By Revenue
SELECT 'Top by Revenue' AS metric, item_code, item_description, promo_revenue AS value
FROM dw.v_promo_uplift_summary
WHERE is_bidco = TRUE
ORDER BY promo_revenue DESC
LIMIT 5;


-- =====================================================================
-- TEST 7: Date Range Check (If you have date filters)
-- =====================================================================
SELECT 
    MIN(d.full_date) AS earliest_sale,
    MAX(d.full_date) AS latest_sale,
    COUNT(DISTINCT d.full_date) AS days_with_data
FROM dw.fact_sales_enriched f
JOIN dw.dim_date d ON f.date_id = d.date_id
JOIN dw.dim_item i ON f.item_id = i.item_id
WHERE i.is_bidco = TRUE AND f.is_promo = TRUE;

-- Expected: Date range showing your data coverage


-- =====================================================================
-- TEST 8: Store List (For Store Filter)
-- =====================================================================
SELECT DISTINCT
    st.store_name,
    COUNT(f.sale_id) AS total_transactions,
    COUNT(CASE WHEN f.is_promo = TRUE THEN 1 END) AS promo_transactions
FROM dw.fact_sales_enriched f
JOIN dw.dim_store st ON f.store_id = st.store_id
JOIN dw.dim_item i ON f.item_id = i.item_id
WHERE i.is_bidco = TRUE
GROUP BY st.store_name
ORDER BY promo_transactions DESC;

-- Expected: List of all stores with Bidco promo activity


-- =====================================================================
-- SUCCESS CRITERIA
-- =====================================================================
-- ✅ All queries return results without errors
-- ✅ Row counts > 0 for all views
-- ✅ Bidco products show reasonable metrics
-- ✅ Date range covers your data period
-- ✅ All categories and tiers represented
-- =====================================================================
