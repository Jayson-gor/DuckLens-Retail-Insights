-- =====================================================================
-- REFRESH ALL PROMO ANALYSIS MATERIALIZED VIEWS
-- =====================================================================
-- Run this script after each data load to update all promo views
-- Usage: docker exec -i postgres_container psql -U user -d ducklens_db < refresh_promo_views.sql
-- =====================================================================

\echo '🔄 Refreshing all promo analysis materialized views...'
\echo ''

\echo '1/6 Refreshing v_promo_uplift_summary...'
REFRESH MATERIALIZED VIEW dw.v_promo_uplift_summary;
\echo '✅ Done'
\echo ''

\echo '2/6 Refreshing v_promo_coverage_analysis...'
REFRESH MATERIALIZED VIEW dw.v_promo_coverage_analysis;
\echo '✅ Done'
\echo ''

\echo '3/6 Refreshing v_promo_price_impact...'
REFRESH MATERIALIZED VIEW dw.v_promo_price_impact;
\echo '✅ Done'
\echo ''

\echo '4/6 Refreshing v_baseline_vs_promo_pricing...'
REFRESH MATERIALIZED VIEW dw.v_baseline_vs_promo_pricing;
\echo '✅ Done'
\echo ''

\echo '5/6 Refreshing v_top_performing_skus...'
REFRESH MATERIALIZED VIEW dw.v_top_performing_skus;
\echo '✅ Done'
\echo ''

\echo '6/6 Refreshing v_bidco_promo_executive_summary...'
REFRESH MATERIALIZED VIEW dw.v_bidco_promo_executive_summary;
\echo '✅ Done'
\echo ''

-- Verify all views have data
\echo '📊 View Row Counts:'
\echo ''

SELECT 
    'v_promo_uplift_summary' AS view_name,
    COUNT(*) AS row_count
FROM dw.v_promo_uplift_summary

UNION ALL

SELECT 
    'v_promo_coverage_analysis',
    COUNT(*)
FROM dw.v_promo_coverage_analysis

UNION ALL

SELECT 
    'v_promo_price_impact',
    COUNT(*)
FROM dw.v_promo_price_impact

UNION ALL

SELECT 
    'v_baseline_vs_promo_pricing',
    COUNT(*)
FROM dw.v_baseline_vs_promo_pricing

UNION ALL

SELECT 
    'v_top_performing_skus',
    COUNT(*)
FROM dw.v_top_performing_skus

UNION ALL

SELECT 
    'v_bidco_promo_executive_summary',
    COUNT(*)
FROM dw.v_bidco_promo_executive_summary

ORDER BY view_name;

\echo ''
\echo '✅ All promo views refreshed successfully!'
\echo 'Ready for Superset dashboard creation.'
