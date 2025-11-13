-- =====================================================================
-- 📊 BIDCO PROMO KPI METRICS (For Superset Big Number Cards)
-- =====================================================================
-- Purpose: Provide clean numeric metrics for KPI cards in Superset
-- This view has proper numeric columns (not text) for visualization
-- =====================================================================

DROP MATERIALIZED VIEW IF EXISTS dw.v_bidco_promo_kpi_metrics CASCADE;

CREATE MATERIALIZED VIEW dw.v_bidco_promo_kpi_metrics AS

WITH bidco_metrics AS (
    SELECT
        -- Transaction metrics
        COUNT(*) AS total_transactions,
        COUNT(CASE WHEN f.is_promo = TRUE THEN 1 END) AS promo_transactions,
        COUNT(CASE WHEN f.is_promo = FALSE THEN 1 END) AS non_promo_transactions,
        
        -- Revenue metrics
        SUM(f.total_sales) AS total_revenue,
        SUM(CASE WHEN f.is_promo = TRUE THEN f.total_sales END) AS promo_revenue,
        SUM(CASE WHEN f.is_promo = FALSE THEN f.total_sales END) AS non_promo_revenue,
        
        -- Units metrics
        SUM(f.quantity) AS total_units_sold,
        SUM(CASE WHEN f.is_promo = TRUE THEN f.quantity END) AS promo_units_sold,
        SUM(CASE WHEN f.is_promo = FALSE THEN f.quantity END) AS non_promo_units_sold,
        AVG(CASE WHEN f.is_promo = FALSE THEN f.quantity END) AS baseline_avg_units,
        AVG(CASE WHEN f.is_promo = TRUE THEN f.quantity END) AS promo_avg_units,
        
        -- SKU and store coverage
        COUNT(DISTINCT f.item_id) AS total_skus,
        COUNT(DISTINCT CASE WHEN f.is_promo = TRUE THEN f.item_id END) AS skus_on_promo,
        COUNT(DISTINCT f.store_id) AS total_stores,
        COUNT(DISTINCT CASE WHEN f.is_promo = TRUE THEN f.store_id END) AS stores_running_promos,
        
        -- Pricing metrics
        AVG(CASE WHEN f.is_promo = TRUE THEN f.discount_pct END) AS avg_promo_discount_pct,
        AVG(CASE WHEN f.is_promo = TRUE THEN f.unit_price END) AS avg_promo_price,
        AVG(CASE WHEN f.is_promo = FALSE THEN f.unit_price END) AS avg_baseline_price,
        AVG(f.rrp) AS avg_rrp
        
    FROM dw.fact_sales_enriched f
    JOIN dw.dim_item i ON f.item_id = i.item_id
    WHERE i.is_bidco = TRUE
)

SELECT
    -- ============================================
    -- KPI METRICS (Numeric values for cards)
    -- ============================================
    
    -- Revenue KPIs
    ROUND(promo_revenue, 2) AS promo_revenue,
    ROUND(non_promo_revenue, 2) AS non_promo_revenue,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND((promo_revenue / NULLIF(total_revenue, 0)) * 100, 2) AS promo_revenue_share_pct,
    
    -- Transaction KPIs
    promo_transactions,
    non_promo_transactions,
    total_transactions,
    ROUND((promo_transactions::NUMERIC / NULLIF(total_transactions, 0)) * 100, 2) AS promo_penetration_pct,
    
    -- Units KPIs
    promo_units_sold,
    non_promo_units_sold,
    total_units_sold,
    ROUND(baseline_avg_units, 2) AS baseline_avg_units,
    ROUND(promo_avg_units, 2) AS promo_avg_units,
    ROUND(((promo_avg_units - baseline_avg_units) / NULLIF(baseline_avg_units, 0)) * 100, 2) AS units_uplift_pct,
    
    -- Coverage KPIs
    skus_on_promo,
    total_skus,
    ROUND((skus_on_promo::NUMERIC / NULLIF(total_skus, 0)) * 100, 2) AS sku_promo_coverage_pct,
    stores_running_promos,
    total_stores,
    ROUND((stores_running_promos::NUMERIC / NULLIF(total_stores, 0)) * 100, 2) AS store_promo_coverage_pct,
    
    -- Pricing KPIs
    ROUND(avg_promo_discount_pct * 100, 2) AS avg_discount_pct,
    ROUND(avg_promo_price, 2) AS avg_promo_price,
    ROUND(avg_baseline_price, 2) AS avg_baseline_price,
    ROUND(avg_rrp, 2) AS avg_rrp,
    ROUND(avg_baseline_price - avg_promo_price, 2) AS avg_price_reduction,
    
    -- Efficiency Metrics
    ROUND(promo_revenue / NULLIF(promo_transactions, 0), 2) AS revenue_per_promo_transaction,
    ROUND(promo_revenue / NULLIF(promo_units_sold, 0), 2) AS revenue_per_promo_unit,
    ROUND(promo_revenue / NULLIF(stores_running_promos, 0), 2) AS revenue_per_promo_store,
    
    -- Incremental Metrics
    ROUND(promo_revenue - non_promo_revenue, 2) AS incremental_revenue,
    ROUND(promo_units_sold - non_promo_units_sold, 0) AS incremental_units,
    
    -- NEW: Additional Coverage KPIs for Cards
    stores_running_promos AS bidco_stores_with_promo,
    skus_on_promo AS bidco_skus_on_promo,
    
    -- Text labels for reference (optional)
    'Bidco Africa Limited' AS supplier_name,
    'All Categories' AS category_filter,
    'All Stores' AS store_filter

FROM bidco_metrics;

-- Create comment
COMMENT ON MATERIALIZED VIEW dw.v_bidco_promo_kpi_metrics IS 
'Bidco promo KPI metrics with NUMERIC columns for Superset Big Number cards.
Single row with all key metrics. Refresh after data loads.';
