-- =====================================================================
-- 📋 BIDCO PROMO PERFORMANCE EXECUTIVE SUMMARY
-- =====================================================================
-- Purpose: Executive dashboard view combining key promo insights for Bidco
-- Business Question: "What are the top 3 commercial insights for Bidco?"
--
-- This view provides:
-- 1. Promo vs Non-Promo Revenue Comparison
-- 2. Category Performance Breakdown
-- 3. Store-level Promo Adoption
-- 4. Competitor Benchmarking (Bidco vs Others)
-- =====================================================================

DROP MATERIALIZED VIEW IF EXISTS dw.v_bidco_promo_executive_summary CASCADE;

CREATE MATERIALIZED VIEW dw.v_bidco_promo_executive_summary AS

-- =====================================================================
-- SECTION 1: Overall Bidco Promo Performance
-- =====================================================================
WITH bidco_overall AS (
    SELECT
        'Bidco Africa Limited' AS supplier_name,
        
        -- Total metrics
        COUNT(*) AS total_transactions,
        COUNT(CASE WHEN f.is_promo = TRUE THEN 1 END) AS promo_transactions,
        ROUND((COUNT(CASE WHEN f.is_promo = TRUE THEN 1 END)::NUMERIC / COUNT(*)) * 100, 2) AS promo_penetration_pct,
        
        -- Revenue metrics
        SUM(f.total_sales) AS total_revenue,
        SUM(CASE WHEN f.is_promo = TRUE THEN f.total_sales END) AS promo_revenue,
        SUM(CASE WHEN f.is_promo = FALSE THEN f.total_sales END) AS non_promo_revenue,
        ROUND((SUM(CASE WHEN f.is_promo = TRUE THEN f.total_sales END) / NULLIF(SUM(f.total_sales), 0)) * 100, 2) AS promo_revenue_share_pct,
        
        -- Units metrics
        SUM(f.quantity) AS total_units_sold,
        SUM(CASE WHEN f.is_promo = TRUE THEN f.quantity END) AS promo_units_sold,
        AVG(CASE WHEN f.is_promo = FALSE THEN f.quantity END) AS baseline_avg_units,
        AVG(CASE WHEN f.is_promo = TRUE THEN f.quantity END) AS promo_avg_units,
        
        -- SKU and store coverage
        COUNT(DISTINCT f.item_id) AS total_skus,
        COUNT(DISTINCT CASE WHEN f.is_promo = TRUE THEN f.item_id END) AS skus_on_promo,
        COUNT(DISTINCT f.store_id) AS total_stores,
        COUNT(DISTINCT CASE WHEN f.is_promo = TRUE THEN f.store_id END) AS stores_running_promos,
        
        -- Average discount
        AVG(CASE WHEN f.is_promo = TRUE THEN f.discount_pct END) AS avg_promo_discount_pct
        
    FROM dw.fact_sales_enriched f
    JOIN dw.dim_item i ON f.item_id = i.item_id
    WHERE i.is_bidco = TRUE
),

-- =====================================================================
-- SECTION 2: Category Performance for Bidco
-- =====================================================================
bidco_by_category AS (
    SELECT
        i.category,
        
        COUNT(CASE WHEN f.is_promo = TRUE THEN 1 END) AS promo_transactions,
        SUM(CASE WHEN f.is_promo = TRUE THEN f.total_sales END) AS promo_revenue,
        AVG(CASE WHEN f.is_promo = FALSE THEN f.quantity END) AS baseline_avg_units,
        AVG(CASE WHEN f.is_promo = TRUE THEN f.quantity END) AS promo_avg_units,
        
        RANK() OVER (ORDER BY SUM(CASE WHEN f.is_promo = TRUE THEN f.total_sales END) DESC) AS category_rank
        
    FROM dw.fact_sales_enriched f
    JOIN dw.dim_item i ON f.item_id = i.item_id
    WHERE i.is_bidco = TRUE AND f.is_promo = TRUE
    GROUP BY i.category
),

-- =====================================================================
-- SECTION 3: Bidco vs Competitor Benchmarking
-- =====================================================================
market_comparison AS (
    SELECT
        CASE WHEN i.is_bidco = TRUE THEN 'Bidco Africa' ELSE 'Competitors' END AS supplier_group,
        
        COUNT(CASE WHEN f.is_promo = TRUE THEN 1 END) AS promo_transactions,
        ROUND((COUNT(CASE WHEN f.is_promo = TRUE THEN 1 END)::NUMERIC / COUNT(*)) * 100, 2) AS promo_penetration_pct,
        
        SUM(CASE WHEN f.is_promo = TRUE THEN f.total_sales END) AS promo_revenue,
        AVG(CASE WHEN f.is_promo = TRUE THEN f.discount_pct END) AS avg_discount_pct,
        AVG(CASE WHEN f.is_promo = TRUE THEN f.quantity END) AS promo_avg_units
        
    FROM dw.fact_sales_enriched f
    JOIN dw.dim_item i ON f.item_id = i.item_id
    GROUP BY CASE WHEN i.is_bidco = TRUE THEN 'Bidco Africa' ELSE 'Competitors' END
)

-- =====================================================================
-- FINAL OUTPUT: Unified Executive Summary
-- =====================================================================
SELECT
    -- Section identifier
    '📊 OVERALL PERFORMANCE' AS metric_section,
    'Bidco Africa Limited' AS entity,
    
    -- Key metrics
    b.total_transactions::TEXT AS value_1,
    b.promo_transactions::TEXT AS value_2,
    ROUND(b.total_revenue, 2)::TEXT AS value_3,
    ROUND(b.promo_revenue, 2)::TEXT AS value_4,
    b.promo_penetration_pct::TEXT || '%' AS value_5,
    b.promo_revenue_share_pct::TEXT || '%' AS value_6,
    
    -- Descriptions
    'Total Transactions' AS label_1,
    'Promo Transactions' AS label_2,
    'Total Revenue' AS label_3,
    'Promo Revenue' AS label_4,
    'Promo Penetration %' AS label_5,
    'Promo Revenue Share %' AS label_6
    
FROM bidco_overall b

UNION ALL

SELECT
    '📈 UPLIFT METRICS',
    'Bidco Africa Limited',
    ROUND(b.baseline_avg_units, 2)::TEXT,
    ROUND(b.promo_avg_units, 2)::TEXT,
    ROUND(((b.promo_avg_units - b.baseline_avg_units) / NULLIF(b.baseline_avg_units, 0)) * 100, 2)::TEXT || '%',
    ROUND(b.avg_promo_discount_pct * 100, 2)::TEXT || '%',
    b.skus_on_promo::TEXT || '/' || b.total_skus::TEXT,
    b.stores_running_promos::TEXT || '/' || b.total_stores::TEXT,
    'Baseline Avg Units',
    'Promo Avg Units',
    'Units Uplift %',
    'Avg Discount %',
    'SKUs on Promo (of Total)',
    'Stores Running Promos (of Total)'
FROM bidco_overall b

UNION ALL

SELECT
    '🏆 TOP CATEGORY',
    category,
    promo_transactions::TEXT,
    ROUND(promo_revenue, 2)::TEXT,
    ROUND(baseline_avg_units, 2)::TEXT,
    ROUND(promo_avg_units, 2)::TEXT,
    ROUND(((promo_avg_units - baseline_avg_units) / NULLIF(baseline_avg_units, 0)) * 100, 2)::TEXT || '%',
    category_rank::TEXT,
    'Promo Transactions',
    'Promo Revenue',
    'Baseline Units',
    'Promo Units',
    'Uplift %',
    'Category Rank'
FROM bidco_by_category
WHERE category_rank = 1

UNION ALL

SELECT
    '📊 MARKET POSITION',
    mc.supplier_group,
    mc.promo_transactions::TEXT,
    mc.promo_penetration_pct::TEXT || '%',
    ROUND(mc.promo_revenue, 2)::TEXT,
    ROUND(mc.avg_discount_pct * 100, 2)::TEXT || '%',
    ROUND(mc.promo_avg_units, 2)::TEXT,
    CASE WHEN mc.supplier_group = 'Bidco Africa' THEN '🎯 Focus Brand' ELSE '🏪 Market Avg' END,
    'Promo Transactions',
    'Promo Penetration %',
    'Promo Revenue',
    'Avg Discount %',
    'Avg Units per Promo',
    'Market Position'
FROM market_comparison mc;

-- Create indexes
CREATE INDEX idx_v_bidco_executive_section ON dw.v_bidco_promo_executive_summary(metric_section);
CREATE INDEX idx_v_bidco_executive_entity ON dw.v_bidco_promo_executive_summary(entity);

COMMENT ON MATERIALIZED VIEW dw.v_bidco_promo_executive_summary IS 
'Executive summary of Bidco promo performance with market comparison.
Designed for non-technical stakeholders and Superset dashboards.
Refresh: Run REFRESH MATERIALIZED VIEW dw.v_bidco_promo_executive_summary; after data loads.';
