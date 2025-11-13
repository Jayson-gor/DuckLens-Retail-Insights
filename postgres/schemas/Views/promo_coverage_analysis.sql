-- =====================================================================
-- 🏪 PROMO COVERAGE ANALYSIS VIEW
-- =====================================================================
-- Purpose: Calculate promo coverage % (how many stores run each promo)
-- Business Question: "How widely distributed is each promo across stores?"
--
-- Metrics:
-- - Stores Running Promo: Count of unique stores with promo
-- - Total Stores Carrying SKU: Total stores that stock the item
-- - Coverage %: (Promo Stores / Total Stores) * 100
-- - Store Distribution: Which stores are running promos
-- =====================================================================

DROP MATERIALIZED VIEW IF EXISTS dw.v_promo_coverage_analysis CASCADE;

CREATE MATERIALIZED VIEW dw.v_promo_coverage_analysis AS

WITH sku_store_metrics AS (
    -- For each SKU, calculate store coverage
    SELECT
        i.item_code,
        i.description AS item_description,
        i.category,
        i.department,
        i.sub_department,
        i.section,
        s.supplier_name,
        i.is_bidco,
        
        -- Total stores carrying this SKU (promo or not)
        COUNT(DISTINCT st.store_id) AS total_stores_carrying_sku,
        
        -- Stores running promo for this SKU
        COUNT(DISTINCT CASE WHEN f.is_promo = TRUE THEN st.store_id END) AS stores_running_promo,
        
        -- Total promo transactions
        COUNT(CASE WHEN f.is_promo = TRUE THEN 1 END) AS promo_transactions,
        
        -- Total transactions (promo + non-promo)
        COUNT(*) AS total_transactions,
        
        -- Promo revenue
        SUM(CASE WHEN f.is_promo = TRUE THEN f.total_sales END) AS promo_revenue,
        SUM(f.total_sales) AS total_revenue,
        
        -- Average discount during promo
        AVG(CASE WHEN f.is_promo = TRUE THEN f.discount_pct END) AS avg_promo_discount_pct
        
    FROM dw.fact_sales_enriched f
    JOIN dw.dim_item i ON f.item_id = i.item_id
    JOIN dw.dim_supplier s ON f.supplier_id = s.supplier_id
    JOIN dw.dim_store st ON f.store_id = st.store_id
    GROUP BY 
        i.item_code, i.description, i.category, i.department,
        i.sub_department, i.section, s.supplier_name, i.is_bidco
),

store_list AS (
    -- Get list of stores running each promo
    SELECT
        i.item_code,
        STRING_AGG(DISTINCT st.store_name, ', ' ORDER BY st.store_name) AS stores_with_promo
    FROM dw.fact_sales_enriched f
    JOIN dw.dim_item i ON f.item_id = i.item_id
    JOIN dw.dim_store st ON f.store_id = st.store_id
    WHERE f.is_promo = TRUE
    GROUP BY i.item_code
)

SELECT
    m.item_code,
    m.item_description,
    m.category,
    m.department,
    m.sub_department,
    m.section,
    m.supplier_name,
    m.is_bidco,
    
    -- Coverage metrics
    m.stores_running_promo,
    m.total_stores_carrying_sku,
    ROUND((m.stores_running_promo::NUMERIC / NULLIF(m.total_stores_carrying_sku, 0)) * 100, 2) AS promo_coverage_pct,
    
    -- Transaction metrics
    m.promo_transactions,
    m.total_transactions,
    ROUND((m.promo_transactions::NUMERIC / NULLIF(m.total_transactions, 0)) * 100, 2) AS promo_transaction_share_pct,
    
    -- Revenue metrics
    ROUND(m.promo_revenue, 2) AS promo_revenue,
    ROUND(m.total_revenue, 2) AS total_revenue,
    ROUND((m.promo_revenue / NULLIF(m.total_revenue, 0)) * 100, 2) AS promo_revenue_share_pct,
    
    -- Discount metrics
    ROUND(m.avg_promo_discount_pct * 100, 2) AS avg_discount_pct,
    
    -- Store list
    sl.stores_with_promo,
    
    -- Coverage rating
    CASE
        WHEN m.stores_running_promo::NUMERIC / NULLIF(m.total_stores_carrying_sku, 0) >= 0.75 THEN '🌟 Wide Coverage (≥75%)'
        WHEN m.stores_running_promo::NUMERIC / NULLIF(m.total_stores_carrying_sku, 0) >= 0.50 THEN '📈 Good Coverage (50-74%)'
        WHEN m.stores_running_promo::NUMERIC / NULLIF(m.total_stores_carrying_sku, 0) >= 0.25 THEN '📊 Moderate Coverage (25-49%)'
        WHEN m.stores_running_promo::NUMERIC / NULLIF(m.total_stores_carrying_sku, 0) > 0 THEN '⚠️ Limited Coverage (<25%)'
        ELSE '❌ No Coverage'
    END AS coverage_rating

FROM sku_store_metrics m
LEFT JOIN store_list sl ON m.item_code = sl.item_code
WHERE m.stores_running_promo > 0  -- Only SKUs with promo activity
ORDER BY promo_coverage_pct DESC, promo_revenue DESC;

-- Create indexes
CREATE INDEX idx_v_promo_coverage_supplier ON dw.v_promo_coverage_analysis(supplier_name);
CREATE INDEX idx_v_promo_coverage_bidco ON dw.v_promo_coverage_analysis(is_bidco);
CREATE INDEX idx_v_promo_coverage_category ON dw.v_promo_coverage_analysis(category);
CREATE INDEX idx_v_promo_coverage_rating ON dw.v_promo_coverage_analysis(coverage_rating);

COMMENT ON MATERIALIZED VIEW dw.v_promo_coverage_analysis IS 
'Promo coverage analysis: how many stores run each promo, coverage %, and distribution.
Refresh: Run REFRESH MATERIALIZED VIEW dw.v_promo_coverage_analysis; after data loads.';
