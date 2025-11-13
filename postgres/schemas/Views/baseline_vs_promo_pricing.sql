-- =====================================================================
-- 📊 BASELINE VS PROMO PRICING COMPARISON VIEW
-- =====================================================================
-- Purpose: Side-by-side comparison of pricing and performance (baseline vs promo)
-- Business Question: "How do baseline and promo periods compare?"
--
-- Metrics:
-- - Baseline Price vs Promo Price
-- - Baseline Units vs Promo Units
-- - Baseline Revenue vs Promo Revenue
-- - Performance Delta (absolute and %)
-- =====================================================================

DROP MATERIALIZED VIEW IF EXISTS dw.v_baseline_vs_promo_pricing CASCADE;

CREATE MATERIALIZED VIEW dw.v_baseline_vs_promo_pricing AS

WITH baseline_metrics AS (
    -- Calculate baseline (non-promo) metrics
    SELECT
        i.item_code,
        i.description AS item_description,
        i.category,
        i.department,
        i.sub_department,
        i.section,
        s.supplier_name,
        i.is_bidco,
        
        -- Baseline pricing
        AVG(f.unit_price) AS baseline_avg_price,
        MIN(f.unit_price) AS baseline_min_price,
        MAX(f.unit_price) AS baseline_max_price,
        AVG(f.rrp) AS baseline_avg_rrp,
        
        -- Baseline units
        AVG(f.quantity) AS baseline_avg_units,
        SUM(f.quantity) AS baseline_total_units,
        
        -- Baseline revenue
        SUM(f.total_sales) AS baseline_revenue,
        COUNT(*) AS baseline_transactions,
        
        -- Baseline stores
        COUNT(DISTINCT f.store_id) AS baseline_stores
        
    FROM dw.fact_sales_enriched f
    JOIN dw.dim_item i ON f.item_id = i.item_id
    JOIN dw.dim_supplier s ON f.supplier_id = s.supplier_id
    WHERE f.is_promo = FALSE
    GROUP BY 
        i.item_code, i.description, i.category, i.department,
        i.sub_department, i.section, s.supplier_name, i.is_bidco
),

promo_metrics AS (
    -- Calculate promo metrics
    SELECT
        i.item_code,
        
        -- Promo pricing
        AVG(f.unit_price) AS promo_avg_price,
        MIN(f.unit_price) AS promo_min_price,
        MAX(f.unit_price) AS promo_max_price,
        AVG(f.rrp) AS promo_avg_rrp,
        AVG(f.discount_pct) AS promo_avg_discount_pct,
        
        -- Promo units
        AVG(f.quantity) AS promo_avg_units,
        SUM(f.quantity) AS promo_total_units,
        
        -- Promo revenue
        SUM(f.total_sales) AS promo_revenue,
        COUNT(*) AS promo_transactions,
        
        -- Promo stores
        COUNT(DISTINCT f.store_id) AS promo_stores
        
    FROM dw.fact_sales_enriched f
    JOIN dw.dim_item i ON f.item_id = i.item_id
    WHERE f.is_promo = TRUE
    GROUP BY i.item_code
)

SELECT
    b.item_code,
    b.item_description,
    b.category,
    b.department,
    b.sub_department,
    b.section,
    b.supplier_name,
    b.is_bidco,
    
    -- ========================================
    -- BASELINE METRICS
    -- ========================================
    ROUND(b.baseline_avg_price, 2) AS baseline_avg_price,
    ROUND(b.baseline_min_price, 2) AS baseline_min_price,
    ROUND(b.baseline_max_price, 2) AS baseline_max_price,
    ROUND(b.baseline_avg_units, 2) AS baseline_avg_units,
    b.baseline_total_units,
    ROUND(b.baseline_revenue, 2) AS baseline_revenue,
    b.baseline_transactions,
    b.baseline_stores,
    
    -- ========================================
    -- PROMO METRICS
    -- ========================================
    ROUND(p.promo_avg_price, 2) AS promo_avg_price,
    ROUND(p.promo_min_price, 2) AS promo_min_price,
    ROUND(p.promo_max_price, 2) AS promo_max_price,
    ROUND(p.promo_avg_units, 2) AS promo_avg_units,
    p.promo_total_units,
    ROUND(p.promo_revenue, 2) AS promo_revenue,
    p.promo_transactions,
    p.promo_stores,
    ROUND(p.promo_avg_discount_pct * 100, 2) AS promo_avg_discount_pct,
    
    -- ========================================
    -- COMPARISON METRICS
    -- ========================================
    
    -- Price delta
    ROUND(b.baseline_avg_price - p.promo_avg_price, 2) AS price_reduction,
    ROUND(((b.baseline_avg_price - p.promo_avg_price) / NULLIF(b.baseline_avg_price, 0)) * 100, 2) AS price_reduction_pct,
    
    -- Units delta
    ROUND(p.promo_avg_units - b.baseline_avg_units, 2) AS units_uplift,
    ROUND(((p.promo_avg_units - b.baseline_avg_units) / NULLIF(b.baseline_avg_units, 0)) * 100, 2) AS units_uplift_pct,
    
    -- Revenue delta
    ROUND(p.promo_revenue - b.baseline_revenue, 2) AS revenue_delta,
    ROUND(((p.promo_revenue - b.baseline_revenue) / NULLIF(b.baseline_revenue, 0)) * 100, 2) AS revenue_delta_pct,
    
    -- Store expansion
    p.promo_stores - b.baseline_stores AS store_expansion,
    ROUND(((p.promo_stores - b.baseline_stores)::NUMERIC / NULLIF(b.baseline_stores, 0)) * 100, 2) AS store_expansion_pct,
    
    -- ========================================
    -- BUSINESS INSIGHTS
    -- ========================================
    
    -- Promo efficiency score (revenue vs discount)
    CASE
        WHEN p.promo_revenue > b.baseline_revenue AND p.promo_avg_discount_pct >= 0.10 THEN 
            ROUND((p.promo_revenue / NULLIF(b.baseline_revenue, 0)) / (p.promo_avg_discount_pct * 100), 2)
        ELSE 0
    END AS promo_efficiency_score,
    
    -- Overall performance rating
    CASE
        WHEN p.promo_avg_units > b.baseline_avg_units * 1.5 AND p.promo_revenue > b.baseline_revenue THEN '🔥 Excellent Performance'
        WHEN p.promo_avg_units > b.baseline_avg_units * 1.2 AND p.promo_revenue > b.baseline_revenue * 0.9 THEN '⭐ Strong Performance'
        WHEN p.promo_avg_units > b.baseline_avg_units THEN '✅ Positive Performance'
        WHEN p.promo_revenue > b.baseline_revenue THEN '📊 Revenue Positive'
        ELSE '⚠️ Underperforming'
    END AS performance_rating,
    
    -- Recommendation
    CASE
        WHEN p.promo_avg_units > b.baseline_avg_units * 1.5 AND p.promo_stores < b.baseline_stores * 2 THEN '📈 Expand to more stores'
        WHEN p.promo_avg_discount_pct < 0.15 AND p.promo_avg_units > b.baseline_avg_units * 1.3 THEN '💰 Consider deeper discount'
        WHEN p.promo_avg_discount_pct >= 0.20 AND p.promo_avg_units < b.baseline_avg_units * 1.2 THEN '⚠️ Discount too deep'
        WHEN p.promo_revenue < b.baseline_revenue THEN '🛑 Review promo strategy'
        ELSE '✅ Continue current approach'
    END AS recommendation

FROM baseline_metrics b
INNER JOIN promo_metrics p ON b.item_code = p.item_code
WHERE p.promo_transactions >= 2  -- At least 2 promo transactions for meaningful comparison
ORDER BY units_uplift_pct DESC NULLS LAST;

-- Create indexes
CREATE INDEX idx_v_baseline_promo_supplier ON dw.v_baseline_vs_promo_pricing(supplier_name);
CREATE INDEX idx_v_baseline_promo_bidco ON dw.v_baseline_vs_promo_pricing(is_bidco);
CREATE INDEX idx_v_baseline_promo_category ON dw.v_baseline_vs_promo_pricing(category);
CREATE INDEX idx_v_baseline_promo_rating ON dw.v_baseline_vs_promo_pricing(performance_rating);

COMMENT ON MATERIALIZED VIEW dw.v_baseline_vs_promo_pricing IS 
'Side-by-side comparison of baseline vs promo performance with actionable recommendations.
Refresh: Run REFRESH MATERIALIZED VIEW dw.v_baseline_vs_promo_pricing; after data loads.';
