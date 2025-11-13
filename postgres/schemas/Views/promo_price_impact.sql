-- =====================================================================
-- 💰 PROMO PRICE IMPACT VIEW
-- =====================================================================
-- Purpose: Analyze discount depth and pricing impact during promos
-- Business Question: "How deep are discounts, and what's the price impact?"
--
-- Metrics:
-- - Discount Depth %: Average discount vs RRP during promos
-- - Price Reduction: Absolute $ amount of discount
-- - RRP vs Promo Price: Price comparison
-- - Discount Distribution: Min/Max/Avg discount levels
-- =====================================================================

DROP MATERIALIZED VIEW IF EXISTS dw.v_promo_price_impact CASCADE;

CREATE MATERIALIZED VIEW dw.v_promo_price_impact AS

WITH promo_pricing AS (
    -- Calculate pricing metrics for promo transactions
    SELECT
        i.item_code,
        i.description AS item_description,
        i.category,
        i.department,
        i.sub_department,
        i.section,
        s.supplier_name,
        i.is_bidco,
        
        -- RRP metrics
        AVG(f.rrp) AS avg_rrp,
        MIN(f.rrp) AS min_rrp,
        MAX(f.rrp) AS max_rrp,
        
        -- Promo pricing
        AVG(CASE WHEN f.is_promo = TRUE THEN f.unit_price END) AS avg_promo_price,
        MIN(CASE WHEN f.is_promo = TRUE THEN f.unit_price END) AS min_promo_price,
        MAX(CASE WHEN f.is_promo = TRUE THEN f.unit_price END) AS max_promo_price,
        
        -- Non-promo pricing (baseline)
        AVG(CASE WHEN f.is_promo = FALSE THEN f.unit_price END) AS avg_baseline_price,
        
        -- Discount metrics
        AVG(CASE WHEN f.is_promo = TRUE THEN f.discount_pct END) AS avg_discount_pct,
        MIN(CASE WHEN f.is_promo = TRUE THEN f.discount_pct END) AS min_discount_pct,
        MAX(CASE WHEN f.is_promo = TRUE THEN f.discount_pct END) AS max_discount_pct,
        
        -- Transaction counts
        COUNT(CASE WHEN f.is_promo = TRUE THEN 1 END) AS promo_transactions,
        COUNT(*) AS total_transactions,
        
        -- Revenue impact
        SUM(CASE WHEN f.is_promo = TRUE THEN f.total_sales END) AS promo_revenue,
        SUM(CASE WHEN f.is_promo = TRUE THEN f.quantity END) AS promo_units_sold,
        
        -- Units metrics
        AVG(CASE WHEN f.is_promo = TRUE THEN f.quantity END) AS avg_promo_units,
        AVG(CASE WHEN f.is_promo = FALSE THEN f.quantity END) AS avg_baseline_units
        
    FROM dw.fact_sales_enriched f
    JOIN dw.dim_item i ON f.item_id = i.item_id
    JOIN dw.dim_supplier s ON f.supplier_id = s.supplier_id
    WHERE f.is_promo = TRUE  -- Focus on promo transactions only
    GROUP BY 
        i.item_code, i.description, i.category, i.department,
        i.sub_department, i.section, s.supplier_name, i.is_bidco
)

SELECT
    item_code,
    item_description,
    category,
    department,
    sub_department,
    section,
    supplier_name,
    is_bidco,
    
    -- RRP reference
    ROUND(avg_rrp, 2) AS avg_rrp,
    ROUND(min_rrp, 2) AS min_rrp,
    ROUND(max_rrp, 2) AS max_rrp,
    
    -- Promo pricing
    ROUND(avg_promo_price, 2) AS avg_promo_price,
    ROUND(min_promo_price, 2) AS min_promo_price,
    ROUND(max_promo_price, 2) AS max_promo_price,
    
    -- Baseline pricing
    ROUND(avg_baseline_price, 2) AS avg_baseline_price,
    
    -- Price reduction (absolute $)
    ROUND(avg_rrp - avg_promo_price, 2) AS price_reduction_vs_rrp,
    ROUND(avg_baseline_price - avg_promo_price, 2) AS price_reduction_vs_baseline,
    
    -- Discount depth %
    ROUND(avg_discount_pct * 100, 2) AS avg_discount_pct,
    ROUND(min_discount_pct * 100, 2) AS min_discount_pct,
    ROUND(max_discount_pct * 100, 2) AS max_discount_pct,
    
    -- Transaction metrics
    promo_transactions,
    total_transactions,
    ROUND((promo_transactions::NUMERIC / NULLIF(total_transactions, 0)) * 100, 2) AS promo_frequency_pct,
    
    -- Revenue and units
    ROUND(promo_revenue, 2) AS promo_revenue,
    promo_units_sold,
    ROUND(avg_promo_units, 2) AS avg_promo_units,
    ROUND(avg_baseline_units, 2) AS avg_baseline_units,
    
    -- Revenue per unit during promo
    ROUND(promo_revenue / NULLIF(promo_units_sold, 0), 2) AS revenue_per_unit,
    
    -- Discount strategy classification
    CASE
        WHEN avg_discount_pct >= 0.25 THEN '🔥 Deep Discount (≥25%)'
        WHEN avg_discount_pct >= 0.15 THEN '💰 Strong Discount (15-24%)'
        WHEN avg_discount_pct >= 0.10 THEN '✅ Standard Discount (10-14%)'
        WHEN avg_discount_pct > 0 THEN '📊 Minimal Discount (<10%)'
        ELSE '⚠️ No Discount'
    END AS discount_strategy,
    
    -- Effectiveness rating (units uplift + discount depth balance)
    CASE
        WHEN avg_promo_units > avg_baseline_units * 1.5 AND avg_discount_pct >= 0.15 THEN '⭐⭐⭐ Highly Effective'
        WHEN avg_promo_units > avg_baseline_units * 1.2 AND avg_discount_pct >= 0.10 THEN '⭐⭐ Effective'
        WHEN avg_promo_units > avg_baseline_units THEN '⭐ Moderately Effective'
        ELSE '⚠️ Low Effectiveness'
    END AS promo_effectiveness

FROM promo_pricing
ORDER BY avg_discount_pct DESC, promo_revenue DESC;

-- Create indexes
CREATE INDEX idx_v_promo_price_supplier ON dw.v_promo_price_impact(supplier_name);
CREATE INDEX idx_v_promo_price_bidco ON dw.v_promo_price_impact(is_bidco);
CREATE INDEX idx_v_promo_price_category ON dw.v_promo_price_impact(category);
CREATE INDEX idx_v_promo_price_strategy ON dw.v_promo_price_impact(discount_strategy);

COMMENT ON MATERIALIZED VIEW dw.v_promo_price_impact IS 
'Promo price impact analysis: discount depth, price reduction, and effectiveness ratings.
Refresh: Run REFRESH MATERIALIZED VIEW dw.v_promo_price_impact; after data loads.';
