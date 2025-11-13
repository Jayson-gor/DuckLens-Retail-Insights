-- =====================================================================
-- 📈 PROMO UPLIFT SUMMARY VIEW
-- =====================================================================
-- Purpose: Calculate promo uplift % (units sold during promo vs baseline)
-- Business Question: "Which SKUs see the biggest sales lift during promos?"
--
-- Metrics:
-- - Baseline Units: Average quantity sold when NOT on promo
-- - Promo Units: Average quantity sold when on promo
-- - Uplift %: (Promo - Baseline) / Baseline * 100
-- - Promo Transactions: Count of promo sales
-- - Total Revenue Impact: Revenue generated during promos
-- =====================================================================

DROP MATERIALIZED VIEW IF EXISTS dw.v_promo_uplift_summary CASCADE;

CREATE MATERIALIZED VIEW dw.v_promo_uplift_summary AS

WITH promo_stats AS (
    -- Calculate promo performance metrics per SKU
    SELECT
        i.item_code,
        i.description AS item_description,
        i.category,
        i.department,
        i.sub_department,
        i.section,
        s.supplier_name,
        i.is_bidco,
        
        -- Baseline metrics (non-promo)
        AVG(CASE WHEN f.is_promo = FALSE THEN f.quantity END) AS baseline_avg_units,
        COUNT(CASE WHEN f.is_promo = FALSE THEN 1 END) AS baseline_transactions,
        SUM(CASE WHEN f.is_promo = FALSE THEN f.total_sales END) AS baseline_revenue,
        
        -- Promo metrics
        AVG(CASE WHEN f.is_promo = TRUE THEN f.quantity END) AS promo_avg_units,
        COUNT(CASE WHEN f.is_promo = TRUE THEN 1 END) AS promo_transactions,
        SUM(CASE WHEN f.is_promo = TRUE THEN f.total_sales END) AS promo_revenue,
        
        -- Average prices
        AVG(CASE WHEN f.is_promo = FALSE THEN f.unit_price END) AS baseline_avg_price,
        AVG(CASE WHEN f.is_promo = TRUE THEN f.unit_price END) AS promo_avg_price,
        AVG(f.rrp) AS avg_rrp,
        
        -- Discount depth during promos
        AVG(CASE WHEN f.is_promo = TRUE THEN f.discount_pct END) AS avg_promo_discount_pct,
        
        -- Total metrics
        COUNT(*) AS total_transactions,
        SUM(f.total_sales) AS total_revenue
        
    FROM dw.fact_sales_enriched f
    JOIN dw.dim_item i ON f.item_id = i.item_id
    JOIN dw.dim_supplier s ON f.supplier_id = s.supplier_id
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
    
    -- Baseline metrics
    ROUND(baseline_avg_units, 2) AS baseline_avg_units,
    baseline_transactions,
    ROUND(baseline_revenue, 2) AS baseline_revenue,
    ROUND(baseline_avg_price, 2) AS baseline_avg_price,
    
    -- Promo metrics
    ROUND(promo_avg_units, 2) AS promo_avg_units,
    promo_transactions,
    ROUND(promo_revenue, 2) AS promo_revenue,
    ROUND(promo_avg_price, 2) AS promo_avg_price,
    
    -- Uplift calculation
    CASE 
        WHEN baseline_avg_units > 0 AND promo_avg_units IS NOT NULL THEN
            ROUND(((promo_avg_units - baseline_avg_units) / baseline_avg_units) * 100, 2)
        ELSE 0
    END AS promo_uplift_pct,
    
    -- Revenue impact
    ROUND(promo_revenue - baseline_revenue, 2) AS incremental_revenue,
    ROUND((promo_revenue / NULLIF(total_revenue, 0)) * 100, 2) AS promo_revenue_share_pct,
    
    -- Discount metrics
    ROUND(avg_promo_discount_pct * 100, 2) AS avg_discount_pct,
    ROUND(avg_rrp, 2) AS avg_rrp,
    
    -- Coverage metrics (will calculate in next view, placeholder here)
    total_transactions,
    ROUND(total_revenue, 2) AS total_revenue,
    
    -- Performance flag
    CASE
        WHEN promo_transactions >= 10 AND promo_avg_units > baseline_avg_units * 1.5 THEN '🔥 High Performer'
        WHEN promo_transactions >= 5 AND promo_avg_units > baseline_avg_units * 1.2 THEN '⭐ Strong Performer'
        WHEN promo_transactions >= 3 AND promo_avg_units > baseline_avg_units THEN '✅ Moderate Uplift'
        WHEN promo_transactions > 0 THEN '⚠️ Low Impact'
        ELSE '❌ No Promo Activity'
    END AS performance_flag

FROM promo_stats
WHERE promo_transactions > 0  -- Only SKUs that had promos
ORDER BY promo_uplift_pct DESC NULLS LAST;

-- Create index for faster queries
CREATE INDEX idx_v_promo_uplift_supplier ON dw.v_promo_uplift_summary(supplier_name);
CREATE INDEX idx_v_promo_uplift_bidco ON dw.v_promo_uplift_summary(is_bidco);
CREATE INDEX idx_v_promo_uplift_category ON dw.v_promo_uplift_summary(category);
CREATE INDEX idx_v_promo_uplift_performance ON dw.v_promo_uplift_summary(performance_flag);

COMMENT ON MATERIALIZED VIEW dw.v_promo_uplift_summary IS 
'Promo uplift analysis per SKU: baseline vs promo performance, revenue impact, and uplift %.
Refresh: Run REFRESH MATERIALIZED VIEW dw.v_promo_uplift_summary; after data loads.';
