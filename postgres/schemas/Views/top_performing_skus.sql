-- =====================================================================
-- 🏆 TOP PERFORMING SKUs - PROMO LEADERBOARD
-- =====================================================================
-- Purpose: Rank SKUs by promo performance (uplift + coverage combined)
-- Business Question: "Which SKUs deliver the best promo results?"
--
-- Metrics:
-- - Performance Score: Weighted score (50% uplift + 30% coverage + 20% revenue)
-- - Rank: Overall ranking
-- - Key Metrics: Uplift %, Coverage %, Revenue, Discount %
-- =====================================================================

DROP MATERIALIZED VIEW IF EXISTS dw.v_top_performing_skus CASCADE;

CREATE MATERIALIZED VIEW dw.v_top_performing_skus AS

WITH performance_metrics AS (
    SELECT
        i.item_code,
        i.description AS item_description,
        i.category,
        i.department,
        i.sub_department,
        i.section,
        s.supplier_name,
        i.is_bidco,
        
        -- Uplift metrics
        AVG(CASE WHEN f.is_promo = FALSE THEN f.quantity END) AS baseline_avg_units,
        AVG(CASE WHEN f.is_promo = TRUE THEN f.quantity END) AS promo_avg_units,
        
        -- Coverage metrics
        COUNT(DISTINCT f.store_id) AS total_stores,
        COUNT(DISTINCT CASE WHEN f.is_promo = TRUE THEN f.store_id END) AS promo_stores,
        
        -- Revenue metrics
        SUM(CASE WHEN f.is_promo = TRUE THEN f.total_sales END) AS promo_revenue,
        SUM(f.total_sales) AS total_revenue,
        
        -- Transaction counts
        COUNT(CASE WHEN f.is_promo = TRUE THEN 1 END) AS promo_transactions,
        COUNT(*) AS total_transactions,
        
        -- Discount metrics
        AVG(CASE WHEN f.is_promo = TRUE THEN f.discount_pct END) AS avg_discount_pct,
        AVG(f.rrp) AS avg_rrp
        
    FROM dw.fact_sales_enriched f
    JOIN dw.dim_item i ON f.item_id = i.item_id
    JOIN dw.dim_supplier s ON f.supplier_id = s.supplier_id
    GROUP BY 
        i.item_code, i.description, i.category, i.department,
        i.sub_department, i.section, s.supplier_name, i.is_bidco
    HAVING COUNT(CASE WHEN f.is_promo = TRUE THEN 1 END) >= 2  -- At least 2 promo transactions
),

calculated_scores AS (
    SELECT
        *,
        
        -- Calculate individual metrics
        CASE 
            WHEN baseline_avg_units > 0 AND promo_avg_units IS NOT NULL THEN
                ((promo_avg_units - baseline_avg_units) / baseline_avg_units) * 100
            ELSE 0
        END AS uplift_pct,
        
        (promo_stores::NUMERIC / NULLIF(total_stores, 0)) * 100 AS coverage_pct,
        
        (promo_revenue / NULLIF(total_revenue, 0)) * 100 AS revenue_contribution_pct,
        
        -- Normalize metrics to 0-100 scale for scoring
        -- Uplift score (cap at 200% = 100 points)
        LEAST(
            CASE 
                WHEN baseline_avg_units > 0 AND promo_avg_units IS NOT NULL THEN
                    ((promo_avg_units - baseline_avg_units) / baseline_avg_units) * 50
                ELSE 0
            END, 100
        ) AS uplift_score,
        
        -- Coverage score (already 0-100%)
        (promo_stores::NUMERIC / NULLIF(total_stores, 0)) * 100 AS coverage_score,
        
        -- Revenue score (cap at 50% contribution = 100 points)
        LEAST(
            (promo_revenue / NULLIF(total_revenue, 0)) * 200,
            100
        ) AS revenue_score
        
    FROM performance_metrics
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
    
    -- Core performance metrics
    ROUND(baseline_avg_units, 2) AS baseline_avg_units,
    ROUND(promo_avg_units, 2) AS promo_avg_units,
    ROUND(uplift_pct, 2) AS uplift_pct,
    
    promo_stores,
    total_stores,
    ROUND(coverage_pct, 2) AS coverage_pct,
    
    ROUND(promo_revenue, 2) AS promo_revenue,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(revenue_contribution_pct, 2) AS revenue_contribution_pct,
    
    promo_transactions,
    total_transactions,
    ROUND(avg_discount_pct * 100, 2) AS avg_discount_pct,
    ROUND(avg_rrp, 2) AS avg_rrp,
    
    -- Performance scoring
    ROUND(uplift_score, 2) AS uplift_score,
    ROUND(coverage_score, 2) AS coverage_score,
    ROUND(revenue_score, 2) AS revenue_score,
    
    -- Composite performance score (weighted)
    ROUND(
        (uplift_score * 0.50) +      -- 50% weight on uplift
        (coverage_score * 0.30) +    -- 30% weight on coverage
        (revenue_score * 0.20),      -- 20% weight on revenue
        2
    ) AS performance_score,
    
    -- Performance tier
    CASE
        WHEN ROUND((uplift_score * 0.50) + (coverage_score * 0.30) + (revenue_score * 0.20), 2) >= 80 THEN '🥇 Elite Performer'
        WHEN ROUND((uplift_score * 0.50) + (coverage_score * 0.30) + (revenue_score * 0.20), 2) >= 60 THEN '🥈 Top Performer'
        WHEN ROUND((uplift_score * 0.50) + (coverage_score * 0.30) + (revenue_score * 0.20), 2) >= 40 THEN '🥉 Strong Performer'
        WHEN ROUND((uplift_score * 0.50) + (coverage_score * 0.30) + (revenue_score * 0.20), 2) >= 20 THEN '📊 Average Performer'
        ELSE '⚠️ Low Performer'
    END AS performance_tier,
    
    -- Ranking within supplier
    RANK() OVER (
        PARTITION BY supplier_name 
        ORDER BY 
            (uplift_score * 0.50) + (coverage_score * 0.30) + (revenue_score * 0.20) DESC
    ) AS rank_within_supplier,
    
    -- Overall ranking
    RANK() OVER (
        ORDER BY 
            (uplift_score * 0.50) + (coverage_score * 0.30) + (revenue_score * 0.20) DESC
    ) AS overall_rank

FROM calculated_scores
ORDER BY performance_score DESC, promo_revenue DESC;

-- Create indexes
CREATE INDEX idx_v_top_performing_supplier ON dw.v_top_performing_skus(supplier_name);
CREATE INDEX idx_v_top_performing_bidco ON dw.v_top_performing_skus(is_bidco);
CREATE INDEX idx_v_top_performing_category ON dw.v_top_performing_skus(category);
CREATE INDEX idx_v_top_performing_tier ON dw.v_top_performing_skus(performance_tier);
CREATE INDEX idx_v_top_performing_score ON dw.v_top_performing_skus(performance_score DESC);

COMMENT ON MATERIALIZED VIEW dw.v_top_performing_skus IS 
'Top performing SKUs ranked by composite score (50% uplift + 30% coverage + 20% revenue).
Includes overall and supplier-specific rankings.
Refresh: Run REFRESH MATERIALIZED VIEW dw.v_top_performing_skus; after data loads.';
