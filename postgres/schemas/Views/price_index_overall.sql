-- =====================================================================
-- 📊 PRICE INDEX - OVERALL ROLLUP VIEW
-- =====================================================================
-- Purpose: Bidco's overall market positioning across all stores
-- Business Question: "Where does Bidco position overall - premium or discount?"
-- =====================================================================

DROP MATERIALIZED VIEW IF EXISTS dw.v_price_index_overall CASCADE;

CREATE MATERIALIZED VIEW dw.v_price_index_overall AS

WITH overall_metrics AS (
    SELECT
        i.sub_department,
        i.section,
        i.category,
        i.is_bidco,
        
        AVG(f.unit_price) AS avg_price,
        AVG(f.rrp) AS avg_rrp,
        COUNT(*) AS transactions,
        COUNT(DISTINCT st.store_id) AS store_count,
        COUNT(DISTINCT i.item_code) AS sku_count,
        SUM(f.total_sales) AS revenue
        
    FROM dw.fact_sales_enriched f
    JOIN dw.dim_item i ON f.item_id = i.item_id
    JOIN dw.dim_store st ON f.store_id = st.store_id
    GROUP BY i.sub_department, i.section, i.category, i.is_bidco
),

bidco_overall AS (
    SELECT
        sub_department,
        section,
        category,
        avg_price AS bidco_price,
        avg_rrp AS bidco_rrp,
        transactions AS bidco_txn,
        store_count AS bidco_stores,
        sku_count AS bidco_skus,
        revenue AS bidco_revenue
    FROM overall_metrics
    WHERE is_bidco = TRUE
),

competitor_overall AS (
    SELECT
        sub_department,
        section,
        category,
        AVG(avg_price) AS competitor_price,
        AVG(avg_rrp) AS competitor_rrp,
        SUM(transactions) AS competitor_txn,
        MAX(store_count) AS competitor_stores,
        SUM(sku_count) AS competitor_skus,
        SUM(revenue) AS competitor_revenue
    FROM overall_metrics
    WHERE is_bidco = FALSE
    GROUP BY sub_department, section, category
)

SELECT
    b.category,
    b.sub_department,
    b.section,
    
    -- Bidco metrics
    ROUND(b.bidco_price, 2) AS bidco_avg_price,
    ROUND(b.bidco_rrp, 2) AS bidco_avg_rrp,
    b.bidco_txn AS bidco_transactions,
    b.bidco_stores,
    b.bidco_skus,
    ROUND(b.bidco_revenue, 2) AS bidco_revenue,
    
    -- Competitor metrics
    ROUND(c.competitor_price, 2) AS competitor_avg_price,
    ROUND(c.competitor_rrp, 2) AS competitor_avg_rrp,
    c.competitor_txn AS competitor_transactions,
    c.competitor_stores,
    c.competitor_skus,
    ROUND(c.competitor_revenue, 2) AS competitor_revenue,
    
    -- Price index
    ROUND(b.bidco_price / NULLIF(c.competitor_price, 0), 4) AS price_index,
    
    -- Overall positioning
    CASE
        WHEN b.bidco_price / NULLIF(c.competitor_price, 0) > 1.10 THEN 'PREMIUM'
        WHEN b.bidco_price / NULLIF(c.competitor_price, 0) > 1.05 THEN 'SLIGHT PREMIUM'
        WHEN b.bidco_price / NULLIF(c.competitor_price, 0) >= 0.95 THEN 'AT MARKET'
        WHEN b.bidco_price / NULLIF(c.competitor_price, 0) >= 0.90 THEN 'SLIGHT DISCOUNT'
        ELSE 'DEEP DISCOUNT'
    END AS overall_positioning,
    
    -- Discount patterns
    ROUND(((b.bidco_rrp - b.bidco_price) / NULLIF(b.bidco_rrp, 0)) * 100, 2) AS bidco_discount_pct,
    ROUND(((c.competitor_rrp - c.competitor_price) / NULLIF(c.competitor_rrp, 0)) * 100, 2) AS competitor_discount_pct,
    
    -- Market share estimate (by transactions)
    ROUND((b.bidco_txn::NUMERIC / NULLIF(b.bidco_txn + c.competitor_txn, 0)) * 100, 2) AS bidco_txn_share_pct,
    
    -- Revenue share
    ROUND((b.bidco_revenue / NULLIF(b.bidco_revenue + c.competitor_revenue, 0)) * 100, 2) AS bidco_revenue_share_pct

FROM bidco_overall b
LEFT JOIN competitor_overall c 
    ON b.sub_department = c.sub_department 
    AND b.section = c.section
    AND b.category = c.category
WHERE c.competitor_price IS NOT NULL
ORDER BY b.category, b.sub_department, b.section;

-- Create indexes
CREATE INDEX idx_v_price_overall_category ON dw.v_price_index_overall(category);
CREATE INDEX idx_v_price_overall_subdept ON dw.v_price_index_overall(sub_department);
CREATE INDEX idx_v_price_overall_positioning ON dw.v_price_index_overall(overall_positioning);

COMMENT ON MATERIALIZED VIEW dw.v_price_index_overall IS 
'Overall price index rollup: Bidco vs market positioning across all stores.
Shows if Bidco is premium/discount and discounting patterns vs RRP.
Refresh: REFRESH MATERIALIZED VIEW dw.v_price_index_overall;';
