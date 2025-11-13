-- =====================================================================
-- 💰 PRICE INDEX - STORE LEVEL VIEW
-- =====================================================================
-- Purpose: Compare Bidco vs competitors at store + sub-dept + section level
-- Business Question: "Is Bidco priced premium or discount vs competitors?"
-- =====================================================================

DROP MATERIALIZED VIEW IF EXISTS dw.v_price_index_store_level CASCADE;

CREATE MATERIALIZED VIEW dw.v_price_index_store_level AS

WITH store_pricing AS (
    SELECT
        st.store_name,
        i.sub_department,
        i.section,
        i.is_bidco,
        
        -- Average realized prices
        AVG(f.unit_price) AS avg_unit_price,
        AVG(f.rrp) AS avg_rrp,
        
        -- Transaction counts
        COUNT(*) AS transaction_count,
        COUNT(DISTINCT i.item_code) AS sku_count,
        
        -- Revenue
        SUM(f.total_sales) AS total_revenue
        
    FROM dw.fact_sales_enriched f
    JOIN dw.dim_item i ON f.item_id = i.item_id
    JOIN dw.dim_store st ON f.store_id = st.store_id
    WHERE i.sub_department IS NOT NULL 
      AND i.section IS NOT NULL
    GROUP BY 
        st.store_name,
        i.sub_department,
        i.section,
        i.is_bidco
),

bidco_prices AS (
    SELECT
        store_name,
        sub_department,
        section,
        avg_unit_price AS bidco_avg_price,
        avg_rrp AS bidco_avg_rrp,
        transaction_count AS bidco_transactions,
        sku_count AS bidco_sku_count,
        total_revenue AS bidco_revenue
    FROM store_pricing
    WHERE is_bidco = TRUE
),

competitor_prices AS (
    SELECT
        store_name,
        sub_department,
        section,
        AVG(avg_unit_price) AS competitor_avg_price,
        AVG(avg_rrp) AS competitor_avg_rrp,
        SUM(transaction_count) AS competitor_transactions,
        SUM(sku_count) AS competitor_sku_count,
        SUM(total_revenue) AS competitor_revenue
    FROM store_pricing
    WHERE is_bidco = FALSE
    GROUP BY store_name, sub_department, section
)

SELECT
    b.store_name,
    b.sub_department,
    b.section,
    
    -- Bidco metrics
    ROUND(b.bidco_avg_price, 2) AS bidco_avg_price,
    ROUND(b.bidco_avg_rrp, 2) AS bidco_avg_rrp,
    b.bidco_transactions,
    b.bidco_sku_count,
    ROUND(b.bidco_revenue, 2) AS bidco_revenue,
    
    -- Competitor metrics
    ROUND(c.competitor_avg_price, 2) AS competitor_avg_price,
    ROUND(c.competitor_avg_rrp, 2) AS competitor_avg_rrp,
    c.competitor_transactions,
    c.competitor_sku_count,
    ROUND(c.competitor_revenue, 2) AS competitor_revenue,
    
    -- Price index (Bidco vs Competitor)
    ROUND(b.bidco_avg_price / NULLIF(c.competitor_avg_price, 0), 4) AS price_index,
    
    -- Price positioning
    CASE
        WHEN b.bidco_avg_price / NULLIF(c.competitor_avg_price, 0) > 1.10 THEN 'PREMIUM (>10% above market)'
        WHEN b.bidco_avg_price / NULLIF(c.competitor_avg_price, 0) > 1.05 THEN 'SLIGHT PREMIUM (5-10% above)'
        WHEN b.bidco_avg_price / NULLIF(c.competitor_avg_price, 0) >= 0.95 THEN 'AT MARKET (±5%)'
        WHEN b.bidco_avg_price / NULLIF(c.competitor_avg_price, 0) >= 0.90 THEN 'SLIGHT DISCOUNT (5-10% below)'
        ELSE 'DEEP DISCOUNT (>10% below market)'
    END AS price_positioning,
    
    -- Discount vs RRP
    ROUND(((b.bidco_avg_rrp - b.bidco_avg_price) / NULLIF(b.bidco_avg_rrp, 0)) * 100, 2) AS bidco_discount_vs_rrp_pct,
    ROUND(((c.competitor_avg_rrp - c.competitor_avg_price) / NULLIF(c.competitor_avg_rrp, 0)) * 100, 2) AS competitor_discount_vs_rrp_pct,
    
    -- Competitive advantage
    ROUND(b.bidco_avg_price - c.competitor_avg_price, 2) AS price_difference,
    ROUND(((b.bidco_avg_price - c.competitor_avg_price) / NULLIF(c.competitor_avg_price, 0)) * 100, 2) AS price_difference_pct

FROM bidco_prices b
LEFT JOIN competitor_prices c 
    ON b.store_name = c.store_name 
    AND b.sub_department = c.sub_department
    AND b.section = c.section
WHERE c.competitor_avg_price IS NOT NULL  -- Only show where competitors exist
ORDER BY b.store_name, b.sub_department, b.section;

-- Create indexes
CREATE INDEX idx_v_price_index_store ON dw.v_price_index_store_level(store_name);
CREATE INDEX idx_v_price_index_subdept ON dw.v_price_index_store_level(sub_department);
CREATE INDEX idx_v_price_index_section ON dw.v_price_index_store_level(section);
CREATE INDEX idx_v_price_index_positioning ON dw.v_price_index_store_level(price_positioning);

COMMENT ON MATERIALIZED VIEW dw.v_price_index_store_level IS 
'Store-level price index: Bidco vs competitors by sub-department and section.
Shows pricing positioning (premium/discount) and realized vs RRP discounting patterns.
Refresh: REFRESH MATERIALIZED VIEW dw.v_price_index_store_level;';
