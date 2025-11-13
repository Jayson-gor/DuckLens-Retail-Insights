-- =============================================
-- DATA HEALTH SCORECARD: Executive Summary
-- Simple health scores per store/supplier with key issues summarized
-- One-page view for management reporting
-- =============================================

CREATE OR REPLACE VIEW dw.v_data_health_scorecard AS

-- ============================================
-- PART 1: STORE HEALTH SCORES
-- ============================================
WITH store_scores AS (
    SELECT
        'STORE' AS entity_type,
        store_name AS entity_name,
        total_transactions,
        total_revenue,
        reliability_score,
        reliability_status,
        total_risk_flags,
        CASE 
            WHEN total_risk_flags = 0 THEN 'No issues detected'
            ELSE primary_issue 
        END AS key_issue,
        critical_quality_issues,
        negative_values,
        extreme_pricing,
        -- Risk level for sorting
        CASE 
            WHEN total_risk_flags >= 2 THEN 1
            WHEN total_risk_flags = 1 THEN 2
            ELSE 3
        END AS risk_level
    FROM dw.v_unreliable_stores_analysis
),

-- ============================================
-- PART 2: SUPPLIER HEALTH SCORES
-- ============================================
supplier_scores AS (
    SELECT
        'SUPPLIER' AS entity_type,
        supplier_name AS entity_name,
        total_transactions,
        total_revenue,
        reliability_score,
        reliability_status,
        total_risk_flags,
        CASE 
            WHEN total_risk_flags = 0 THEN 'No issues detected'
            WHEN issues_detected = '' THEN 'No issues detected'
            ELSE issues_detected 
        END AS key_issue,
        critical_quality_issues,
        negative_values,
        extreme_pricing,
        -- Risk level for sorting
        CASE 
            WHEN total_risk_flags >= 3 THEN 1
            WHEN total_risk_flags = 2 THEN 2
            WHEN total_risk_flags = 1 THEN 3
            ELSE 4
        END AS risk_level
    FROM dw.v_unreliable_suppliers_analysis
),

-- ============================================
-- PART 3: COMBINE AND RANK
-- ============================================
combined AS (
    SELECT * FROM store_scores
    UNION ALL
    SELECT * FROM supplier_scores
)

-- ============================================
-- FINAL OUTPUT: SORTED BY RISK
-- ============================================
SELECT
    entity_type,
    entity_name,
    total_transactions,
    total_revenue,
    reliability_score,
    CASE 
        WHEN reliability_status LIKE '%UNRELIABLE%' THEN '🔴 CRITICAL'
        WHEN reliability_status LIKE '%HIGH RISK%' THEN '🟠 HIGH'
        WHEN reliability_status LIKE '%MEDIUM RISK%' OR reliability_status LIKE '%MONITOR%' THEN '🟡 MEDIUM'
        WHEN reliability_status LIKE '%LOW RISK%' THEN '🟢 LOW'
        ELSE '✅ NONE'
    END AS risk_level,
    key_issue,
    critical_quality_issues AS critical_issues,
    negative_values AS negative_records,
    extreme_pricing AS pricing_issues,
    total_risk_flags,
    -- Grade the score
    CASE 
        WHEN CAST(SPLIT_PART(reliability_score, ' ', 1) AS NUMERIC) >= 99.5 THEN 'A+'
        WHEN CAST(SPLIT_PART(reliability_score, ' ', 1) AS NUMERIC) >= 99.0 THEN 'A'
        WHEN CAST(SPLIT_PART(reliability_score, ' ', 1) AS NUMERIC) >= 98.0 THEN 'B+'
        WHEN CAST(SPLIT_PART(reliability_score, ' ', 1) AS NUMERIC) >= 95.0 THEN 'B'
        WHEN CAST(SPLIT_PART(reliability_score, ' ', 1) AS NUMERIC) >= 90.0 THEN 'C'
        WHEN CAST(SPLIT_PART(reliability_score, ' ', 1) AS NUMERIC) >= 80.0 THEN 'D'
        ELSE 'F'
    END AS grade
FROM combined
ORDER BY 
    entity_type DESC,  -- Suppliers first, then stores
    risk_level ASC,    -- Highest risk first
    CAST(SPLIT_PART(reliability_score, ' ', 1) AS NUMERIC) ASC,  -- Lowest score first
    total_transactions DESC;  -- Highest volume first
