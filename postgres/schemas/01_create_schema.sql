-- DuckLens Retail Insights Database Schema
-- Auto-executed on container startup via docker-entrypoint-initdb.d/

-- ============================================================================
-- CREATE SCHEMAS
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dw;

-- ============================================================================
-- STAGING SCHEMA
-- Purpose: Raw, untrusted data dump (as-is from Excel/Airbyte)
-- ============================================================================

CREATE TABLE IF NOT EXISTS staging.stg_sales_raw (
    store_name TEXT,
    item_code TEXT,
    item_barcode TEXT,
    description TEXT,
    category TEXT,
    department TEXT,
    sub_department TEXT,
    section TEXT,
    quantity NUMERIC,
    total_sales NUMERIC,
    rrp NUMERIC,
    supplier TEXT,
    date_of_sale DATE
);

-- ============================================================================
-- DATA WAREHOUSE SCHEMA - DIMENSION TABLES
-- ============================================================================

CREATE TABLE IF NOT EXISTS dw.dim_store (
    store_id SERIAL PRIMARY KEY,
    store_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dw.dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE,
    year INT,
    month INT,
    day INT,
    weekday_name TEXT,
    is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS dw.dim_item (
    item_id SERIAL PRIMARY KEY,
    item_code TEXT,
    item_barcode TEXT,
    description TEXT,
    category TEXT,
    department TEXT,
    sub_department TEXT,
    section TEXT,
    is_bidco BOOLEAN
);

-- ============================================================================
-- DATA WAREHOUSE SCHEMA - FACT TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS dw.fact_sales_enriched (
    sale_id BIGSERIAL PRIMARY KEY,
    store_id INT REFERENCES dw.dim_store(store_id),
    item_id INT REFERENCES dw.dim_item(item_id),
    supplier_id INT REFERENCES dw.dim_supplier(supplier_id),
    date_id INT REFERENCES dw.dim_date(date_id),
    quantity INT,
    total_sales NUMERIC(12,2),
    rrp NUMERIC(10,2),
    unit_price NUMERIC(10,2),
    discount_pct NUMERIC(5,4),
    is_promo BOOLEAN,
    baseline_units NUMERIC(10,2),
    promo_uplift_pct NUMERIC(6,2),
    price_index_vs_comp NUMERIC(6,4),
    data_quality_flag TEXT
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Fact table indexes
CREATE INDEX IF NOT EXISTS idx_fact_sales_store_id ON dw.fact_sales_enriched(store_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_item_id ON dw.fact_sales_enriched(item_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_supplier_id ON dw.fact_sales_enriched(supplier_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date_id ON dw.fact_sales_enriched(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_is_promo ON dw.fact_sales_enriched(is_promo);

-- Staging table indexes
CREATE INDEX IF NOT EXISTS idx_staging_date ON staging.stg_sales_raw(date_of_sale);
CREATE INDEX IF NOT EXISTS idx_staging_store ON staging.stg_sales_raw(store_name);

-- ============================================================================
-- GRANTS (if needed for other users)
-- ============================================================================

GRANT USAGE ON SCHEMA staging TO user;
GRANT USAGE ON SCHEMA dw TO user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dw TO user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA dw TO user;
