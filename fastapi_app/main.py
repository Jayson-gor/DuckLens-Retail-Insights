from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import os
import sqlalchemy as sa
from decimal import Decimal

app = FastAPI(
    title="🦆 DuckLens Retail Insights API",
    description="""
    ## 📊 Comprehensive Retail Analytics API
    
    **DuckLens** provides real-time insights into:
    - 📈 **Promotional Performance** - Track promo uplift, coverage, and ROI
    - 💰 **Price Index Analysis** - Compare Bidco vs competitors across stores
    - ✅ **Data Quality Monitoring** - Ensure data integrity and reliability
    
    ### 🎯 Key Features:
    - **Real-time KPIs** from PostgreSQL data warehouse
    - **Store-level granularity** for pricing and promo analysis
    - **Bidco-focused insights** for strategic decision-making
    
    ### 📚 Quick Start:
    1. Try `/promo_summary` for high-level promo KPIs
    2. Explore `/price_index/by_category` for pricing positioning
    3. Check `/data_quality` for data health metrics
    
    **Built with:** FastAPI + PostgreSQL + Docker
    """,
    version="2.0.0",
    contact={
        "name": "Jayson GOR",
        "email": "analytics@123.com"
    },
    license_info={
        "name": "Jayson GOR"
    }
)

# Enable CORS for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://user:password@db:5432/ducklens_db")
engine = sa.create_engine(DATABASE_URL)

# Helper function to convert Decimal to float
def decimal_to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    return obj

# =============================================================================
# RESPONSE MODELS
# =============================================================================

class KPIResponse(BaseModel):
    metric: str
    data: list

class HealthResponse(BaseModel):
    status: str
    database: str
    api_version: str

# =============================================================================
# ENDPOINTS
# =============================================================================

@app.get("/", tags=["Root"])
def root():
    """API Welcome Message"""
    return {
        "message": "Welcome to DuckLens Retail Insights API",
        "version": "2.0.0",
        "endpoints": {
            "health": "/health",
            "data_quality": "/data_quality",
            "promo_summary": "/promo_summary",
            "promo_kpis": "/promo_kpis",
            "price_index_store": "/price_index/store_level",
            "price_index_overall": "/price_index/overall",
            "price_index_by_category": "/price_index/by_category",
            "docs": "/docs"
        }
    }

@app.get("/health", response_model=HealthResponse, tags=["System"])
def health():
    """Health check endpoint"""
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
        db_status = "connected"
    except:
        db_status = "disconnected"
    
    return {
        "status": "ok",
        "database": db_status,
        "api_version": "2.0.0"
    }

# =============================================================================
# DATA QUALITY ENDPOINT
# =============================================================================

@app.get("/data_quality", response_model=KPIResponse, tags=["Data Quality"])
def data_quality():
    """
    Get overall data quality metrics
    
    Returns:
    - Total records
    - Data quality score
    - Issues breakdown (negatives, duplicates, missing values)
    - Entity reliability summary
    """
    with engine.connect() as conn:
        # Get overall metrics
        result = conn.exec_driver_sql("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN is_promo IS NULL THEN 1 END) as missing_promo_flag,
                COUNT(CASE WHEN quantity < 0 THEN 1 END) as negative_quantity,
                COUNT(CASE WHEN total_sales < 0 THEN 1 END) as negative_sales,
                ROUND(AVG(CASE 
                    WHEN quantity > 0 AND total_sales > 0 THEN 100.0 
                    ELSE 0 
                END), 2) as quality_score
            FROM dw.fact_sales_enriched
        """).mappings().fetchone()
        
        data = {
            "total_records": result['total_records'],
            "data_quality_score": float(result['quality_score']) if result['quality_score'] else 0,
            "issues": {
                "missing_promo_flag": result['missing_promo_flag'],
                "negative_quantity": result['negative_quantity'],
                "negative_sales": result['negative_sales']
            }
        }
    
    return {"metric": "data_quality", "data": [data]}

# =============================================================================
# PROMO SUMMARY ENDPOINT
# =============================================================================

@app.get("/promo_summary", tags=["📈 Promotions"])
def promo_summary():
    """
    ## 📊 Get Bidco Promotional Performance KPIs
    
    Returns comprehensive promo metrics for Bidco Africa products.
    
    ### 📌 Key Metrics:
    - **Promo Revenue**: Total revenue generated from promotions ($293,717)
    - **Promo Penetration**: % of transactions that are promotional (28.54%)
    - **Average Discount**: Typical discount depth during promos (17.25%)
    - **Store Coverage**: Number of stores running Bidco promos (35/35)
    - **SKU Coverage**: Number of SKUs on promotion (50/99)
    - **Units Uplift**: % increase in units sold during promos vs baseline (-6.51%)
    
    ### 💡 Business Use:
    - Monitor promo effectiveness at a glance
    - Track promotional penetration trends
    - Identify optimization opportunities
    
    ### 📊 Example Response:
    ```json
    {
        "promo_revenue": 293717.19,
        "promo_penetration_pct": 28.54,
        "avg_discount_pct": 17.25,
        "stores_with_promo": 35,
        "skus_on_promo": 50,
        "units_uplift_pct": -6.51
    }
    ```
    """
    with engine.connect() as conn:
        result = conn.exec_driver_sql("""
            SELECT 
                promo_revenue,
                promo_penetration_pct,
                avg_discount_pct,
                bidco_stores_with_promo as stores_with_promo,
                bidco_skus_on_promo as skus_on_promo,
                units_uplift_pct,
                total_stores,
                total_skus,
                promo_transactions,
                total_transactions
            FROM dw.v_bidco_promo_kpi_metrics
        """).mappings().fetchone()
        
        data = {k: decimal_to_float(v) for k, v in dict(result).items()}
    
    return data

# =============================================================================
# PROMO KPIs ENDPOINT (Top Performing SKUs)
# =============================================================================

@app.get("/promo_kpis", response_model=KPIResponse, tags=["Promotions"])
def promo_kpis(limit: int = Query(default=20, ge=1, le=100)):
    """
    Get top performing promo SKUs
    
    Parameters:
    - limit: Number of SKUs to return (default 20, max 100)
    
    Returns:
    - Top SKUs by performance score
    - Uplift %, coverage %, revenue
    - Performance tier
    """
    with engine.connect() as conn:
        res = conn.exec_driver_sql(f"""
            SELECT 
                item_code,
                item_description,
                category,
                uplift_pct,
                coverage_pct,
                promo_revenue,
                performance_score,
                performance_tier,
                overall_rank
            FROM dw.v_top_performing_skus
            WHERE is_bidco = TRUE
            ORDER BY performance_score DESC
            LIMIT {limit}
        """).mappings().all()
        
        data = [{k: decimal_to_float(v) for k, v in dict(r).items()} for r in res]
    
    return {"metric": "top_promo_skus", "data": data}

# =============================================================================
# PRICE INDEX - STORE LEVEL
# =============================================================================

@app.get("/price_index/store_level", tags=["💰 Price Index"])
def price_index_store_level(
    store: Optional[str] = Query(None, description="Filter by store name (e.g., 'Kilimani')"),
    sub_department: Optional[str] = Query(None, description="Filter by sub-department (e.g., 'Cooking Oil')"),
    positioning: Optional[str] = Query(None, description="Filter by positioning (PREMIUM, DISCOUNT, AT MARKET)"),
    limit: int = Query(default=50, ge=1, le=500, description="Number of records to return")
):
    """
    ## 🏪 Get Store-Level Price Index: Bidco vs Competitors
    
    Compare Bidco's prices against competitors at individual store + category level.
    
    ### 🔍 Query Parameters:
    - **store**: Filter by store name (optional)
    - **sub_department**: Filter by product sub-department (optional)
    - **positioning**: Filter by price position type (optional)
    - **limit**: Max records to return (default: 50, max: 500)
    
    ### 📊 Returns for Each Store/Category:
    - Bidco average price vs competitor average price
    - Price index (ratio)
    - Price positioning classification
    - Discount vs RRP patterns for both Bidco and competitors
    - Transaction counts
    
    ### 💡 Business Use Cases:
    - **Identify premium stores**: Where Bidco charges more than market
    - **Find discount opportunities**: Where Bidco is priced below competitors
    - **Optimize store-level pricing**: Adjust prices per location
    - **Competitive analysis**: See exact price differences per store
    
    ### 📈 Example Use:
    - `/price_index/store_level?store=Kilimani` - See Kilimani pricing
    - `/price_index/store_level?positioning=PREMIUM` - Find premium positions
    - `/price_index/store_level?sub_department=Cooking Oil` - Oil category analysis
    
    ### 📊 Example Response:
    ```json
    {
        "data": [
            {
                "store_name": "Kilimani",
                "sub_department": "Cooking Oil",
                "bidco_avg_price": 379.50,
                "competitor_avg_price": 425.80,
                "price_index": 0.89,
                "price_positioning": "SLIGHT DISCOUNT (5-10% below)",
                "price_difference_pct": -10.87
            }
        ]
    }
    ```
    """
    query = """
        SELECT 
            store_name,
            sub_department,
            section,
            bidco_avg_price,
            competitor_avg_price,
            price_index,
            price_positioning,
            bidco_discount_vs_rrp_pct,
            competitor_discount_vs_rrp_pct,
            price_difference,
            price_difference_pct,
            bidco_transactions,
            competitor_transactions
        FROM dw.v_price_index_store_level
        WHERE 1=1
    """
    
    params = []
    if store:
        query += " AND LOWER(store_name) LIKE LOWER(%s)"
        params.append(f"%{store}%")
    if sub_department:
        query += " AND LOWER(sub_department) LIKE LOWER(%s)"
        params.append(f"%{sub_department}%")
    if positioning:
        query += " AND LOWER(price_positioning) LIKE LOWER(%s)"
        params.append(f"%{positioning}%")
    
    query += f" ORDER BY price_index DESC LIMIT {limit}"
    
    with engine.connect() as conn:
        if params:
            res = conn.execute(sa.text(query), params).mappings().all()
        else:
            res = conn.exec_driver_sql(query).mappings().all()
        
        data = [{k: decimal_to_float(v) for k, v in dict(r).items()} for r in res]
    
    return {"data": data, "count": len(data)}

# =============================================================================
# PRICE INDEX - OVERALL ROLLUP
# =============================================================================

@app.get("/price_index/overall", tags=["Price Index"])
def price_index_overall(
    category: Optional[str] = Query(None, description="Filter by category")
):
    """
    Get overall price index rollup: Bidco's market positioning
    
    Parameters:
    - category: Filter by category (optional)
    
    Returns:
    - Overall Bidco vs market positioning
    - Price index across all stores
    - Discount patterns vs RRP
    - Market share estimates
    """
    query = """
        SELECT 
            category,
            sub_department,
            section,
            bidco_avg_price,
            competitor_avg_price,
            price_index,
            overall_positioning,
            bidco_discount_pct,
            competitor_discount_pct,
            bidco_txn_share_pct,
            bidco_revenue_share_pct,
            bidco_stores,
            competitor_stores
        FROM dw.v_price_index_overall
        WHERE 1=1
    """
    
    params = []
    if category:
        query += " AND LOWER(category) LIKE LOWER(%s)"
        params.append(f"%{category}%")
    
    # Order by a selected field to avoid runtime errors
    query += " ORDER BY bidco_revenue_share_pct DESC"
    
    with engine.connect() as conn:
        if params:
            res = conn.execute(sa.text(query), params).mappings().all()
        else:
            res = conn.exec_driver_sql(query).mappings().all()
        
        data = [{k: decimal_to_float(v) for k, v in dict(r).items()} for r in res]
    
    return {"data": data, "count": len(data)}

# =============================================================================
# PRICE INDEX - BY CATEGORY SUMMARY
# =============================================================================

@app.get("/price_index/by_category", tags=["💰 Price Index"])
def price_index_by_category():
    """
    ## 💰 Get Price Index Summary by Category
    
    Compare Bidco's pricing position vs competitors across product categories.
    
    ### 📊 Metrics Returned:
    - **Average Price Index**: Ratio of Bidco price to competitor price
      - `< 0.90` = **DEEP DISCOUNT** (>10% cheaper)
      - `0.90-0.95` = **SLIGHT DISCOUNT** (5-10% cheaper)
      - `0.95-1.05` = **AT MARKET** (±5%)
      - `1.05-1.10` = **SLIGHT PREMIUM** (5-10% more expensive)
      - `> 1.10` = **PREMIUM** (>10% more expensive)
    
    ### 🎯 Business Insights:
    - **Foods**: Deep discount positioning (0.80 index = 20% cheaper)
    - **Home Care**: Deep discount positioning (0.73 index = 27% cheaper)
    - **Personal Care**: Premium positioning (1.29 index = 29% more expensive)
    
    ### 💡 Use Cases:
    - Identify pricing strategies by category
    - Spot premium vs value positioning
    - Guide pricing decisions
    
    ### 📊 Example Response:
    ```json
    {
        "data": [
            {
                "category": "Foods",
                "avg_price_index": 0.8043,
                "overall_positioning": "DEEP DISCOUNT",
                "total_bidco_revenue": 569376.66
            }
        ]
    }
    ```
    """
    with engine.connect() as conn:
        res = conn.exec_driver_sql("""
            SELECT 
                category,
                COUNT(*) as segment_count,
                ROUND(AVG(price_index), 4) as avg_price_index,
                ROUND(AVG(bidco_avg_price), 2) as avg_bidco_price,
                ROUND(AVG(competitor_avg_price), 2) as avg_competitor_price,
                CASE
                    WHEN AVG(price_index) > 1.10 THEN 'PREMIUM'
                    WHEN AVG(price_index) > 1.05 THEN 'SLIGHT PREMIUM'
                    WHEN AVG(price_index) >= 0.95 THEN 'AT MARKET'
                    WHEN AVG(price_index) >= 0.90 THEN 'SLIGHT DISCOUNT'
                    ELSE 'DEEP DISCOUNT'
                END as overall_positioning,
                ROUND(SUM(bidco_revenue), 2) as total_bidco_revenue,
                ROUND(SUM(competitor_revenue), 2) as total_competitor_revenue
            FROM dw.v_price_index_overall
            GROUP BY category
            ORDER BY total_bidco_revenue DESC
        """).mappings().all()
        
        data = [{k: decimal_to_float(v) for k, v in dict(r).items()} for r in res]
    
    return {"data": data, "count": len(data)}
