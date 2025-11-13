# 🦆 DuckLens Retail Insights - Data Pipeline Architecture

**Project:** Retail Analytics & Competitive Intelligence Platform  
**Client:** Bidco Africa  
**Data Engineer:** Jayson Gor  
**Last Updated:** November 13, 2025

---

## 📊 **PIPELINE OVERVIEW**

This is an end-to-end data engineering solution that transforms raw retail transaction data into actionable business intelligence for promotional analysis and competitive pricing insights.

---

## 🏗️ **ARCHITECTURE DIAGRAM**

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DUCKLENS DATA PIPELINE                             │
│                     (Dockerized Retail Analytics Platform)                   │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────┐
│   1️⃣  DATA SOURCE         │
│                          │
│  📄 Test_Data.xlsx       │
│  ├─ Transactions         │
│  ├─ Store Info           │
│  ├─ Supplier Info        │
│  └─ Item Catalog         │
│                          │
│  30,088 Records          │
│  190 Suppliers           │
│  61 Stores               │
└──────────┬───────────────┘
           │
           │ Extract (pandas.read_excel)
           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│   2️⃣  PYTHON ETL PIPELINE (ducklens_pipeline/)                               │
│                                                                               │
│   ┌─────────────────┐    ┌─────────────────┐    ┌──────────────────┐       │
│   │ 01_cleaning.py  │───▶│ 02_transform.py │───▶│ 03_loading.py    │       │
│   └─────────────────┘    └─────────────────┘    └──────────────────┘       │
│                                                                               │
│   📌 STAGE 1: CLEANING                                                        │
│   ├─ Standardize text (lowercase, strip whitespace)                          │
│   ├─ Remove special characters                                               │
│   ├─ Convert data types (dates, decimals, integers)                          │
│   ├─ Handle missing values                                                   │
│   └─ Preserve supplier names (Bug Fix: Line 88)                              │
│                                                                               │
│   📌 STAGE 2: TRANSFORMATION                                                  │
│   ├─ detect_promos() → Flag promo transactions (≥10% discount, ≥2 days)      │
│   ├─ calculate_uplift() → Promo vs Baseline units sold comparison            │
│   ├─ calculate_price_index() → Bidco vs Competitor pricing                   │
│   └─ calculate_promo_coverage() → Store penetration per SKU                  │
│                                                                               │
│   📌 STAGE 3: LOADING                                                         │
│   └─ Bulk insert to PostgreSQL using psycopg2                                │
│                                                                               │
│   🐍 Python 3.11 | pandas | numpy | psycopg2                                 │
└───────────────────────────────────────┬───────────────────────────────────────┘
                                        │
                                        │ INSERT INTO staging.raw_transactions
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│   3️⃣  STAGING DATABASE (PostgreSQL Container: postgres_container)            │
│                                                                               │
│   📦 Schema: staging                                                          │
│   ├─ raw_transactions (30,088 rows)                                          │
│   │   ├─ transaction_id, date, store, item_code                              │
│   │   ├─ quantity_sold, unit_price, total_sales                              │
│   │   ├─ supplier_name, department, section                                  │
│   │   └─ rrp (Recommended Retail Price)                                      │
│   │                                                                           │
│   └─ Purpose: Persist cleaned data, audit trail                              │
│                                                                               │
│   🐘 PostgreSQL 15+ | Port 5432 | Database: ducklens_db                      │
└───────────────────────────────────────┬───────────────────────────────────────┘
                                        │
                                        │ SQL Transformations
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│   4️⃣  TRANSFORMATION LOGIC (SQL Business Rules)                              │
│                                                                               │
│   🔄 Promo Detection Logic:                                                   │
│   ├─ IF (unit_price < rrp * 0.90) AND (duration >= 2 days)                   │
│   └─ THEN promo_flag = TRUE                                                  │
│                                                                               │
│   🔄 Promo Uplift Calculation:                                                │
│   ├─ baseline_avg_units = AVG(quantity) WHERE promo_flag = FALSE             │
│   ├─ promo_avg_units = AVG(quantity) WHERE promo_flag = TRUE                 │
│   └─ uplift_pct = ((promo_avg - baseline_avg) / baseline_avg) * 100          │
│                                                                               │
│   🔄 Price Index Calculation:                                                 │
│   ├─ bidco_avg_price = AVG(unit_price) WHERE supplier = 'Bidco'              │
│   ├─ competitor_avg_price = AVG(unit_price) WHERE supplier != 'Bidco'        │
│   ├─ price_index = bidco_avg_price / competitor_avg_price                    │
│   └─ Positioning:                                                            │
│       ├─ > 1.10 = PREMIUM                                                    │
│       ├─ 0.95 - 1.10 = AT MARKET                                             │
│       ├─ 0.90 - 0.95 = COMPETITIVE                                           │
│       └─ < 0.90 = DEEP DISCOUNT                                              │
│                                                                               │
│   🔄 Coverage Analysis:                                                       │
│   └─ coverage_pct = (stores_with_promo / total_stores) * 100                 │
│                                                                               │
└───────────────────────────────────────┬───────────────────────────────────────┘
                                        │
                                        │ Aggregate & Store
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│   5️⃣  DATA WAREHOUSE (PostgreSQL Container: postgres_container)              │
│                                                                               │
│   📊 Schema: dw (Data Warehouse)                                              │
│                                                                               │
│   Fact Tables:                                                                │
│   └─ fact_transactions (enriched with promo flags, price indices)            │
│                                                                               │
│   Dimension Tables:                                                           │
│   ├─ dim_stores (store_id, store_name, location)                             │
│   ├─ dim_suppliers (supplier_id, supplier_name, category)                    │
│   ├─ dim_items (item_code, item_name, department, section)                   │
│   └─ dim_date (date, year, month, quarter, week)                             │
│                                                                               │
│   🎯 Star Schema Design for OLAP queries                                     │
│                                                                               │
└───────────────────────────────────────┬───────────────────────────────────────┘
                                        │
                                        │ CREATE MATERIALIZED VIEW
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│   6️⃣  ANALYTICAL VIEWS (postgres/schemas/Views/)                             │
│                                                                               │
│   📈 Data Quality Views:                                                      │
│   ├─ v_staging_data_health                                                   │
│   ├─ v_unreliable_stores                                                     │
│   ├─ v_unreliable_suppliers                                                  │
│   └─ v_data_quality_scorecard                                                │
│                                                                               │
│   🎯 Promotional Analysis Views:                                              │
│   ├─ v_promo_uplift_summary (SKU-level uplift %)                             │
│   ├─ v_promo_coverage_analysis (store penetration per promo)                 │
│   ├─ v_promo_price_impact (discount depth analysis)                          │
│   ├─ v_baseline_vs_promo_pricing (side-by-side comparison)                   │
│   ├─ v_top_performing_skus (composite performance score)                     │
│   ├─ v_bidco_promo_executive_summary (executive rollup)                      │
│   └─ v_bidco_promo_kpi_metrics (numeric KPIs for cards)                      │
│                                                                               │
│   💰 Price Index Views:                                                       │
│   ├─ v_price_index_store_level (328 rows: store x category pricing)          │
│   └─ v_price_index_overall (26 rows: category rollup positioning)            │
│                                                                               │
│   🔄 Materialized Views = Pre-computed, fast query performance                │
│   📌 Refresh: REFRESH MATERIALIZED VIEW view_name;                            │
│                                                                               │
└───────────────────────────────────────┬───────────────────────────────────────┘
                                        │
                        ┌───────────────┴────────────────┐
                        │                                │
                        ▼                                ▼
┌────────────────────────────────┐    ┌─────────────────────────────────────┐
│   7️⃣  APACHE SUPERSET          │    │   8️⃣  FASTAPI REST API             │
│   (Business Intelligence)       │    │   (Data Exposure Layer)             │
│                                 │    │                                     │
│   🎨 Dashboard Components:      │    │   🚀 Endpoints:                     │
│                                 │    │                                     │
│   📊 KPI Cards (6):             │    │   GET /                             │
│   ├─ Promo Revenue: $293,717    │    │   └─ API welcome page               │
│   ├─ Penetration: 28.54%        │    │                                     │
│   ├─ Avg Discount: 17.25%       │    │   GET /health                       │
│   ├─ Stores with Promo: 35      │    │   └─ System health check            │
│   ├─ SKUs on Promo: 50          │    │                                     │
│   └─ Units Uplift: -6.51%       │    │   GET /data_quality                 │
│                                 │    │   └─ Data health metrics            │
│   📈 Charts (10):                │    │                                     │
│   ├─ Top SKUs Table              │    │   GET /promo_summary                │
│   ├─ Promo Coverage Bar Chart    │    │   └─ Promo KPIs (revenue, %)       │
│   ├─ Price Impact by SKU         │    │                                     │
│   ├─ Baseline vs Promo Pricing   │    │   GET /promo_kpis?limit=20          │
│   ├─ Uplift Distribution         │    │   └─ Top performing SKUs            │
│   ├─ Price Index Heatmap         │    │                                     │
│   ├─ Category Positioning        │    │   GET /price_index/store_level      │
│   ├─ Store-Level Pricing         │    │   └─ Store x category pricing       │
│   ├─ Promo Type Distribution     │    │                                     │
│   └─ Revenue Contribution Pie    │    │   GET /price_index/overall          │
│                                 │    │   └─ Category positioning           │
│   🌐 Port: 8088                  │    │                                     │
│   📖 Setup: See                  │    │   GET /price_index/by_category      │
│      SUPERSET_DASHBOARD_SETUP.md │    │   └─ Category summary               │
│                                 │    │                                     │
│   🎯 Users: Business Analysts,   │    │   🌐 Port: 8001                     │
│            Marketing Teams       │    │   📖 Docs: /docs (Swagger UI)       │
│                                 │    │   📖 ReDoc: /redoc                  │
│                                 │    │                                     │
│                                 │    │   🎯 Users: Developers, Apps,       │
│                                 │    │            Data Scientists          │
└────────────────────────────────┘    └─────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│   🐳 DOCKER ORCHESTRATION (docker-compose.yaml)                              │
│                                                                               │
│   Containers:                                                                 │
│   ├─ postgres_container (PostgreSQL 15+)                                     │
│   │   ├─ Port: 5432                                                          │
│   │   ├─ Volume: ./postgres/data (persistent storage)                        │
│   │   └─ Init: ducklens_init.sql (schema creation)                           │
│   │                                                                           │
│   ├─ fastapi_container (Python 3.11)                                         │
│   │   ├─ Port: 8001                                                          │
│   │   ├─ Volume: ./fastapi_app                                               │
│   │   └─ Depends: postgres_container                                         │
│   │                                                                           │
│   └─ superset_container (Apache Superset)                                    │
│       ├─ Port: 8088                                                          │
│       ├─ Volume: ./docker/superset_config.py                                 │
│       └─ Depends: postgres_container                                         │
│                                                                               │
│   Network: ducklens_network (bridge)                                         │
│                                                                               │
│   Commands:                                                                   │
│   ├─ docker-compose up -d       # Start all services                         │
│   ├─ docker-compose down         # Stop all services                         │
│   └─ docker-compose logs -f      # View logs                                 │
│                                                                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🔄 **DATA FLOW EXPLANATION**

### **Step-by-Step Journey of Data**

#### **1️⃣ Data Source → Python ETL**
- **Input:** `Test_Data.xlsx` (30,088 rows of retail transactions)
- **Process:** Python script reads Excel using `pandas.read_excel()`
- **Output:** DataFrame in memory

#### **2️⃣ Python ETL → Staging Database**
- **Cleaning:** Standardize text, convert types, handle nulls
- **Transformation:** Detect promos, calculate uplift, price indices
- **Loading:** Bulk insert to `staging.raw_transactions` via psycopg2
- **Output:** Cleaned, enriched data in PostgreSQL

#### **3️⃣ Staging → Data Warehouse**
- **Process:** SQL transformations aggregate data into fact/dimension tables
- **Schema:** Star schema design in `dw` schema
- **Output:** Normalized, queryable data warehouse

#### **4️⃣ Data Warehouse → Materialized Views**
- **Process:** SQL queries create pre-computed aggregations
- **Views:** 12 materialized views for analytics (promo, price index, data quality)
- **Output:** Fast-query analytical datasets

#### **5️⃣ Materialized Views → Superset**
- **Process:** Superset connects to PostgreSQL, registers views as datasets
- **Visualization:** Build charts, KPI cards, dashboards
- **Output:** Interactive BI dashboards for business users

#### **6️⃣ Materialized Views → FastAPI**
- **Process:** FastAPI queries views, returns JSON responses
- **Endpoints:** 7 REST endpoints exposing KPIs
- **Output:** API for programmatic access (apps, scripts, data science)

---

## 🛠️ **TECHNOLOGY STACK**

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Data Source** | Excel (.xlsx) | Raw transaction data from retail systems |
| **ETL Pipeline** | Python 3.11, pandas, numpy | Data cleaning, transformation, business logic |
| **Database** | PostgreSQL 15+ | Staging, data warehouse, analytical views |
| **Orchestration** | Docker Compose | Multi-container deployment |
| **API Layer** | FastAPI, Uvicorn | REST API for data exposure |
| **BI Tool** | Apache Superset | Interactive dashboards, visualizations |
| **Version Control** | Git, GitHub | Code versioning, collaboration |

---

## 📊 **KEY METRICS & BUSINESS LOGIC**

### **Promotional Analysis**

**Promo Detection:**
```python
promo_flag = (unit_price < rrp * 0.90) AND (duration >= 2 days)
```

**Results:**
- 8,210 promo transactions (27.3% of total)
- 50 SKUs on promotion
- 35 stores running promos
- Average discount: 17.25%

### **Price Index Analysis**

**Formula:**
```python
price_index = bidco_avg_price / competitor_avg_price
```

**Positioning:**
- **Foods:** 0.80 index → DEEP DISCOUNT (20% cheaper)
- **Home Care:** 0.73 index → DEEP DISCOUNT (27% cheaper)
- **Personal Care:** 1.29 index → PREMIUM (29% more expensive)

### **Data Quality**

- **Total Records:** 30,088
- **Quality Score:** 99.96%
- **Reliability:** 190 suppliers, 61 stores validated

---

## 🚀 **DEPLOYMENT WORKFLOW**

### **First-Time Setup:**

```bash
# 1. Clone repository
git clone <repo_url>
cd DuckLens-Retail-Insights

# 2. Start Docker containers
docker-compose up -d

# 3. Wait for services to be ready (30-60 seconds)
docker-compose ps

# 4. Run ETL pipeline
python3 ducklens_pipeline/01_cleaning.py
python3 ducklens_pipeline/02_transform.py
python3 ducklens_pipeline/03_loading.py

# 5. Create materialized views
psql -h localhost -U user -d ducklens_db -f postgres/schemas/Views/*.sql

# 6. Access services
# - Superset: http://localhost:8088
# - FastAPI: http://localhost:8001/docs
# - PostgreSQL: localhost:5432
```

### **Daily Operations:**

```bash
# Refresh data
python3 ducklens_pipeline/01_cleaning.py  # New data arrives
python3 ducklens_pipeline/02_transform.py
python3 ducklens_pipeline/03_loading.py

# Refresh views
psql -h localhost -U user -d ducklens_db -c "REFRESH MATERIALIZED VIEW dw.v_promo_uplift_summary;"
psql -h localhost -U user -d ducklens_db -c "REFRESH MATERIALIZED VIEW dw.v_price_index_store_level;"

# Restart API (if needed)
docker restart fastapi_container
```

---

## 📁 **PROJECT STRUCTURE**

```
DuckLens-Retail-Insights/
│
├── Test_Data.xlsx                          # 📄 Source data
│
├── ducklens_pipeline/                      # 🐍 ETL Scripts
│   ├── 01_cleaning.py                      # Data cleaning
│   ├── 02_transform.py                     # Business logic transformations
│   └── 03_loading.py                       # Load to PostgreSQL
│
├── postgres/                               # 🐘 Database
│   ├── ducklens_init.sql                   # Schema initialization
│   ├── data/                               # Persistent volume
│   └── schemas/
│       └── Views/                          # 📊 Materialized views (12 files)
│           ├── promo_uplift_summary.sql
│           ├── promo_coverage_analysis.sql
│           ├── price_index_store_level.sql
│           └── ...
│
├── fastapi_app/                            # 🚀 REST API
│   ├── main.py                             # FastAPI application (7 endpoints)
│   └── requirements.txt                    # Python dependencies
│
├── docker/                                 # 🐳 Docker configs
│   ├── docker-init.sh                      # Initialization script
│   └── superset_config.py                  # Superset configuration
│
├── docker-compose.yaml                     # 🐳 Multi-container orchestration
│
├── .env                                    # 🔒 Environment variables
│
└── Documentation/                          # 📖 Guides
    ├── README.md                           # Project overview
    ├── SUPERSET_DASHBOARD_SETUP.md         # BI dashboard guide
    ├── SWAGGER_SCREENSHOT_GUIDE.md         # API documentation guide
    └── PIPELINE_ARCHITECTURE.md            # This file
```

---

## 🎯 **USE CASES**

### **For Business Analysts:**
- Monitor promotional effectiveness in Superset dashboards
- Identify underperforming SKUs
- Track competitive pricing positioning

### **For Marketing Teams:**
- Analyze promo uplift and ROI
- Determine optimal discount levels
- Identify store coverage gaps

### **For Data Scientists:**
- Query FastAPI endpoints for ML models
- Analyze price elasticity
- Build predictive promo models

### **For Executives:**
- Executive summary dashboard with KPIs
- Strategic pricing insights
- Performance scorecards

---

## 🔐 **SECURITY & BEST PRACTICES**

✅ **Environment Variables:** Credentials in `.env` file (not committed)  
✅ **Docker Isolation:** Each service in separate container  
✅ **Read-Only Views:** Materialized views prevent accidental data modification  
✅ **SQL Injection Prevention:** Parameterized queries in ETL scripts  
✅ **Data Validation:** Quality checks in cleaning stage  

---

## 📈 **SCALABILITY CONSIDERATIONS**

| Component | Current | Scalable To |
|-----------|---------|-------------|
| **Data Volume** | 30K rows | 10M+ rows (partition tables) |
| **ETL Runtime** | ~5 seconds | Add Apache Airflow for scheduling |
| **API Throughput** | Single instance | Load balancer + multiple FastAPI containers |
| **Superset Users** | 10-20 users | Add Redis cache, scale workers |
| **Database** | Single PostgreSQL | Read replicas, partitioning, indexing |

---

## 🐛 **TROUBLESHOOTING**

### **Issue: Containers won't start**
```bash
docker-compose down -v
docker-compose up -d --build
```

### **Issue: Materialized views are stale**
```bash
psql -h localhost -U user -d ducklens_db -c "REFRESH MATERIALIZED VIEW dw.view_name;"
```

### **Issue: FastAPI returns empty data**
```bash
# Check if data is loaded
docker exec -it postgres_container psql -U user -d ducklens_db -c "SELECT COUNT(*) FROM staging.raw_transactions;"

# Restart API
docker restart fastapi_container
```

---

## 📚 **ADDITIONAL DOCUMENTATION**

- **Superset Dashboard Setup:** `SUPERSET_DASHBOARD_SETUP.md`
- **API Documentation:** http://localhost:8001/docs (Swagger UI)
- **API Screenshot Guide:** `SWAGGER_SCREENSHOT_GUIDE.md`
- **Database Schema:** `postgres/ducklens_init.sql`

---

## 👨‍💻 **ABOUT THIS PIPELINE**

**Built by:** Jayson Gor  
**Purpose:** Retail analytics & competitive intelligence for Bidco Africa  
**Architecture:** Modern data engineering stack with Docker-based deployment  
**Highlights:**
- ✅ End-to-end ETL pipeline
- ✅ Star schema data warehouse
- ✅ 12 pre-computed analytical views
- ✅ REST API for programmatic access
- ✅ Interactive BI dashboards
- ✅ 99.96% data quality score

---

## 🚀 **NEXT STEPS**

1. ✅ **Screenshot Swagger UI** for API documentation
2. ⏳ **Build Superset Dashboard** using setup guide
3. ⏳ **Schedule ETL** with Apache Airflow (optional)
4. ⏳ **Add Authentication** to FastAPI (optional)
5. ⏳ **Create Streamlit Dashboard** (optional)

---

**Questions? Check the documentation or review the code!** 🦆✨
