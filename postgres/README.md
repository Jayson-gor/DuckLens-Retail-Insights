# Database Schema Setup

## Automatic Schema Creation

The database schema is automatically created when the `postgres_container` starts up.

### How it works:

1. Docker mounts SQL files from `./postgres/schemas/` into `/docker-entrypoint-initdb.d/`
2. PostgreSQL executes all `.sql` files in alphabetical order on first startup
3. The schema is created before any services connect

### Files:

- `01_create_schema.sql` - Creates the `staging` and `dw` schemas with all tables

## Manual Schema Recreation

If you need to recreate the schema:

```bash
# From the project root
docker exec -i postgres_container psql -U user -d ducklens_db < postgres/schemas/01_create_schema.sql
```

## Viewing the Schema

### Option 1: pgAdmin (GUI)

1. Open http://localhost:5050
2. Login: admin@admin.com / admin
3. Register Server:
   - **General Tab:**
     - Name: `DuckLens-DB`
   - **Connection Tab:**
     - Host: `db` (or `postgres_container`)
     - Port: `5432`
     - Maintenance database: `ducklens_db`
     - Username: `user`
     - Password: `password`

### Option 2: psql (CLI)

```bash
# Connect from host (port 5000)
psql -h localhost -p 5000 -U user -d ducklens_db

# Connect from inside container
docker exec -it postgres_container psql -U user -d ducklens_db
```

### Verify Schema:

```sql
-- List all schemas
\dn

-- List tables in staging
\dt staging.*

-- List tables in dw
\dt dw.*

-- Check table structure
\d dw.fact_sales_enriched
```

## Schema Design

### Staging Schema
- **Purpose**: Raw data dump (as-is from source)
- **Tables**: `stg_sales_raw`

### Data Warehouse Schema (dw)
- **Purpose**: Clean, modeled star schema for analytics
- **Dimensions**:
  - `dim_store`
  - `dim_supplier`
  - `dim_date`
  - `dim_item`
- **Fact**:
  - `fact_sales_enriched`
