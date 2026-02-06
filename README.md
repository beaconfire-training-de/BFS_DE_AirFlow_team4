# Team 4 Airflow DAGs

# Stock ETL Pipeline – Airflow + Snowflake

## Project Overview
This project implements an end-to-end **Stock Market ETL Pipeline** using **Apache Airflow** and **Snowflake**.  
The pipeline supports both **Full Load** and **Incremental Load** modes controlled by a **metadata & watermark mechanism**.  
It ingests raw stock and company data into staging tables, transforms them into dimension tables, loads a fact table, updates metadata, and runs data quality checks to ensure data integrity.

---

## Tech Stack
- **Apache Airflow** – Workflow orchestration
- **Snowflake** – Cloud data warehouse
- **Python** – DAG control logic and data quality checks
- **SQL** – Transformations and loading logic
- **Git / GitHub** – Version control and collaboration

---

## Project Structure

```text
BFS_DE_AIRFLOW_TEAM4/
│
├── dags/
│ ├── init_dag.py # One-time initialization DAG
│ ├── main_dag.py # Main ETL pipeline DAG
│ └── test_dag.py # Test / experimentation DAG
│
├── sql/
│ ├── staging/ # Staging refresh SQL
│ ├── init/ # Full load SQL (create/replace tables)
│ ├── incremental/ # Incremental load SQL
│ ├── metadata/ # Metadata & watermark SQL
│ └── dq/ # Data quality SQL checks
│
└── docs/ # Documentation and diagrams
```

---

## Pipeline Logic

### Full Load Path
Triggered when metadata indicates the system is not initialized.
Start
→ Refresh Staging Tables
→ Create / Replace Dimension Tables
→ Create / Replace Fact Table
→ Update Watermark
→ Data Quality Checks
→ End


### Incremental Load Path
Triggered when metadata indicates the system is already initialized.
Start
→ Incremental Dimension Load
→ Incremental Fact Load
→ Update Watermark
→ Data Quality Checks
→ End


---

## Metadata & Watermark
The pipeline uses the table:

AIRFLOW0105.DEV.ETL_METADATA_4


Key Fields:
- **IS_INITIALIZED** – Determines Full vs Incremental mode
- **LAST_LOADED_DATE** – Watermark for incremental filtering
- **LAST_RUN_TS** – Timestamp of last successful run

**Branching Logic**
- `IS_INITIALIZED = FALSE` → Full Load
- `IS_INITIALIZED = TRUE` → Incremental Load

---

## Data Model

### Staging Tables
- `STG_STOCK_HISTORY_4`
- `STG_COMPANY_PROFILE_4`

### Dimension Tables
- `DIM_DATE_4`
- `DIM_COMPANY_CORE_4` (SCD Type 2)
- `DIM_COMPANY_FINANCIAL_4`

### Fact Table
- `FACT_STOCK_PRICE_4`

---

## Data Quality Checks
The pipeline enforces multiple DQ validations:
- No null or duplicate symbols
- Company profile completeness
- Symbol consistency between tables
- Fact foreign keys must match dimension keys

If any DQ check fails, the DAG stops to prevent bad data propagation.

---

## How to Run

### First-Time Setup
1. Run `init_dag.py` to create schemas, staging tables, and metadata record.
2. Trigger `main_stock_etl_4` DAG.

### Daily / Regular Runs
Trigger `main_stock_etl_4`.  
The DAG automatically chooses Full or Incremental mode based on metadata.

---

## Reset and Re-Run Full Load
To fully reset the pipeline before testing or demo:

```sql
TRUNCATE TABLE FACT_STOCK_PRICE_4;
TRUNCATE TABLE DIM_DATE_4;
TRUNCATE TABLE DIM_COMPANY_CORE_4;
TRUNCATE TABLE DIM_COMPANY_FINANCIAL_4;

UPDATE ETL_METADATA_4
SET LAST_LOADED_DATE = NULL,
    IS_INITIALIZED = FALSE,
    LAST_RUN_TS = NULL
WHERE JOB_NAME = 'stock_etl_4';
Then trigger main_stock_etl_4 again.

Design Principles
Idempotent ETL Design

Metadata-Driven Branching

Full vs Incremental Separation

SCD Type 2 Dimension Handling

Data Quality Enforcement

Modular SQL Organization

Surrogate Key Generation

Scalable Fact Table Design

Future Improvements
Implement MERGE for fact incremental load

Add Snowflake clustering keys for performance

Add alerting and monitoring

Parameterized DAG triggers

CI/CD pipeline integration

Author
Team 4 – Data Engineering Project# Team 4 Airflow DAGs
