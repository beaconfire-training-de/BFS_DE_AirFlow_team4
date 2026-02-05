# dags/test_snowflake_conn.py
from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# =========================================================
# Config
# =========================================================
SNOWFLAKE_CONN_ID = "jan_airflow_snowflake"
DB = "AIRFLOW0105"
SCHEMA = "DEV"
PIPELINE_NAME = "stock_pipeline"

# Target tables (group 4)
TBL_CONTROL = f"{DB}.{SCHEMA}.etl_control_4"
TBL_DIM_DATE = f"{DB}.{SCHEMA}.dim_date_4"
TBL_DIM_CORE = f"{DB}.{SCHEMA}.dim_company_core_4"
TBL_DIM_FIN = f"{DB}.{SCHEMA}.dim_company_financial_4"
TBL_FACT = f"{DB}.{SCHEMA}.fact_stock_price_4"

# Source tables
SRC_STOCK = "US_STOCK_DAILY.DCCM.STOCK_HISTORY"
SRC_COMP = "US_STOCK_DAILY.DCCM.COMPANY_PROFILE"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

# =========================================================
# Branching logic: INIT vs INCREMENTAL
# =========================================================
def choose_path() -> str:
    """
    Decide INIT or INCREMENTAL based on etl_control_4.is_initialized.
    - If control row missing OR is_initialized = FALSE => INIT
    - Else => INCREMENTAL
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    # Ensure DB/SCHEMA context; SnowflakeHook runs on same connection
    sql = f"""
    USE DATABASE {DB};
    USE SCHEMA {SCHEMA};

    SELECT is_initialized
    FROM {TBL_CONTROL}
    WHERE pipeline_name = %s
    """
    try:
        res = hook.get_first(sql, parameters=(PIPELINE_NAME,))
    except Exception:
        # If control table does not exist yet or query fails -> INIT
        return "init_start"

    if (res is None) or (res[0] is None) or (res[0] is False):
        return "init_start"
    return "inc_start"


# =========================================================
# SQL blocks
# =========================================================
SQL_USE_CTX = f"USE DATABASE {DB}; USE SCHEMA {SCHEMA};"

# 1) Create control table + seed row
SQL_CONTROL_INIT = f"""
{SQL_USE_CTX}

CREATE TABLE IF NOT EXISTS {TBL_CONTROL} (
  pipeline_name   VARCHAR PRIMARY KEY,
  is_initialized  BOOLEAN,
  last_fact_date  DATE,
  last_run_ts     TIMESTAMP_NTZ,
  status          VARCHAR,
  last_error      VARCHAR
);

INSERT INTO {TBL_CONTROL} (pipeline_name, is_initialized, last_fact_date, last_run_ts, status)
SELECT %s, FALSE, NULL, CURRENT_TIMESTAMP(), 'NEW'
WHERE NOT EXISTS (SELECT 1 FROM {TBL_CONTROL} WHERE pipeline_name = %s);
"""

# 2) (Optional) Create target tables if not exists
# NOTE: If you already created tables manually, you can keep this task; it is idempotent.
SQL_CREATE_TABLES = f"""
{SQL_USE_CTX}

-- dim_date_4
CREATE TABLE IF NOT EXISTS {TBL_DIM_DATE} (
  date_sk        NUMBER AUTOINCREMENT START 1 INCREMENT 1,
  date           DATE        NOT NULL,
  year           NUMBER(4,0),
  quarter        NUMBER(1,0),
  month          NUMBER(2,0),
  day            NUMBER(2,0),
  weekday        NUMBER(1,0),
  is_weekend     BOOLEAN,
  is_month_end   BOOLEAN,
  is_quarter_end BOOLEAN,
  CONSTRAINT pk_dim_date_4 PRIMARY KEY (date_sk),
  CONSTRAINT uq_dim_date_date_4 UNIQUE (date)
);

-- dim_company_core_4 (SCD2)
CREATE TABLE IF NOT EXISTS {TBL_DIM_CORE} (
  company_sk           NUMBER AUTOINCREMENT START 1 INCREMENT 1,
  symbol               VARCHAR     NOT NULL,
  company_name         VARCHAR,
  industry             VARCHAR,
  sector               VARCHAR,
  exchange             VARCHAR,
  website              VARCHAR,
  ceo                  VARCHAR,
  effective_start_date DATE        NOT NULL,
  effective_end_date   DATE        NOT NULL,
  is_current           BOOLEAN     NOT NULL,
  CONSTRAINT pk_dim_company_core_4 PRIMARY KEY (company_sk)
);

-- dim_company_financial_4 (snapshot by symbol)
CREATE TABLE IF NOT EXISTS {TBL_DIM_FIN} (
  financial_sk  NUMBER AUTOINCREMENT START 1 INCREMENT 1,
  symbol        VARCHAR NOT NULL,
  range         VARCHAR,
  price         NUMBER,
  beta          NUMBER,
  volavg        NUMBER,
  mktcap        NUMBER,
  lastdiv       NUMBER,
  dcf           NUMBER,
  dcfdiff       NUMBER,
  changes       NUMBER,
  CONSTRAINT pk_dim_company_financial_4 PRIMARY KEY (financial_sk),
  CONSTRAINT uq_dim_company_financial_symbol_4 UNIQUE (symbol)
);

-- fact_stock_price_4
CREATE TABLE IF NOT EXISTS {TBL_FACT} (
  stock_fact_sk  NUMBER AUTOINCREMENT START 1 INCREMENT 1,
  company_sk     NUMBER NOT NULL,
  date_sk        NUMBER NOT NULL,
  financial_sk   NUMBER,
  open           NUMBER,
  high           NUMBER,
  low            NUMBER,
  close          NUMBER,
  adjclose       NUMBER,
  volume         NUMBER,
  CONSTRAINT pk_fact_stock_price_4 PRIMARY KEY (stock_fact_sk),
  CONSTRAINT fk_fact_company_4 FOREIGN KEY (company_sk) REFERENCES {TBL_DIM_CORE}(company_sk),
  CONSTRAINT fk_fact_date_4 FOREIGN KEY (date_sk) REFERENCES {TBL_DIM_DATE}(date_sk),
  CONSTRAINT fk_fact_financial_4 FOREIGN KEY (financial_sk) REFERENCES {TBL_DIM_FIN}(financial_sk)
);
"""

# 3) dim_date append-only (works for init + incremental)
SQL_DIM_DATE_APPEND = f"""
{SQL_USE_CTX}

INSERT INTO {TBL_DIM_DATE} (
  date, year, quarter, month, day, weekday,
  is_weekend, is_month_end, is_quarter_end
)
SELECT
  d AS date,
  YEAR(d) AS year,
  QUARTER(d) AS quarter,
  MONTH(d) AS month,
  DAY(d) AS day,
  DAYOFWEEK(d) AS weekday,
  IFF(DAYOFWEEK(d) IN (6,7), TRUE, FALSE) AS is_weekend,
  IFF(d = LAST_DAY(d), TRUE, FALSE) AS is_month_end,
  IFF(d = LAST_DAY(d, 'QUARTER'), TRUE, FALSE) AS is_quarter_end
FROM (
  SELECT DISTINCT DATE AS d
  FROM {SRC_STOCK}
) src
WHERE NOT EXISTS (
  SELECT 1 FROM {TBL_DIM_DATE} t WHERE t.date = src.d
);
"""

# 4) dim_company_financial upsert (works for init + incremental)
SQL_DIM_FIN_UPSERT = f"""
{SQL_USE_CTX}

MERGE INTO {TBL_DIM_FIN} tgt
USING (
  SELECT
    SYMBOL AS symbol,
    RANGE  AS range,
    PRICE  AS price,
    BETA   AS beta,
    VOLAVG AS volavg,
    MKTCAP AS mktcap,
    LASTDIV AS lastdiv,
    DCF     AS dcf,
    DCFDIFF AS dcfdiff,
    CHANGES AS changes
  FROM {SRC_COMP}
) src
ON tgt.symbol = src.symbol
WHEN MATCHED THEN UPDATE SET
  tgt.range   = src.range,
  tgt.price   = src.price,
  tgt.beta    = src.beta,
  tgt.volavg  = src.volavg,
  tgt.mktcap  = src.mktcap,
  tgt.lastdiv = src.lastdiv,
  tgt.dcf     = src.dcf,
  tgt.dcfdiff = src.dcfdiff,
  tgt.changes = src.changes
WHEN NOT MATCHED THEN INSERT (
  symbol, range, price, beta, volavg, mktcap, lastdiv, dcf, dcfdiff, changes
) VALUES (
  src.symbol, src.range, src.price, src.beta, src.volavg, src.mktcap,
  src.lastdiv, src.dcf, src.dcfdiff, src.changes
);
"""

# 5) dim_company_core SCD2 (Type 2)
# Logic:
# - If symbol has no current record -> insert new current
# - If has current and attributes changed -> close old current (end_date = yesterday), insert new current (start=today)
SQL_DIM_CORE_SCD2 = f"""
{SQL_USE_CTX}

-- Step A: close changed current records
UPDATE {TBL_DIM_CORE} tgt
SET
  tgt.is_current = FALSE,
  tgt.effective_end_date = DATEADD(day, -1, CURRENT_DATE())
WHERE tgt.is_current = TRUE
AND EXISTS (
  SELECT 1
  FROM {SRC_COMP} src
  WHERE src.SYMBOL = tgt.symbol
    AND (
      NVL(tgt.company_name, '') <> NVL(src.COMPANYNAME, '')
      OR NVL(tgt.industry, '')  <> NVL(src.INDUSTRY, '')
      OR NVL(tgt.sector, '')    <> NVL(src.SECTOR, '')
      OR NVL(tgt.exchange, '')  <> NVL(src.EXCHANGE, '')
      OR NVL(tgt.website, '')   <> NVL(src.WEBSITE, '')
      OR NVL(tgt.ceo, '')       <> NVL(src.CEO, '')
    )
);

-- Step B: insert new current records for (1) brand-new symbols OR (2) symbols just closed above
INSERT INTO {TBL_DIM_CORE} (
  symbol, company_name, industry, sector, exchange, website, ceo,
  effective_start_date, effective_end_date, is_current
)
SELECT
  src.SYMBOL AS symbol,
  src.COMPANYNAME AS company_name,
  src.INDUSTRY AS industry,
  src.SECTOR AS sector,
  src.EXCHANGE AS exchange,
  src.WEBSITE AS website,
  src.CEO AS ceo,
  CURRENT_DATE() AS effective_start_date,
  TO_DATE('9999-12-31') AS effective_end_date,
  TRUE AS is_current
FROM {SRC_COMP} src
WHERE
  -- no current exists for this symbol (either never existed, or just got closed in Step A)
  NOT EXISTS (
    SELECT 1 FROM {TBL_DIM_CORE} c
    WHERE c.symbol = src.SYMBOL AND c.is_current = TRUE
  );
"""

# 6) fact_stock_price incremental by watermark
# - Use etl_control.last_fact_date
# - If last_fact_date is NULL (INIT), load all history
SQL_FACT_MERGE_BY_WATERMARK = f"""
{SQL_USE_CTX}

MERGE INTO {TBL_FACT} tgt
USING (
  SELECT
    core.company_sk AS company_sk,
    dd.date_sk      AS date_sk,
    fin.financial_sk AS financial_sk,
    sh.OPEN  AS open,
    sh.HIGH  AS high,
    sh.LOW   AS low,
    sh.CLOSE AS close,
    sh.ADJCLOSE AS adjclose,
    sh.VOLUME AS volume
  FROM {SRC_STOCK} sh
  JOIN {TBL_DIM_DATE} dd
    ON dd.date = sh.DATE
  JOIN {TBL_DIM_CORE} core
    ON core.symbol = sh.SYMBOL
   AND core.is_current = TRUE
  LEFT JOIN {TBL_DIM_FIN} fin
    ON fin.symbol = sh.SYMBOL
  WHERE
    (
      (SELECT last_fact_date FROM {TBL_CONTROL} WHERE pipeline_name = %s) IS NULL
      OR sh.DATE > (SELECT last_fact_date FROM {TBL_CONTROL} WHERE pipeline_name = %s)
    )
) src
ON tgt.company_sk = src.company_sk
AND tgt.date_sk   = src.date_sk
WHEN MATCHED THEN UPDATE SET
  tgt.financial_sk = src.financial_sk,
  tgt.open     = src.open,
  tgt.high     = src.high,
  tgt.low      = src.low,
  tgt.close    = src.close,
  tgt.adjclose = src.adjclose,
  tgt.volume   = src.volume
WHEN NOT MATCHED THEN INSERT (
  company_sk, date_sk, financial_sk, open, high, low, close, adjclose, volume
) VALUES (
  src.company_sk, src.date_sk, src.financial_sk, src.open, src.high,
  src.low, src.close, src.adjclose, src.volume
);
"""

# 7) update watermark + mark initialized
SQL_UPDATE_CONTROL_SUCCESS = f"""
{SQL_USE_CTX}

UPDATE {TBL_CONTROL}
SET
  is_initialized = TRUE,
  last_fact_date = (SELECT MAX(DATE) FROM {SRC_STOCK}),
  last_run_ts = CURRENT_TIMESTAMP(),
  status = 'SUCCESS',
  last_error = NULL
WHERE pipeline_name = %s;
"""

SQL_UPDATE_CONTROL_FAIL = f"""
{SQL_USE_CTX}

UPDATE {TBL_CONTROL}
SET
  last_run_ts = CURRENT_TIMESTAMP(),
  status = 'FAILED',
  last_error = %s
WHERE pipeline_name = %s;
"""

# 8) DQ checks (fail the task if any check fails)
# - duplicates on (company_sk, date_sk)
# - unmatched fk (date/company) should be 0
# - basic rowcount > 0 for fact
SQL_DQ_CHECKS = f"""
{SQL_USE_CTX}

-- 1) Fact rowcount
SELECT IFF(COUNT(*) > 0, 1, TO_NUMBER('DQ_FAIL')) AS ok
FROM {TBL_FACT};

-- 2) Duplicate business key in fact
SELECT IFF(COUNT(*) = 0, 1, TO_NUMBER('DQ_FAIL')) AS ok
FROM (
  SELECT company_sk, date_sk, COUNT(*) c
  FROM {TBL_FACT}
  GROUP BY company_sk, date_sk
  HAVING COUNT(*) > 1
);

-- 3) Unmatched date_sk (should be 0)
SELECT IFF(COUNT(*) = 0, 1, TO_NUMBER('DQ_FAIL')) AS ok
FROM {TBL_FACT} f
LEFT JOIN {TBL_DIM_DATE} d
  ON f.date_sk = d.date_sk
WHERE d.date_sk IS NULL;

-- 4) Unmatched company_sk (should be 0)
SELECT IFF(COUNT(*) = 0, 1, TO_NUMBER('DQ_FAIL')) AS ok
FROM {TBL_FACT} f
LEFT JOIN {TBL_DIM_CORE} c
  ON f.company_sk = c.company_sk
WHERE c.company_sk IS NULL;
"""


# =========================================================
# DAG
# =========================================================
with DAG(
    dag_id="stock_pipeline_group4_scd2",
    default_args=default_args,
    schedule_interval=None,  # 手动触发最稳（作业）
    catchup=False,
    tags=["group4", "snowflake", "scd2", "incremental", "dq"],
) as dag:

    start = EmptyOperator(task_id="start")

    ensure_control = SnowflakeOperator(
        task_id="ensure_control_table_and_seed",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_CONTROL_INIT,
        parameters=(PIPELINE_NAME, PIPELINE_NAME),
    )

    decide = BranchPythonOperator(
        task_id="decide_init_or_incremental",
        python_callable=choose_path,
    )

    # ---------------- INIT path ----------------
    init_start = EmptyOperator(task_id="init_start")

    create_tables = SnowflakeOperator(
        task_id="create_tables_if_not_exists",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_CREATE_TABLES,
    )

    init_dim_date = SnowflakeOperator(
        task_id="init_dim_date_append",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_DIM_DATE_APPEND,
    )

    init_dim_fin = SnowflakeOperator(
        task_id="init_dim_company_financial_upsert",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_DIM_FIN_UPSERT,
    )

    init_dim_core = SnowflakeOperator(
        task_id="init_dim_company_core_scd2",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_DIM_CORE_SCD2,
    )

    init_fact = SnowflakeOperator(
        task_id="init_fact_stock_price_merge",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_FACT_MERGE_BY_WATERMARK,
        parameters=(PIPELINE_NAME, PIPELINE_NAME),
    )

    init_update_control = SnowflakeOperator(
        task_id="init_update_control_success",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_UPDATE_CONTROL_SUCCESS,
        parameters=(PIPELINE_NAME,),
    )

    # ---------------- INCREMENTAL path ----------------
    inc_start = EmptyOperator(task_id="inc_start")

    inc_dim_date = SnowflakeOperator(
        task_id="inc_dim_date_append",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_DIM_DATE_APPEND,
    )

    inc_dim_fin = SnowflakeOperator(
        task_id="inc_dim_company_financial_upsert",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_DIM_FIN_UPSERT,
    )

    inc_dim_core = SnowflakeOperator(
        task_id="inc_dim_company_core_scd2",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_DIM_CORE_SCD2,
    )

    inc_fact = SnowflakeOperator(
        task_id="inc_fact_stock_price_merge",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_FACT_MERGE_BY_WATERMARK,
        parameters=(PIPELINE_NAME, PIPELINE_NAME),
    )

    inc_update_control = SnowflakeOperator(
        task_id="inc_update_control_success",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_UPDATE_CONTROL_SUCCESS,
        parameters=(PIPELINE_NAME,),
    )

    # ---------------- DQ + End ----------------
    dq_checks = SnowflakeOperator(
        task_id="dq_checks",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=SQL_DQ_CHECKS,
    )

    end = EmptyOperator(task_id="end")

    # =================================================
    # Dependencies
    # =================================================
    start >> ensure_control >> decide

    # INIT branch
    decide >> init_start >> create_tables >> init_dim_date >> init_dim_fin >> init_dim_core >> init_fact >> init_update_control >> dq_checks >> end

    # INCREMENTAL branch
    decide >> inc_start >> inc_dim_date >> inc_dim_fin >> inc_dim_core >> inc_fact >> inc_update_control >> dq_checks >> end
