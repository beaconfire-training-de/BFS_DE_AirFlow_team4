from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

with DAG(
    dag_id = "init_stock_etl_4",
    start_date = datetime(2024, 1, 1),
    schedule_interval = None,
    catchup = False
) as dag:
    
    create_schemas = SnowflakeOperator(
        task_id = "create_schemas",
        snowflake_conn_id="jan_airflow_snowflake",
        sql = "sql/init/00_create_schemas.sql"
    )

    create_metadata = SnowflakeOperator(
        task_id="create_metadata",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="sql/metadata/create_metadata_table_4.sql"
    )

    init_metadata = SnowflakeOperator(
        task_id="init_metadata",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="sql/metadata/init_metadata_record_4.sql"
    )

    refresh_staging = SnowflakeOperator(
        task_id="refresh_staging",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="sql/staging/refresh_staging.sql"
    )

    dim_date = SnowflakeOperator(
        task_id="dim_date",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="sql/init/create_dim_date_4.sql"
    )

    dim_company_core = SnowflakeOperator(
        task_id="dim_company_core",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="sql/init/create_dim_company_core_4.sql"
    )

    dim_company_financial = SnowflakeOperator(
        task_id="dim_company_financial",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="sql/init/create_dim_company_financial_4.sql"
    )

    fact = SnowflakeOperator(
        task_id="fact",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="sql/init/create_fact_stock_price_4.sql"
    )

    update_watermark = SnowflakeOperator(
        task_id="update_watermark",
        snowflake_conn_id="jan_airflow_snowflake",
        sql="sql/metadata/update_watermark_4.sql"
    )

    create_schemas >> create_metadata >> init_metadata >> refresh_staging >> dim_date >> dim_company_core >> dim_company_financial >> fact >> update_watermark