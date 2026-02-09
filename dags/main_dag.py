import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


d

    # ---------- Incremental path tasks ----------
    run_incremental_path = EmptyOperator(task_id="run_incremental_path")

    incremental_load = SnowflakeOperator(
        task_id="incremental_load_dims_fact",
        sql=";\n".join([
            load_sql("incremental", "01_dim_date_incremental_4.sql"),
            load_sql("incremental", "02_dim_company_core_scd2_4.sql"),
            load_sql("incremental", "03_dim_company_financial_upsert_4.sql"),
            load_sql("incremental", "04_fact_stock_price_incremental_4.sql"),
        ]),
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # ---------- 3) Join branch back ----------
    join = EmptyOperator(
        task_id="join",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ---------- Update watermark (after load) ----------
    update_watermark = SnowflakeOperator(
        task_id="update_watermark",
        sql=load_sql("metadata", "update_watermark_4.sql"),
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # ---------- 4) DQ using STG_SYMBOLS + XCom watermark ----------
    def run_dq_checks(ti):
        
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        last_loaded = ti.xcom_pull(task_ids="get_mode_and_watermark", key="last_loaded_date")
        checks = [
            ("dq_01_symbols_null_or_dup", load_sql("dq", "dq_01_symbols_null_or_dup_4.sql")),
            ("dq_02_symbols_missing_company_profile", load_sql("dq", "dq_02_symbols_missing_company_profile_4.sql")),
            ("dq_03_stock_history_symbol_not_in_symbols", load_sql("dq", "dq_03_stock_history_symbol_not_in_symbols_4.sql")),
            ("dq_04_fact_missing_dim_keys_recent", load_sql("dq", "dq_04_fact_missing_dim_keys_recent_4.sql").replace("{{LAST_LOADED_DATE}}", str(last_loaded) if last_loaded else "NULL")),
        ]

        failed = []
        results = {}
        for name, sql in checks:
            cnt = hook.get_first(sql)[0]
            results[name] = int(cnt)
            if int(cnt) > 0:
                failed.append((name, int(cnt)))

        ti.xcom_push(key="dq_results", value=results)

        if failed:
            msg = "DQ failed: " + ", ".join([f"{n}={c}" for n, c in failed])
            raise ValueError(msg)

    dq = PythonOperator(
        task_id="dq_checks",
        python_callable=run_dq_checks,
    )

    end = EmptyOperator(task_id="end")

    # ---------- Dependencies ----------
    start >> get_state >> branch

    # Full path
    branch >> run_full_path >> refresh_staging_full >> full_load_dims >> full_load_fact >> join

    # Incremental path
    branch >> run_incremental_path >> incremental_load >> join

    join >> update_watermark >> dq >> end
