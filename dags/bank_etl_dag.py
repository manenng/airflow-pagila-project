from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


def transfer_data_source_to_dw():
    """Hàm Python hút data từ bank_source và bơm thẳng sang bank_dw"""
    source_hook = PostgresHook(postgres_conn_id='postgres_source')
    dw_hook = PostgresHook(postgres_conn_id='postgres_dw')
    tables = ['source_customer', 'source_campaign', 'source_marketing_fact']

    for table in tables:
        print(f"Đang copy bảng {table}...")
        df = source_hook.get_pandas_df(f"SELECT * FROM {table}")
        engine = dw_hook.get_sqlalchemy_engine()
        df.to_sql(table, engine, schema='public', if_exists='replace', index=False)
        print(f"Xong bảng {table}!")


default_args = {
    'owner': 'member_2',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='bank_etl_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='@hourly',
    catchup=False,
    template_searchpath=['/opt/airflow/dags/sql'],
) as dag:

    load_raw = SQLExecuteQueryOperator(
        task_id='load_raw',
        conn_id='postgres_source',
        sql=['01_create_raw.sql', '02_load_raw.sql'],
    )

    create_source = SQLExecuteQueryOperator(
        task_id='create_source',
        conn_id='postgres_source',
        sql='03_create_source.sql',
    )

    transfer_data = PythonOperator(
        task_id='transfer_data_to_dw',
        python_callable=transfer_data_source_to_dw,
    )

    create_dw = SQLExecuteQueryOperator(
        task_id='create_dw',
        conn_id='postgres_dw',
        sql='04_create_dw.sql',
    )

    load_dw = SQLExecuteQueryOperator(
        task_id='load_dw',
        conn_id='postgres_dw',
        sql='05_load_dw.sql',
    )

    load_raw >> create_source >> transfer_data >> create_dw >> load_dw