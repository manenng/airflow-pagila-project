from airflow import DAG
from airflow.providers.standard.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id='pagila_fact_build',
    start_date=datetime(2026, 4, 1),
    schedule_interval=None, # We'll trigger it manually
    catchup=False
) as dag:

    # fact table creation
  # Use the new universal operator
    build_fact_table = SQLExecuteQueryOperator(
        task_id='create_rental_fact',
        conn_id='postgres_pagila', # Note: it's just 'conn_id' now, not 'postgres_conn_id'
        sql="""
            CREATE TABLE IF NOT EXISTS fact_rental (
                rental_id INTEGER PRIMARY KEY,
                rental_date TIMESTAMP,
                customer_id INTEGER,
                film_id INTEGER,
                amount DECIMAL(10,2)
            );

            INSERT INTO fact_rental (rental_id, rental_date, customer_id, film_id, amount)
            SELECT 
                r.rental_id,
                r.rental_date,
                r.customer_id,
                i.film_id,
                p.amount
            FROM rental r
            JOIN payment p ON r.rental_id = p.rental_id
            JOIN inventory i ON r.inventory_id = i.inventory_id
            ON CONFLICT (rental_id) DO NOTHING;
        """
    )