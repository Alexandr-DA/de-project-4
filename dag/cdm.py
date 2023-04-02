import logging
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from typing import Dict, List
import datetime as dt
import os

import pendulum
from airflow.decorators import dag, task
import requests
import time
import random
import json
from datetime import datetime
from typing import Any, Dict

from bson.objectid import ObjectId
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.variable import Variable

conn_name = "pg_connect"


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 3, 30),
}

dag = DAG(
    'cdm',
    default_args=default_args,
    schedule_interval='0/60 * * * *', 
)

def rate(ds, connect):
    print(f"The value of ds is {ds}")
    postgres_hook = PostgresHook(postgres_conn_id=connect)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
                        f""" 
                INSERT INTO cdm.couriers_rate (courier_id, "year", "month", average_rate)
                SELECT courier_id, extract(YEAR FROM o.order_ts), extract(MONTH FROM o.order_ts), AVG(DISTINCT rate) 
                FROM dds.deliveries d
                        LEFT JOIN dds.orders o 
                            ON o.id = d.order_id 
                WHERE o.order_ts BETWEEN date_trunc('month', '{ds}'::DATE) AND '{ds}'::DATE
                GROUP BY courier_id, extract(YEAR FROM o.order_ts), extract(MONTH FROM o.order_ts)
                ON CONFLICT (courier_id, "year", "month") DO UPDATE
                            SET
                            average_rate = EXCLUDED.average_rate
                        """,
                       )
    conn.commit()

def courier_ledger (ds, connect):
    print(f"The value of ds is {ds}")
    postgres_hook = PostgresHook(postgres_conn_id=connect)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
                        f""" 
INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
SELECT q.courier_id
		, q.courier_name
		, q."YEAR" 
		, q."MONTH"  
		, COUNT(q.id)  
		, SUM(q.order_sum)  
		, AVG(q.rate) 
		, SUM(q.order_processing_fee)  
		, SUM(q.order_sum_cour)  
		, SUM(q.tip_sum) 
		, SUM(q.order_sum)  + SUM(q.tip_sum)*0.95  
FROM (
		                            
	SELECT d.courier_id 
	, c."name" courier_name 
	, extract (YEAR FROM o.order_ts) "YEAR"  
	, extract (MONTH FROM o.order_ts) "MONTH"  
	, o.id
	, o.order_sum
	, d.rate
	, o.order_sum * 0.25 order_processing_fee
	, d.tip_sum
	, o.order_sum * 0.05 order_sum_cour
	, CASE 
		
		WHEN r.average_rate < 4 THEN 
						CASE WHEN o.order_sum::NUMERIC * 0.05 < 100 THEN 100::MONEY
							ELSE o.order_sum::NUMERIC  * 0.05::MONEY
						END
		WHEN r.average_rate >= 4 AND r.average_rate < 4.5 THEN 
						CASE WHEN o.order_sum::NUMERIC * 0.05 < 150 THEN 150::MONEY
							ELSE o.order_sum::NUMERIC * 0.07::MONEY
						END
		WHEN r.average_rate >= 4.5 AND r.average_rate < 4.9 THEN
						CASE WHEN o.order_sum::NUMERIC * 0.08 < 175 THEN 175::MONEY
							ELSE o.order_sum::NUMERIC * 0.08::MONEY
						END
		ELSE CASE WHEN o.order_sum::NUMERIC * 0.1 < 200 THEN 200::MONEY
				ELSE (o.order_sum::NUMERIC * 0.1)::MONEY
			END
	END courier_order_sum
	FROM dds.deliveries d  
	LEFT JOIN  dds.couriers c 
		ON c.id = d.courier_id 
	LEFT JOIN dds.adress a 
		ON a.id = d.adress_id 
	LEFT JOIN dds.orders o 
		ON o.id = d.order_id  
	LEFT JOIN cdm.couriers_rate r
		ON r."year" = EXTRACT(year FROM o.order_ts)
			AND r."month" = EXTRACT(month FROM o.order_ts)
			AND r.courier_id = c.id
    WHERE o.order_ts BETWEEN date_trunc('month', '{ds}'::DATE) AND '{ds}'::DATE
						
			) q
GROUP BY q.courier_id
		, q.courier_name
		, q."YEAR" 
		, q."MONTH"  
ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
SET courier_name = EXCLUDED.courier_name,
	orders_count = EXCLUDED.orders_count,
	orders_total_sum = EXCLUDED.orders_total_sum,
	rate_avg = EXCLUDED.rate_avg,
	order_processing_fee = EXCLUDED.order_processing_fee,
	courier_order_sum = EXCLUDED.courier_order_sum,
	courier_tips_sum = EXCLUDED.courier_tips_sum,
	courier_reward_sum = EXCLUDED.courier_reward_sum
                                        """,
                       )
    conn.commit()

rate_task = PythonOperator(
    task_id='rate',
    python_callable=rate,
    op_kwargs={'ds' : '{{ ds }}',
               'connect' : conn_name},
    dag=dag,
)


ledger_task = PythonOperator(
    task_id='courier_ledger',
    python_callable=courier_ledger,
    op_kwargs={'ds' : '{{ ds }}',
               'connect' : conn_name},
    dag=dag,
)

rate_task >> ledger_task