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


# переменные для запросов 
restaurants = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants'
couriers = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers'
deliveries = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?sort_field="_id"&sort_direction="asc"'

limit = 1000
headers = {
           'X-Nickname':'yashmaobl',
           'X-Cohort': '8',
           'X-API-KEY':'25c27781-8fde-4b30-a22e-524044a7580f',
        }

conn_name = "pg_connect"
sql_query = 'SELECT 1'

# функции для работы с json
def json2str(obj: Any) -> str:
    return json.dumps(to_dict(obj), sort_keys=True, ensure_ascii=False)


def str2json(str: str) -> Dict:
    return json.loads(str)


def to_dict(obj, classkey=None):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, dict):
        data = {}
        for (k, v) in obj.items():
            data[k] = to_dict(v, classkey)
        return data
    elif hasattr(obj, "_ast"):
        return to_dict(obj._ast())
    elif hasattr(obj, "__iter__") and not isinstance(obj, str):
        return [to_dict(v, classkey) for v in obj]
    elif hasattr(obj, "__dict__"):
        data = dict([(key, to_dict(value, classkey))
                     for key, value in obj.__dict__.items()
                     if not callable(value) and not key.startswith('_')])
        if classkey is not None and hasattr(obj, "__class__"):
            data[classkey] = obj.__class__.__name__
        return data
    else:
        return obj
    
# функция для обращения к API 
def get_query(date, query, headers, limit):
    list_to_return = []
    offset = 0
    print('DATE =====================================', date)

    try:
        start_dt = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')
        prev_dt = (datetime.strptime(date, '%Y-%m-%dT%H:%M:%S%z') + timedelta(hours=-1)).strftime('%Y-%m-%d %H:%M:%S')
    except:
        start_dt = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f%z').strftime('%Y-%m-%d %H:%M:%S')
        prev_dt = (datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f%z') + timedelta(hours=-1)).strftime('%Y-%m-%d %H:%M:%S')
    while True:
        data = {
        'limit' : limit,
        'offset' : offset,
        'from' : prev_dt,
        'to' : start_dt,
        }
        print(offset)
        r = requests.get(query, params = data, headers=headers)
        offset += limit
        query_res = r.json()
        if len(query_res) == 0:
            break
        else:
            for item in query_res:
                list_to_return.append(item)
        time.sleep(random.random())   
    return list_to_return

# функция для ETL ресторанов
def etl_rest(date, query, headers, limit, connect, sql):
    # коннект к БД 
    postgres_hook = PostgresHook(postgres_conn_id=connect)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # получим рестораны
    rests = get_query(date, query, headers, limit)
    rests = json2str(rests)
    #print('================================================')
    #print(type(rests))
    #print(rests)
    cursor.execute(
                        """
                            INSERT INTO stg.restaraunts (ts, value)
                            VALUES (%(ts)s, %(value)s)
                            ON CONFLICT (ts) DO UPDATE
                            SET
                            value = EXCLUDED.value;
                        """,
                        {
                            "ts": date,
                            "value": rests,
                        },)
    conn.commit()
  
# функция для ETL курьеров
def etl_cours(date, query, headers, limit, connect, sql):
    # коннект к БД 
    postgres_hook = PostgresHook(postgres_conn_id=connect)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # получим курьеров
    cours = get_query(date, query, headers, limit)
    cours = json2str(cours)
    #print('================================================')
    #print(type(cours))
    #print(cours)
    cursor.execute(
                        """
                            INSERT INTO stg.couriers (ts, value)
                            VALUES (%(ts)s, %(value)s)
                            ON CONFLICT (ts) DO UPDATE
                            SET
                            value = EXCLUDED.value;
                        """,
                        {
                            "ts": date,
                            "value": cours,
                        },)
    conn.commit()

# функция для ETL заказов
def etl_deliveries(date, query, headers, limit, connect):
    # коннект к БД 
    postgres_hook = PostgresHook(postgres_conn_id=connect)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # получим заказы
    deliveries = get_query(date, query, headers, limit)
    deliveries = json2str(deliveries)
    #print('================================================')
    #print(type(deliveries))
    #print(deliveries)
    cursor.execute(
                        """
                            INSERT INTO stg.deliveries (ts, value)
                            VALUES (%(ts)s, %(value)s)
                            ON CONFLICT (ts) DO UPDATE
                            SET
                            value = EXCLUDED.value;
                        """,
                        {
                            "ts": date,
                            "value": deliveries,
                        },)
    conn.commit()
            


args = {
   'owner': 'airflow',  # Информация о владельце DAG
   'start_date': dt.datetime(2023, 3, 30),
   'retries': 7,  # Количество повторений в случае неудач
   'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
}


with DAG(
    dag_id='stg',  # Имя DAG
    schedule_interval='0/60 * * * *',  # Периодичность запуска, например, "00 15 * * *"
    default_args=args,  # Базовые аргументы
) as dag:
    
    # рестораны
    restaraunts_etl =  PythonOperator(
    task_id='restaraunts_from_api',
    python_callable = etl_rest,
    provide_context=True,
    op_kwargs={'query': restaurants,
                'headers': headers,
                'limit' : limit,
                'date' : '{{ ts }}',
                'connect' : conn_name,
                'sql' : sql_query
                #'from_dt':prev_ts,
                #'to_dt':start_ts
                },
)
    # курьеры
    couriers_etl =  PythonOperator(
    task_id='couriers_api',
    python_callable = etl_cours,
    provide_context=True,
    op_kwargs={'query': couriers,
                'headers': headers,
                'limit' : limit,
                'date' : '{{ ts }}',
                'connect' : conn_name,
                'sql' : sql_query
                #'from_dt':prev_ts,
                #'to_dt':start_ts
                },

    dag = dag
)
    # доставки
    deliveries_etl =  PythonOperator(
    task_id='deliveries_api',
    python_callable = etl_deliveries,
    provide_context=True,
    op_kwargs={'query': deliveries,
                'headers': headers,
                'limit' : limit,
                'date' : '{{ ts }}',
                'connect' : conn_name,
                'sql' : sql_query
                #'from_dt':prev_ts,
                #'to_dt':start_ts
                },

    dag = dag
)
    
    [restaraunts_etl, couriers_etl, deliveries_etl]