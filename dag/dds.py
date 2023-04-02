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


conn_name = "pg_connect"

restaraunts_query = """
                SELECT *
                  FROM stg.restaraunts
                 WHERE ts = '{last_ts}'
                    """
rest_status = """
                SELECT value
                  FROM dds.hooks
                 WHERE name = 'last_restaraunts_load' 
            """

couriers_query = """
                SELECT *
                  FROM stg.couriers
                 WHERE ts = '{last_ts}'
                    """

cours_status = """
                SELECT value
                  FROM dds.hooks
                 WHERE name = 'last_couriers_load' 
            """

deliveries_query = """
                SELECT *
                  FROM stg.deliveries
                 WHERE ts = '{last_ts}'
                    """

adress_status = """
                SELECT value
                  FROM dds.hooks
                 WHERE name = 'last_adress_load' 
            """

orders_status = """
                SELECT value
                  FROM dds.hooks
                 WHERE name = 'last_orders_load' 
            """

delivery_status = """
                SELECT value
                  FROM dds.hooks
                 WHERE name = 'last_delivery_load' 
            """

# функция для чтения таблиц
def get_table(connect, query_get):
    hook = PostgresHook(postgres_conn_id=connect)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query_get)
    table = cursor.fetchall()
    return table

# функция для ETL rest
def etl_rest(connect, query_get_status, query_get, date):
    # получим последнюю дату загрузки
    last_ts = get_table(connect, query_get_status)
    print('=============================================')
    print(last_ts[0][0])
    # получим макс тс для фиксации
    max_ts = date
    #max_ts = get_table(connect, 'SELECT MAX(ts) FROM stg.deliveries')
    #max_ts = str(max_ts[0][0])
    print(connect, max_ts)
    # формируем запрос на получение ресторанов
    query_get = query_get.format(last_ts = last_ts[0][0])
    print(query_get)
    # сохраняем в переменную
    restaraunts = get_table(connect, query_get)
    # установим коннект для записи
    postgres_hook = PostgresHook(postgres_conn_id=connect)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    # в итерации запишем в базу ресторанов
    for record in restaraunts:
        list_of_restaraunts = str2json(record[1])
        for rest in list_of_restaraunts:
            #print(rest)
            cursor.execute(
                        """
                            INSERT INTO dds.restaraunts (_id, name)
                            VALUES (%(_id)s, %(name)s)
                            ON CONFLICT (_id) DO UPDATE
                            SET
                            name = EXCLUDED.name;
                        """,
                        {
                            "_id": rest['_id'],
                            "name": rest['name'],
                        },)
        conn.commit()
    # поменяем дату последней загрузки
    cursor.execute(
                        """
                            UPDATE dds.hooks
                            SET VALUE = %(ts)s
                            WHERE name = 'last_restaraunts_load' 
                        """,
                        {
                            "ts": max_ts,
                        },)
    conn.commit()

# функция для ETL couriers
def etl_cours(connect, query_get_status, query_get, date):
    # получим последнюю дату загрузки
    last_ts = get_table(connect, query_get_status)
    print('=============================================')
    print(last_ts[0][0])
    # получим макс тс для фиксации
    max_ts = date
    #max_ts = get_table(connect, 'SELECT MAX(ts) FROM stg.couriers')
    #max_ts = str(max_ts[0][0])
    print(connect, max_ts)
    # формируем запрос на получение курьеров
    query_get = query_get.format(last_ts = last_ts[0][0])
    print(query_get)
    # сохраняем в переменную
    couriers = get_table(connect, query_get)
    # установим коннект для записи
    postgres_hook = PostgresHook(postgres_conn_id=connect)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    # в итерации запишем в базу ресторанов
    for record in couriers:
        list_of_couriers = str2json(record[1])
        for cour in list_of_couriers:
            #print(rest)
            cursor.execute(
                        """
                            INSERT INTO dds.couriers (_id, name)
                            VALUES (%(_id)s, %(name)s)
                            ON CONFLICT (_id) DO UPDATE
                            SET
                            name = EXCLUDED.name;
                        """,
                        {
                            "_id": cour['_id'],
                            "name": cour['name'],
                        },)
        conn.commit()
    # поменяем дату последней загрузки
    cursor.execute(
                        """
                            UPDATE dds.hooks
                            SET VALUE = %(ts)s
                            WHERE name = 'last_couriers_load' 
                        """,
                        {
                            "ts": max_ts,
                        },)
    conn.commit()

# функция для ETL adress
def etl_adress(connect, query_get_status, query_get, date):
    # получим последнюю дату загрузки
    last_ts = get_table(connect, query_get_status)
    print('=============================================')
    print(last_ts[0][0])
    # получим макс тс для фиксации
    max_ts = date
    #max_ts = get_table(connect, 'SELECT MAX(ts) FROM stg.deliveries')
    #max_ts = str(max_ts[0][0])
    print(connect, max_ts)
    # формируем запрос на получение адрессов
    query_get = query_get.format(last_ts = last_ts[0][0])
    print(query_get)
    # сохраняем в переменную
    delieveries = get_table(connect, query_get)
    # установим коннект для записи
    postgres_hook = PostgresHook(postgres_conn_id=connect)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    for record in delieveries:
        # в итерации запишем в базу адресов
        list_of_deliveries = str2json(record[1])
        for deliver in list_of_deliveries:
            cursor.execute(
                        """
                            INSERT INTO dds.adress (name)
                            VALUES (%(name)s)
                            ON CONFLICT (name) DO UPDATE
                            SET
                            name = EXCLUDED.name;
                        """,
                        {
                            "name": deliver['address'],
                        },)
            conn.commit()

    # поменяем дату последней загрузки
    cursor.execute(
                        """
                            UPDATE dds.hooks
                            SET VALUE = %(ts)s
                            WHERE name = 'last_adress_load' 
                        """,
                        {
                            "ts": max_ts,
                        },)
    conn.commit()

# функция для ETL orders
def etl_orders(connect, query_get_status, query_get, date):
    # получим последнюю дату загрузки
    last_ts = get_table(connect, query_get_status)
    print('=============================================')
    print(last_ts[0][0])
    # получим макс тс для фиксации
    max_ts = date
    #max_ts = get_table(connect, 'SELECT MAX(ts) FROM stg.deliveries')
    #max_ts = str(max_ts[0][0])
    print(connect, max_ts)
    # формируем запрос на получение адрессов
    query_get = query_get.format(last_ts = last_ts[0][0])
    print(query_get)
    # сохраняем в переменную
    delieveries = get_table(connect, query_get)
    # установим коннект для записи
    postgres_hook = PostgresHook(postgres_conn_id=connect)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    for record in delieveries:
        # в итерации запишем в базу адресов
        list_of_deliveries = str2json(record[1])
        for deliver in list_of_deliveries:
            cursor.execute(
                        """
                            INSERT INTO dds.orders (name, order_ts)
                            VALUES (%(name)s, %(ts)s)
                            ON CONFLICT (name) DO UPDATE
                            SET
                            name = EXCLUDED.name,
                            order_ts = EXCLUDED.order_ts;
                        """,
                        {
                            "name": deliver['order_id'],
                            "ts": deliver['order_ts'],
                        },)
            conn.commit()

    # поменяем дату последней загрузки
    cursor.execute(
                        """
                            UPDATE dds.hooks
                            SET VALUE = %(ts)s
                            WHERE name = 'last_orders_load' 
                        """,
                        {
                            "ts": max_ts,
                        },)
    conn.commit()


    
# функция для ETL orders
def etl_orders(connect, query_get_status, query_get, date):
    # получим последнюю дату загрузки
    last_ts = get_table(connect, query_get_status)
    print('=============================================')
    print(last_ts[0][0])
    # получим макс тс для фиксации
    max_ts = date
    #max_ts = get_table(connect, 'SELECT MAX(ts) FROM stg.deliveries')
    #max_ts = str(max_ts[0][0])
    print(connect, max_ts)
    # формируем запрос на получение адрессов
    query_get = query_get.format(last_ts = last_ts[0][0])
    print(query_get)
    # сохраняем в переменную
    delieveries = get_table(connect, query_get)
    # установим коннект для записи
    postgres_hook = PostgresHook(postgres_conn_id=connect)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    for record in delieveries:
        print(record)
        # в итерации запишем в базу адресов
        list_of_deliveries = str2json(record[1])
        for deliver in list_of_deliveries:
            print(deliver)
            cursor.execute(
                        """
                            INSERT INTO dds.orders (name, order_ts, order_sum)
                            VALUES (%(name)s, %(order_ts)s, %(order_sum)s)
                            ON CONFLICT (name) DO UPDATE
                            SET
                            order_ts = EXCLUDED.order_ts,
                            order_sum = EXCLUDED.order_sum;
                        """,
                        {
                            "name": deliver['order_id'],
                            "order_ts": deliver['order_ts'],
                            "order_sum": deliver['sum'],
                        },)
            conn.commit()

    # поменяем дату последней загрузки
    cursor.execute(
                        """
                            UPDATE dds.hooks
                            SET VALUE = %(ts)s
                            WHERE name = 'last_orders_load' 
                        """,
                        {
                            "ts": max_ts,
                        },)
    conn.commit()



# функция для ETL delieveries
def etl_delieveries(connect, query_get_status, query_get, date):
    # получим последнюю дату загрузки
    last_ts = get_table(connect, query_get_status)
    print('=============================================')
    print(last_ts[0][0])
    # получим макс тс для фиксации
    max_ts = date
    #max_ts = get_table(connect, 'SELECT MAX(ts) FROM stg.deliveries')
    #max_ts = str(max_ts[0][0])
    # формируем запрос на получение доставок
    query_get = query_get.format(last_ts = last_ts[0][0])
    # сохраняем в переменную
    delieveries = get_table(connect, query_get)
    # установим коннект для записи
    postgres_hook = PostgresHook(postgres_conn_id=connect)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    for record in delieveries:
        # в итерации запишем в базу адресов
        list_of_deliveries = str2json(record[1])
        for deliver in list_of_deliveries:
            courier_id = get_table(connect, f'''
                            SELECT id 
                              FROM dds.couriers
                             WHERE "_id" = '{deliver['courier_id']}'
                            ''')
            if len(courier_id) == 0:
                cursor.execute(
                        """
                            INSERT INTO dds.couriers (_id, name)
                            VALUES (%(_id)s, %(name)s)
                            ON CONFLICT (_id) DO UPDATE
                            SET
                            name = EXCLUDED.name;
                        """,
                        {
                            "_id": deliver['courier_id'],
                            "name": 'Unknown',
                        },)
                conn.commit()
                courier_id = get_table(connect, f'''
                            SELECT id 
                              FROM dds.couriers
                             WHERE "_id" = '{deliver['courier_id']}'
                            ''')
                courier_id = courier_id[0][0]
            else:
                courier_id = courier_id[0][0]
            print('===============================')
            print(deliver['address'])
            adress_id = get_table(connect, f'''
                            SELECT id 
                              FROM dds.adress
                             WHERE "name" = '{deliver['address']}'
                            ''')
            order_id = get_table(connect, f'''
                            SELECT id 
                              FROM dds.orders
                             WHERE "name" = '{deliver['order_id']}'
                            ''')
            cursor.execute(
                        """
                            INSERT INTO dds.deliveries (courier_id, adress_id, name, delivery_ts, order_id, rate, tip_sum)
                            VALUES (%(courier_id)s, %(adress_id)s, %(name)s, %(delivery_ts)s, %(order_id)s, %(rate)s, %(tip_sum)s)
                            ON CONFLICT (name) DO UPDATE
                            SET
                            courier_id = EXCLUDED.courier_id,
                            adress_id = EXCLUDED.adress_id,
                            delivery_ts = EXCLUDED.delivery_ts,
                            order_id = EXCLUDED.order_id,
                            rate = EXCLUDED.rate,
                            tip_sum = EXCLUDED.tip_sum
                        """,
                        {
                            "courier_id": courier_id,
                            "adress_id": adress_id[0][0],
                            "name": deliver['delivery_id'],
                            "delivery_ts": deliver['delivery_ts'],
                            "order_id": order_id[0][0],
                            "rate": deliver['rate'],
                            "tip_sum": deliver['tip_sum'],
                        },)
            conn.commit()

    # поменяем дату последней загрузки
    cursor.execute(
                        """
                            UPDATE dds.hooks
                            SET VALUE = %(ts)s
                            WHERE name = 'last_delivery_load' 
                        """,
                        {
                            "ts": max_ts,
                        },)
    conn.commit()

args = {
   'owner': 'airflow',  # Информация о владельце DAG
   'start_date': dt.datetime(2023, 3, 30),
   'retries': 7,  # Количество повторений в случае неудач
   'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
}



with DAG(
    dag_id='dds',  # Имя DAG
    schedule_interval='0/60 * * * *',  # Периодичность запуска, например, "00 15 * * *"
    default_args=args,  # Базовые аргументы
) as dag:
    
    # рестораны
    restaraunts_etl =  PythonOperator(
    task_id='restaurants_dds',
    python_callable = etl_rest,
    provide_context=True,
    op_kwargs={
            'connect' : conn_name,
            'query_get_status': rest_status,
            'query_get' : restaraunts_query,
            'date' : '{{ts}}'
                },
    )

    # курьеры
    couriers_etl =  PythonOperator(
    task_id='couriers_dds',
    python_callable = etl_cours,
    provide_context=True,
    op_kwargs={
            'connect' : conn_name,
            'query_get_status': cours_status,
            'query_get' : couriers_query,
            'date' : '{{ts}}'
                },
    )

    # адреса
    adress_etl =  PythonOperator(
    task_id='adress_dds',
    python_callable = etl_adress,
    provide_context=True,
    op_kwargs={
            'connect' : conn_name,
            'query_get_status': adress_status,
            'query_get' : deliveries_query,
            'date' : '{{ts}}'
                },
    )

    # заказы
    orders_etl =  PythonOperator(
    task_id='orders_dds',
    python_callable = etl_orders,
    provide_context=True,
    op_kwargs={
            'connect' : conn_name,
            'query_get_status': orders_status,
            'query_get' : deliveries_query,
            'date' : '{{ts}}'
                },
    )
    # доставки
    deliveries_etl =  PythonOperator(
    task_id='deliveries_dds',
    python_callable = etl_delieveries,
    provide_context=True,
    op_kwargs={
            'connect' : conn_name,
            'query_get_status': delivery_status,
            'query_get' : deliveries_query,
            'date' : '{{ts}}'
                },
    )

    [adress_etl, couriers_etl, restaraunts_etl, orders_etl] >> deliveries_etl