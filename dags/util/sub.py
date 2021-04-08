import pandas as pd
import numpy as np
import sqlalchemy
import psycopg2
import logging
import json
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from util.deco import python_operator

# ===============================================================

def from_db(url, query):
    # url = "dbname='data_warehouse' user='airflow' host='127.0.0.1' port='5432' password='airflow'"
    # engine = psycopg2.connect(url)
    # ===============================
    # url = 'postgresql://{}:{}@{}:{}/{}'
    # url = url.format(user, password, host, port, db)
    # db = 'data_warehouse'
    # url = 'postgresql://airflow:airflow@localhost:5432/data_warehouse'
    # url = 'postgresql+psycopg2://airflow:airflow@localhost:5432/data_warehouse'
    # The return value of create_engine() is our connection object
    engine = sqlalchemy.create_engine(url, client_encoding='utf8')
    df = pandas.read_sql(query, con=engine)
    return df


def to_db(url, table, df):
    # url = 'postgresql://airflow:airflow@localhost:5432/data_warehouse'
    # url = 'postgresql+psycopg2://airflow:airflow@localhost:5432/data_warehouse'
    engine = sqlalchemy.create_engine(url, client_encoding='utf8')
    # df.to_sql(table, con=engine, if_exists='append')
    df.to_sql(table, con=engine, if_exists='replace', index=False)

@python_operator()
def connection_operator(**context):
    hook = BaseHook.get_hook('airflow')
    hook.get_records('SELECT * FROM connection')
    import pdb;
    pdb.set_trace()  # дебагер - отладка программы в интерактивном режиме

# ===============================================================

@python_operator()
def download_dataset(init, **context):
    # скачиваем датасет
    df = pd.read_csv(init['dataset_url'])
    # скачанный датасет пушится в XCom (он весит ~50 КБ)
    context['dataset_src'].xcom_push(init['xcom_dataset_name'], df.to_json(orient="index"))


@python_operator()
def pivot_dataset(init, **context):
    table_name = init['table_name']
    try:
        # Имена таблиц в PostgreSQL заданы в Variables
        table_name = Variable.get(table_name)
        # датасет пуллится из XCom и передается в pivot
        data = context['dataset_src'].xcom_pull(task_ids=init['core_ops'], key=init['xcom_dataset_name'])
        df = pd.DataFrame.from_dict(json.loads(data), orient="index")
        df = df.pivot_table(index=['Sex'],
                            columns=['Pclass'],
                            values='Name',
                            aggfunc='count').reset_index()
        to_db(init['url'], df, table_name)
    except KeyError as e:
        logging.warning(f'Table: ({table_name}) variable is undefined, error: {e}')
    else:
        logging.info('TableName from variable equals %s', table_name)


@python_operator()
def mean_fare_per_class(init, **context):
    table_name = init['table_name']
    try:
        # Имена таблиц в PostgreSQL заданы в Variables
        table_name = Variable.get(table_name)
        # датасет пуллится из XCom и передается в mean_fare
        data = context['dataset_src'].xcom_pull(task_ids=init['core_ops'], key=init['xcom_dataset_name'])
        df = pd.DataFrame.from_dict(json.loads(data), orient="index")
        df = np.round(df.pivot_table(index=['Pclass'], values='Fare', aggfunc='sum').reset_index(), 2)
        to_db(init['url'], df, table_name)
    except KeyError as e:
        logging.warning(f'Table: ({table_name}) variable is undefined, error: {e}')
    else:
        logging.info('TableName from variable equals %s', table_name)
