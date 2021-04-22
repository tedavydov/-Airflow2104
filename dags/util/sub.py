import os
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2
import logging
import json
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from util.deco import python_operator


# ===============================================================
def load_settings(file_name=None, set_name=None):
    '''
    Считывает настройки паука из файла JSON
    :param file_name: по умолч. "./search_settings.json"
    :return: список стартовых ссылок и параметры поиска для паука
    '''
    if file_name and isinstance(file_name, str):
        file_name = "./" + file_name + "_settings.json"
    else:
        file_name = "./dag_settings.json"
    if set_name and isinstance(set_name, str):
        get_key = set_name
    else:
        get_key = "core"
    dag_settings = {}
    try:
        with open(file_name, "r", encoding="utf-8") as json_file:
            sett = json.load(json_file)
        if sett:
            for px in range(sett.get("init_param_count")):
                init_key = "init_param_" + str(px)
                tmp_init = sett.get(init_key)
                if tmp_init:
                    for kx in tmp_init.keys:
                        key_new = sett.get(tmp_init.get(kx))
                        if key_new:
                            tmp_init[kx] = key_new
                sett[init_key] = tmp_init
            dag_settings = sett.get(get_key)
    # =======================================================================
    except Exception as e:
        print('=' * 100, f'\nload_settings ERROR: Ошибка чтения JSON файла {file_name}\n{e}')
    # =======================================================================
    return dag_settings


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
    # engine = sqlalchemy.create_engine(url, client_encoding='utf8')
    engine = create_engine('sqlite:///.\\data_warehouse.db', echo=True)
    df = pd.read_sql(query, con=engine)
    return df


def to_db(url, table, df):
    # url = 'postgresql://airflow:airflow@localhost:5432/data_warehouse'
    # url = 'postgresql+psycopg2://airflow:airflow@localhost:5432/data_warehouse'
    # engine = sqlalchemy.create_engine(url, client_encoding='utf8')
    engine = create_engine('sqlite:///.\\data_warehouse.db', echo=True)

    # df.to_sql(table, con=engine, if_exists='append')
    # import pdb; pdb.set_trace()  # дебагер - отладка программы в интерактивном режиме
    df.to_sql(table, con=engine, if_exists='replace', index=False)


# ===============================================================

def file_path(file_name):
    if isinstance(file_name, str):
        res = os.path.join(os.path.expanduser('~'), file_name)
    else:
        res = file_name
    return res


# ===============================================================

@python_operator()
def connection_operator(**context):
    hook = BaseHook.get_hook('airflow')
    hook.get_records('SELECT * FROM connection')
    # import pdb; pdb.set_trace()  # дебагер - отладка программы в интерактивном режиме


# ===============================================================

@python_operator()
def download_dataset_2(init, **context):
    # скачиваем датасет
    df = pd.read_csv(init['dataset_url'])
    # скачанный датасет пушится в XCom (он весит ~50 КБ)
    # import pdb;  pdb.set_trace()  # дебагер - отладка программы в интерактивном режиме
    context['task_instance'].xcom_push(init['dataset_name'], df.to_json(orient="index"))


@python_operator()
def pivot_dataset_2(init, **context):
    table_name = init['table_name']
    try:
        # Имена таблиц в PostgreSQL заданы в Variables
        table_name = Variable.get(table_name)
        # датасет пуллится из XCom и передается в pivot
        data = context['task_instance'].xcom_pull(task_ids=init['core_ops'], key=init['dataset_name'])
        # import pdb; pdb.set_trace()  # дебагер - отладка программы в интерактивном режиме
        df = pd.DataFrame.from_string().from_dict(json.loads(data), orient="index")
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
def mean_fare_per_class_2(init, **context):
    table_name = init['table_name']
    try:
        # Имена таблиц в PostgreSQL заданы в Variables
        table_name = Variable.get(table_name)
        # датасет пуллится из XCom и передается в mean_fare
        data = context['task_instance'].xcom_pull(task_ids=init['core_ops'], key=init['dataset_name'])
        df = pd.DataFrame.from_dict(json.loads(data), orient="index")
        df = np.round(df.pivot_table(index=['Pclass'], values='Fare', aggfunc='sum').reset_index(), 2)
        to_db(init['url'], df, table_name)
    except KeyError as e:
        logging.warning(f'Table: ({table_name}) variable is undefined, error: {e}')
    else:
        logging.info('TableName from variable equals %s', table_name)


# ===============================================================

@python_operator()
def download_dataset(init, **context):
    # import pdb; pdb.set_trace()  # дебагер - отладка программы в интерактивном режиме
    # скачиваем датасет
    df = pd.read_csv(init['dataset_url'])
    # скачанный датасет сохраним в файл
    df.to_csv(file_path('titanic.csv'), encoding='utf-8')


@python_operator()
def pivot_dataset(init, **context):
    table_name = init['table_name']
    try:
        # Имена таблиц в PostgreSQL заданы в Variables
        table_name = Variable.get(table_name)

        # датасет загружаем из файла 'titanic.csv'
        titanic_df = pd.read_csv(file_path('titanic.csv'))
        df = titanic_df.pivot_table(index=['Sex'],
                                    columns=['Pclass'],
                                    values='Name',
                                    aggfunc='count').reset_index()
        # результат сохраняем в файл 'titanic_pivot.csv'
        df.to_csv(file_path('titanic_pivot.csv'))
        # # датасет пуллится из XCom и передается в pivot
        # data = context['task_instance'].xcom_pull(task_ids=init['core_ops'], key=init['dataset_name'])
        # import pdb; pdb.set_trace()  # дебагер - отладка программы в интерактивном режиме
        # df = pd.DataFrame.from_string() .from_dict(json.loads(data), orient="index")
        # df = df.pivot_table(index=['Sex'],
        #                     columns=['Pclass'],
        #                     values='Name',
        #                     aggfunc='count').reset_index()
        # to_db(init['url'], df, table_name)
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

        # датасет загружаем из файла 'titanic.csv'
        titanic_df = pd.read_csv(file_path('titanic.csv'))
        # import pdb; pdb.set_trace()  # дебагер - отладка программы в интерактивном режиме
        df = titanic_df.groupby(['Pclass']).agg({'Fare': 'mean'}).reset_index()

        # результат сохраняем в файл 'titanic_mean_fares.csv'
        df.to_csv(file_path('titanic_mean_fares.csv'))

        # # датасет пуллится из XCom и передается в mean_fare
        # data = context['task_instance'].xcom_pull(task_ids=init['core_ops'], key=init['dataset_name'])
        # df = pd.DataFrame.from_dict(json.loads(data), orient="index")
        # df = np.round(df.pivot_table(index=['Pclass'], values='Fare', aggfunc='sum').reset_index(), 2)
        # to_db(init['url'], df, table_name)
    except KeyError as e:
        logging.warning(f'Table: ({table_name}) variable is undefined, error: {e}')
    else:
        logging.info('TableName from variable equals %s', table_name)
