import os
import json
from sqlalchemy import create_engine

import pandas as pd
import logging
from airflow.models import Variable
from airflow.decorators import task


# ===============================================================

def from_db(url, query):
    engine = create_engine(url, echo=True)
    df = pd.read_sql(query, con=engine)
    return df


def to_db(url, df, table):
    engine = create_engine(url, echo=True)
    df.to_sql(table, con=engine, if_exists='replace', index=False)


# ===============================================================

def file_path(file_name, local):
    if isinstance(file_name, str):
        if isinstance(local, str):
            res = os.path.join(os.path.expanduser('.'), os.path.expanduser(local), file_name)
        elif isinstance(local, bool):
            if local:
                res = os.path.join(os.path.expanduser('.'), file_name)
                # res = os.path.expanduser(file_name)
            else:
                res = os.path.join(os.path.expanduser('~'), file_name)
    else:
        res = file_name
    return res


# ===============================================================
# параметры для инициализации DAG-а
@task
def load_settings(file_name=None, set_name=None):
    '''
    Считывает настройки DAG из файла JSON
    :param file_name: по умолч. "dag_settings.json"
    :return: dag_settings - словарь параметров
    '''
    if isinstance(file_name, str):
        # file_name = file_path(file_name + "_settings.json", True)
        file_name = file_path(file_name + "_settings.json", "dags")
    else:
        # file_name = file_path("dag_settings.json", True)
        file_name = file_path("dag_settings.json", "dags")
    logging.info('File load from %s', file_name)
    if set_name and isinstance(set_name, str):
        get_key = set_name
    else:
        get_key = "core"
    dag_settings = {}
    try:
        with open(file_name, "r", encoding="utf-8") as json_file:
            sett = json.load(json_file)
        if sett:
            for px in range(sett.get(get_key).get("init_param_count")):
                init_key = "init_param_" + str(px + 1)
                tmp_init = sett.get(init_key)
                if tmp_init:
                    for kx in tmp_init.keys():
                        key_new = sett.get(get_key).get(tmp_init.get(kx))
                        if key_new:
                            tmp_init[kx] = key_new
                sett[get_key][init_key] = tmp_init
            dag_settings = sett.get(get_key)
    # =======================================================================
    except Exception as e:
        logging.warning(f'load_settings ERROR: Ошибка чтения JSON файла ({file_name}) :: Load ERROR: {e}')
    # =======================================================================
    # import pdb; pdb.set_trace()  # дебагер - отладка программы в интерактивном режиме
    return dag_settings


# ===============================================================
# компоненты DAG-ов

@task
def download_dataset(init: dict):
    # import pdb; pdb.set_trace()  # дебагер - отладка программы в интерактивном режиме
    file_read = init.get('dataset_url')
    file_save = init.get('dataset_file')
    # скачиваем датасет
    df = pd.read_csv(file_read)
    # скачанный датасет сохраним в файл
    f_save = file_path(file_save, "dags")
    logging.info('File save to %s', f_save)
    df.to_csv(f_save, encoding='utf-8')
    # путь к скачанному датасету автоматически пушится в XCom при вызове return
    init.update({'file_path': f_save})
    return init


@task
def pivot_dataset(init: dict):
    file = init.get('file_path')
    logging.info('File init load from %s', file)
    table_name = init.get('init_param_2').get('table_name')
    logging.info('table_name 1 = %s', table_name)
    db_url = init.get('init_param_2').get('url')
    # db_url = init.get('connect_url_2')
    try:
        # Имена таблиц в PostgreSQL заданы в Variables
        table_name = Variable.get(table_name)
        logging.info('table_name 2 = %s', table_name)

        # датасет загружаем из файла 'titanic.csv'
        titanic_df = pd.read_csv(file)
        df = titanic_df.pivot_table(index=['Sex'],
                                    columns=['Pclass'],
                                    values='Name',
                                    aggfunc='count').reset_index()

        # результат сохраняем в базу данных sqlite
        to_db(db_url, df, table_name)
        # return table_name

        # проверим результат в базе данных sqlite
        df2 = from_db(db_url, f'select * from {table_name};')
        file_res = file_path('from_db_pivot.csv', "dags")
        logging.info('File result save to %s', file_res)
        df2.to_csv(file_res)
        return file_res

    except KeyError as e:
        logging.warning(f'Table: ({table_name}) variable is undefined, error: {e}')
    else:
        logging.info('TableName from variable equals %s', table_name)


@task
def mean_fare_per_class(init: dict):
    file = init.get('file_path')
    logging.info('File init load from %s', file)
    table_name = init.get('init_param_3').get('table_name')
    logging.info('table_name 1 = %s', table_name)
    db_url = init.get('init_param_3').get('url')
    # db_url = init.get('connect_url_2')
    try:
        # Имена таблиц заданы в Variables
        table_name = Variable.get(table_name)
        logging.info('table_name 2 = %s', table_name)

        # датасет загружаем из файла 'titanic.csv'
        titanic_df = pd.read_csv(file)
        df = titanic_df.groupby(['Pclass']).agg({'Fare': 'mean'}).reset_index()
        # df = np.round(df.pivot_table(index=['Pclass'], values='Fare', aggfunc='sum').reset_index(), 2)

        # результат сохраняем в базу данных sqlite
        to_db(db_url, df, table_name)
        # return table_name

        # проверим результат в базе данных sqlite
        df2 = from_db(db_url, f'select * from {table_name};')
        file_res = file_path('from_db_mean_fares.csv', "dags")
        logging.info('File result save to %s', file_res)
        df2.to_csv(file_res)
        return file_res

    except KeyError as e:
        logging.warning(f'Table: ({table_name}) variable is undefined, error: {e}')
    else:
        logging.info('TableName from variable equals %s', table_name)
