import os
import datetime as dt
import pandas as pd
import numpy as np
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# базовые настройки DAG
dataset_url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
file_name_in = 'titanic.csv'
file_name_pivot = 'titanic_pivot.csv'
file_name_out = 'titanic_mean_fares.csv'
default_settings = {  # эти настройки подключены ниже при инициализации ДАГа
    'dag_id': 'titanic_pivot',  # Имя DAG
    # Периодичность запуска, например: cron format "00 15 * * *" или @daily @hourly
    'schedule_interval': '@hourly',  # None, '00 15 * * *',  '@daily', '@hourly',
    # ===============================================================
    # базовые аргументы DAG - которые попадут во ВСЕ ТАСКИ
    'args': {
        'owner': 'airflow',  # Информация о владельце DAG
        'retries': 1,  # Количество повторений в случае неудач
        'start_date': dt.datetime(2020, 7, 7, 0, 2),  # Время начала выполнения пайплайна
        'depends_on_past': False,  # Запуск DAG зависит ли от успешности окончания предыдущего запуска по расписанию
        'retry_delay': dt.timedelta(minutes=1)  # Пауза между повторами
    }
}


# ===================================================================================


def file_path(file_name):
    if isinstance(file_name, str):
        res = os.path.join(os.path.expanduser('~'), file_name)
    else:
        res = file_name
    return res


def pivot_dataset():
    titanic_df = pd.read_csv(file_path(file_name_in))
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    df.to_csv(file_path(file_name_pivot))


def mean_fare_per_class():
    df1 = pd.read_csv(file_path(file_name_in))
    df = np.round(df1.pivot_table(index=['Pclass'], values='Fare', aggfunc='sum').reset_index(), 2)
    df.to_csv(file_path(file_name_out))


def download_titanic_dataset():
    df = pd.read_csv(dataset_url)
    df.to_csv(file_path(file_name_in), encoding='utf-8')


# В контексте DAG'а зададим набор task'ок
# Объект-инстанс Operator'а - это и есть task
with DAG(
        dag_id=default_settings['dag_id'],  # Имя DAG
        schedule_interval=default_settings['schedule_interval'],  # Периодичность запуска, например, "00 15 * * *"
        default_args=default_settings['args'],  # Базовые аргументы
) as dag:
    # BashOperator, выполняющий указанную bash-команду
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
        dag=dag,
    )
    # Загрузка датасета
    create_titanic_dataset = PythonOperator(
        task_id='download_dataset',
        python_callable=download_titanic_dataset,
        dag=dag,
    )
    # Чтение, преобразование и запись датасета
    pivot_titanic_dataset = PythonOperator(
        task_id='pivot_dataset',
        python_callable=pivot_dataset,
        dag=dag,
    )
    # Подсчет среднеого арифметического цены билета по классу
    mean_fares_titanic_dataset = PythonOperator(
        task_id='mean_fare_per_class',
        python_callable=mean_fare_per_class,
        dag=dag,
    )
    # Вывод времени завершения дага
    # Пример строки: "Pipeline finished! Execution date is 2020-12-28"
    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is {{ ds }}"',
        dag=dag,
    )

    # Порядок выполнения тасок
    first_task >> create_titanic_dataset
    create_titanic_dataset >> pivot_titanic_dataset >> last_task
    create_titanic_dataset >> mean_fares_titanic_dataset >> last_task
    # Изначальный Порядок выполнения тасок
    # first_task >> create_titanic_dataset >> pivot_titanic_dataset
