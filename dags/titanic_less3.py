from airflow.models import DAG

from util.settings import default_settings
from util.sub import download_dataset, pivot_dataset, mean_fare_per_class

# ===============================================================
# параметры для инициализации DAG-а
dataset_name = 'titanic'
dataset_url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
core_ops_1 = 'download_dataset'
# connect_url = 'postgresql://airflow:airflow@localhost:5432/data_warehouse'
connect_url = 'postgresql+psycopg2://airflow:airflow@localhost:5432/data_warehouse'

init_param_download = {
    'dataset_url': dataset_url,
    'xcom_dataset_name': dataset_name
}

init_param_pivot = {
    'url': connect_url,
    'core_ops': core_ops_1,
    'table_name': 'tab_pivot_dataset',
    'xcom_dataset_name': dataset_name
}

init_param_mean = {
    'url': connect_url,
    'core_ops': core_ops_1,
    'table_name': 'tab_mean_fare',
    'xcom_dataset_name': dataset_name
}

# ===================================================================================
# построение ДАГа
with DAG(**default_settings()) as dag:
    download_dataset(init=init_param_download) >> (pivot_dataset(init=init_param_pivot), mean_fare_per_class(init=init_param_mean))

