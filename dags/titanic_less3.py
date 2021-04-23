from airflow.models import DAG

from util.settings import default_settings
from util.sub import download_dataset_2, pivot_dataset_2, mean_fare_per_class_2

# from util.sub import download_dataset, pivot_dataset, mean_fare_per_class
# from util.sub import load_settings, download_dataset, pivot_dataset, mean_fare_per_class
# ===============================================================
# параметры для инициализации DAG-а
# dag_settings = load_settings(file_name='titanic')
# init_param_download = dag_settings.get('init_param_1')
# init_param_pivot = dag_settings.get('init_param_2')
# init_param_mean = dag_settings.get('init_param_3')
#
# import pdb; pdb.set_trace()  # дебагер - отладка программы в интерактивном режиме
# # print(init_param_download)
# # print(init_param_pivot)
# # print(init_param_mean)

dataset_name = 'titanic'
dataset_url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
core_ops_1 = 'download_dataset'
# connect_url = 'postgresql://airflow:airflow@localhost:5432/data_warehouse'
# connect_url = 'postgresql+psycopg2://airflow:airflow@localhost:5432/data_warehouse'
connect_url = 'postgresql+psycopg2://airflow:airflow@localhost:5432/data_warehouse'

init_param_download = {
    'dataset_url': dataset_url,
    'dataset_name': dataset_name
}

init_param_pivot = {
    'url': connect_url,
    'core_ops': core_ops_1,
    'table_name': 'tab_pivot_dataset',
    'dataset_name': dataset_name
}

init_param_mean = {
    'url': connect_url,
    'core_ops': core_ops_1,
    'table_name': 'tab_mean_fare',
    'dataset_name': dataset_name
}

# ===================================================================================
# построение ДАГа
with DAG(**default_settings()) as dag:
    download_dataset_2(init=init_param_download) >> (pivot_dataset_2(init=init_param_pivot), mean_fare_per_class_2(init=init_param_mean))

