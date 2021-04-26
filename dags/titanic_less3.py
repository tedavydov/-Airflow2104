from airflow.decorators import dag
from util.settings import default_settings
from util.sub import load_settings, download_dataset, pivot_dataset, mean_fare_per_class


# ===============================================================
# построение ДАГа
@dag(**default_settings())
def example_taskflow_api_etl():
    # параметры для инициализации DAG-а
    init_settings = load_settings(file_name='titanic')

    set_with_file = download_dataset(init=init_settings)

    pivot_dataset(set_with_file)
    mean_fare_per_class(set_with_file)


# ===============================================================
# вызов ДАГа
example_etl_dag = example_taskflow_api_etl()
# ===============================================================
