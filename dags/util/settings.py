import datetime as dt
import inspect
import pathlib


def default_settings():
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    filename = module.__file__

    # базовые настройки DAG
    settings = {  # эти настройки подключены ниже при инициализации ДАГа
        # базовые аргументы DAG
        # ===============================================================
        # было 'dag_id': 'titanic_pivot',  # Имя DAG
        'dag_id': pathlib.Path(filename).stem,  # Имя DAG достаем из имени файла
        # ===============================================================
        # Периодичность запуска, например: cron format "00 15 * * *" или @daily @hourly @monthly
        # 'schedule_interval': '@hourly',  # None, '00 15 * * *',  '@daily', '@hourly', '@monthly',
        'schedule_interval': '@hourly',
        'catchup': False,  # Выполняем только последний запуск
        # ===============================================================
        'default_args': {  # Базовые аргументы для каждого оператора - которые попадут во ВСЕ ТАСКИ
            'owner': 'geekbrains',  # Информация о владельце DAG
            'start_date': dt.datetime(2020, 12, 23),  # Время начала выполнения пайплайна
            'retries': 1,  # Количество повторений в случае неудач
            'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
            'depends_on_past': False,  # Запуск DAG зависит ли от успешности окончания предыдущего запуска по расписанию
        }
    }

    return settings
