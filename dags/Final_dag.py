import datetime
import pandas as pd
import importlib.util
import os
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

Variable.set("PARAM_DATE", '2024-03-01') 

args = {
    'owner': 'Sergeev Vladimir',
    'start_date':datetime.datetime(2025, 1, 6),
    'provide_context':True
}


def extract_data(**kwargs):
    file_path = 'data/profit_table.csv'
    try:
        # Загружаем CSV файл
        data = pd.read_csv(file_path)

        # Сохранение DataFrame в таблицу 'profit_table' базы данных
        database_url = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'
        engine = create_engine(database_url)
        
        # Сохранение DataFrame в таблицу 'profit_table' базы данных
        data.to_sql('profit_table', engine, if_exists='replace', index=False)
        print("Данные успешно загружены.")

    except FileNotFoundError:
        print(f"Ошибка: Файл по пути {file_path} не найден.")
    except pd.errors.EmptyDataError:
        print("Ошибка: Файл пуст.")
    except pd.errors.ParserError:
        print("Ошибка: Ошибка парсинга файла.")
    except Exception as e:
        print(f"Произошла ошибка: {e}")



def transform_data(**kwargs):
    # Подключение к БД
    database_url = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'
    engine = create_engine(database_url)
    query = "SELECT * FROM profit_table"
    profit_data = pd.read_sql(query, engine)

    
    # Загружаем модуль и  указываем путь к скрипту
    script_path = os.path.join('scripts', 'transform_script.py')
    spec = importlib.util.spec_from_file_location("transform_script", script_path)
    transform_script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(transform_script)

    param_date = Variable.get("PARAM_DATE")

    print(f'выполняю преобразование данных от даты расчёта флагов активности {param_date}')

    df_tmp = transform_script.transfrom(profit_data, param_date)

    df_tmp.to_sql('tmp_table', engine, if_exists='replace', index=False)
    print("Данные df_tmp успешно загружены.")


def load_data(**kwargs):
    database_url = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'
    engine = create_engine(database_url)
    query = "SELECT * FROM tmp_table"
    profit_data = pd.read_sql(query, engine)

    file_path = './data/flags_activity.csv'
    
    # Записываем данные в CSV файл, не перезаписывая, а добавляя
    profit_data.to_csv(file_path, mode='a', index=False, header=False)
    print("Данные df_tmp успешно добавлены в flags_activity")
        




with DAG('final_dag', description='dag для итогового проекта', schedule_interval='0 0 5 * *', catchup=False, default_args=args) as dag:
    extract_data    = PythonOperator(task_id='extract_data', python_callable=extract_data)
    transform_data  = PythonOperator(task_id='transform_data', python_callable=transform_data)
    load_data       = PythonOperator(task_id='load_data', python_callable=load_data)





    extract_data >> transform_data >> load_data