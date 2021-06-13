from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime

default_args = {
    'owner' : 'd_okhremenko', # собственник dag
    'depends_on_past': False, # если True, то в случае если файл не запуститься, то в следущий раз сам запускаться он не будет
    'start_date': datetime(2018, 1, 1),  # с какой даты будет запускаться DAG
    'retries': 2 # сколько раз будет пытаться запуститься файл в случае ошибки
}

dag = DAF('calculate_example', # название DAG любое
    owner = default_args,
    catchup = False,
    scheduke_interval = '00 20 * * *')

def hello ():
  return print('Hello, world!')

def sum_int():
  return print(2+2)

t1 = PythonOperatoe(
    task_id = 'calculate_task',
    python_callable = hello,
    dag = dag)

t1 = PythonOperatoe(
    task_id = 'calculate_task',
    python_callable = sum_int,
    dag = dag)

t1 >> t2
