from airflow import DAG
from airflow.operators.python import PythonOperator
from hooks.elastic.elastic_hook import ElasticHook

from datetime import datetime

def print_es_info():
    hook = ElasticHook()
    print("Here is the info: ")
    print(hook.info())
    print("End of info")
    
with DAG("elastic_dag",
         start_date = datetime(2023,1,1),
         schedule_interval = "@daily",
         catchup = False):
    
    print_es_info = PythonOperator(
        task_id = "print_es_info",
        python_callable = print_es_info
    )

    print_es_info