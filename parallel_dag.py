from airflow import DAG
from airflow.operators.bash import BashOperator
 
from datetime import datetime
 
with DAG('parallel_dag', start_date=datetime(2023, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:
 
    extract_a = BashOperator(
        task_id='extract_a',
        #queue = "ML",
        bash_command='sleep 10'
    )
 
    extract_b = BashOperator(
        task_id='extract_b',
        #queue = "ML",
        bash_command='sleep 10'
    )
 
    load_a = BashOperator(
        task_id='load_a',
        #queue = "ML",
        bash_command='sleep 10'
    )
 
    load_b = BashOperator(
        task_id='load_b',
        #queue = "ML",
        bash_command='sleep 10'
    )
 
    transform = BashOperator(
        task_id='transform',
        #queue = "ML",
        bash_command='sleep 10'
    )
 
    extract_a >> load_a
    extract_b >> load_b
    [load_a, load_b] >> transform