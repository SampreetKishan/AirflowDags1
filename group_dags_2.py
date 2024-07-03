from airflow import DAG 
from airflow.operators.bash import BashOperator
from groups.group_downloads import download_task
from groups.group_transforms import transform_task

from datetime import datetime

#Code to illustrate task groups

with DAG(
    "group_dags_2",
    start_date = datetime(2023,1,1),
    schedule_interval = "@daily",
    catchup = False
    ) as dag:
    
    downloads = download_task()
    
    check_files = BashOperator(
        task_id = "check_files",
        bash_command = "sleep 5 "
    )
    
    transforms = transform_task()
    
    downloads >> check_files >> transforms