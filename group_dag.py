from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator 
from subdags.subdag_downloads import subdag_downloads
from subdags.subdag_transforms import subdag_transforms

from datetime import datetime

with DAG (
    "group_dag",
    start_date = datetime(2023,1,1),
    catchup=False,
    schedule_interval = '@daily'
 ) as dag:
    
    
    args = {   
            "start_date": dag.start_date,
            "schedule_interval":dag.schedule_interval, 
            "catchup": dag.catchup 
            }
    
    downloads = SubDagOperator(
        task_id ="downloads",
        subdag = subdag_downloads(dag.dag_id, "downloads", args)
    )
    
    check_files = BashOperator(
        task_id = "check_files",
        bash_command = "sleep 10"
    ),
    
    transform = SubDagOperator(
        task_id = "transform",
        subdag = subdag_transforms(dag.dag_id, "transform", args)
    )
    
    downloads >> check_files >> transform
    
    