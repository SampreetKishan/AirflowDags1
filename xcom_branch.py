from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator 

from datetime import datetime
import random

with DAG(
    "xcom_branch",
    description = "random number generator",
    start_date = datetime(2023,1,1),
    schedule_interval = "@daily"
):
    
    def random_generator(ti):
        print("Generated random number and pushed to xcom")
        value = random.randint(0,100)
        print("Number pushing to xcom: ", value)
        ti.xcom_push(key="even_odd", value=value)
    
    generate_number = PythonOperator(
        task_id = "generate_number",
        python_callable = random_generator 
        
    )
    
    
    def even_odd(ti):
        number = int(ti.xcom_pull(key="even_odd", task_ids = "generate_number"))
        if (number%2==0):
            print("Even number")
            return "even_number"
        else:
            print("Odd Number")
            return "odd_number" 
        
        
        
    check_number = BranchPythonOperator(
        task_id = "check_number",
        python_callable = even_odd
        
    )
    
    even_number = BashOperator(
        task_id = "even_number",
        bash_command = "echo even"
    )
    
    odd_number = BashOperator(
        task_id = "odd_number",
        bash_command = "echo odd"
    )
    
    conclusion = BashOperator(
        task_id = "conclusion",
        bash_command = "sleep 5",
        trigger_rule = "one_success"
    )
    generate_number >> check_number >> [even_number, odd_number] >> conclusion