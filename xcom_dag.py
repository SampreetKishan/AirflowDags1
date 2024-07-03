from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

#Lets use xcoms to share tasks between tasks

from datetime import datetime

with DAG("xcom_dag",
         start_date =datetime(2023,1,1),
         schedule_interval = "@daily",
         catchup = False):
    
    def _ti(ti):
        #use "ti" short for task instance and use their xcom_push and pull methods to add and retreive values from xcoms
        #note: xcoms are used to share small amounts of data between tasks.
        print("Let's push to Xcom")
        list_a = [1,2,3]
        ti.xcom_push(key="list_a",value=list_a)
        
        
    def _t2(ti):
        print("let's pull the xcom a and add a new element ") 
        list_b = list(ti.xcom_pull(key="list_a", task_ids = "t1"))
        print("Here is the retrieved list: ")
        print(list_b)
        list_b.append(4)
        print("Here is the list after appending a new value: ")
        print(list_b)
        ti.xcom_push(key="list_b", value=list_b)
        
    
    
    t1 = PythonOperator(
        task_id = "t1",
        python_callable = _ti
    )
    
    t2 = PythonOperator(
        task_id = "t2",
        python_callable = _t2
    )
    
    t3 = BashOperator(
        task_id = "t3",
        bash_command = "sleep 10"
    )
    
    t1 >> t2 >> t3
    