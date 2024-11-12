from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import time
import socket

def print_hostname(task_name):
    print(f"Task {task_name} is running on host: {socket.gethostname()}")
    time.sleep(10)  # 模擬較長時間的任務

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(dag_id="test_distributed_execution", default_args=default_args, schedule_interval=None) as dag:
    tasks = []
    for i in range(5):  # 創建多個任務
        task = PythonOperator(
            task_id=f"task_{i}",
            python_callable=print_hostname,
            op_args=[f"task_{i}"],
        )
        tasks.append(task)
