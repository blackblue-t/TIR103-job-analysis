from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

# 定義 DAG
with DAG(
    'test_dag',
    default_args={'owner': 'airflow'},
    start_date=datetime(2024, 11, 17),
    schedule_interval=None,
    catchup=False
) as dag:

    # 添加測試任務
    start = DummyOperator(task_id='start')

