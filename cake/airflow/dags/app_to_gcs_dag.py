from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from tasks.app import run_all
from tasks.gcs_upload import upload_to_gcs

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['daniel183.shen@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'main_dag',
    default_args=default_args,
    description='running running go go',
    schedule="0 0 * * *",  # 每天凌晨0點運行一次
    start_date=datetime(2024, 11, 10),
    catchup=False
)

# 定義執行爬蟲的任務
run_spider_task = PythonOperator(
    task_id='run_spider',
    python_callable=run_all,  # 呼叫爬蟲函數
    dag=dag,
)

# 定義 GCS 上傳的任務
gcs_upload_task = PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_to_gcs,
    op_args=['cake_bucket_one', '/opt/airflow/dags/data/output.csv', 'output.csv'],
    dag=dag,
)

# 設定任務依賴順序
run_spider_task >> gcs_upload_task  # 爬蟲執行後才會上傳到 GCS
