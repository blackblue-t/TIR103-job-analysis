from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from pathlib import Path
from tasks.merge_all_data.csv_104 import csv_104
from tasks.merge_all_data.csv_1111 import csv_1111
from tasks.merge_all_data.csv_yes123 import csv_123
from tasks.merge_all_data.csv_cake import csv_cake
from tasks.merge_all_data.merge import merge
from airflow.models import Variable

# Define DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define save directory and filename
save_dir_path = Path(__file__).parent.parent / 'tasks' / 'raw_data4merge'
save_dir_path.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists
expected_files = [
    'all_raw_data_123.csv',
    'all_raw_data_1111.csv',
    'all_raw_data_104.csv',
    'all_raw_data_cake.csv',
]

# Define DAG
with DAG(
    dag_id='merge_data_trans',
    default_args=default_args,
    schedule_interval='@weekly',  # Adjust as needed
    catchup=False,
) as dag:

    # Task 1: List all objects in the GCS folder
    list_files = GCSListObjectsOperator(
        task_id='list_files',
        bucket='job_raw_data',
        prefix='raw_data/',
    )

    # Task 2: Check for missing files
    def check_missing_files(**context):
        available_files = context['ti'].xcom_pull(task_ids='list_files')
        missing_files = [f for f in expected_files if f'raw_data/{f}' not in available_files]
        
        if missing_files:
            raise FileNotFoundError(f"Missing files in GCS: {', '.join(missing_files)}")

    check_files = PythonOperator(
        task_id='check_missing_files',
        python_callable=check_missing_files,
        provide_context=True,
    )

    # Task 3: Download each file
    download_tasks = []
    for file_name in expected_files:
        download_task = GCSToLocalFilesystemOperator(
            task_id=f'download_{file_name}',
            bucket='job_raw_data',
            object_name=f'raw_data/{file_name}',
            filename=str(save_dir_path / file_name),
        )
        download_tasks.append(download_task)

    # Transformation tasks for each file
    trans_1111 = PythonOperator(
        task_id='trans_1111',
        python_callable=csv_1111,
    )
    trans_104 = PythonOperator(
        task_id='trans_104',
        python_callable=csv_104,
    )
    trans_cake = PythonOperator(
        task_id='trans_cake',
        python_callable=csv_cake,
    )
    trans_123 = PythonOperator(
        task_id='trans_123',
        python_callable=csv_123,
    )

    merge_all = PythonOperator(
        task_id='merge_all',
        python_callable=merge,
    )
    # 定義 GCS 配置 job_raw_data/ready_for_bq
    GCS_BUCKET_NAME = Variable.get("GCS_BUCKET_NAME")
    DATA_DIR = Path(__file__).parent.parent / 'tasks' / 'trans_data'

    # 上傳至 GCS 任務
    upload_to_gcs_tasks = []
    for i in range(1, 3):
        local_file_path = DATA_DIR / f'combined{i}_file.csv'
        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_to_gcs_{i}',
            src=str(local_file_path),
            dst=f'ready_for_bq/combined{i}_file.csv',
            bucket=GCS_BUCKET_NAME,
        )
        upload_to_gcs_tasks.append(upload_task)

    # 定義基礎依賴
    list_files >> check_files >> download_tasks

    # 當所有下載任務完成後，進行所有轉換任務
    chain(*download_tasks, trans_1111, trans_104, trans_cake, trans_123)

    # 當所有轉換任務完成後，進行所有上傳任務
    [trans_1111, trans_104, trans_cake, trans_123]>> merge_all >> upload_to_gcs_tasks
