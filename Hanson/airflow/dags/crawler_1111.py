from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from pathlib import Path
from airflow.models import Variable
import os
from datetime import date

# 導入自定義模組
from tasks.crawler_1111 import crawler_1111
from tasks.trans_data_1111 import trans_data_1111
from tasks.crawler_1111_flex import crawler_1111_flex

# 定義 GCS 配置
GCS_BUCKET_NAME = Variable.get("GCS_BUCKET_NAME")
GCS_UPLOAD_PATH = f'raw_data/all_data_1111-{date.today()}.csv'


# 定義 DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 1),
    'retries': 1,
}

dag = DAG(
    'crawler_to_gcs',
    default_args=default_args,
    schedule_interval='@weekly',  # 每周執行
    catchup=False
)



# 清理資料夾
def clean_directory():
    clean_dir = Path(__file__).parent.parent / 'tasks' / 'raw_data_1111'
    for file in clean_dir.iterdir():
        if file.is_file():
            file.unlink()  # 刪除檔案
        elif file.is_dir():
            # 若有子資料夾，遞迴刪除其內的檔案
            for sub_file in file.iterdir():
                sub_file.unlink()
            file.rmdir()

# 爬蟲 Task
crawl_website_primary = PythonOperator(
    task_id='crawl_website_primary',
    python_callable=crawler_1111,
    dag=dag,
)

crawl_website_fallback = PythonOperator(
    task_id='crawl_website_fallback',
    python_callable=crawler_1111_flex,
    trigger_rule=TriggerRule.ONE_FAILED,  # 如果 primary 任務失敗才執行此任務
    dag=dag,
)

#資料處理
trans_data_task = PythonOperator(
    task_id='trans_data',
    python_callable=trans_data_1111,
    dag=dag,
)

after_trans = Path(__file__).parent.parent / 'tasks'/'raw_data_1111'/'all_raw_data_1111.csv'
path_str = str(after_trans)

# 上傳 GCS Task
upload_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs',
    src=path_str,
    dst=GCS_UPLOAD_PATH,
    bucket=GCS_BUCKET_NAME,
    dag=dag,
)

# 清理資料夾 Task
clean_task = PythonOperator(
    task_id='clean_raw_data',
    python_callable=clean_directory,
    dag=dag,
)

# 設定任務執行順序
crawl_website_primary >> crawl_website_fallback
[crawl_website_primary, crawl_website_fallback] >> trans_data_task >> upload_to_gcs_task >> clean_task
