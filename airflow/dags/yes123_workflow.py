from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime, timedelta
from pathlib import Path
import time
from datetime import date
from tasks.scripts_123.script1 import main as script1_main
from tasks.scripts_123.script2 import main as script2_main
from tasks.scripts_123.script3 import main as script3_main
from tasks.scripts_123.script4 import main as script4_main
from airflow.utils.dates import days_ago
from airflow.models import Variable

# 定義 DAG 的默認參數
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 初始化 DAG
dag = DAG(
    'yes123_workflow_dag',
    default_args=default_args,
    schedule_interval='@weekly', 
    catchup=False
)

# 設定 GCS 配置，使用模板變數 {{ ds }} 來表示執行日期
GCS_BUCKET_NAME = Variable.get("GCS_BUCKET_NAME")
GCS_UPLOAD_PATH = f'raw_data/all_data_123-{date.today()}.csv'

# 爬蟲（擷取連結）
def crawl_links():
    script1_main()

# 爬蟲（擷取內容）
def crawl_content():
    script2_main()

# 數據處理（清洗文字）
def clean_text():
    script3_main()

# 數據處理（爆開）
def explode_data():
    script4_main()

# 定義資料夾清理任務
def clean_directory():
    clean_dir = Path(__file__).parent.parent / 'tasks' / 'raw_data_123'  # 資料夾路徑

    # 遞迴刪除文件夾中的所有文件和資料夾
    for file in clean_dir.rglob('*'):
        try:
            if file.is_file():
                file.unlink()  # 刪除文件
            elif file.is_dir():
                file.rmdir()  # 刪除空資料夾
        except Exception as e:
            print(f"刪除 {file} 時發生錯誤：{e}")

# 爬蟲（擷取連結）
crawl_links_task = PythonOperator(
    task_id='crawl_links',
    python_callable=crawl_links,
    dag=dag,
)

# 爬蟲（擷取內容）
crawl_content_task = PythonOperator(
    task_id='crawl_content',
    python_callable=crawl_content,
    dag=dag,
)

# 數據處理（清洗文字）
clean_text_task = PythonOperator(
    task_id='clean_text',
    python_callable=clean_text,
    dag=dag,
)

# 數據處理（爆開）
explode_data_task = PythonOperator(
    task_id='explode_data',
    python_callable=explode_data,
    dag=dag,
)

# 上傳 GCS 
# 找到匹配的 CSV 文件
final_csv_path = Path(__file__).parent.parent / 'tasks'/'raw_data_123' / 'after_3'/'all_raw_data_123.csv'

upload_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs',
    src=str(final_csv_path),  # 將Path轉換為字符串
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

# 設定 Task 執行順序
crawl_links_task >> crawl_content_task >> clean_text_task >> explode_data_task >> upload_to_gcs_task >> clean_task
