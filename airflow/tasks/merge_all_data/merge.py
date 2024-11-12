import pandas as pd
from pathlib import Path

def merge():
    # 讀取四個 CSV 文件
    df1 = pd.read_csv('opt/airflow/tasks/trans_data/data_104.csv')
    df2 = pd.read_csv('opt/airflow/tasks/trans_data/data_1111.csv')
    df3 = pd.read_csv('opt/airflow/tasks/trans_data/data_123.csv')
    df4 = pd.read_csv('opt/airflow/tasks/trans_data/data_cake.csv')

    # 使用 concat 合併，按行（上下）
    df1_combined = pd.concat([df1, df2], ignore_index=True)
    df2_combined = pd.concat([df3,df4], ignore_index=True)


    # 保存合併後的資料
    df1_combined.to_csv('opt/airflow/tasks/trans_data/combined1_file.csv', encoding='utf-8', index=False)
    df2_combined.to_csv('opt/airflow/tasks/trans_data/combined2_file.csv', encoding='utf-8', index=False)
