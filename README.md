# TIR103-job-analysis

目標:
在GCP啟動虛擬機VM，爬取1111、104、yes123、cake四個網站的求職資料，並進行資料處理與清洗，上傳GCS儲存空間與Big Querry 連動。  
透過dbt 將資料從BQ取出來處理後作為後續tableau的前置資料，最後以tableau 來進行視覺化呈現

使用技術:  
  Python 爬蟲  
  Python pandas  
  Python jieba 分析字詞  
  Airflow  
  dbt  
  Big Querry  
  Tableau  




