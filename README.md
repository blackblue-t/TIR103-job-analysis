# TIR103-job-analysis
An automated ETL pipeline for collecting and analyzing job market data from 1111/Yes123/104/cake Job Bank, powered by Apache Airflow.

## 📁 Project Structure
<pre>
/airflow
├── 1111&yes123/        # ETL for 1111,yes123 / merge all sources data and cleaning
├── 104/                # ETL for 104 / dbt processing
├── cake/               # ETL for cake
└── README.md           # Project documentation
</pre>
please read the detail README in the directory

## 🛠 Tech Stack
- 🐍 Python: Core development
- 🌪 Apache Airflow: Workflow management
- ☁️ Google Cloud Platform:
  - 📦 Cloud Storage: Data storage
  - 📊 BigQuery: Data warehousing
- 🕷 BeautifulSoup4: Web parsing
- 🐼 Pandas: Data processing
- 🔄 DBT: Data transformation





