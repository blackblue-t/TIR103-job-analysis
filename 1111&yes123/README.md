# 1111,yes123 Job Market Data Pipeline

An automated ETL pipeline for collecting and analyzing job market data from 1111/Yes123 Job Bank, powered by Apache Airflow.


## 📁 Project Structure

<pre>
/airflow
├── dags/               # Airflow DAG definitions
│   ├── crawler_1111.py               # 1111 crawler
│   ├── dag_test_vm_distribution.py   # test dags distribution in different vm
│   ├── merge4data.py                 # merge all source data
│   └── yes123_workflow.py            # Yes123 crawler
├── tasks/              # Task modules
│   ├── __init__.py
│   ├── merge_all_data                # for data cleaning process
|   |   ├──csv_104.py                 # data cleaning for 104
|   |   ├──csv_1111.py                # data cleaning for 1111
|   |   ├──csv_cake.py                # data cleaning for cake
|   |   ├──csv_yes123.py              # data cleaning for yes123
|   |   ├──csv_gcptobqtest.py         # data upload from gcs to bq
|   |   └──merge.py                   # merge 4 sources data
│   └── script_123                    # yes123 crawler code
├── utils/              # Utility functions
│   └── __init__.py
├── resources/          # Resource files
│   └── word_dict                     # word dictionary
|       ├──dict.txt.big               # dictionary from jieba
|       ├──lowercase_detail_word.txt  # dictionary for detail words
|       └── stop_words.txt            # dictionary for stop words
├── data/               # Data storage
│   ├── raw_data_1111                 # storage 1111 raw data 
│   ├── raw_data_123                  # storage yes123 raw data 
│   ├── raw_data4merge                # storage all raw data 
│   └── trans_data                    # storage data after cleaning
├── .gitignore          # Git ignore rules
├── Dockerfile          # build image
├── docker-compose.yml  # build vm enviroment
└── README.md           # Project documentation
</pre>

## 🔄 Pipeline Flow

| Step       | Description                 | Script               |
|------------|-----------------------------|----------------------|
| 🌐 Source  | 1111,yes123 Website         | -                    |
| 🔍 Scraper | Web Scraper                 | `crawler_1111.py`,`yes123_workflow.py` |
| 🗂️ Categorize | Job Categorization        | `crawler_1111.py`    |
| 🧹 Clean   | Data Cleaning               | `csv_104.py`, `csv_cake.py`,`csv_1111.py`,`csv_yes123.py` |
| ☁️ Upload  | GCS Upload                  | `crawler_1111.py upload_to_gcs`       |
| 🗄️ Load    | BigQuery Load               | `csv_gcptobqtest.py`    |
| 📊 Visualize | Data Analysis & Visualization | -               |


## 🌟 Key Features
- 💼 Automated job data collection
- 🧹 Intelligent data cleaning
- 📊 Job classification and analysis
- ☁️ Cloud storage integration
- 📝 Comprehensive error handling
- ⚙️ Flexible configuration options

## 🛠 Tech Stack
- 🐍 Python: Core development
- 🌪 Apache Airflow: Workflow management
- ☁️ Google Cloud Platform:
  - 📦 Cloud Storage: Data storage
  - 📊 BigQuery: Data warehousing
- 🕷 BeautifulSoup4: Web parsing
- 🐼 Pandas: Data processing

## 🚀 Getting Started

### Prerequisites
- Python >= 3.8
- Apache Airflow >= 2.7.1
- GCP Account with enabled services
- Stable internet connection
- Minimum 8GB RAM
- Access to a Google Cloud Platform (GCP) Virtual Machine (VM) with SSH capabilities

### Installation
1. Clone the repository:
\`\`\`bash
git clone https://github.com/blackblue-t/TIR103-job-analysis.git
\`\`\`

2. Build environment
\`\`\`bash
cd directory
docker build 
docker-compose up
\`\`\`

3. Configure GCP credentials:
\`\`\`bash
export GOOGLE_APPLICATION_CREDENTIALS="your-credentials.json"
\`\`\`


### System Startup
1. Initialize Airflow:
\`\`\`bash
airflow webserver -p 8080
airflow scheduler
\`\`\`

2. Access web interface: http://localhost:8080

## 📋 Data Processing

### 1️⃣ Data Collection
- **Automated Web Scraping**: Uses `crawler_1111.py`,`yes123_workflow.py` to scrape job listings from the Yes123/1111 Job Bank.
  - **Keyword Splitting**: Divides keywords into smaller chunks for efficient processing.
  - **Retry Mechanism**: Implements retries and backoff strategies for robust scraping.
  - **Timeout Handling**: Ensures each keyword is processed within a specified time limit.
  - **Random Delays**: Introduces random delays to mimic human behavior and avoid detection.
  - **Switch Scripts**: Switch to another scrapping script when the website change html structure.
  - **Custom Dictionary**: Utilizes Jieba with a custom dictionary for precise text segmentation.
  - **Skill Extraction**: Extracts relevant skills and tools from job descriptions and requirements.

### 2️⃣ Data Cleaning
- **Data Consolidation**: Uses `csv_104.py`, `csv_cake.py`,`csv_1111.py`,`csv_yes123.py` to  clean data from different sources.Use `merge.py` to merge all data
  - **Data Normalization**: Standardizes date formats and handles missing values.
  - **Column Mapping**: Renames and reorders columns for consistency and clarity.

### 3️⃣ Data Analysis
- **Job Classification**: Categorizes jobs based on extracted skills and job titles.
- **Salary Range Analysis**: Analyzes salary data to provide insights into market trends.
- **Skill Requirement Statistics**: Aggregates data on required skills and tools.

### 4️⃣ Data Storage
- **Cloud Backup**: Stores cleaned data in Google Cloud Storage for durability.
- **Data Warehouse Integration**: Loads data into BigQuery for advanced querying and analysis.
- **Query Optimization**: Implements partitioning and indexing strategies for efficient data retrieval.



## 📈 Monitoring

### DAG Overview
- **DAG Name**: `crawler_1111`
  - **Description**: Automates the ETL process for scraping job listings from 1111 Job Bank.
  - **Schedule**: Runs every 7 days at midnight.

### Task Groups
- **Scraping Tasks**: 
  - **Function**: Executes web scraping for different keyword chunks.
  - **Script**: `scipt_123`,`crawler_1111`,`crawler_1111_flex`
  - **Retries**: Each task retries up to 3 times with a 5-minute delay between attempts.

- **Data Processing Tasks**:
  - **merge_all_data**: Cleans and processes raw data for analysis.
  - **trans_data_1111**: Clean 1111 data and add job categories




### Logging and Error Handling
- **Logging**: 
  - Logs are configured to capture detailed information about each task's execution.
  - Includes timestamps, task IDs, and error messages for troubleshooting.

- **Error Handling**:
  - Tasks are wrapped in try-except blocks to catch and log exceptions.
  - Airflow's retry mechanism is used to handle transient failures.

### Environment Information
- **Python Path**: Logs the Python interpreter path for debugging.
- **DAG ID**: Logs the DAG ID to track execution in the Airflow UI.

### Performance Metrics
- **Task Duration**: Monitors the execution time of each task to identify bottlenecks.
- **Success Rate**: Tracks the success rate of tasks to ensure reliability.
- **Resource Usage**: Monitors CPU and memory usage to optimize performance.

## ⚠️ Important Notes
- Respect 1111,yes123 website's robots.txt
- Regular keyword updates
- Monitor cloud usage

## 💡 Additional Information

### Data Schema
- report_date: Date of the report
- job_title: Position name
- company_name: Employer
- main_category: Main job category
- sub_category: Sub job category
- job_category: Job classification
- salary: Salary offered
- location_region: Region of the job location
- experience: Required experience
- industry: Industry type
- job_url: URL of the job listing
- job_skills: Skills required for the job
- tools: Tools required for the job
- insert_timestamp: Timestamp of data insertion (used for partitioning)

### Common Issues
1. Modifying scraping keywords:
   - Edit resources/word_dict/lowercase_detail_word.txt
   - Customize keyword list

2. Adjusting scraping frequency:
   - Modify DAG schedule
   - Default: Daily update

3. Data storage locations:
   - CSV: data/104data.cleaning.csv
   - GCS: {project-id}-104-job-data
   - BigQuery: job_data_dataset.job_table

### Performance Guidelines
- Scraping interval: 5-10 seconds
- Batch size: 500-1000 records
- Update schedule: Off-peak hours
- BigQuery optimization:
  - Date partitioning
  - Proper indexing
  - Regular maintenance

### Development Roadmap
- [ ] Job trend analysis
- [ ] Enhanced data cleaning
- [ ] Salary prediction model
- [ ] Extended job categories

### Dependencies
- Python >= 3.8
- Apache Airflow >= 2.7.1
- pandas >= 1.5.0
- google-cloud-storage >= 2.10.0
- google-cloud-bigquery >= 3.11.0
- beautifulsoup4 >= 4.12.0
- requests >= 2.31.0


### System Requirements
- Linux/macOS/Windows WSL2
- Docker (optional)
- GCP Account
- 2core, 8GB+ RAM
- Stable network connection

### Environment Setup
- The pipeline is deployed on a Google Cloud Platform (GCP) Virtual Machine (VM).
- Secure Shell (SSH) is used for remote access and management of the VM.
- Ensure that the VM has the necessary permissions and access to GCP services like BigQuery and Cloud Storage.

### Performance Metrics
- Average scraping rate: ~0.1 pages/second (approximately 1 page every 10 seconds)
- Data cleaning rate: 60 records/minute
- Typical daily volume: 5000-10000 records
- Storage requirement: ~1.5GB to 3GB/month

### Version History
- 2024/11: Initial Release
  - Basic scraping functionality
  - Data cleaning pipeline
  - GCP integration
  - Airflow DAG setup
  - BigQuery warehouse integration
- 2024/11: v1.1
  - Improved documentation

## 🤝 Contributing
Contributions are welcome!
- Submit issues for bug reports
- Create pull requests for improvements

## 📚 Useful Links
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Google Cloud Documentation](https://cloud.google.com/docs)
- [1111 Job Bank](https://www.1111.com.tw/)
- [yes123 Job Bank](https://www.yes123.com.tw/)
---
Last Updated: November 2024
