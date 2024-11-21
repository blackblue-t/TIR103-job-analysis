# cake resume Job Market Data Pipeline

An automated ETL pipeline for collecting and analyzing job market data from cake resume Job Bank, powered by Apache Airflow.

<pre>
/airflow
├── dags/                         # Airflow DAG definitions
│   ├── app_to_gcs_dag.py         # Main workflow
│   └── test_dag.py               # Case of testing for airflow
├── tasks/                        # Task modules
│   ├── templates/
│   │   └── index.html            # Website Framework 
│   ├── __init__.py
│   ├── app.py                    # run flask
│   ├── gcs_upload.py             # GCS file upload
│   └── test.py                   # crawl website + data organization
├── .gitignore                    # Git ignore rules
├── crawl1.py                     # first try of crawling  
├── crawl2.py                     # turn crawl1.py into an automated process
└── README.md                     # Project documentation
</pre>

## 🔄 Pipeline Flow

| Step       | Description                 | Script               |
|------------|-----------------------------|----------------------|
| 🌐 Source  | cake Website                 | -                    |
| 🔍 Scraper | Web Scraper                 | `test.py`      |
| 🗂️ Categorize | Job Categorization        | `test.py`    |
| 🧹 Clean   | Data Cleaning               | `test.py`      |
| ☁️ Upload  | GCS Upload                  | `gcs_upload.py`      |
| 🗄️ Load    | BigQuery Load               | -                    |
| 🔄 Transform| DBT Data Transformation     | -                    |
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
- 🔄 DBT: Data transformation

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
git clone https://github.com/daniel561105/TIR103-job-analysis.git
\`\`\`

## 2. Build Environment

For detailed instructions, refer to the [Notion page](https://jewel-beginner-1f2.notion.site/GPT-airflow-123a91e3b7fa806383aaf899459f06c0?pvs=4)
Follow these steps to set up the Airflow environment:

```bash
# Download the official Docker Compose configuration
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.2/docker-compose.yaml'

# Create necessary directories for Airflow
mkdir -p ./dags ./logs ./plugins

# Set up environment variables
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

# Start the Airflow environment
docker-compose up -d
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
- **Automated Web Scraping**: Uses `test.py` to scrape job listings from the cake Job Bank.
  - **Keyword Splitting**: Divides keywords into smaller chunks for efficient processing.
  - **Retry Mechanism**: Implements retries and backoff strategies for robust scraping.
  - **Timeout Handling**: Ensures each keyword is processed within a specified time limit.

### 2️⃣ Data Cleaning
- **Data Consolidation**: Uses `test.py` to merge and clean data from multiple CSV files.
  - **Format Changing**: Find the pattern of data and use regular expression to clean.
  - **Skill Extraction**: Extracts relevant skills and tools from job descriptions and requirements.
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
- **DAG Name**: `main_dag`
  - **Description**: Automates the ETL process for scraping job listings from cake Job Bank.

### Task Groups
- **Scraping Tasks**: 
  - **Function**: Executes web scraping for different keyword chunks.

- **Data Processing Tasks**:
  - **Categorize Jobs**: Classifies job listings into categories.
  - **Clean Data**: Cleans and processes raw data for analysis.
  - **Upload to GCS**: Uploads cleaned data to Google Cloud Storage.
  - **Dependencies**: Tasks are executed sequentially to ensure data integrity.

- **BigQuery Load Task**:
  - **Function**: Loads processed data into BigQuery for analysis.


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
- Respect cake website's robots.txt
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
- dbt-core >= 1.5.0
- dbt-bigquery >= 1.5.0

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

### Version History
- 2024/11: Initial Release
  - Basic scraping functionality
  - Data cleaning pipeline
  - GCP integration
  - Airflow DAG setup
  - BigQuery warehouse integration
- 2024/11: v1.1
  - Added DBT integration
  - Enhanced data transformation pipeline
  - Improved documentation

## 🤝 Contributing
Contributions are welcome!
- Submit issues for bug reports
- Create pull requests for improvements

## 📚 Useful Links
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Google Cloud Documentation](https://cloud.google.com/docs)
- [cake resume Job Bank](https://www.cake.me/jobs)

---
Last Updated: November 2024