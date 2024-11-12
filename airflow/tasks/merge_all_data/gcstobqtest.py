import os
from pathlib import Path

from google.cloud import bigquery
from google.oauth2.service_account import Credentials

# Using credentials file
BIGQUERY_CREDENTIALS_FILE_PATH = "opt/airflow/tasks/tir103-job-analysis-3712de3c562b.json"
CREDENTIALS = Credentials.from_service_account_file(BIGQUERY_CREDENTIALS_FILE_PATH)

def query_stackoverflow():
    client = bigquery.Client(
        credentials=CREDENTIALS,
    )
    query_job = client.query(
        """
        CREATE OR REPLACE EXTERNAL TABLE test.Products_external_csv (
source String,
report_date Date,
jobtitle String,
company_name String,
education String,
job_category String,
salary  Float64,
location_region String,
experience String,
industry String,
tools String
)
OPTIONS (
format = 'CSV',
uris = ['gs://henry0826/combined1_file.csv',
        'gs://henry0826/combined2_file.csv'],
skip_leading_rows = 1,
max_bad_records = 101
);"""
    )

