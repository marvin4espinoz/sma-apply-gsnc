#python file: sma_gsnc_v1.py
#specific path: C:\Users\MarvinEspinoza-Leiva\github-repo-folder\sma-apply-gsnc\sma_gsnc_v1.py
#need to run within python virtual environment: C:\Users\MarvinEspinoza-Leiva\github-repo-folder\sma-apply-gsnc\env_311\Scripts\python.exe

import requests
import yaml
import json
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import csv

class SMApplyConnect:
    def __init__(self, config_file):
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
        self.server = config['server']
        self.access_token = config['accessToken']
        self.headers = {'Authorization': f'Bearer {self.access_token}'}

    def get_applications(self, params=None):
        url = f'{self.server}/api/applications/'
        all_applications = []

        while url:
            response = requests.get(url, params=params, headers=self.headers)

            if response.status_code != 200:
                print(f"Error: {response.status_code}")
                print(response.text)
                break

            data = response.json()
            all_applications.extend(data['results'])

            url = data.get('next')
            params = None

        return all_applications

def format_datetime(date_string):
    if date_string is None:
        return "N/A"
    dt = datetime.fromisoformat(date_string.replace("Z", "+00:00"))
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def process_application(app):
    return {
        "application_id": app['reference_id'],
        "application": app['title'],
        "applicant": f"{app['applicant']['first_name']} {app['applicant']['last_name']}",
        "applicant_id": app['applicant']['id'],
        "application_email": app['applicant']['email'],
        "program": app['program']['name'],
        "current_stage": app['current_stage']['title'],
        "current_status": app['status']['name'] if app['status'] else "N/A",
        "last_submitted": format_datetime(app['last_submitted_at']),
        "average_score": app.get('average_score', 'N/A'),
        "overall_score": app.get('overall_score', 'N/A'),
        "weighted_score": app.get('weighted_score', 'N/A')
    }

def upload_to_bigquery(df, project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)

    # Define the new schema
    schema = [
        bigquery.SchemaField("application_id", "STRING"),
        bigquery.SchemaField("application", "STRING"),
        bigquery.SchemaField("applicant", "STRING"),
        bigquery.SchemaField("applicant_id", "STRING"),
        bigquery.SchemaField("application_email", "STRING"),
        bigquery.SchemaField("program", "STRING"),
        bigquery.SchemaField("current_stage", "STRING"),
        bigquery.SchemaField("current_status", "STRING"),
        bigquery.SchemaField("last_submitted", "STRING"),
        bigquery.SchemaField("average_score", "FLOAT"),
        bigquery.SchemaField("overall_score", "FLOAT"),
        bigquery.SchemaField("weighted_score", "FLOAT")
    ]

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        allow_quoted_newlines=True,
    )

    # Get the table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    # Load the data
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)

    # Wait for the job to complete
    job.result()

    print(f"Loaded {job.output_rows} rows into {project_id}:{dataset_id}.{table_id}")
    print(f"The new table schema is: {[field.name for field in client.get_table(table_ref).schema]}")

def main():
    connect = SMApplyConnect(r'C:\Users\MarvinEspinoza-Leiva\github-repo-folder\sma-apply-gsnc\connect.yml')
    params = {'per_page': 100}
    applications = connect.get_applications(params)

    print(f"Retrieved {len(applications)} applications.")

    processed_applications = []
    for app in applications:
        try:
            processed_app = process_application(app)
            processed_applications.append(processed_app)
        except Exception as e:
            print(f"Error processing application {app.get('id', 'unknown')}: {str(e)}")

    df = pd.DataFrame(processed_applications)

    print("\nDataFrame of Applications:")
    print(df)

    # Data cleaning and validation
    df['last_submitted'] = df['last_submitted'].fillna('N/A')
    df['current_status'] = df['current_status'].fillna('N/A')

    # Convert all columns to string type, except for score columns
    for column in df.columns:
        if column not in ['average_score', 'overall_score', 'weighted_score']:
            df[column] = df[column].astype(str)

    # Remove any newline characters
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].str.replace('\n', ' ').str.replace('\r', '')

    csv_filename = 'applications_data.csv'
    df.to_csv(csv_filename, index=False, quoting=csv.QUOTE_ALL)
    print(f"\nDataFrame saved to {csv_filename}")

    project_id = 'gsnc-datawarehouse-v1'
    dataset_id = 'nc_school_database'
    table_id = 'gsnc_sma_processed_apps'

    upload_to_bigquery(df, project_id, dataset_id, table_id)

    with open('processed_applications.json', 'w') as f:
        json.dump(processed_applications, f, indent=2)
    print("Processed applications saved to 'processed_applications.json'")

if __name__ == "__main__":
    main()