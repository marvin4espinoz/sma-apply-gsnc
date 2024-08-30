import requests
import yaml
import json
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os

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
        "last_submitted": format_datetime(app['last_submitted_at'])
    }

def upload_to_bigquery(df, project_id, dataset_id, table_id):
    # Construct a BigQuery client object
    client = bigquery.Client(project=project_id)

	# Define the schema
    schema = [
        bigquery.SchemaField("application_id", "STRING"),
        bigquery.SchemaField("application", "STRING"),
        bigquery.SchemaField("applicant", "STRING"),
        bigquery.SchemaField("applicant_id", "STRING"),
        bigquery.SchemaField("application_email", "STRING"),
        bigquery.SchemaField("program", "STRING"),
        bigquery.SchemaField("current_stage", "STRING"),
        bigquery.SchemaField("current_status", "STRING"),
        bigquery.SchemaField("last_submitted", "DATETIME"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Configure the load job
    #job_config = bigquery.LoadJobConfig(
    #    autodetect=True,
    #    source_format=bigquery.SourceFormat.CSV,
    #    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    #)

    # Get the dataset reference
    dataset_ref = client.dataset(dataset_id)

    # Get the table reference
    table_ref = dataset_ref.table(table_id)

    # Start the load job
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)

    # Wait for the job to complete
    job.result()

    print(f"Loaded {job.output_rows} rows into {project_id}:{dataset_id}.{table_id}")


def main():
    connect = SMApplyConnect('connect.yml')
    params = {'per_page': 100}
    applications = connect.get_applications(params)

    print(f"Retrieved {len(applications)} applications.")

    processed_applications = [process_application(app) for app in applications]

    # Create DataFrame
    df = pd.DataFrame(processed_applications)

    # Display the DataFrame
    print("\nDataFrame of Applications:")
    print(df)

    # Save DataFrame to CSV
    csv_filename = 'applications_data.csv'
    df.to_csv(csv_filename, index=False)
    print(f"\nDataFrame saved to {csv_filename}")

	# Upload to BigQuery
    project_id = 'gsnc-datawarehouse-v1'
    dataset_id = 'nc_school_database'
    table_id = 'gsnc_sma_processed_apps'

    upload_to_bigquery(df, project_id, dataset_id, table_id)

    # Optionally, save processed applications to a JSON file
    with open('processed_applications.json', 'w') as f:
        json.dump(processed_applications, f, indent=2)
    print("Processed applications saved to 'processed_applications.json'")

if __name__ == "__main__":
    main()

