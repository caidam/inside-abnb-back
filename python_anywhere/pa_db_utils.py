from google.cloud import bigquery
from google.oauth2 import service_account

# Load credentials from the service account JSON
bigquery_credentials = service_account.Credentials.from_service_account_file(r'path_to_google_service_keyfile.json')

def create_bq_client():

    # Construct a BigQuery client object
    client = bigquery.Client(credentials=bigquery_credentials, project=bigquery_credentials.project_id)
    return client
