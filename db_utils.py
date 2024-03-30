from decouple import config
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# Load credentials from the service account JSON content
# service_account_info = json.loads('your_service_account_json_string')
# credentials = service_account.Credentials.from_service_account_info(service_account_info)
bigquery_credentials = service_account.Credentials.from_service_account_file(config('GOOGLE_APPLICATION_CREDENTIALS'))




def create_bq_client():

    # Construct a BigQuery client object
    client = bigquery.Client(credentials=bigquery_credentials, project=bigquery_credentials.project_id)
    return client

# postgres engine function
def create_db_engine():
    db_user = config('POSTGRES_USER')
    db_password = config('POSTGRES_PASSWORD')
    db_name = config('POSTGRES_DB')
    db_host = config('POSTGRES_SERVICE_IP') # "51.44.20.166" #
    db_port = config('POSTGRES_PORT')

    db_uri = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(db_uri)

    return engine

# def create_duckdb_engine():

#     token = config('MOTHERDUCK_TOKEN')
#     database = config('MOTHERDUCK_DB')

#     db_uri = f"duckdb:///md:{database}?motherduck_token={token}"

#     engine = create_engine(db_uri)

#     return engine

# if __name__ == "__main__" :

#     engine = create_db_engine()

#     df = pd.read_csv(config('FILE_URL'))

#     try:

#         with engine.connect() as connection:
#             df.to_sql('datastats', engine, if_exists='replace', index=False)
        
#     except Exception as e:
#         print(f"Error: {e}")
