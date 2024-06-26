from decouple import config
from sqlalchemy import create_engine

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
