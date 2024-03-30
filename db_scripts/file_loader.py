from decouple import config
import pandas as pd
import requests
import io
import gzip
import json
from db_utils import create_db_engine
from scraper import scrape_data

def process_files(file_name=None, engine=None, target_table=None):

    df = scrape_data(config('DATA_URL'))

    for index, row in df.iterrows():
        city = row['city']
        state = row['state']
        country = row['country']
        file_date = row['formatted_date']
        current_file_name = row['file_name']
        file_url = row['file_url']

        # Determine file type
        file_extension = file_url.split('.')[-1]

        # Check if the file name matches the specified name (if provided)
        if file_name and current_file_name != file_name:
            continue

        # Download and read the file based on the type
        if file_extension == 'gz':
            response_file = requests.get(file_url)
            with gzip.open(io.BytesIO(response_file.content), 'rt', encoding='utf-8') as f:
                data = pd.read_csv(f)
        elif file_extension == 'csv':
            response_file = requests.get(file_url)
            data = pd.read_csv(io.StringIO(response_file.text))
        elif file_extension == 'geojson':
            response_file = requests.get(file_url)
            data = pd.json_normalize(json.loads(response_file.text))

        # Check for existing data in the database
        if engine is not None:
            existing_data_query = f"SELECT * FROM {target_table} WHERE file_url = '{file_url}'"
            existing_data = pd.read_sql(existing_data_query, engine)

            if not existing_data.empty:
                print(f"Data for {city}, {current_file_name} - {file_date} already exists in the database. Skipping...")
                continue

        # Append data to the aggregated DataFrame
        # aggregated_data = pd.concat([aggregated_data, data], ignore_index=True)
        print(f'Retrieved data for {city}, {current_file_name} - {file_date}. Loading data...')

        if '<html>' in data.columns:
            # Drop the '<html>' column if it exists
            # data = data.drop('<html>', axis=1)
            print(f'Unable to load data for {city}, {file_name}. Skipping...')
            continue
        else:
            try:

                # Add city information to the data
                data['city'] = city
                data['state'] = state
                data['country'] = country
                data['file_date'] = file_date
                data['file_name'] = current_file_name
                data['file_url'] = file_url

                # handle type issues
                data['license'] = data['license'].astype(str)
                data['neighbourhood_group'] = data['neighbourhood_group'].astype(str)

                with engine.connect() as connection:
                    data.to_sql(target_table, engine, if_exists='append', index=False)
                    print(f'Successfully loaded {file_name} data for {city}, on to the next...')
                
            except Exception as e:
                print(f"Error: {e}")
                print(f'failed to load data for {city}, {file_name} - {file_date}. Continuing...')

    # return aggregated_data

if __name__ == "__main__" :

    file_name = 'listings.csv'
    target_table = 'listings'
    engine = create_db_engine()

    process_files(file_name=file_name, engine=engine, target_table=target_table)