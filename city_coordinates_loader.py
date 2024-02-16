import requests
import time
from db_utils import create_db_engine
import pandas as pd
from sqlalchemy import text

def create_cities_table(engine):
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS cities (
        id SERIAL PRIMARY KEY,
        country VARCHAR(255),
        state VARCHAR(255),
        city VARCHAR(255),
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION
    );
    '''
    with engine.connect() as connection:
        connection.execute(text(create_table_query))

def get_city_coordinates(city_name, country):
    base_url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": f"{city_name}, {country}",
        "format": "json",
    }

    time.sleep(1)
    
    try:
        response = requests.get(base_url, params=params)
        data = response.json()

        if data and len(data):
            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])
            return lat, lon
        else:
            print(f"Could not find coordinates for {city_name}, {country}")
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None

def load_city_coordinates(sql_query):
    # Create a database engine
    # Replace 'your_database_engine' with the actual database engine information

    engine = create_db_engine()

    # Create the cities table if it doesn't exist
    create_cities_table(engine)

    df = pd.read_sql(sql_query, engine)

    with engine.connect() as connection:
    # Iterate over the cities in the DataFrame
        for index, row in df.iterrows():
            country = row['country']
            state = row['state']
            city_name = row['city']
            if city_name == 'Twin Cities MSA':
                coordinates = get_city_coordinates('Twin Cities', 'United States')
            else:
                coordinates = get_city_coordinates(city_name, country)

            if coordinates:
                lat, lon = coordinates
                # Check if the city already exists in the cities table
                existing_query = f"SELECT id FROM cities WHERE country = '{country}' AND state = '{state}' AND city = '{city_name}' LIMIT 1;"
                existing_id = connection.execute(text(existing_query)).scalar()

                if existing_id is None:
                    # Insert the city coordinates into the "cities" table
                    insert_query = f"INSERT INTO cities (country, state, city, latitude, longitude) VALUES ('{country}', '{state}', '{city_name}', {lat}, {lon}) RETURNING id;"
                    new_id = connection.execute(text(insert_query)).scalar()
                    print(f"Coordinates for {city_name}, {country} successfully inserted with id: {new_id}.")
                    # Commit the transaction
                    connection.execute(text("COMMIT;"))
                else:
                    print(f"Coordinates for {city_name}, {country} already exist in the database. Skipping.")

if __name__ == "__main__" :

    # Example usage
    sql_query = "SELECT DISTINCT city, state, country FROM listings;"

    load_city_coordinates(sql_query)