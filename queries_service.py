
import pandas as pd
from db_utils import create_bq_client
from google.cloud import bigquery

def execute_query(sql_query, params):
    client = create_bq_client()
    query_parameters = [bigquery.ScalarQueryParameter(key, "STRING", value) for key, value in params.items()]
    query_job = client.query(sql_query, job_config=bigquery.QueryJobConfig(query_parameters=query_parameters))
    df = query_job.to_dataframe()
    json_data = df.to_json(orient='records')
    return json_data

def get_5_listings():
    # sql_query = "select * from adventureworks-warehousing.listings limit 5"
    sql_query = "SELECT  * FROM adventureworks-warehousing.abnb_raw.listings LIMIT 5"
    return execute_query(sql_query)

def get_cities():
    sql_query = """
    select *
    from adventureworks-warehousing.abnb_raw.cities
    order by city;
    """
    return execute_query(sql_query)

def get_city(city='Paris'):
    # Use COALESCE to handle NULL values and provide a default value ('%') if job_search is not provided
    sql_query = """
    select *
    from adventureworks-warehousing.abnb_raw.cities
    where city = @city
    """
    return execute_query(sql_query, {'city' : city})

def get_markers(city='Paris', neighbourhood=None):
    # Use COALESCE to handle NULL values and provide a default value ('%') if job_search is not provided
    sql_query = """
    select *
    from adventureworks-warehousing.abnb_raw.listings
    where city = @city
    and neighbourhood = coalesce(@neighbourhood, neighbourhood)
    order by number_of_reviews_ltm desc
    limit 3000
    """
    return execute_query(sql_query, {'city' : city, 'neighbourhood' : neighbourhood})

def get_neigbourhoods(city='Paris'):
    # Use COALESCE to handle NULL values and provide a default value ('%') if job_search is not provided
    sql_query = """
    select 
        concat(lower(city), '_', lower(neighbourhood)) as id
        , neighbourhood
    from adventureworks-warehousing.abnb_raw.city_kpis
    where city = @city
    order by city, is_total desc, neighbourhood
    """
    return execute_query(sql_query, { 'city' : city })

def get_city_kpis(city='Paris', neighbourhood='None'):
    # Use COALESCE to handle NULL values and provide a default value ('%') if job_search is not provided
    sql_query = """
    select *
    from adventureworks-warehousing.abnb_raw.city_kpis
    where city = @city
    and neighbourhood = coalesce(@neighbourhood, neighbourhood)
    order by city, is_total desc, neighbourhood
    """
    return execute_query(sql_query, { 'city' : city, 'neighbourhood' : neighbourhood })

def get_top_hosts(city='Paris', neighbourhood='None'):
    # Use COALESCE to handle NULL values and provide a default value ('%') if job_search is not provided
    sql_query = """
    select 
        host_name,
        sum(case when room_type = 'Entire home/apt' then 1 else 0 end) as entire_homes_apts,
        sum(case when room_type = 'Hotel room' then 1 else 0 end) as hotel_rooms,
        sum(case when room_type = 'Private room' then 1 else 0 end) as private_rooms,
        sum(case when room_type = 'Shared room' then 1 else 0 end) as shared_rooms,
        count(distinct id) as nb_listings,
        host_id
    from adventureworks-warehousing.abnb_raw.listings
    where city = @city
    and neighbourhood = coalesce(@neighbourhood, neighbourhood)
    and host_name is not null
    group by host_name, host_id
    order by 6 desc
    limit 100
    """
    return execute_query(sql_query, { 'city' : city, 'neighbourhood' : neighbourhood })
