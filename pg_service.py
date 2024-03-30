
import pandas as pd
from db_utils import create_db_engine, create_bq_client

def execute_query(sql_query, *params):

    engine = create_db_engine()
    
    with engine.connect() as connection:
        df = pd.read_sql_query(sql_query, connection, params=params)
        json_data = df.to_json(orient='records')
    return json_data

def execute_bigquery(sql_query):
    client = create_bq_client()
    df = client.query(sql_query).to_dataframe()
    json_data = df.to_json(orient='records')
    return json_data

def execute_query_3(sql_query, *params):

    engine = create_db_engine()


    with engine.connect() as connection:
        if isinstance(params[0], tuple):
            params = params[0]
        df = pd.read_sql_query(sql_query, connection, params=params)
        json_data = df.to_json(orient='records')
    return json_data

def get_5_listings():
    sql_query = "select * from listings limit 5"
    # sql_query = "SELECT  * FROM adventureworks-warehousing.abnb_raw.listings LIMIT 5"
    return execute_query(sql_query)

def get_cities():
    sql_query = """
    select *
    from cities
    order by city;
    """
    return execute_query(sql_query)

def get_city(city='Paris'):
    # Use COALESCE to handle NULL values and provide a default value ('%') if job_search is not provided
    sql_query = """
    select *
    from cities
    where city = %s
    """
    return execute_query(sql_query, city)

def get_markers(city='Paris', neighbourhood=None):
    # Use COALESCE to handle NULL values and provide a default value ('%') if job_search is not provided
    sql_query = """
    select *
    from listings
    where city = %s
    and neighbourhood = coalesce(%s, neighbourhood)
    order by number_of_reviews_ltm desc
    limit 3000
    """
    return execute_query_3(sql_query, (city, neighbourhood))

def get_neigbourhoods(city='Paris'):
    # Use COALESCE to handle NULL values and provide a default value ('%') if job_search is not provided
    sql_query = """
    select 
        concat(lower(city), '_', lower(neighbourhood)) as id
        , neighbourhood
    from city_kpis
    where city = %s
    order by city, is_total desc, neighbourhood
    """
    return execute_query_3(sql_query, city)

def get_city_kpis(city='Paris', neighbourhood='None'):
    # Use COALESCE to handle NULL values and provide a default value ('%') if job_search is not provided
    sql_query = """
    select *
    from city_kpis
    where city = %s
    and neighbourhood = coalesce(%s, neighbourhood)
    order by city, is_total desc, neighbourhood
    """
    return execute_query_3(sql_query, (city, neighbourhood))

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
    from listings
    where city = %s
    and neighbourhood = coalesce(%s, neighbourhood)
    and host_name is not null
    group by host_name, host_id
    order by 6 desc
    limit 100
    """
    return execute_query_3(sql_query, (city, neighbourhood))

# def get_top_skills_data(job_search=None):
#     # Use COALESCE to handle NULL values and provide a default value ('%') if job_search is not provided
#     sql_query = """
#     select technologie, count(*) as nb_offer
#     from datastats
#     where job_search = coalesce(%s, job_search)
#     group by 1
#     order by 2 desc
#     limit 10;
#     """
#     return execute_query(sql_query, job_search)

# def get_top_5_jobs():
#     sql_query="""
#     select job_search, count(*) as nb_jobs
#     from datastats d 
#     group by 1
#     order by 2 desc
#     limit 5;
#     """
#     return execute_query(sql_query)