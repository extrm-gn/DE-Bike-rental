from city_sampler import select_cities_to_csv
from data_ingestion import generate_weather_data
from generate_sql_inserts import generate_country_sql_statements, generate_city_sql_statements
from generate_sql_inserts import generate_city_country_statements_asset, generate_date_sql_statements
from generate_sql_inserts import generate_bike_sql_statements, ingest_data
from dagster import repository

#my_assets = AssetGroup([select_cities_to_csv, generate_weather_data, generate_country_sql_statements, 
#                        generate_city_sql_statements, generate_city_country_statements_asset, 
#                        generate_date_sql_statements, generate_bike_sql_statements, save_sql_statements_to_file, 
#                        ingest_data])

@repository
def my_repository():
    return [
        select_cities_to_csv,
        generate_weather_data,
        generate_country_sql_statements,
        generate_city_sql_statements,
        generate_city_country_statements_asset,
        generate_date_sql_statements,
        generate_bike_sql_statements,
        ingest_data
    ]
