from city_sampler import select_cities_to_csv
from data_ingestion import generate_weather_data
from generate_sql_inserts import generate_country_sql_statements, generate_city_sql_statements
from generate_sql_inserts import generate_city_country_statements_asset, generate_date_sql_statements
from generate_sql_inserts import generate_bike_sql_statements, ingest_data
from dagster import repository
from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,

)

all_assets = [
    select_cities_to_csv,
    generate_weather_data,
    generate_country_sql_statements,
    generate_city_sql_statements,
    generate_city_country_statements_asset,
    generate_date_sql_statements,
    generate_bike_sql_statements,
    ingest_data
]

#initialize dagster job
data_ingestion_job = define_asset_job("data_ingestion_job", selection=AssetSelection.all())

#initialize dagster schedule that runs the job every minute (for testing purposes)
ingestion_schedule = ScheduleDefinition(
    job=data_ingestion_job,
    cron_schedule="* * * * *",  # every minute
)

@repository
def my_repository():
    return [
        data_ingestion_job,
        ingestion_schedule,
        *all_assets
    ]