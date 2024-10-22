import pandas as pd
from dagster import asset
import psycopg2
import logging


def main():

    print("Creating SQL statements.")


def generate_sql_statements(csv_filename, table_name, int_columns,
                             str_table_columns, bool_columns = [], str_columns = [], float_columns = []):
    """
    Generates sql statements for the country, city, bike_rental and date table
    """
    df = pd.read_csv(csv_filename)

    id_counter = 0

    sql_commands = []

    for index, value in df.iterrows():
        values_int = value[int_columns].values
        values_int = [str(int(value[col])) if not pd.isnull(value[col]) else 'NULL' for col in int_columns]
        values_int = ", ".join(values_int)

        if table_name == 'date_table':
            values_bool = [value[col] for col in bool_columns]
            values_bool = ["'Yes'" if val == 1 else "'No'" for val in values_bool]
            values_bool = ", ".join(values_bool)     

            sql_command = f"""INSERT INTO {table_name} ({str_table_columns}) VALUES ( 
                '{value['dteday']}' ,{values_int}, {values_bool});"""
        elif table_name == 'bike_rental_table':
            values_float = [value[col] for col in float_columns]
            values_float = [safe_float_conversion(val) for val in values_float]
            values_float = ", ".join(values_float)

            sql_command = f"""INSERT INTO {table_name} ({str_table_columns}) VALUES ( 
                '{value['dteday']}' ,{values_int}, {values_float});"""

        else:
            values_float = [value[col] for col in float_columns]
            values_float = [safe_float_conversion(val) for val in values_float]
            values_float = ", ".join(values_float)

            values_str = value[str_columns].values
            values_str = [str(val).replace("'", "''").strip() for val in values_str]
            values_str = "', '".join(values_str)

            sql_command = f"""INSERT INTO {table_name} ({str_table_columns}) VALUES ({id_counter}, 
                '{values_str}', {values_int}, {values_float}, '01/01/2017', '01/01/9999', 'ACTIVE');"""
        
        id_counter += 1

        #add the sql_command for that particular row to the total sql_commands
        sql_commands.append(sql_command)

    return sql_commands


def generate_city_country_statements(temp_csv_filename, city_csv_filename, country_csv_filename, table_name):
    """
    Generate the fact table dataframe and insert commands to insert data to city_country_table
    """
    temp_df = pd.read_csv(temp_csv_filename)
    city_df = pd.read_csv(city_csv_filename)
    country_df = pd.read_csv(country_csv_filename)

    #determine the needed columns for city and country df
    city_columns = ['city', 'country', 'lat', 'lng']
    country_columns = ['country']

    #set each dataframes to only show the needed columns 
    city_df = city_df[city_columns]
    country_df = country_df[country_columns]

    #added an index to the dataframes
    city_df = city_df.reset_index().rename(columns = {'index':'city_id'})
    country_df = country_df.reset_index().rename(columns = {'index':'country_id'})

    #merge temp_df and city_df first
    city_temp_merge_df = pd.merge(temp_df,city_df, right_on = ['lat', 'lng','city', 'country'],
                                  left_on = ['latitude', 'longitude', 'city', 'country'])
    
    #merge the merged temp_df and city_df with the country_df
    fact_df = pd.merge(city_temp_merge_df,country_df, right_on = ['country'],
                                  left_on = ['country'])
    fact_df = fact_df[['city_id', 'country_id', 'Day','Month', 'Year', 'AvgTemperature']]

    #add a date column
    fact_df['date_gathered'] = pd.to_datetime(fact_df[['Year', 'Month', 'Day']], errors='coerce').dt.strftime('%Y-%m-%d')

    sql_table_columns_string = '''city_id, country_id, mean_temperature, date_gathered'''

    #place holder for the sql commands
    sql_commands = []

    #this loop would iterate each rows
    for index, value in fact_df.iterrows():

        sql_command = f"""INSERT INTO {table_name} ({sql_table_columns_string}) VALUES ({value['city_id']}, 
        {value['country_id']}, {value['AvgTemperature']}, '{value['date_gathered']}');"""

        #add the sql_command for that particular row to the total sql_commands
        sql_commands.append(sql_command)

    return sql_commands


def save_to_sql_file(insert_statements, file_path):
    """
    Save SQL INSERT statements to a .sql file.
    """
    with open(file_path, 'w') as f:
        for statement in insert_statements:
            f.write(statement + '\n')


def safe_float_conversion(value):
    """
    exception handling for float
    """
    try:
        return f"{float(value):.2f}"
    except ValueError:
        return 'NULL'


def ingest(db):
    """
    Connects to database and execute all sql commands to insert data to all table
    """

    cursor = db.cursor()
    logging.info("Cursor created.")

    with db:
        cursor.execute("SELECT count(*) FROM city_table")
        city_table_count = cursor.fetchone()[0]

        cursor.execute("SELECT count(*) FROM country_table")
        country_table_count = cursor.fetchone()[0]

        cursor.execute("SELECT count(*) FROM date_table")
        date_table_count = cursor.fetchone()[0]

        cursor.execute("SELECT count(*) FROM bike_rental_table")
        bike_rental_table_count = cursor.fetchone()[0]

        #checks if country_table already has data to ensure no repetition in primary key
        if country_table_count == 0:
            with open('01_country_table_inserts.sql', 'r') as f:
                cursor.execute(f.read())
        
        #checks if city_table already has data to ensure no repetition in primary key
        if city_table_count == 0:
            with open('02_city_table_inserts.sql', 'r') as f:
                cursor.execute(f.read())

        with open('03_city_country_table_inserts.sql', 'r') as f:
            cursor.execute(f.read())
        
        if date_table_count == 0:
            with open('04_date_table_inserts.sql', 'r') as f:
                cursor.execute(f.read())
        
        if bike_rental_table_count == 0:
            with open('05_bike_rental_inserts.sql', 'r') as f:
                cursor.execute(f.read())

    db.commit()

@asset
def generate_country_sql_statements(context, generate_weather_data):
    country_params = {
        'csv_filename': 'Datasets/country_profile_variables.csv', 'table_name': 'country_table',
        'int_columns': ['Population in thousands (2017)'], 'str_columns': ['country', 'Region'],
        'float_columns': ['Population density (per km2, 2017)','GDP: Gross domestic product (million current US$)', 
                          'GDP per capita (current US$)', 'Surface area (km2)', 'Sex ratio (m per 100 f, 2017)'],
        'str_table_columns': '''country_id, country_name, country_region, country_population, country_population_density,
        country_GDP, country_GDP_per_capita, country_surface_area, country_sex_ratio, activation_date,
        expiration_date, status'''
    }
    
    if check_if_empty('country_table') == 0:
        save_to_sql_file(generate_sql_statements(**country_params), 
                        '01_country_table_inserts.sql')
        
    return 0

@asset
def generate_city_sql_statements(context, generate_weather_data):
    city_params = {
        'csv_filename': 'Datasets/worldcities.csv', 'table_name': 'city_table',
        'int_columns': ['population'], 'str_columns':['city', 'iso3', 'capital'],
        'float_columns': ['lat', 'lng'], 
        'str_table_columns': '''city_id, city_name, city_ISO3, city_capital, city_population, city_latitude, 
        city_longitude, activation_date, expiration_date, status'''
    }

    if check_if_empty('city_table') == 0:
        save_to_sql_file(generate_sql_statements(**city_params), 
                     '02_city_table_inserts.sql')
        
    return 0

@asset
def generate_city_country_statements_asset(context, generate_country_sql_statements, generate_city_sql_statements):
    city_country_statements = generate_city_country_statements('Datasets/city_weather.csv', 'Datasets/worldcities.csv', 
                                                               'Datasets/country_profile_variables.csv', 'city_country_table')
    save_to_sql_file(city_country_statements, '03_city_country_table_inserts.sql')

@asset
def generate_date_sql_statements(context, generate_city_country_statements_asset):
    date_params = {
        'csv_filename': 'Datasets/day.csv', 'table_name': 'date_table',
        'int_columns': ['season', 'yr', 'mnth', 'weekday'],
        'str_table_columns': 'date_id, season, yr, mnth, weekday_, workingday, holiday',
        'bool_columns': ['workingday', 'holiday']
    }

    if check_if_empty('date_table') == 0:
        save_to_sql_file(generate_sql_statements(**date_params), '04_date_table_inserts.sql')

    return 0

@asset
def generate_bike_sql_statements(context,generate_date_sql_statements):
    bike_params = {
        'csv_filename': 'Datasets/day.csv', 'table_name': 'bike_rental_table',
        'int_columns': ['weathersit', 'casual', 'registered', 'cnt'], 'float_columns' : ['temp', 'atemp', 'hum', 'windspeed'],
        'str_table_columns': "date_id, weathersit, casual, registered, cnt, temp, atemp, hum, windspeed"
    }

    if check_if_empty('bike_rental_table') == 0:
        save_to_sql_file(generate_sql_statements(**bike_params), '05_bike_rental_inserts.sql')

    return 0

@asset
def ingest_data(context, generate_bike_sql_statements):
    
    context.log.info("ingesting data to database")

    CONN = psycopg2.connect(
        host="db",
        user='root',
        password='root',
        database='bike_db'
    )
    ingest(CONN)


def check_if_empty(table_name):
    """
    Checks if there are already data inside of table, returns the amount of rows 
    """
    CONN = psycopg2.connect(
        host="db",
        user='root',
        password='root',
        database='bike_db'
    )

    cursor = CONN.cursor()

    with CONN:
        query = f"SELECT count(*) FROM {table_name}"
        cursor.execute(query)
        table_count = cursor.fetchone()[0]

    cursor.close()
    CONN.close()

    return table_count

if __name__ == '__main__':
    main()