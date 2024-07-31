import pandas as pd
import os
import psycopg2
import logging


def main():
     #path for local sql inserts
    country_sql_insert_filename = '01_country_table_inserts.sql'
    city_sql_insert_filename = '02_city_table_inserts.sql'

    country_insert_statements = country_data_to_sql('Datasets/country_profile_variables.csv', 'country_table')
    city_insert_statements = city_data_to_sql('Datasets/worldcities.csv', 'city_table')

    save_to_sql_file(country_insert_statements, country_sql_insert_filename)
    save_to_sql_file(city_insert_statements, city_sql_insert_filename)

    temp_fact_df = generate_temp_fact_df('Datasets/city_weather.csv', 'Datasets/worldcities.csv', 'Datasets/country_profile_variables.csv', 
                             'Datasets/city_country_table')
    city_country_inserts = temp_data_to_sql(temp_fact_df, 'city_country_table')
    save_to_sql_file(city_country_inserts, '03_city_country_table_inserts.sql')

    date_inserts = date_data_to_sql('Datasets/day.csv', 'date_table')
    save_to_sql_file(date_inserts, '04_date_table_inserts.sql')

    print("connecting to database now....")
    
    """
    CONN = psycopg2.connect(**{
    "host": "db",        
    "user": 'root',
    "password": 'root',
    "database": 'bike_db'
    })

    ingest(CONN)"""

    print("done inserting data")


def country_data_to_sql(csv_filename, table_name):
    """
    Create the SQL inserts for the country_table
    """
    df = pd.read_csv(csv_filename)

    #made list of column names according to dtypes to be used in transformation
    country_columns_str = ['country', 'Region']
    country_columns_int = 'Population in thousands (2017)'
    country_columns_float = ['Population density (per km2, 2017)',
                       'GDP: Gross domestic product (million current US$)', 'GDP per capita (current US$)', 
                       'Surface area (km2)', 'Sex ratio (m per 100 f, 2017)']
    
    sql_table_columns_string = '''country_id, country_name, country_region, country_population, country_population_density,
                         country_GDP, country_GDP_per_capita, country_surface_area, country_sex_ratio, 
                         activation_date, expiration_date, status'''

    id_counter = 0

    #place holder for the sql commands
    sql_commands = []

    for index, value in df.iterrows():

        #remove the square brackets in the country value list and add seperator
        country_values_str = value[country_columns_str].values
        country_values_str = [str(val).replace("'", "''").strip() for val in country_values_str]
        country_values_str = "', '".join(country_values_str)

        # Handle integer values
        country_values_int = value[country_columns_int]
        country_values_int = int(country_values_int) if not pd.isnull(country_values_int) else 'NULL'
        
        # Handle float values with error handling
        country_values_float = [value[col] for col in country_columns_float]
        country_values_float = [safe_float_conversion(val) for val in country_values_float]
        country_values_float = ", ".join(country_values_float)
        
        sql_command = f"""INSERT INTO {table_name} ({sql_table_columns_string}) VALUES ({id_counter}, 
              '{country_values_str}', {value[country_columns_int]}, {country_values_float}, '01/01/2017', '01/01/9999', 'ACTIVE');"""
        id_counter += 1

        #add the sql_command for that particular row to the total sql_commands
        sql_commands.append(sql_command)

    return sql_commands


def city_data_to_sql(csv_filename, table_name):
    """
    Create SQL Inserts for the city_table
    """
    df = pd.read_csv(csv_filename)

    #made list of column names according to dtypes to be used in transformation
    city_columns_str = ['city', 'iso3', 'capital']
    city_columns_int = 'population'
    city_columns_float = ['lat', 'lng']
    
    sql_table_columns_string = '''city_id, city_name, city_ISO3, city_capital, city_population, city_latitude, 
                                  city_longitude, activation_date, expiration_date, status'''

    id_counter = 0

    #place holder for the sql commands
    sql_commands = []

    #this loop would iterate each rows
    for index, value in df.iterrows():

        #remove the square brackets in the country value list and add seperator
        city_values_str = value[city_columns_str].values
        city_values_str = [str(val).replace("'", "''").strip() for val in city_values_str]
        city_values_str = "', '".join(city_values_str)

        # Handle integer values
        city_values_int = value[city_columns_int]
        city_values_int = int(city_values_int) if not pd.isnull(city_values_int) or city_values_int == 'nan' else 'NULL'
        
        # Handle float values with error handling
        city_values_float = [value[col] for col in city_columns_float ]
        city_values_float = [safe_float_conversion(val) for val in city_values_float]
        city_values_float = ", ".join(city_values_float)
        
        sql_command = f"""INSERT INTO {table_name} ({sql_table_columns_string}) VALUES ({id_counter}, 
              '{city_values_str}', {city_values_int}, {city_values_float}, '01/01/2017', '01/01/9999', 'ACTIVE');"""
        id_counter += 1

        #add the sql_command for that particular row to the total sql_commands
        sql_commands.append(sql_command)

    return sql_commands


def generate_temp_fact_df(temp_csv_filename, city_csv_filename, country_csv_filename, table_name):
    """
    Generate the fact table dataframe which would be used to insert data to city_country_table
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

    return fact_df


def temp_data_to_sql(df, table_name):
    """
    Create the SQL insert command for the city_country_table
    """

    #add a date column
    df['date_gathered'] = pd.to_datetime(df[['Year', 'Month', 'Day']], errors='coerce').dt.strftime('%Y-%m-%d')

    sql_table_columns_string = '''city_id, country_id, mean_temperature, date_gathered'''

    #place holder for the sql commands
    sql_commands = []

    #this loop would iterate each rows
    for index, value in df.iterrows():

        sql_command = f"""INSERT INTO {table_name} ({sql_table_columns_string}) VALUES ({value['city_id']}, 
        {value['country_id']}, {value['AvgTemperature']}, '{value['date_gathered']}');"""

        #add the sql_command for that particular row to the total sql_commands
        sql_commands.append(sql_command)

    return sql_commands


def date_data_to_sql(csv_filename, table_name):
    df = pd.read_csv(csv_filename)

    df = df[['dteday', 'season', 'yr', 'mnth', 'holiday', 'weekday', 'workingday']]

    date_columns_int = ['season', 'yr', 'mnth', 'weekday']
    date_columns_bool = ['workingday', 'holiday']

    sql_table_columns_string = 'dteday, season, yr, mnth, weekday, workingday, holiday'

    #place holder for the sql commands
    sql_commands = []

    #this loop would iterate each rows
    for index, value in df.iterrows():

        # Handle integer values
        date_values_int = value[date_columns_int].values
        #date_values_int = str(int(date_values_int)) if not pd.isnull(date_values_int) else 'NULL'
        date_values_int = [str(int(value[col])) if not pd.isnull(value[col]) else 'NULL' for col in date_columns_int]
        date_values_int = ", ".join(date_values_int)

        date_values_bool = [value[col] for col in date_columns_bool]
        date_values_bool = ["'Yes'" if val == 1 else "'No'" for val in date_values_bool]
        date_values_bool = ", ".join(date_values_bool)     

        sql_command = f"""INSERT INTO {table_name} ({sql_table_columns_string}) VALUES ( 
            {value['dteday']} ,{date_values_int}, {date_values_bool});"""
        

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

    db.commit()


if __name__ == '__main__':
    main()