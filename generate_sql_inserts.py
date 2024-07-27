import pandas as pd
import numpy as np


def country_data_to_sql(csv_filename, table_name):
    df = pd.read_csv(csv_filename)

    #made a list for the column names in the country_profile_variables divided into each data typ
    country_columns_str = ['country', 'Region']
    country_columns_int = 'Population in thousands (2017)'
    country_columns_float = ['Population density (per km2, 2017)',
                       'GDP: Gross domestic product (million current US$)', 'GDP per capita (current US$)', 
                       'Surface area (km2)', 'Sex ratio (m per 100 f, 2017)']
    
    #list for the column names in the sql table
    sql_table_columns = ['country_id', 'country_name', 'country_region', 'country_population', 'country_population_density',
                         'country_GDP', 'country_GDP_per_capita', 'country_surface_area', 'country_sex_ratio', 
                         'activation_date', 'expiration_date', 'status']
    
    sql_table_columns_string = '''country_id, country_name, country_region, country_population, country_population_density,
                         country_GDP, country_GDP_per_capita, country_surface_area, country_sex_ratio, 
                         activation_date, expiration_date, status'''

    #transformed the sql table columns into a string and removed the square brackets
    sql_table_columns = str(sql_table_columns)
    sql_table_columns = sql_table_columns.replace('[', '')
    sql_table_columns = sql_table_columns.replace(']', '')


    #id counter for the table primary key
    id_counter = 0

    #place holder for the sql commands
    sql_commands = []

    #this loop would iterate each rows
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
    df = pd.read_csv(csv_filename)

    #transform data using the function transform data
    df = transform_data(df)

    #made a list for the column names in the country_profile_variables divided into each data typ
    city_columns_str = ['city', 'iso3', 'capital']
    city_columns_int = 'population'
    city_columns_float = ['lat', 'lng']
    
    #list for the column names in the sql table
    sql_table_columns = ['city_id', 'city_name', 'city_ISO3', 'city_capital', 'city_population', 'city_latitude', 'city_longitude',
                        'activation_date', 'expiration_date', 'status']
    
    sql_table_columns_string = '''city_id, city_name, city_ISO3, city_capital, city_population, city_latitude, 
                                  city_longitude, activation_date, expiration_date, status'''

    #transformed the sql table columns into a string and removed the square brackets
    sql_table_columns = str(sql_table_columns)
    sql_table_columns = sql_table_columns.replace('[', '')
    sql_table_columns = sql_table_columns.replace(']', '')


    #id counter for the table primary key
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
    
    #add a date column
    df['date_gathered'] = pd.to_datetime(df[['Year', 'Month', 'Day']], errors='coerce')

    #list for the column names in the sql table
    sql_table_columns = ['city_id', 'country_id', 'mean_temperature', 'date_gathered']

    
    sql_table_columns_string = '''city_id, country_id, mean_temperature date_gathered'''

    #place holder for the sql commands
    sql_commands = []

    #this loop would iterate each rows
    for index, value in df.iterrows():

        # Handle integer values
        city_values_int = value[['city_id', 'country_id']]
        
        sql_command = f"""INSERT INTO {table_name} ({sql_table_columns_string}) VALUES ({city_values_int}, 
        {value['AvgTemperature']}, {value['date_gathered']});"""

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


#exception handling for float
def safe_float_conversion(value):
    try:
        return f"{float(value):.2f}"
    except ValueError:
        return 'NULL'
    

def transform_data(df):

    #drop null values
    df['population'] = df['population'].replace('nan', 'hehe')

    
    return df

def main():
    country_sql_insert_filename = 'zcountry_table_inserts.sql'
    city_sql_insert_filename = 'zcity_table_inserts.sql'

    country_insert_statements = country_data_to_sql('Datasets/country_profile_variables.csv', 'country_table')
    city_insert_statements = city_data_to_sql('Datasets/worldcities.csv', 'city_table')

    #save_to_sql_file(country_insert_statements, country_sql_insert_filename)
    #save_to_sql_file(city_insert_statements, city_sql_insert_filename)
    temp_fact_df = generate_temp_fact_df('Datasets/city_weather.csv', 'Datasets/worldcities.csv', 'Datasets/country_profile_variables.csv', 
                             'Datasets/city_country_table')
    city_country_inserts = temp_data_to_sql(temp_fact_df, 'city_country_table')
    save_to_sql_file(city_country_inserts, 'zcity_country_table_inserts.sql')

if __name__ == '__main__':
    main()