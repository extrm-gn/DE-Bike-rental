import pandas as pd
import datetime


def country_data_to_sql(csv_filename, table_name):
    df = pd.read_csv(csv_filename)

    #made a list for the column names in the country_profile_variables
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


def save_to_sql_file(insert_statements, file_path):
    """
    Save SQL INSERT statements to a .sql file.
    """
    with open(file_path, 'w') as f:
        for statement in insert_statements:
            f.write(statement + '\n')


def safe_float_conversion(value):
    try:
        return f"{float(value):.2f}"
    except ValueError:
        return 'NULL'
    

def main():
    country_sql_insert_filename = 'zcountry_table_inserts.sql'

    country_insert_statements = country_data_to_sql('Datasets/country_profile_variables.csv', 'country_table')

    save_to_sql_file(country_insert_statements, country_sql_insert_filename)
    print('done')

if __name__ == '__main__':
    main()