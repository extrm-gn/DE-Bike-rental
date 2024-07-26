import pandas as pd
import datetime


def country_data_to_sql(csv_filename, table_name):
    df = pd.read_csv(csv_filename)

    #made a list for the column names in the country_profile_variables
    country_columns = ['country', 'Region','Population in thousands (2017)', 'Population density (per km2, 2017)',
                       'GDP: Gross domestic product (million current US$)', 'GDP per capita (current US$)', 
                       'Surface area (km2)', 'Sex ratio (m per 100 f, 2017)']
    
    #list for the column names in the sql table
    sql_table_columns = ['country_id', 'country_name', 'country_region', 'country_population', 'country_population_density',
                         'country_GDP', 'country_GDP_per_capita', 'country_surface_area', 'country_sex_ratio', 
                         'activation_date', 'expiration_date', 'status']
   

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
        country_values = value[country_columns].values
        country_values = [str(value).replace("'", "''").strip() for value in country_values]
        country_values = "', '".join(country_values)

        sql_command = f"""INSERT INTO {table_name} ({str(sql_table_columns)}) VALUES ({id_counter}, 
              '{country_values}', '01/01/2017', '01/01/9999', 'ACTIVE');"""
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


def main():
    country_sql_insert_filename = 'country_table_inserts.sql'

    country_insert_statements = country_data_to_sql('Datasets/country_profile_variables.csv', 'country_table')

    save_to_sql_file(country_insert_statements, country_sql_insert_filename)
    print('done')

if __name__ == '__main__':
    main()