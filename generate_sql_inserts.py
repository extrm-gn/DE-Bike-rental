import pandas as pd
import datetime

def country_data_to_sql(csv_filename, table_name):
    df = pd.read_csv(csv_filename)
    country_columns = ['country', 'Region','Population in thousands (2017)', 'Population density (per km2, 2017)',
                       'GDP: Gross domestic product (million current US$)', 'GDP per capita (current US$)', 
                       'Surface area (km2)', 'Sex ratio (m per 100 f, 2017)']
    sql_table_columns = ['country_id', 'country_name', 'country_region', 'country_population', 'country_population_density',
                         'country_GDP', 'country_GDP_per_capita', 'country_surface_area', 'country_sex_ratio', 
                         'activation_date', 'expiration_date', 'status']
    sql_table_columns = str(sql_table_columns)

    sql_table_columns = sql_table_columns.replace('[', '')
    sql_table_columns = sql_table_columns.replace(']', '')


    id_counter = 0
    sql_commands = []
    for index, value in df.iterrows():
        #print(value[country_columns])
        print(f"""INSERT INTO {table_name} ({str(sql_table_columns)}) VALUES ({id_counter},{value[country_columns]},
              {datetime.datetime.today},agik, ACTIVE)""")
        id_counter += 1







def main():
    country_data_to_sql('Datasets/country_profile_variables.csv', 'blah')

if __name__ == '__main__':
    main()