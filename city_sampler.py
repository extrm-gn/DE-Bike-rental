import pandas as pd
import random

#Index(['city', 'city_ascii', 'lat', 'lng', 'country', 'iso2', 'iso3',
#       'admin_name', 'capital', 'population', 'id'],
#      dtype='object')


df = pd.read_csv("Datasets/worldcities.csv")
country_unique = list(set(df.country.to_list()))
country_group = df.groupby(by='country')

random_num = random.randint(0,len(df) - 1)

print(len(country_group.get_group('Malaysia')))

city_num = []

flag = 4
country_counter = 0
for i in range(1230, 1250):   
    if len(country_group.get_group(country_unique[country_counter])) > 2:
        print('over')
    else:
        print('under')
    country_counter += 1
