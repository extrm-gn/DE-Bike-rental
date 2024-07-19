import pandas as pd
import random

#Index(['city', 'city_ascii', 'lat', 'lng', 'country', 'iso2', 'iso3',
#       'admin_name', 'capital', 'population', 'id'],
#      dtype='object')


df = pd.read_csv("Datasets/worldcities.csv")

#get the unique list of countries in the dataset
country_unique = list(set(df.country.to_list()))

#group the countries
country_group = df.groupby(by='country')

print(len(country_group.get_group('Malaysia')))

cities_selected = []

country_counter = 1

for i in range(1):
    selected_country = country_unique[country_counter]
    countries =  country_group.get_group(selected_country)
    if len(countries) > 2:
        country_selected = []
        for i in range(2):
            random_idx = random.randint(0,len(countries) - 1)
            #country_selected.append(countries['city'].iloc[random_idx])
            #cities_selected['city'] = countries['city'].iloc[random_idx]
            #cities_selected['country'] = selected_country
            cities_selected.append({'city':countries['city'].iloc[random_idx], 'country':selected_country})
    else:
        print('under')

    country_counter += 1

print(pd.DataFrame(cities_selected))