import pandas as pd
import random

df = pd.read_csv("Datasets/worldcities.csv")

#get the unique list of countries in the dataset
country_unique = list(set(df.country.to_list()))

#group the dataframe by country
country_group = df.groupby(by='country')

#initialized the placeholder for the selected city in a country
selected_city_country = []

#counter for the country to keep in track the countries selected
country_counter = 0

for i in range(1):
    #select a country from the unique list of countries
    selected_country = country_unique[country_counter]

    #select a country from the dataframe group
    countries =  country_group.get_group(selected_country)

    if len(countries) > 2:
        for i in range(2):
            #pick a random city index
            random_city = random.randint(0,len(countries) - 1)

            #add the selected country and city to the list
            selected_city_country.append({'city':countries['city'].iloc[random_city], 
                                          'country':selected_country})
    else:
        for i in range(len(countries)):
            selected_city_country.append({'city':countries['city'].iloc[i],
                                          'country':selected_country})

    country_counter += 1

print(pd.DataFrame(selected_city_country))