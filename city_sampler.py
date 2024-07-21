import pandas as pd
import random
import os
from dotenv import load_dotenv


def generate_city_country_dataframe(filepath):

    df = pd.read_csv(filepath)

    #get the unique list of countries in the dataset
    country_unique = df['country'].unique().tolist()

    #group the dataframe by country
    country_group = df.groupby(by='country')

    #initialized the placeholder for the selected city in a country
    selected_city_country = []

    #counter for the country to keep in track the countries selected
    country_counter = 0

    #main for loop to handle each selected country
    for i in range(0, 10):
        #select a country from the unique list of countries
        selected_country = country_unique[country_counter]

        #select a country from the dataframe group
        countries =  country_group.get_group(selected_country)

        if len(countries) > 2:
            for i in range(2):
                #pick a random city index
                random_city = random.randint(0,len(countries) - 1)

                #initialized the city name, latitude, and longitude of selected city
                city_name = countries['city'].iloc[random_city]
                latitude = countries['lat'].iloc[random_city]
                longitude = countries['lng'].iloc[random_city]

                #add the selected country and city to the list
                selected_city_country.append({'city':city_name, 'country':selected_country, 
                                            'latitude':latitude, 'longitude':longitude})
        else:
            #since there are only <2 cities in a country, just append the available city
            for i in range(len(countries)):

                #initialized the city name, latitude, and longitude of selected city
                city_name = countries['city'].iloc[i]
                latitude = countries['lat'].iloc[i]
                longitude = countries['lng'].iloc[i]

                #add the selected country and city to the list
                selected_city_country.append({'city':city_name, 'country':selected_country, 
                                            'latitude':latitude, 'longitude':longitude})

        country_counter += 1

    #made the city and its location into a dataframe
    selected_city_df = pd.DataFrame(selected_city_country)

    #exported the dataframe into a csv so other file could access it
    selected_city_df.to_csv('Datasets/selected_city.csv', mode='w', index=False)


if __name__ == "__main__":
    #load environment variables
    load_dotenv()

    #get the filepath of the location and city name 
    filepath = os.getenv('WORLD_CITIES_FILE')
    generate_city_country_dataframe(f"{filepath}")