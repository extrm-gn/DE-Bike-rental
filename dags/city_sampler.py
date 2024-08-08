import pandas as pd
import random
import os
from dotenv import load_dotenv
from dagster import asset


def main():
    #select_cities_to_csv()
    print("INside city_sampler.py")

@asset
def select_cities_to_csv(context):
    """
    Select at least 2 cities per country.
    """

    context.log.info("city_sampler executing.")

    #load environment variables
    load_dotenv()

    filepath = os.getenv('WORLD_CITIES_FILE')

    df = pd.read_csv(filepath)

    # Extract unique coutries and group the dataframe by country for further processing.
    country_unique = df['country'].unique().tolist()
    country_group = df.groupby(by='country')
    selected_city_country = []

    country_counter = 0

    for i in range(len(country_unique)):
        #select a country from the unique list of countries
        selected_country = country_unique[country_counter]

        #select city from the dataframe group
        cities =  country_group.get_group(selected_country)

        if len(cities) > 2:
            for i in range(2):
                #pick a random city index
                random_city = random.randint(0,len(cities) - 1)

                #initialized the city name, latitude, and longitude of selected city
                city_name = cities['city'].iloc[random_city]
                latitude = cities['lat'].iloc[random_city]
                longitude = cities['lng'].iloc[random_city]

                selected_city_country.append({'city':city_name, 'country':selected_country, 
                                            'latitude':latitude, 'longitude':longitude})
        else:
            for i in range(len(cities)):

                #initialized the city name, latitude, and longitude of selected city
                city_name = cities['city'].iloc[i]
                latitude = cities['lat'].iloc[i]
                longitude = cities['lng'].iloc[i]

                selected_city_country.append({'city':city_name, 'country':selected_country, 
                                            'latitude':latitude, 'longitude':longitude})

        country_counter += 1

    #turn the dict to a dataframe to be able to convert it to a csv
    selected_city_df = pd.DataFrame(selected_city_country)
    selected_city_df.to_csv('Datasets/selected_city.csv', mode='w', index=False)

    print("Done with sampling city.")
    context.log.info("city_sampler executed.")


if __name__ == "__main__":
    main()