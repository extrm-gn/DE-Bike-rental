import requests
import pandas as pd
import statistics
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from dagster import asset


def main():
    #generate_weather_data()
    print("Inside data_ingestion.py")
    
@asset
def generate_weather_data(context, select_cities_to_csv):
    context.log.info("generating weather data.")

    #get the day today and subract 2 days from it since api doesn't update as quickly
    today = datetime.today()
    day_before_yesterday = today - timedelta(days = 2)

    year = day_before_yesterday.year
    month = day_before_yesterday.month
    day = day_before_yesterday.day

    #loads variables in the env
    load_dotenv()

    #check if month and day is less than 10, if so then add 0 in the beginning
    if month < 10:
        month = '0' + str(month)
    if day < 10:
        day = '0' + str(day)
    
    #concatenate the year month and day
    date_weather = f'{year}-{month}-{day}'

    location_df = pd.read_csv('Datasets/selected_city.csv')

    #gets api key in the env file then initialize it in API var
    API = os.getenv('WEATHER_API')

    #placeholder for the temperature that would be appended to the location_df once loop is done
    temp_list = []

    for i in range(len(location_df)):

        #initialized lat and long of the city
        lat = location_df['latitude'][i]
        long = location_df['longitude'][i]

        parameters = {"longitude":long,"latitude":lat,"start_date":date_weather,
                    "end_date":date_weather,"hourly":"temperature_2m"}

        #initializing the request module that makes accessing API available
        response = requests.get(url=API, params=parameters)
        data = response.json()  

        #get the mean_temp of city rounded to first decimal place and get date string, formatted according to datetime module
        mean_temp_fahr = round((statistics.mean(data['hourly']['temperature_2m']) * 9/5) + 32, 1)
        time_str = data['hourly']['time'][0]
        time_dt = datetime.strptime(time_str, '%Y-%m-%dT%H:%M')
        
        temp_list.append({'AvgTemperature':mean_temp_fahr, 'Month':time_dt.month,
                          'Day':time_dt.day, 'Year':time_dt.year, 'latitude':lat,
                          'longitude':long})

    #make the list placeholder as a dataframe and merge it with citylocation_df
    temp_df = pd.DataFrame(temp_list)
    final_weather_df = pd.merge(location_df, temp_df, on=['latitude', 'longitude'])

    #convert the city and temp dataframe into csv
    final_weather_df.to_csv('Datasets/city_weather.csv', sep=',', mode='w',
                            index=False, header=True)

    print(final_weather_df.head())
    print("Data ingestion done")

    return final_weather_df


if __name__ == "__main__":
    main()