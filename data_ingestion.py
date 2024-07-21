import requests
import pandas as pd
import statistics
from datetime import datetime
import os
from dotenv import load_dotenv

def generate_weather_data(year, month, day):
    #loads variables in the env
    load_dotenv()

    #check if month is less than 10, if so then add 0 in the beginning
    if month < 10:
        month = '0' + str(month)
    
    #check if day is less than 10, if so then add 0 in the beginning
    if day < 10:
        day = '0' + str(day)
    
    #concatenate the year month and day
    date_weather = f'{year}-{month}-{day}'

    #read the selected_city.csv from the city_sampler as a dataframe
    location_df = pd.read_csv('/app/Datasets/selected_city.csv')

    #gets api key in the env file then initialize it in API var
    API = os.getenv('WEATHER_API')

    #placeholder for the temperature that would be appended to the location_df once loop is done
    temp_list = []

    #main loop to traverse the selected cities
    for i in range(10):

        #initialized lat and long of the city
        lat = location_df['latitude'][i]
        long = location_df['longitude'][i]

        #parameters for the API
        parameters = {"longitude":long,"latitude":lat,"start_date":date_weather,
                    "end_date":date_weather,"hourly":"temperature_2m"}

        #initializing the request module that makes accessing API available
        response = requests.get(url=API, params=parameters)
        data = response.json()  

        #get the mean temp of the city and round to first decimal place
        mean_temp_fahr = round((statistics.mean(data['hourly']['temperature_2m']) * 9/5) + 32, 1)

        #get the first date string that comes up
        time_str = data['hourly']['time'][0]

        #format the date according to datetime module
        time_dt = datetime.strptime(time_str, '%Y-%m-%dT%H:%M')
        
        #append the mean temp to the placeholder temp list
        temp_list.append({'AvgTemperature':mean_temp_fahr, 'Month':time_dt.month,
                          'Day':time_dt.day, 'Year':time_dt.year, 'latitude':lat,
                          'longitude':long})

    #make the list placeholder as a dataframe
    temp_df = pd.DataFrame(temp_list)

    #merge the city location df and the temp df
    final_weather_df = pd.merge(location_df, temp_df, on=['latitude', 'longitude'])

    #check if city_weather.csv is already available
    if os.path.exists('Datasets/city_weather.csv'):
        #put df to csv format in append mode
        final_weather_df.to_csv('Datasets/city_weather.csv', sep=',', mode='a',
                            index=False, header=False)
    else:
        #put df to csv format if city_weather is not present
        final_weather_df.to_csv('Datasets/city_weather.csv',
                                index=False, header=True)

    print("Data ingestion done")

    return final_weather_df

if __name__ == "__main__":
    final_weather_df = generate_weather_data(2023, 10, 6)
