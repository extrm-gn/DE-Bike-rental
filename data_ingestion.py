import requests
import pandas as pd
import statistics
from datetime import datetime
from city_sampler import generate_city_country_dataframe

#dict_keys(['latitude', 'longitude', 'generationtime_ms', 
# 'utc_offset_seconds', 'timezone', 'timezone_abbreviation', 'elevation', 'hourly_units', 'hourly']

def generate_weather_data(year, month, day):

    #check if month is less than 10, if so then add 0 in the beginning
    if month < 10:
        month = '0' + str(month)
    
    #check if day is less than 10, if so then add 0 in the beginning
    if day < 10:
        day = '0' + str(day)
    
    #concatenate the year month and day
    date_weather = f'{year}-{month}-{day}'

    location_df = generate_city_country_dataframe("Datasets/worldcities.csv")

    API = "https://archive-api.open-meteo.com/v1/archive"

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

    return final_weather_df

if __name__ == "__main__":
    final_weather_df = generate_weather_data(2023, 10, 6)
    print(final_weather_df)