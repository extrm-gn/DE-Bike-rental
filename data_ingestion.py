import requests
import pandas as pd
import statistics
from datetime import datetime
from city_sampler import generate_city_country_dataframe

#dict_keys(['latitude', 'longitude', 'generationtime_ms', 
# 'utc_offset_seconds', 'timezone', 'timezone_abbreviation', 'elevation', 'hourly_units', 'hourly']

location_df = generate_city_country_dataframe("Datasets/worldcities.csv")

API = "https://archive-api.open-meteo.com/v1/archive"

#placeholder for the temperature that would be appended to the location_df once loop is done
temp_list = []

#main loop to traverse the selected cities
for i in range(2):

    #initialized lat and long of the city
    lat = location_df['latitude'][i]
    long = location_df['longitude'][i]

    #parameters for the API
    parameters = {"longitude":long,"latitude":lat,"start_date":"2024-07-10",
                "end_date":"2024-07-10","hourly":"temperature_2m"}

    #initializing the request module that makes accessing API available
    response = requests.get(url=API, params=parameters)
    data = response.json()   

    #get the mean temp of the city and round to first decimal place
    mean_temp_fahr = round((statistics.mean(data['hourly']['temperature_2m']) * 9/5) + 32, 1)

    print(data['hourly'])
    dt = datetime.strftime(data['hourly'][i], '%Y-%m-%d')
    #append the mean temp to the placeholder temp list
    temp_list.append(mean_temp_fahr)

    print(data['latitude'], lat)


location_df['AvgTemperature'] = pd.Series(temp_list)
print(location_df[0:10])