import requests
import pandas as pd
import statistics
from city_sampler import generate_city_country_dataframe

#dict_keys(['latitude', 'longitude', 'generationtime_ms', 
# 'utc_offset_seconds', 'timezone', 'timezone_abbreviation', 'elevation', 'hourly_units', 'hourly']

location_df = generate_city_country_dataframe("Datasets/worldcities.csv")

API = "https://archive-api.open-meteo.com/v1/archive"


for i in range(2):
    lat = location_df['latitude'][i]
    long = location_df['longitude'][i]
    parameters = {"longitude":long,"latitude":lat,"start_date":"2024-07-10",
                "end_date":"2024-07-10","hourly":"temperature_2m"}

    response = requests.get(url=API, params=parameters)
    data = response.json()   

    mean_temp_fahr = round((statistics.mean(data['hourly']['temperature_2m']) * 9/5) + 32, 1)
    print(mean_temp_fahr)

    location_df['AvgTemperature'] = pd.Series(mean_temp_fahr)
    print(data['latitude'], lat)
    print(location_df[0:5])



#print(data['hourly'])
hourly = data['hourly']
temp = hourly['temperature_2m']

#for i in hourly['time']:
    #print(i)

#print(pd.DataFrame(hourly))