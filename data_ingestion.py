import requests
import pandas as pd

API = "https://archive-api.open-meteo.com/v1/archive"

parameters = {"longitude":"13.41","latitude":"52.52","start_date":"2024-07-10",
              "end_date":"2024-07-12","hourly":"temperature_2m"}
#parameters = {"longitude":"121.0583","latitude":"13.7565","hourly":"temperature_2m","current_weather":True}

response = requests.get(url=API, params=parameters)
data = response.json()

#dict_keys(['latitude', 'longitude', 'generationtime_ms', 
# 'utc_offset_seconds', 'timezone', 'timezone_abbreviation', 'elevation', 'hourly_units', 'hourly']

#print(data['hourly'])
hourly = data['hourly']
temp = hourly['temperature_2m']

#for i in hourly['time']:
    #print(i)

print(pd.DataFrame(hourly))