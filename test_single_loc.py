import polars as pl
import pandas as pd
from io import StringIO
import requests
import json
import time
from datetime import datetime

ns_file = "/workspaces/weather_data_api/weatherapi/scripts_to_build_api_files/nasapower_raw/4.parquet"

df = pd.read_parquet(ns_file)
print(df)

start_date = '2024-07-11'
end_date = '2024-07-15'

# Filter rows between the 10th and 16th of July 2024
filtered_df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
print(filtered_df)

# print(pd.to_datetime(df['date']).dt.date.unique())

# ns_file = "/workspaces/weather_data_api/weatherapi/historic_weather_api/static/historic/nasapower/temperature_2m_4.parquet"

# df = pd.read_parquet(ns_file)
# print(df)


ns_file = "/workspaces/weather_data_api/weatherapi/historic_weather_api/static/current/era5/temperature_2m_4.parquet"

df = pd.read_parquet(ns_file)
print(df)
filtered_df = df[(df['time'] >= start_date) & (df['time'] <= end_date)]
print(filtered_df)

# print(pd.to_datetime(df['time']).dt.date.unique())
# def get_single_loc_df(data_source, lat, lon, var_name, start_date, end_date, interp_mode):

#     url = f"http://localhost:8000/historic/?format=json&data_source={data_source}&lat={lat}&lon={lon}&var_name={var_name}&start_date={start_date}&end_date={end_date}&interp_mode={interp_mode}"

#     start_time = time.time()
#     # Make a GET request to the API
#     response = requests.get(url)

#     if response.status_code == 200:
#         outer_resp = response.json()
#         df = pl.read_json(StringIO(outer_resp['Data']))
#         # print(outer_resp['message'])
#         df = df.with_columns(
#             pl.col("time").str.to_datetime("%Y-%m-%d %H:%M:%S").alias("time")
#         )
#         # print(df)
#         # print(df.shape)
#         return df
    
#     elif response.status_code == 400:
#         print(response.json()['error'])
        
#     else:
#         print("Error:", response.status_code)
#         print(response.reason)
        
#     reform_time = time.time()

#     # print("Execution Time:", reform_time-start_time, "seconds")




# var_name = 'temperature_2m'
# start_date="2024-01-01"
# end_date="2025-05-31"
# interp_mode = 'snap'
# data_source = 'era5_land'
# lon =172
# lat = -42

# print(get_single_loc_df(data_source, lat, lon, var_name, start_date, end_date, interp_mode))