import polars as pl
import pandas as pd
from io import StringIO
import requests
import json
import time
from datetime import datetime


df = pd.read_parquet("/workspaces/weather_data_api/weatherapi/historic_weather_api/static/current/era5_land/10m_u_component_of_wind_0.parquet")
print(df)

def get_single_loc_df(data_source, lat, lon, var_name, start_date, end_date, interp_mode):

    url = f"http://127.0.0.1:8000/historic/?format=json&data_source={data_source}&lat={lat}&lon={lon}&var_name={var_name}&start_date={start_date}&end_date={end_date}&interp_mode={interp_mode}"

    start_time = time.time()
    # Make a GET request to the API
    response = requests.get(url)

    if response.status_code == 200:
        outer_resp = response.json()
        df = pl.read_json(StringIO(outer_resp['Data']))
        # print(outer_resp['message'])
        df = df.with_columns(
            pl.col("time").str.to_datetime("%Y-%m-%d %H:%M:%S").alias("time")
        )
        # print(df)
        # print(df.shape)
        return df
    
    elif response.status_code == 400:
        print(response.json()['error'])
        
    else:
        print("Error:", response.status_code)
        print(response.reason)
        
    reform_time = time.time()

    # print("Execution Time:", reform_time-start_time, "seconds")




var_name = 'temperature_2m'
start_date="2024-06-01"
end_date="2025-05-31"
interp_mode = 'snap'
data_source = 'era5_land'
lon =166.639
lat = -45.740

print(get_single_loc_df(data_source, lat, lon, var_name, start_date, end_date, interp_mode))