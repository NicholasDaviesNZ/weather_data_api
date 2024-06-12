import polars as pl
from io import StringIO
import requests
import json
import time

# Define the API endpoint URL
lat = -42.2
lon = 172
data_source = 'nasapower'
# var_name=','.join([str(elem) for elem in [
#          'temperature_2m', 'relative_humidity_2m', 
#          'precipitation', 'snowfall', 'snow_depth', 'surface_pressure',
#          'cloud_cover', 'wind_speed_10m', 'wind_direction_10m',
#          'wind_speed_50m', 'wind_direction_50m'
#      ]])
var_name = 'temperature_2m'
start_date="2001-06-01"
end_date="2002-05-31"
interp_mode = 'idw'

url = f"http://127.0.0.1:8000/historic/{data_source}/?format=json&lat={lat}&lon={lon}&var_name={var_name}&start_date={start_date}&end_date={end_date}&interp_mode={interp_mode}"

start_time = time.time()
# Make a GET request to the API
response = requests.get(url)

if response.status_code == 200:
    outer_resp = response.json()
    df = pl.read_json(StringIO(outer_resp['Data']))
    print(outer_resp['message'])
    print(df)
elif response.status_code == 400:
    print(response.json()['error'])
    
else:
    print("Error:", response.status_code)
    print(response.reason)
    
reform_time = time.time()

print("Execution Time:", reform_time-start_time, "seconds")