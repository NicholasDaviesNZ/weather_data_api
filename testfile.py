import pandas as pd
import requests
import json

# Define the API endpoint URL
lat = -42.2
lon = 172
var_name=','.join([str(elem) for elem in [
        'temperature_2m', 'relative_humidity_2m', 
        'precipitation', 'snowfall', 'snow_depth', 'surface_pressure',
        'cloud_cover', 'wind_speed_10m', 'wind_direction_10m',
        'wind_speed_50m', 'wind_direction_50m'
    ]])
start_date="2001-01-01"
end_date="2020-01-01"

url = f"http://127.0.0.1:8000/historic/nasapower/?format=json&lat={lat}&lon={lon}&var_name={var_name}&start_date={start_date}&end_date={end_date}"

# Make a GET request to the API
response = requests.get(url)

if response.status_code == 200:
    outer_resp = response.json()
    data = json.loads(outer_resp['Data'])
    df = pd.DataFrame(data)

    print(outer_resp['message'])
    print(df)
elif response.status_code == 400:
    print(response.json()['error'])
    
else:
    print("Error:", response.status_code)
    print(response.reason)