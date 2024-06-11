import pandas as pd
import requests
import json

# Define the API endpoint URL
url = "http://127.0.0.1:8000/historic/nasapower/?format=json&lat=-42.2&lon=172&var_name=temperature_2m&start_date=2005-01-01&end_date=2005-01-01"

# Make a GET request to the API
response = requests.get(url)

outer_resp = response.json()

data = json.loads(outer_resp['Data'])

print(outer_resp['Data'])

df = pd.DataFrame(data)

print(df)

if response.status_code == 200:
    outer_resp = response.json()
    data = json.loads(outer_resp['Data'])
    df = pd.DataFrame(data)

    print(outer_resp['message'])
    print(df)
else:
    print("Error:", response.status_code)