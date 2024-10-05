import polars as pl
import pandas as pd
from io import StringIO
import requests
import json
import time
from datetime import datetime


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
start_date="2022-06-01"
end_date="2023-05-31"
interp_mode = 'snap'
data_source = 'era5_land'


csv_file = 'nz_coords_era5_land.csv' 
coords = pd.read_csv(csv_file)

coords_dict = coords.set_index('loc_id').T.to_dict()
coords_dict = {str(key): {"lat": round(value['latitude'], 3), "lon": round(value['longitude'], 3)} for key, value in coords_dict.items()}

dfs = []
for loc_id, coords in coords_dict.items():
    lat = coords['lat']
    lon = coords['lon']
    df = get_single_loc_df(data_source, lat, lon, var_name, start_date, end_date, interp_mode)
    df = df.to_pandas()
    df[var_name] = df[var_name].round(4)
    df = df.rename(columns={var_name: loc_id})
    dfs.append(df)
    
dfs = [df.set_index('time') for df in dfs]
df = pd.concat(dfs, axis=1)
df['time'] = df.index.astype(int) // 10**6
df = df.reset_index(drop=True)
columns = ['time'] + [col for col in df.columns if col != 'time']

df = df[columns]
print(df.shape)
df =  df.dropna(axis=1, how='any')
print(df)
print(df.shape)

df = df.set_index('time')
data_dict = df.T.to_dict(orient='index')

transformed_dict = {}
for col, values in data_dict.items():
    transformed_dict[col] = values

# Save to JSON file
with open(f'{data_source}_{var_name}_{interp_mode}_{start_date}_{end_date}.json', 'w') as json_file:
    json.dump(transformed_dict, json_file, indent=4)
    
with open(f'{csv_file.split(".")[0]}_locs.json', 'w') as json_file:
    json.dump(coords_dict, json_file, indent=4)



# Define the API endpoint URL
# lat = -42.2
# lon = 172

# # nasapower test
# data_source = 'nasapower'
# var_name=','.join([str(elem) for elem in [
#           'temperature_2m', 'relative_humidity_2m', 'dewpoint_temperature_2m',
#           'precipitation', 'surface_pressure',
#           'wind_speed_10m', 'wind_direction_10m',
#           'wind_speed_50m', 'wind_direction_50m',
#           'cloud_cover', 'snowfall', 'snow_depth',
#       ]])

#era5 test 
# data_source = 'era5'
# var_name=','.join([str(elem) for elem in [
#           '10m_u_component_of_wind', '10m_v_component_of_wind','dewpoint_temperature_2m',
#            'temperature_2m','soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
#                      'runoff','soil_temperature_level_1', 'soil_temperature_level_2', 
#                      'soil_temperature_level_3','sub_surface_runoff', 'surface_runoff','surface_pressure','total_precipitation',
#                       'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2', 
#                       'volumetric_soil_water_layer_3', 'evaporation','high_cloud_cover','medium_cloud_cover',
#                       'low_cloud_cover','potential_evaporation','snow_depth','snowfall',
#                       'soil_type','total_cloud_cover','cloud_base_height',
#       ]])


#era5_land test 
# data_source = 'era5_land'
# var_name=','.join([str(elem) for elem in [
#             'evaporation_from_bare_soil', 'evaporation_from_open_water_surfaces_excluding_oceans', 'evaporation_from_the_top_of_canopy',
#             'evaporation_from_vegetation_transpiration', 'potential_evaporation', 'runoff',
#             'soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
#             'sub_surface_runoff', 'surface_runoff', 'total_evaporation',
#             'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2', 'volumetric_soil_water_layer_3', 
#             '10m_u_component_of_wind', '10m_v_component_of_wind', 'dewpoint_temperature_2m',
#             'temperature_2m', 'snow_depth', 'snowfall',
#             'surface_pressure', 'precipitation'
#             ]])

# #fenz test
# data_source = 'fenz'
# var_name=','.join([str(elem) for elem in [
#          'precipitation', 'relative_humidity_2m','soil_temperature_level_2','temperature_2m','volumetric_soil_water_layer_2','wind_direction_2m','wind_speed_2m'
#      ]])




