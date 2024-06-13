import polars as pl
from io import StringIO
import requests
import json
import time

# Define the API endpoint URL
lat = -42.2
lon = 172
data_source = 'era5'
# nasapower test
# var_name=','.join([str(elem) for elem in [
#          'temperature_2m', 'relative_humidity_2m', 
#          'precipitation', 'snowfall', 'snow_depth', 'surface_pressure',
#          'cloud_cover', 'wind_speed_10m', 'wind_direction_10m',
#          'wind_speed_50m', 'wind_direction_50m'
#      ]])
#fenz test
# var_name=','.join([str(elem) for elem in [
#          'precipitation', 'relative_humidity_2m','soil_temperature_level_2','temperature_2m','volumetric_soil_water_layer_2','wind_direction_2m','wind_speed_2m'
#      ]])
#era5 test - need fixing '2m_temperature' and need to remove loc_id from file write
#  var_name=','.join([str(elem) for elem in [
#           '10m_u_component_of_wind', '10m_v_component_of_wind','2m_dewpoint_temperature','2m_temperature','soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
#                  'runoff','soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3','sub_surface_runoff', 'surface_runoff','surface_pressure','total_precipitation',
#                   'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2', 'volumetric_soil_water_layer_3', 'cloud_base_height','evaporation','high_cloud_cover','medium_cloud_cover',
#                   'low_cloud_cover','potential_evaporation','snow_depth','snowfall','soil_type','total_cloud_cover'
#       ]])

#era5_land test - need fixing evaporation_from_bare_soil, evaporation_from_open_water_surfaces_excluding_oceans, evaporation_from_the_top_of_canopy, evaporation_from_vegetation_transpiration, total_evaporation, 2m_temperature
#and need to remove loc_id from file write
var_name=','.join([str(elem) for elem in [
            'potential_evaporation', 'runoff',
            'soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
            'sub_surface_runoff', 'surface_runoff', 
            'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2', 'volumetric_soil_water_layer_3', 
            '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_dewpoint_temperature',
             'snow_depth', 'snowfall',
            'surface_pressure', 'total_precipitation'
      ]])


#var_name = 'soil_temperature_level_1'
start_date="2021-06-01"
end_date="2022-05-31"
interp_mode = 'snap'

url = f"http://127.0.0.1:8000/historic/?format=json&data_source={data_source}&lat={lat}&lon={lon}&var_name={var_name}&start_date={start_date}&end_date={end_date}&interp_mode={interp_mode}"

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