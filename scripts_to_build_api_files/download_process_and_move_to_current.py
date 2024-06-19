
# currently doing manaully

# Get the requird date range for each data scource to be dowloaded
# download the raw data
# process the raw data
# check that the processed data has loc_id files for every variable which is represented
# move the processed data to the current dir
# move the raw data to raw data backup


import os
import pandas as pd

era5_locs_df = pd.read_csv('./scripts_to_build_api_files/nz_coords_era5_proper.csv')
era5_land_locs_df = pd.read_csv('./scripts_to_build_api_files/nz_coords_era5_land.csv')
nasapower_locs_df = pd.read_csv('./scripts_to_build_api_files/nz_coords_merra2.csv')
fenz_locs_df = pd.read_csv('./scripts_to_build_api_files/nz_coords_FENZ.csv')


era5_coords_locs = list(era5_locs_df['loc_id'])
era5_land_coords_locs = list(era5_land_locs_df['loc_id'])
nasapower_coords_locs = list(nasapower_locs_df['loc_id'])
fenz_coords_locs = list(fenz_locs_df['loc_id'])



era5_valid_var_names = [
            '10m_u_component_of_wind', '10m_v_component_of_wind','dewpoint_temperature_2m','temperature_2m','soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
            'runoff','soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3','sub_surface_runoff', 'surface_runoff','surface_pressure','total_precipitation',
            'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2', 'volumetric_soil_water_layer_3', 'cloud_base_height','evaporation','high_cloud_cover','medium_cloud_cover',
            'low_cloud_cover','potential_evaporation','snow_depth','snowfall','soil_type','total_cloud_cover'
            ]

era5_land_valid_var_names = [
            'evaporation_from_bare_soil', 'evaporation_from_open_water_surfaces_excluding_oceans', 'evaporation_from_the_top_of_canopy',
            'evaporation_from_vegetation_transpiration', 'potential_evaporation', 'runoff',
            'soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
            'sub_surface_runoff', 'surface_runoff', 'total_evaporation',
            'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2', 'volumetric_soil_water_layer_3', 
            '10m_u_component_of_wind', '10m_v_component_of_wind', 'dewpoint_temperature_2m',
            'temperature_2m', 'snow_depth', 'snowfall',
            'surface_pressure', 'precipitation'
            ]


nasapower_valid_var_names = [
            'temperature_2m', 'relative_humidity_2m', 
            'precipitation', 'snowfall', 'snow_depth', 'surface_pressure',
            'cloud_cover', 'wind_speed_10m', 'wind_direction_10m',
            'wind_speed_50m', 'wind_direction_50m'
            ]

fenz_valid_var_names = [
            'precipitation', 'relative_humidity_2m','soil_temperature_level_2','temperature_2m','volumetric_soil_water_layer_2','wind_direction_2m','wind_speed_2m'
            ]

era5_files = os.listdir('/workspaces/weather_data_api/weatherapi/historic_weather_api/static/historic/era5/')
era5_land_files = os.listdir('/workspaces/weather_data_api/weatherapi/historic_weather_api/static/historic/era5_land/')
nasapower_files = os.listdir('/workspaces/weather_data_api/weatherapi/historic_weather_api/static/historic/nasapower/')
fenz_files = os.listdir('/workspaces/weather_data_api/weatherapi/historic_weather_api/static/historic/fenz/')


for var_name in era5_valid_var_names:
    vn_files = [file for file in era5_files if file.startswith(var_name)]
    loc_ids = [int(file.split('_')[-1].split('.')[0]) for file in vn_files]
    if set(era5_coords_locs) != set(loc_ids):
        print('era5')
        print(len(loc_ids))
        
for var_name in era5_land_valid_var_names:
    vn_files = [file for file in era5_land_files if file.startswith(var_name)]
    loc_ids = [int(file.split('_')[-1].split('.')[0]) for file in vn_files]
    if set(era5_land_coords_locs) != set(loc_ids):
        print('era5_land')
        print(len(loc_ids))
        
for var_name in nasapower_valid_var_names:
    vn_files = [file for file in nasapower_files if file.startswith(var_name)]
    loc_ids = [int(file.split('_')[-1].split('.')[0]) for file in vn_files]
    if set(nasapower_coords_locs) != set(loc_ids):
        print('nasapower')
        print(len(loc_ids))
        
for var_name in fenz_valid_var_names:
    vn_files = [file for file in fenz_files if file.startswith(var_name)]
    loc_ids = [int(file.split('_')[-1].split('.')[0]) for file in vn_files]
    if set(fenz_coords_locs) != set(loc_ids):
        print('fenz', var_name)
        diff_in_fenz =  set(fenz_coords_locs) - set(loc_ids)
        diff_in_loc =  set(loc_ids) - set(fenz_coords_locs)
        print(diff_in_fenz)
        
        
df = pd.read_parquet('/workspaces/weather_data_api/weatherapi/historic_weather_api/static/historic/fenz/temperature_2m_1.parquet')
print(df)

df = pd.read_parquet('/workspaces/weather_data_api/./scripts_to_build_api_files/fenz/temperature_2m_1.parquet')
print(df)