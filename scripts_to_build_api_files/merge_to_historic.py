# check that what is in static/current is valid, ie all variables have all valid loc_ids and they will be complete - except fenz 

import os
import pandas as pd
import polars as pl
import warnings
from datetime import datetime
from dateutil.relativedelta import relativedelta

base_path = '/workspaces/weather_data_api/weatherapi/historic_weather_api/static/'

era5_locs_df = pd.read_csv(f'{base_path}coords/nz_coords_era5_proper.csv')
era5_land_locs_df = pd.read_csv(f'{base_path}coords/nz_coords_era5_land.csv')
nasapower_locs_df = pd.read_csv(f'{base_path}coords/nz_coords_merra2.csv')
#fenz_locs_df = pd.read_csv('./scripts_to_build_api_files/nz_coords_FENZ.csv')


era5_coords_locs = list(era5_locs_df['loc_id'])
era5_land_coords_locs = list(era5_land_locs_df['loc_id'])
nasapower_coords_locs = list(nasapower_locs_df['loc_id'])
#fenz_coords_locs = list(fenz_locs_df['loc_id'])



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
            'temperature_2m', 'relative_humidity_2m', 'dewpoint_temperature_2m',
            'precipitation', 'snowfall', 'snow_depth', 'surface_pressure',
            'cloud_cover', 'wind_speed_10m', 'wind_direction_10m',
            'wind_speed_50m', 'wind_direction_50m'
            ]

# fenz_valid_var_names = [
#             'precipitation', 'relative_humidity_2m','soil_temperature_level_2','temperature_2m','volumetric_soil_water_layer_2','wind_direction_2m','wind_speed_2m'
#             ]


era5_files = os.listdir(f'{base_path}current/era5/')
era5_land_files = os.listdir(f'{base_path}current/era5_land/')
nasapower_files = os.listdir(f'{base_path}current/nasapower/')
#fenz_files = os.listdir(f'{base_path}current/fenz/')

def test_if_all_locs_exist(data_source, var_names, files_list, coords_list):
    for var_name in var_names:
        vn_files = [file for file in files_list if file.startswith(var_name)]
        loc_ids = [int(file.split('_')[-1].split('.')[0]) for file in vn_files]

        if set(coords_list) != set(loc_ids):
            warnings.warn(f'not all expected locations are in the {data_source} {var_name} currents. Will not proceed with the {data_source} merge')
            return(True)
    return(False)
        
# print(test_if_all_locs_exist('era5_land', era5_land_valid_var_names, era5_land_files, era5_land_coords_locs))
        
# print(test_if_all_locs_exist('era5', era5_valid_var_names, era5_files, era5_coords_locs))

# print(test_if_all_locs_exist('nasapower', nasapower_valid_var_names, nasapower_files, nasapower_coords_locs))

# print(test_if_all_locs_exist('fenz', fenz_valid_var_names, fenz_files, fenz_coords_locs)) # dont bother testing 
        



def update_historic_records(data_source, valid_var_names, files_list, coord_locs):
    if test_if_all_locs_exist(data_source, valid_var_names, files_list, coord_locs):
        return
    
    if data_source == 'era5' or data_source == 'era5_land':
        months_to_remove = 5
    elif data_source == 'nasapower':
        months_to_remove = 1
    else:
        warnings.warn('data_sorce is not picked up in the if block to set the time to hold onto for current data')
        
    hist_files = os.listdir(f'{base_path}historic/{data_source}/')
    for hf in hist_files:
        hdf = pl.read_parquet(f'{base_path}historic/{data_source}/{hf}')
        cdf = pl.read_parquet(f'{base_path}current/{data_source}/{hf}') # need some error catching around this
        latest_historic_date = max(hdf['time'])
        date_to_copy_current_up_to = (datetime.now()- relativedelta(months=months_to_remove)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if latest_historic_date < date_to_copy_current_up_to:
            merged_df = pl.concat([cdf, hdf], how="vertical")
            merged_df = merged_df.unique(subset=["time"], keep="first") # where there are files in both the historic and current folders, keep the newest one
            var_name = [col for col in merged_df.columns if col != 'time']
            if len(var_name) != 1:
                return
            merged_df = merged_df.filter(pl.col(var_name) != -999.0)
            merged_df = merged_df.sort("time")
            merged_df.write_parquet(f'{base_path}historic/{data_source}/{hf}')
    



#update_historic_records('era5_land', era5_land_valid_var_names, era5_land_files, era5_land_coords_locs)
#update_historic_records('era5', era5_valid_var_names, era5_files, era5_coords_locs)
#update_historic_records('nasapower', nasapower_valid_var_names, nasapower_files, nasapower_coords_locs)

# this works well for everythign except fenz, need to think about a good way to do this for the fenz data - some stations ahve been turned off so are in 
# historic but not current, and some a brand new and in currnet but not historic, in these cases, we also need to update the coords files
# maybe we get a list of all cur fenz files and look for the accocated historic files and if they exist concat, if not, 
# create a new loc id, write the current into historic and update the coords file 
#update_historic_records('fenz', fenz_valid_var_names, fenz_files, fenz_coords_locs)