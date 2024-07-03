import pandas as pd
import os
from datetime import datetime
from download_process_update_funcs import *
#from .celery_app import app
# name_shortcuts = {
#                 'evabs':'evaporation_from_bare_soil',
#                 'evaow':'evaporation_from_open_water_surfaces_excluding_oceans',
#                 'evatc':'evaporation_from_the_top_of_canopy',
#                 'evavt':'evaporation_from_vegetation_transpiration',
#                 'pev':'potential_evaporation',
#                 'ro':'runoff',
#                 'stl1':'soil_temperature_level_1',
#                 'stl2':'soil_temperature_level_2',
#                 'stl3':'soil_temperature_level_3',
#                 'ssro':'sub_surface_runoff',
#                 'sro':'surface_runoff',
#                 'e':'total_evaporation',
#                 'swvl1':'volumetric_soil_water_layer_1',
#                 'swvl2':'volumetric_soil_water_layer_2',
#                 'swvl3':'volumetric_soil_water_layer_3',
#                 'u10':'10m_u_component_of_wind',
#                 'v10':'10m_v_component_of_wind',
#                 'd2m':'dewpoint_temperature_2m',
#                 't2m':'temperature_2m',
#                 'sde':'snow_depth',
#                 'sf':'snowfall',
#                 'sp':'surface_pressure',
#                 'tp':'precipitation'}


# data_source = 'era5_land'
# raw_dir = f'/workspaces/weather_data_api/scripts_to_build_api_files/{data_source}_raw/'
# base_path = '/workspaces/weather_data_api/weatherapi/historic_weather_api/static/'
# coords = pd.read_csv(f'{base_path}/coords/nz_coords_{data_source}.csv')
    
# locs_df_int = coords.copy()
# locs_df_int[['longitude_int', 'latitude_int']] = (coords[['longitude', 'latitude']]*1000).round().astype(int)
# locs_df_int = locs_df_int.drop(columns=['longitude', 'latitude'])
# locs_df_int = pl.from_pandas(locs_df_int)
# locs_df_int = locs_df_int.with_columns(pl.col('loc_id').cast(pl.Int64))

# df = load_nc_file(data_source, '2m_temperature_2024_02.netcdf.zip', raw_dir, name_shortcuts, locs_df_int)
# print(df)
# Refactored function
def process_weather_data(data_source, num_threads=19):
    base_path = '/workspaces/weather_data_api/weatherapi/historic_weather_api/static/'
    coords = pd.read_csv(f'{base_path}/coords/nz_coords_{data_source}.csv')  # note that this was manually created
    raw_dir = f'/workspaces/weather_data_api/scripts_to_build_api_files/{data_source}_raw/'
    hist_dir = f'{base_path}historic/{data_source}/'
    current_dir = f'{base_path}current/{data_source}/'
    max_dates_path = f'/workspaces/weather_data_api/scripts_to_build_api_files/{data_source}_max_dates.csv'

    if not os.path.exists(raw_dir):
        os.makedirs(raw_dir)
    
    if not os.path.exists(hist_dir):
        os.makedirs(hist_dir)
    
    if not os.path.exists(current_dir):
        os.makedirs(current_dir)

    raw_list = os.listdir(raw_dir)
    hist_list = os.listdir(hist_dir)

    end_date = datetime.now()

    # Note: all of these are parallel and are set with max_threads=19 by default
    #end_hist_dates_df = get_or_build_max_dates(data_source, max_dates_path, hist_list, hist_dir, coords)
    #get_and_write_raw(data_source, end_hist_dates_df, end_date, raw_dir, coords) 
    convert_raw_to_parquet_current(data_source, coords, raw_dir, current_dir) # era5 need remove everything that is not in the cooreds file
    merge_current_to_historic(data_source, current_dir, hist_dir, force_copy_all_current=False)

# Celery task
#@app.task
def process_weather_data_task(data_source, num_threads=19):
    process_weather_data(data_source)


process_weather_data_task('era5_land')

