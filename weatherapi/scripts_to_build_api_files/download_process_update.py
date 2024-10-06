import pandas as pd
import os
from datetime import datetime
from download_process_update_funcs import *

def process_weather_data(data_source, num_threads=19):
    base_path = '/workspaces/weather_data_api/weatherapi/historic_weather_api/static/'
    coords = pd.read_csv(f'{base_path}/coords/nz_coords_{data_source}.csv')  # note that this was manually created
    raw_dir = f'/workspaces/weather_data_api/weatherapi/scripts_to_build_api_files/{data_source}_raw/'
    hist_dir = f'{base_path}historic/{data_source}/'
    current_dir = f'{base_path}current/{data_source}/'
    max_dates_path = f'/workspaces/weather_data_api/weatherapi/scripts_to_build_api_files/{data_source}_max_dates.csv'

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
    end_hist_dates_df = get_or_build_max_dates(data_source, max_dates_path, hist_list, hist_dir, coords, write_to_file=True)
    get_and_write_raw(data_source, end_hist_dates_df, end_date, raw_dir, coords) 
    convert_raw_to_parquet_current(data_source, coords, raw_dir, current_dir, max_threads=1) # era5 need remove everything that is not in the cooreds file
    merge_current_to_historic(data_source, current_dir, hist_dir, force_copy_all_current=False, max_threads=1)

# Celery task
#@app.task
def process_weather_data_task(data_source, num_threads=19):
    process_weather_data(data_source)


process_weather_data_task('era5_land')
process_weather_data_task('era5')
process_weather_data_task('nasapower')


