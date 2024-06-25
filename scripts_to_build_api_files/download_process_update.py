import pandas as pd
import os
from datetime import datetime
from download_process_update_funcs import *
#from .celery_app import app

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
    end_hist_dates_df = get_or_build_max_dates(max_dates_path, hist_list, hist_dir, coords)
    get_and_write_raw(data_source, end_hist_dates_df, end_date, raw_dir, coords) 
    convert_raw_to_parquet_current(data_source, coords, raw_dir, current_dir)
    merge_current_to_historic(data_source, current_dir, hist_dir, force_copy_all_current=False)

# Celery task
#@app.task
def process_weather_data_task(data_source, num_threads=19):
    process_weather_data(data_source, num_threads=19)


process_weather_data_task('era5')