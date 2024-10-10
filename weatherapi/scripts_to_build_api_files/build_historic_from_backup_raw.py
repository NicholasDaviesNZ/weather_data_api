import pandas as pd
import os
from datetime import datetime
from download_process_update_funcs import *

def process_weather_data(data_source, num_threads=19):
    base_path = '/workspaces/weather_data_api/weatherapi/historic_weather_api/static/'
    coords = pd.read_csv(f'{base_path}/coords/nz_coords_{data_source}.csv')  # note that this was manually created
    raw_dir = f'{base_path}raw_backup/{data_source}/'
    hist_dir = f'{base_path}historic/{data_source}/'
    
    if not os.path.exists(hist_dir):
        os.makedirs(hist_dir)

    convert_raw_to_parquet_current(data_source, coords, raw_dir, hist_dir, max_threads=1) # era5 need remove everything that is not in the cooreds file

def process_weather_data_task(data_source, num_threads=19):
    process_weather_data(data_source)


# process_weather_data_task('era5_land')
# process_weather_data_task('era5')
process_weather_data_task('nasapower') 


