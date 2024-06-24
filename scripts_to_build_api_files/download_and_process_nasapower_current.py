import pandas as pd
from pandas.api.types import CategoricalDtype
import os
import requests
import json
from tqdm import tqdm
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import concurrent.futures
import warnings
from .download_process_update_funcs import *

data_source = 'nasapower' # need ot refactor to make this generic through the script
base_path = '/workspaces/weather_data_api/weatherapi/historic_weather_api/static/'

nasapower_coords = pd.read_csv(f'{base_path}/coords/nz_coords_merra2.csv') # note that this was manaully created

nasapower_raw_dir = '/workspaces/weather_data_api/scripts_to_build_api_files/nasapower_raw/'
nasapower_parquet_dir = '/workspaces/weather_data_api/weatherapi/historic_weather_api/static/historic/nasapower/'
nasapower_parquet_current_dir = '/workspaces/weather_data_api/weatherapi/historic_weather_api/static/current/nasapower/'


if not os.path.exists(nasapower_raw_dir):
    os.makedirs(nasapower_raw_dir)
    
if not os.path.exists(nasapower_parquet_dir):
    os.makedirs(nasapower_parquet_dir)
    
if not os.path.exists(nasapower_parquet_current_dir):
    os.makedirs(nasapower_parquet_current_dir)


nasapower_raw_list = os.listdir(nasapower_raw_dir)
nasapower_parquet_list = os.listdir(nasapower_parquet_dir)



# note this is really slow, so we avoid doing it if we can
# note cpu bound task
if not os.path.exists(f'/workspaces/weather_data_api/scripts_to_build_api_files/{data_source}_max_dates.csv'):
    max_dates_dict = {}
    max_date_tracker_base = datetime.now()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        future_to_file = {executor.submit(get_max_date, int(row['loc_id']), max_date_tracker_base, nasapower_parquet_list, nasapower_parquet_dir): row['loc_id'] for _, row in nasapower_coords.iterrows()}

        for future in tqdm(concurrent.futures.as_completed(future_to_file), total=len(future_to_file)):
            file = future_to_file[future]
            try:
                result = future.result()
                max_dates_dict[result[0]] = result[1]
            except Exception as exc:
                print(f'{file} generated an exception: {exc}')

    dfout = pd.DataFrame(list(max_dates_dict.items()), columns=['loc_id', 'time'])
    dfout.to_csv(f'/workspaces/weather_data_api/scripts_to_build_api_files/{data_source}_max_dates.csv', index=False)
    
end_hist_dates_df = pd.read_csv(f'/workspaces/weather_data_api/scripts_to_build_api_files/{data_source}_max_dates.csv')
    
end_date = datetime.now()

# note io bound task, could increase workers, but unsure what the nasapower api limit on cocurant connections is
with concurrent.futures.ProcessPoolExecutor() as executor:
    futures = {executor.submit(get_and_write_raw, row, end_hist_dates_df, end_date): row['loc_id'] for _, row in nasapower_coords.iterrows()}
    for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
        try:
            result = future.result()
        except Exception as exc:
            print(f'Error in processing: {exc}')

futures = []
with concurrent.futures.ProcessPoolExecutor() as executor:
    for _, row in nasapower_coords.iterrows():
        file_path = f"{nasapower_raw_dir}{int(row.loc_id)}.parquet"
        if os.path.exists(file_path):
            futures.append(executor.submit(convert_raw_to_parquet_current, row, file_path, nasapower_parquet_current_dir))
    
    for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
        try:
            result = future.result()
        except Exception as exc:
            print(f'Error in processing: {exc}')
            