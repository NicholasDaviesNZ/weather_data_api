

import os
import pandas as pd
import polars as pl
import warnings
from dateutil.relativedelta import relativedelta
import concurrent.futures
from tqdm import tqdm

data_source = 'nasapower' # need ot refactor to make this generic through the script
base_path = '/workspaces/weather_data_api/weatherapi/historic_weather_api/static/'

nasapower_coords = pd.read_csv(f'{base_path}/coords/nz_coords_merra2.csv') # note that this was manaully created

nasapower_raw_dir = '/workspaces/weather_data_api/scripts_to_build_api_files/nasapower_raw/'
nasapower_parquet_dir = '/workspaces/weather_data_api/weatherapi/historic_weather_api/static/historic/nasapower/'
nasapower_parquet_current_dir = '/workspaces/weather_data_api/weatherapi/historic_weather_api/static/current/nasapower/'

end_hist_dates_df = pd.read_csv(f'/workspaces/weather_data_api/scripts_to_build_api_files/{data_source}_max_dates.csv')

nasapower_cur_files = set(os.listdir(nasapower_parquet_current_dir))
nasapower_hist_files = set(os.listdir(nasapower_parquet_dir))

mismatched_files = list(nasapower_cur_files-nasapower_hist_files)+list(nasapower_hist_files-nasapower_cur_files)

print('files in current but not historic: ',nasapower_cur_files-nasapower_hist_files)
print('files in historic but not current: ',nasapower_hist_files-nasapower_cur_files)


def merge_cur_hist(cur_file, nasapower_parquet_current_dir, nasapower_parquet_dir):
    cur_file_path = f'{nasapower_parquet_current_dir}{cur_file}'
    hist_file_path = f'{nasapower_parquet_dir}{cur_file}'
    
    cur_file = pl.read_parquet(cur_file_path).select(pl.all().exclude("^__index_level_.*$"))
    if cur_file.is_empty():
        return
    hist_file = pl.read_parquet(hist_file_path).select(pl.all().exclude("^__index_level_.*$"))
    if data_source == 'era5' or data_source == 'era5_land':
        months_to_remove = 5
    elif data_source == 'nasapower':
        months_to_remove = 1
    else:
        warnings.warn('data_sorce is not picked up in the if block to set the time to hold onto for current data')
        
    cur_max_date = cur_file.select(pl.col('time')).max()
    date_to_copy_current_up_to = (pd.to_datetime(cur_max_date['time'][0])- relativedelta(months=months_to_remove)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    cur_to_hist = cur_file.filter(pl.col('time') < date_to_copy_current_up_to)
    if cur_to_hist.is_empty():
        return

    hist_file = hist_file.with_columns(pl.col(hist_file.columns[1]).cast(pl.Float64))
    new_hist = pl.concat([cur_to_hist, hist_file], how='vertical')
    new_hist = new_hist.unique(subset=["time"], keep="first")
    new_hist = new_hist.sort('time') 
    new_hist.write_parquet(hist_file_path)
    
    replace_cur = cur_file.filter(pl.col('time') >= date_to_copy_current_up_to)
    replace_cur.write_parquet(cur_file_path)

# if the historic and current files dont match, do not run merge
if not mismatched_files:
    futures = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for cur_file in nasapower_cur_files:
            futures.append(executor.submit(merge_cur_hist, cur_file, nasapower_parquet_current_dir, nasapower_parquet_dir))
    
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            try:
                result = future.result()
            except Exception as exc:
                print(f'Error in processing: {exc}')