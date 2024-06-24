import pandas as pd
import os
from tqdm import tqdm
from datetime import datetime
import concurrent.futures
from .download_process_update_funcs import *

data_source = 'nasapower' # need ot refactor to make this generic through the script
base_path = '/workspaces/weather_data_api/weatherapi/historic_weather_api/static/'

coords = pd.read_csv(f'{base_path}/coords/nz_coords_{data_source}.csv') # note that this was manaully created

raw_dir = f'/workspaces/weather_data_api/scripts_to_build_api_files/{data_source}_raw/'
hist_dir = f'{base_path}historic/{data_source}/'
current_dir = f'{base_path}current/{data_source}/'


if not os.path.exists(raw_dir):
    os.makedirs(raw_dir)
    
if not os.path.exists(hist_dir):
    os.makedirs(hist_dir)
    
if not os.path.exists(current_dir):
    os.makedirs(current_dir)


raw_list = os.listdir(raw_dir)
hist_list = os.listdir(hist_dir)

end_date = datetime.now()


# note this is really slow, so we avoid doing it if we can
# note cpu bound task
if not os.path.exists(f'/workspaces/weather_data_api/scripts_to_build_api_files/{data_source}_max_dates.csv'):
    max_dates_dict = {}
    max_date_tracker_base = datetime.now()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        future_to_file = {executor.submit(get_max_date, int(row['loc_id']), max_date_tracker_base, hist_list, hist_dir): row['loc_id'] for _, row in coords.iterrows()}

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
    


# note io bound task, could increase workers, but unsure what the nasapower api limit on cocurant connections is
with concurrent.futures.ProcessPoolExecutor() as executor:
    futures = {executor.submit(get_and_write_raw, data_source, row, end_hist_dates_df, end_date): row['loc_id'] for _, row in coords.iterrows()}
    for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
        try:
            result = future.result()
        except Exception as exc:
            print(f'Error in processing: {exc}')


futures = []
with concurrent.futures.ProcessPoolExecutor() as executor:
    for _, row in coords.iterrows():
        file_path = f"{raw_dir}{int(row.loc_id)}.parquet"
        if os.path.exists(file_path):
            futures.append(executor.submit(convert_raw_to_parquet_current, row, file_path, current_dir))
    
    for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
        try:
            result = future.result()
        except Exception as exc:
            print(f'Error in processing: {exc}')
            