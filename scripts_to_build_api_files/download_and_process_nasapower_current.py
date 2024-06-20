import pandas as pd
from pandas.api.types import CategoricalDtype
import os
import requests
import json
from tqdm import tqdm
from datetime import datetime, timedelta
import concurrent.futures
import warnings
from dateutil.relativedelta import relativedelta

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

nasapower_vars = [
         'temperature_2m', 'relative_humidity_2m', 
         'precipitation', 'snowfall', 'snow_depth', 'surface_pressure',
         'cloud_cover', 'wind_speed_10m', 'wind_direction_10m',
         'wind_speed_50m', 'wind_direction_50m'
     ]


nasapower_raw_list = os.listdir(nasapower_raw_dir)
nasapower_parquet_list = os.listdir(nasapower_parquet_dir)


def get_max_date(loc_id, max_date_tracker, parquet_list, parquet_dir):
    """ for a given loc_id, goes through the files and finds the lowest date stored in the historic parquet files """
    subset_files_list = [f for f in parquet_list if f.endswith(f'{loc_id}.parquet')]
    max_date = max_date_tracker
    for f in subset_files_list:
        df = pd.read_parquet(f'{parquet_dir}{f}', columns=['time'])
        if not df.empty:
            max_date = max(df['time'])    
            if max_date < max_date_tracker:
                max_date_tracker = max_date
    return loc_id, max_date_tracker
        
def get_nasapower_year(latitude,longitude,start_date,end_date):
    start_date = int(start_date.replace("-", ""))
    end_date = int(end_date.replace("-", "")) 
    base_url = r"https://power.larc.nasa.gov//api/temporal/hourly/point?parameters=T2M,RH2M,T2MDEW,PRECTOTCORR,PRECSNOLAND,SNODP,PS,CLOUD_AMT,WS10M,WD10M,WS50M,WD50M&time-standard=UTC&community=SB&longitude={longitude}&latitude={latitude}&start={start}&end={end}&format=JSON"
    api_request_url = base_url.format(longitude=longitude, latitude=latitude, start=str(start_date), end=str(end_date))
    response = requests.get(url=api_request_url, verify=True, timeout=30.00)
    content = json.loads(response.content.decode('utf-8'))
    df = pd.DataFrame(content['properties']['parameter'])
    df['date'] = pd.to_datetime(df.index, format='%Y%m%d%H')
    df = df.reset_index(drop=True)
    df = df.rename(columns={'T2M':'temperature_2m', 'RH2M':'relative_humidity_2m', 'T2MDEW':'dewpoint_temperature_2m', 'PRECTOTCORR':'precipitation', 'PRECSNOLAND':'snowfall', 'SNODP':'snow_depth', 'PS':'surface_pressure', 'CLOUD_AMT':'cloud_cover','WS10M':'wind_speed_10m', 'WD10M':'wind_direction_10m','WS50M':'wind_speed_50m', 'WD50M':'wind_direction_50m'})
    return(df)
    
    
def create_full_nasapower_df(latitude,longitude,start_date,end_date):
    # Convert start and end dates to Timestamp objects
    start_date_ts = pd.to_datetime(start_date)
    end_date_ts = pd.to_datetime(end_date)

    # Initialize list to hold DataFrames for each year
    dfs = []

    # Loop through each year
    for year in range(start_date_ts.year, end_date_ts.year + 1):
        # Define start and end dates for the current year
        year_start_date = pd.Timestamp(year, 1, 1)
        year_end_date = pd.Timestamp(year, 12, 31)
    
        # Ensure start and end dates are within the specified range
        year_start_date = max(start_date_ts, year_start_date)
        year_end_date = min(end_date_ts, year_end_date)
    
        # Get data for the current year
        df = get_nasapower_year(latitude, longitude, year_start_date.strftime('%Y-%m-%d'), year_end_date.strftime('%Y-%m-%d'))
    
        # Append the DataFrame to the list
        dfs.append(df)

    # Concatenate all DataFrames in the list into one DataFrame
    final_df = pd.concat(dfs, ignore_index=True)
    return(final_df)

# note this is really slow, so we avoid doing it if we can
# note cpu bound task
if not os.path.exists(f'/workspaces/weather_data_api/scripts_to_build_api_files/nasapower_max_dates.csv'):
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


def get_and_write_raw(row, end_hist_dates_df, end_date):
    try:
        start_date = pd.to_datetime(end_hist_dates_df[end_hist_dates_df.loc_id == int(row.loc_id)].time.values[0])
        start_date = (start_date - relativedelta(months=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        df = create_full_nasapower_df(row.latitude, row.longitude, start_date, end_date)
        df.to_parquet(f'{nasapower_raw_dir}{int(row.loc_id)}.parquet', compression='brotli')
    except Exception as e:
        print(f'Error processing loc_id {row.loc_id}: {e}')

# note io bound task, could increase workers, but unsure what the nasapower api limit on cocurant connections is
with concurrent.futures.ProcessPoolExecutor() as executor:
    futures = {executor.submit(get_and_write_raw, row, end_hist_dates_df, end_date): row['loc_id'] for _, row in nasapower_coords.iterrows()}
    for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
        try:
            result = future.result()
        except Exception as exc:
            print(f'Error in processing: {exc}')



def convert_raw_nasapower_to_parquet_current(row, file_path, nasapower_parquet_current_dir):
    try:
        df = pd.read_parquet(file_path)
        df = df.rename(columns={'date':'time'})
        var_list = df.columns.to_list()
        var_list.remove('time')
    except Exception as exc:
        print(f'Error loading raw file: {exc}')
    for var_name in var_list:
        try:
            df_out = df[['time',var_name]]
            df_out.to_parquet(f"{nasapower_parquet_current_dir}{var_name}_{int(row.loc_id)}.parquet")
        except Exception as exc:
            print(f'Error saving file: {exc}')
        

futures = []
with concurrent.futures.ProcessPoolExecutor() as executor:
    for _, row in nasapower_coords.iterrows():
        file_path = f"{nasapower_raw_dir}{int(row.loc_id)}.parquet"
        if os.path.exists(file_path):
            futures.append(executor.submit(convert_raw_nasapower_to_parquet_current, row, file_path, nasapower_parquet_current_dir))
    
    for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
        try:
            result = future.result()
        except Exception as exc:
            print(f'Error in processing: {exc}')
            
# split nasapower to varable_loc.parquet
for index, row in tqdm(nasapower_coords.iterrows()):
    file_path = f"{nasapower_raw_dir}{int(row.loc_id)}.parquet"
    if os.path.exists(file_path):
        convert_raw_nasapower_to_parquet_current(row, file_path, nasapower_parquet_current_dir)