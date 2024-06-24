import pandas as pd
import polars as pl
import requests
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import warnings
import cdsapi
import concurrent.futures
from tqdm import tqdm
import itertools
import os
import zipfile
import xarray as xr
from dotenv import load_dotenv
load_dotenv() # load the env variables from the .env file, needs to have CDSAPI_URL and CDSAPI_KEY from your cds store account




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

# note this is really slow, so we avoid doing it if we can
# note cpu bound task
def get_or_build_max_dates(max_dates_path,hist_list, hist_dir, coords):
    if not os.path.exists(max_dates_path):
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

        end_hist_dates_df = pd.DataFrame(list(max_dates_dict.items()), columns=['loc_id', 'time'])
        end_hist_dates_df.to_csv(max_dates_path, index=False)
    
    else: 
        end_hist_dates_df = pd.read_csv(max_dates_path)
        
    return end_hist_dates_df

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



def create_full_nasapower_df(row,start_date,end_date, raw_dir):
    
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
        df = get_nasapower_year(row.latitude, row.longitude, year_start_date.strftime('%Y-%m-%d'), year_end_date.strftime('%Y-%m-%d'))
    
        # Append the DataFrame to the list
        dfs.append(df)

    # Concatenate all DataFrames in the list into one DataFrame
    final_df = pd.concat(dfs, ignore_index=True)
    
    final_df.to_parquet(f'{raw_dir}{int(row.loc_id)}.parquet', compression='brotli')

            
def get_unique_months_years(start_date, end_date):    
    # Get unique months
    unique_months = set()
    current_date = start_date
    while current_date <= end_date:
        unique_months.add((current_date.month))
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)
    # Get unique years
    unique_years = set()
    current_date = start_date
    while current_date.year <= end_date.year:
        unique_years.add(current_date.year)
        current_date = current_date.replace(year=current_date.year + 1)
    
    # Convert sets to sorted lists
    unique_months = sorted(unique_months)
    unique_years = sorted(unique_years)
    
    return unique_months, unique_years



def get_era5_data(year, month, var_to_collect, raw_dir, ds_name_string):
    c = cdsapi.Client()

    data = c.retrieve(
        ds_name_string,
        {
            'product_type': 'reanalysis',
            'format': 'netcdf',
            'variable': var_to_collect,
            'year': str(year),
            'month': str(month),
            'day': [
                '01', '02', '03',
                '04', '05', '06',
                '07', '08', '09',
                '10', '11', '12',
                '13', '14', '15',
                '16', '17', '18',
                '19', '20', '21',
                '22', '23', '24',
                '25', '26', '27',
                '28', '29', '30',
                '31',
            ],
            'time': [
                '00:00', '01:00', '02:00',
                '03:00', '04:00', '05:00',
                '06:00', '07:00', '08:00',
                '09:00', '10:00', '11:00',
                '12:00', '13:00', '14:00',
                '15:00', '16:00', '17:00',
                '18:00', '19:00', '20:00',
                '21:00', '22:00', '23:00',
            ],
            'area': [-33.3, 164.64, -47.24, 179.53],
        },
        f"{raw_dir}{var_to_collect}_{year}_{month}.nc")
    

    
def get_and_write_raw(data_source,end_hist_dates_df, end_date, raw_dir, coords):
    try:
        if data_source == 'nasapower':
            futures = []
            with concurrent.futures.ProcessPoolExecutor() as executor:
                for _, row in coords.iterrows():
                    start_date = pd.to_datetime(end_hist_dates_df[end_hist_dates_df.loc_id == int(row.loc_id)].time.values[0])
                    start_date = (start_date - relativedelta(months=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    futures.append(executor.submit(create_full_nasapower_df, row, start_date, end_date, raw_dir))
        
                for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
                    try:
                        result = future.result()
                    except Exception as exc:
                        print(f'Error in processing: {exc}')
                        
        elif data_source == 'era5' or data_source == 'era5_land':
            start_date = pd.to_datetime(min(end_hist_dates_df.time).values[0])
            start_date = (start_date - relativedelta(months=5)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            monthly_list, yearly_list = get_unique_months_years(start_date, end_date)
            
            if data_source == 'era5':
                ds_name_string = 'reanalysis-era5-single-levels'
                var_to_collect = ['10m_u_component_of_wind', '10m_v_component_of_wind','2m_dewpoint_temperature','2m_temperature','soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
                    'runoff','soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3','sub_surface_runoff', 'surface_runoff','surface_pressure','total_precipitation',
                    'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2', 'volumetric_soil_water_layer_3', 'cloud_base_height','evaporation','high_cloud_cover','medium_cloud_cover',
                    'low_cloud_cover','potential_evaporation','snow_depth','snowfall','soil_type','total_cloud_cover']
            elif data_source == 'era5_land':
                ds_name_string = 'reanalysis-era5-land'
                var_to_collect = ['evaporation_from_bare_soil', 'evaporation_from_open_water_surfaces_excluding_oceans', 'evaporation_from_the_top_of_canopy',
                    'evaporation_from_vegetation_transpiration', 'potential_evaporation', 'runoff',
                    'soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
                    'sub_surface_runoff', 'surface_runoff', 'total_evaporation',
                    'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2', 'volumetric_soil_water_layer_3', 
                    '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_dewpoint_temperature',
                    '2m_temperature', 'snow_depth', 'snowfall',
                    'surface_pressure', 'total_precipitation']
            
            requested_combinations = list(itertools.product(yearly_list, monthly_list, var_to_collect))
            
            futures = []
            with concurrent.futures.ProcessPoolExecutor() as executor:
                for year, month, var_to_collect in requested_combinations:
                    future = executor.submit(get_era5_data, year, month, var_to_collect, raw_dir, ds_name_string)
                    futures.append(future)

                for future in futures:
                    try:
                        result = future.result()
                    except Exception as exc:
                        print(f'Error in processing: {exc}')
            
    except Exception as e:
        print(f'Error processing loc_id {row.loc_id}: {e}')
        
        
        
        
def nasapower_convert_raw_to_parquet_current(row, file_path, parquet_current_dir):
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
            df_out = df_out[df_out[var_name] != -999.0]
            df_out.to_parquet(f"{parquet_current_dir}{var_name}_{int(row.loc_id)}.parquet")
        except Exception as exc:
            print(f'Error saving file: {exc}')
            
def get_cur_var_name(col_names, name_shortcuts):
    variables_to_remove = ['longitude', 'latitude', 'time']
    variable_names = [var for var in col_names]
    filtered_variable_names = [var for var in variable_names if var not in variables_to_remove]
    if len(filtered_variable_names) > 1:
        warnings.warn(f"there are multiple varables in the nc file {filtered_variable_names}, you need to check what is going on here, we will atempt to resolve the problem, but this needs checking")
        filtered_variable_names = [var for var in filtered_variable_names if var in name_shortcuts]
        if len(filtered_variable_names) == 1:
            warnings.warn(f"error resolved by retaining only {filtered_variable_names}, you should still check this manaully")
            return filtered_variable_names[0]
        else:
            return filtered_variable_names
    else:
        return filtered_variable_names[0]
    
def load_nc_file(data_source, file_name, raw_dir, name_shortcuts, locs_df_int):
    if data_source == "era5":
        df = xr.open_dataset(f'{raw_dir}{file_name}').to_dataframe().reset_index().dropna()
    elif data_source == "era5_land":
        with zipfile.ZipFile(f'{raw_dir}{file_name}', 'r') as zip_ref:
            zip_ref.extractall()  # Extract files to a temporary folder
            df = xr.open_dataset('data.nc').to_dataframe().reset_index().dropna()
            
    var_name = get_cur_var_name(df.columns.tolist())
    df = df[['longitude', 'latitude', 'time', var_name]]
    df.rename(columns=name_shortcuts, inplace=True)
    long_name = name_shortcuts.get(var_name)
    df = pl.from_pandas(df)
    if long_name in ['soil_temperature_level_1','soil_temperature_level_2','soil_temperature_level_3','2m_dewpoint_temperature','temperature_2m']:
        df = (df.select([pl.col(long_name) - 273.15, pl.exclude(long_name)]))

    df = df.with_columns([
        (pl.col('longitude') * 1000).round().cast(pl.Int64).alias('longitude_int'), 
        (pl.col('latitude') * 1000).round().cast(pl.Int64).alias('latitude_int')
    ])

    df_merge = df.join(locs_df_int, on=['longitude_int','latitude_int'])
    df_merge = df_merge.drop(['longitude', 'latitude', 'longitude_int', 'latitude_int'])
    return df_merge 

def merge_dataframes(data_source, long_name, raw_dir, name_shortcuts):
    if long_name == 'dewpoint_temperature_2m':
        long_name_og = '2m_dewpoint_temperature'
    elif long_name == 'temperature_2m':
        long_name_og = '2m_temperature'
    elif long_name == 'precipitation':
        long_name_og = 'total_precipitation'
    else:
        long_name_og = long_name
    merged_df = None
    for file_name in os.listdir(raw_dir):
        
        if file_name.split("_")[:-2]==long_name_og.split("_") and (file_name.endswith('.netcdf.zip') or  file_name.endswith('.nc')):
            df = load_nc_file(data_source, file_name, raw_dir, name_shortcuts)
            if merged_df is None:
                merged_df = df
            else:
                merged_df = pl.concat([merged_df, df])
    merged_df = merged_df.sort("time")
    return merged_df

def write_var_loc_to_para(data_source, long_name, raw_dir, current_dir, unique_locs, name_shortcuts):
    
    df = merge_dataframes(data_source, long_name, raw_dir, name_shortcuts)
    for loc in unique_locs:
        filtered_df = df.filter(pl.col('loc_id') == loc)
        filtered_df = filtered_df.drop(['loc_id'])
        output_file = f"{current_dir}{long_name}_{loc}.parquet"
        filtered_df.write_parquet(output_file)
        filtered_df = None


def convert_raw_to_parquet_current(data_source, coords, raw_dir, current_dir):
    if data_source == 'nasapower':
        futures = []
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for _, row in coords.iterrows():
                file_path = f"{raw_dir}{int(row.loc_id)}.parquet"
                if os.path.exists(file_path):
                    futures.append(executor.submit(nasapower_convert_raw_to_parquet_current, row, file_path, current_dir))
    
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
                try:
                    result = future.result()
                except Exception as exc:
                    print(f'Error in processing: {exc}')
                    
    elif data_source == 'era5' or data_source == 'era5_land':
        locs_df_int = coords.copy()
        locs_df_int[['longitude_int', 'latitude_int']] = (coords[['longitude', 'latitude']]*1000).round().astype(int)
        locs_df_int = locs_df_int.drop(columns=['longitude', 'latitude'])
        locs_df_int = pl.from_pandas(locs_df_int)
        unique_locs = locs_df_int['loc_id'].unique().to_list()
        
        if data_source == 'era5':
            name_shortcuts = {'u10': '10m_u_component_of_wind',
                'v10': '10m_v_component_of_wind',
                'd2m': 'dewpoint_temperature_2m',
                't2m': 'temperature_2m',
                'stl1': 'soil_temperature_level_1',
                'stl2': 'soil_temperature_level_2',
                'stl3': 'soil_temperature_level_3',
                'ro': 'runoff',
                'ssro': 'sub_surface_runoff',
                'sro': 'surface_runoff',
                'sp': 'surface_pressure',
                'tp': 'total_precipitation',
                'swvl1': 'volumetric_soil_water_layer_1',
                'swvl2': 'volumetric_soil_water_layer_2',
                'swvl3': 'volumetric_soil_water_layer_3',
                'cbh': 'cloud_base_height',
                'e': 'evaporation',
                'hcc': 'high_cloud_cover',
                'mcc': 'medium_cloud_cover',
                'lcc': 'low_cloud_cover',
                'pev': 'potential_evaporation',
                'sd': 'snow_depth',
                'sf': 'snowfall',
                'slt': 'soil_type',
                'tcc': 'total_cloud_cover'}
        elif data_source == 'era5_land':
            name_shortcuts = {
                'evabs':'evaporation_from_bare_soil',
                'evaow':'evaporation_from_open_water_surfaces_excluding_oceans',
                'evatc':'evaporation_from_the_top_of_canopy',
                'evavt':'evaporation_from_vegetation_transpiration',
                'pev':'potential_evaporation',
                'ro':'runoff',
                'stl1':'soil_temperature_level_1',
                'stl2':'soil_temperature_level_2',
                'stl3':'soil_temperature_level_3',
                'ssro':'sub_surface_runoff',
                'sro':'surface_runoff',
                'e':'total_evaporation',
                'swvl1':'volumetric_soil_water_layer_1',
                'swvl2':'volumetric_soil_water_layer_2',
                'swvl3':'volumetric_soil_water_layer_3',
                'u10':'10m_u_component_of_wind',
                'v10':'10m_v_component_of_wind',
                'd2m':'dewpoint_temperature_2m',
                't2m':'temperature_2m',
                'sde':'snow_depth',
                'sf':'snowfall',
                'sp':'surface_pressure',
                'tp':'precipitation'}
            
        futures = []
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for long_name in name_shortcuts.values():

                if os.path.exists(file_path):
                    futures.append(executor.submit(write_var_loc_to_para, data_source, long_name, raw_dir, current_dir, unique_locs, name_shortcuts))
    
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
                try:
                    result = future.result()
                except Exception as exc:
                    print(f'Error in processing: {exc}')
                    
                    
                    
                    
def merge_cur_hist(data_source, cur_file, current_dir, hist_dir, force_copy_all_current = False):
    cur_file_path = f'{current_dir}{cur_file}' 
    hist_file_path = f'{hist_dir}{cur_file}'
    try:
        cur_file = pl.read_parquet(cur_file_path).select(pl.all().exclude("^__index_level_.*$"))
        if cur_file.is_empty():
            print("cur file is empty")
            return
    except Exception as e:
        print(f"Error reading {cur_file_path}: {e}")
        return

    try:
        hist_file = pl.read_parquet(hist_file_path).select(pl.all().exclude("^__index_level_.*$")) 
    except Exception as e:
        print(f"Error reading {hist_file_path}: {e}")
        pass
    
    if data_source == 'era5' or data_source == 'era5_land':
        months_to_remove = 5
    elif data_source == 'nasapower':
        months_to_remove = 1
    else:
        warnings.warn('data_sorce is not picked up in the if block to set the time to hold onto for current data')
    
    if force_copy_all_current != True:
        cur_max_date = cur_file.select(pl.col('time')).max()
        date_to_copy_current_up_to = (pd.to_datetime(cur_max_date['time'][0])- relativedelta(months=months_to_remove)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        cur_to_hist = cur_file.filter(pl.col('time') < date_to_copy_current_up_to)
    else:
        cur_to_hist = cur_file
        
    if cur_to_hist.is_empty():
        return

    hist_file = hist_file.with_columns(pl.col(hist_file.columns[1]).cast(pl.Float64))
    new_hist = pl.concat([cur_to_hist, hist_file], how='vertical')
    new_hist = new_hist.unique(subset=["time"], keep="first")
    new_hist = new_hist.sort('time') 
    try:
        new_hist.write_parquet(hist_file_path)
    except Exception as e:
        print(f"Error writing {hist_file_path}: {e}")
    
    if force_copy_all_current != True:
        replace_cur = cur_file.filter(pl.col('time') >= date_to_copy_current_up_to)
        try:
            replace_cur.write_parquet(cur_file_path)
        except Exception as e:
            print(f"Error writing {cur_file_path}: {e}")
        
    else:
        try:
            os.remove(cur_file_path)
        except Exception as e:
            print(f"Error deleting {cur_file_path}: {e}")
            
            
def merge_current_to_historic(data_source, current_dir, hist_dir, force_copy_all_current = False):
    # move into function
    current_files = set(os.listdir(current_dir))
    hist_files = set(os.listdir(hist_dir))

    mismatched_files = list(current_files-hist_files)+list(hist_files-current_files)

    warnings.warn(f'files in current but not historic: {current_files-hist_files} will not perform merge')
    warnings.warn(f'files in historic but not current: {hist_files-current_files} will not perform merge')

    # if the historic and current files dont match, do not run merge
    if not mismatched_files:
        futures = []
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for cur_file in current_files:
                futures.append(executor.submit(merge_cur_hist, data_source, cur_file, current_dir, hist_dir, force_copy_all_current = False))
    
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
                try:
                    result = future.result()
                except Exception as exc:
                    print(f'Error in processing: {exc}')