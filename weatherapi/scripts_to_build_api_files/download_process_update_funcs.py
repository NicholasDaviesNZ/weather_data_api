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

var_to_collect_era5 = ['10m_u_component_of_wind', '10m_v_component_of_wind','2m_dewpoint_temperature','2m_temperature','soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
                    'runoff','soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3','sub_surface_runoff', 'surface_runoff','surface_pressure','total_precipitation',
                    'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2', 'volumetric_soil_water_layer_3', 'cloud_base_height','evaporation','high_cloud_cover','medium_cloud_cover',
                    'low_cloud_cover','potential_evaporation','snow_depth','snowfall','soil_type','total_cloud_cover']

var_to_collect_era5_land = ['evaporation_from_bare_soil', 'evaporation_from_open_water_surfaces_excluding_oceans', 'evaporation_from_the_top_of_canopy',
                    'evaporation_from_vegetation_transpiration', 'potential_evaporation', 'runoff',
                    'soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
                    'sub_surface_runoff', 'surface_runoff', 'total_evaporation',
                    'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2', 'volumetric_soil_water_layer_3', 
                    '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_dewpoint_temperature',
                    '2m_temperature', 'snow_depth', 'snowfall',
                    'surface_pressure', 'total_precipitation']


# def get_max_date(loc_id, max_date_tracker, parquet_list, parquet_dir):
#     """ for a given loc_id, goes through the files and finds the lowest date stored in the historic parquet files """
#     subset_files_list = [f for f in parquet_list if f.endswith(f'{loc_id}.parquet')]
#     max_date = max_date_tracker
#     for f in subset_files_list:
#         df = pd.read_parquet(f'{parquet_dir}{f}', columns=['time'])
#         if not df.empty:
#             max_date = max(df['time'])    
#             if max_date < max_date_tracker:
#                 max_date_tracker = max_date
#     return loc_id, max_date_tracker

def get_max_date(data_source, vname, max_date_tracker, parquet_list, parquet_dir):
    """ for a given loc_id, goes through the files and finds the lowest date stored in the historic parquet files """
    if data_source == 'nasapower':
        subset_files_list = [f for f in parquet_list if f.endswith(f'{vname}.parquet')]
    elif data_source == 'era5' or 'era5_land':
        subset_files_list = [f for f in parquet_list if f.startswith(f'{vname}.parquet')]
    else:
        warnings.warn('data source not known')
    max_date = max_date_tracker
    for f in subset_files_list:
        df = pd.read_parquet(f'{parquet_dir}{f}', columns=['time'])
        if not df.empty:
            max_date = max(df['time'])    
            if max_date < max_date_tracker:
                max_date_tracker = max_date
    return vname, max_date_tracker


# note cpu bound task
def get_or_build_max_dates(data_source, max_dates_path, hist_list, hist_dir, coords = None, start_date = '2001-01-01', max_threads = 19):
    
    if not os.path.exists(max_dates_path) and hist_list:
        print('building the max dates file, this may take a while')
        max_dates_dict = {}
        max_date_tracker_base = datetime.now()
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_threads) as executor:
            if data_source == 'nasapower':
                if not coords.empty:
                    future_to_file = {executor.submit(get_max_date, data_source, int(row['loc_id']), max_date_tracker_base, hist_list, hist_dir): row['loc_id'] for _, row in coords.iterrows()}
                else:
                    warnings.warn('nasapower needs to have the coords dataframe passed in')
            elif data_source == 'era5':
                future_to_file = {executor.submit(get_max_date, data_source, var_name, max_date_tracker_base, hist_list, hist_dir): var_name for var_name in var_to_collect_era5} 
            elif data_source == 'era5_land':
                future_to_file = {executor.submit(get_max_date, data_source, var_name, max_date_tracker_base, hist_list, hist_dir): var_name for var_name in var_to_collect_era5_land} 
            else:
                warnings.warn('data soruce unkown')
            for future in tqdm(concurrent.futures.as_completed(future_to_file), total=len(future_to_file)):
                file = future_to_file[future]
                try:
                    result = future.result()
                    max_dates_dict[result[0]] = result[1]
                except Exception as exc:
                    print(f'{file} generated an exception: {exc}')

        end_hist_dates_df = pd.DataFrame(list(max_dates_dict.items()), columns=['vname', 'time'])
        end_hist_dates_df.to_csv(max_dates_path, index=False)
    
    elif os.path.exists(max_dates_path):
        end_hist_dates_df = pd.read_csv(max_dates_path)
        
    else: 
        end_hist_dates_df = pd.DataFrame(list(max_dates_dict.items()), columns=['vname', 'time'])
        new_row = pd.DataFrame({'vname': [0], 'time': [pd.Timestamp(start_date + ' 00:00:00')]})
        end_hist_dates_df = pd.concat([new_row, end_hist_dates_df], ignore_index=True)
        
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
    # Generate a list of all dates between start_date and end_date
    current_date = start_date
    all_dates = []
    while current_date <= end_date:
        all_dates.append(current_date)
        current_date += timedelta(days=1)

    # Extract month-year pairs and remove duplicates
    unique_months_years = {(date.month, date.year) for date in all_dates}

    # Convert set to sorted list of tuples
    unique_months_years = sorted(unique_months_years, key=lambda x: (x[1], x[0]))

    return unique_months_years





def get_era5_data(year, month, var_to_collect, raw_dir, ds_name_string):
    try:
        c = cdsapi.Client()
        print(f"Requesting data for {var_to_collect} - Year: {year}, Month: {month}")
        c.retrieve(
            ds_name_string,
            {
                'product_type': 'reanalysis',
                'format': 'netcdf',
                'variable': var_to_collect,
                'year': str(year),
                'month': str(month),
                'day': [str(d).zfill(2) for d in range(1, 32)],
                'time': [f"{str(h).zfill(2)}:00" for h in range(24)],
                'area': [-33.3, 164.64, -47.24, 179.53],
            },
            f"{raw_dir}{var_to_collect}_{year}_{month}.nc"
        )
        return f"Successfully downloaded data for {var_to_collect} - Year: {year}, Month: {month}"
    except Exception as e:
        return f"Failed to download data for {var_to_collect} - Year: {year}, Month: {month}: {str(e)}"

def get_and_write_raw(data_source, end_hist_dates_df, end_date, raw_dir, coords, max_threads=19):
    try:
        print(f'Downloading the raw data for {data_source}')
        if data_source == 'nasapower':
            futures = []
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_threads) as executor:
                for _, row in coords.iterrows():
                    start_date = pd.to_datetime(end_hist_dates_df[end_hist_dates_df.vname == int(row.loc_id)].time.values[0])
                    start_date = (start_date - relativedelta(months=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    futures.append(executor.submit(create_full_nasapower_df, row, start_date, end_date, raw_dir))
                
                for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
                    try:
                        result = future.result()
                    except Exception as exc:
                        print(f'Error in processing: {exc}')
                        
        elif data_source in ['era5', 'era5_land']:            
            ds_name_string = 'reanalysis-era5-single-levels' if data_source == 'era5' else 'reanalysis-era5-land'
            vars_to_collect = var_to_collect_era5 if data_source == 'era5' else var_to_collect_era5_land
            
            requested_combinations = []
            for vc in vars_to_collect:
                start_date = pd.to_datetime(min(end_hist_dates_df[end_hist_dates_df['vname'] == vc]['time']))
                start_date = (start_date - relativedelta(months=5)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                monthly_yearly_list = get_unique_months_years(start_date, end_date)
                requested_combinations.extend(list(itertools.product(monthly_yearly_list, [vc])))

            requested_combinations = [(year, month, var) for (month, year), var in requested_combinations]

            futures = []
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_threads) as executor:
                for year, month, var_to_collect in requested_combinations:
                    future = executor.submit(get_era5_data, year, month, var_to_collect, raw_dir, ds_name_string)
                    futures.append(future)

                for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
                    try:
                        result = future.result()
                        print(result)  
                    except Exception as exc:
                        print(f'Error in processing: {exc}')
            
    except Exception as e:
        print(f'Error processing: {e}')

        
        
        
        
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
    if file_name.endswith('.nc'):
        df = xr.open_dataset(f'{raw_dir}{file_name}').to_dataframe().reset_index().dropna()
    elif file_name.endswith('.netcdf.zip'):
        with zipfile.ZipFile(f'{raw_dir}{file_name}', 'r') as zip_ref:
            zip_ref.extractall()  # Extract files to a temporary folder
            df = xr.open_dataset('data.nc', engine='netcdf4').to_dataframe().reset_index().dropna()
    var_name = get_cur_var_name(df.columns.tolist(), name_shortcuts)

    if 'time' in df.columns:
        df = df[['longitude', 'latitude', 'time', var_name]]
    elif 'valid_time' in df.columns:
        df = df[['longitude', 'latitude', 'valid_time', var_name]]
        df.rename({'valid_time': 'time'})
    else:
        UserWarning("time or valid time are missing")
    
    df.rename(columns=name_shortcuts, inplace=True)
    long_name = name_shortcuts.get(var_name)
    df = pl.from_pandas(df)
    if long_name in ['soil_temperature_level_1','soil_temperature_level_2','soil_temperature_level_3','dewpoint_temperature_2m','temperature_2m']:
        df = (df.select([pl.col(long_name) - 273.15, pl.exclude(long_name)]))

    df = df.with_columns([
        (pl.col('longitude') * 1000).round().cast(pl.Int64).alias('longitude_int'), 
        (pl.col('latitude') * 1000).round().cast(pl.Int64).alias('latitude_int')
    ])

    df_merge = df.join(locs_df_int, on=['longitude_int','latitude_int'])
    df_merge = df_merge.drop(['longitude', 'latitude', 'longitude_int', 'latitude_int'])

    return df_merge 

def merge_dataframes(data_source, long_name, raw_dir, name_shortcuts, locs_df_int):
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
            df = load_nc_file(data_source, file_name, raw_dir, name_shortcuts, locs_df_int)
            if merged_df is None:
                merged_df = df
            else:
                merged_df = pl.concat([merged_df, df])
    if 'valid_time' in merged_df.columns:
        merged_df = merged_df.rename({"valid_time": "time"})
    merged_df = merged_df.sort("time")
    
    return merged_df

def write_var_loc_to_para(data_source, long_name, raw_dir, current_dir, unique_locs, locs_df_int, name_shortcuts):
    df = merge_dataframes(data_source, long_name, raw_dir, name_shortcuts, locs_df_int)
    for loc in unique_locs:
        filtered_df = df.filter(pl.col('loc_id') == loc)
        filtered_df = filtered_df.drop(['loc_id'])
        output_file = f"{current_dir}{long_name}_{loc}.parquet"
        filtered_df.write_parquet(output_file)
        filtered_df = None


def convert_raw_to_parquet_current(data_source, coords, raw_dir, current_dir, max_threads=19):
    print(f'converting raw data into paraquat format for {data_source}')
    if data_source == 'nasapower':
        futures = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_threads) as executor:
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
        locs_df_int = locs_df_int.with_columns(pl.col('loc_id').cast(pl.Int64))
        
        
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
        unique_locs = locs_df_int['loc_id'].unique().to_list()
        # futures = []
        # with concurrent.futures.ProcessPoolExecutor(max_workers=max_threads) as executor:
        #     for long_name in name_shortcuts.values():
        #         futures.append(executor.submit(write_var_loc_to_para, data_source, long_name, raw_dir, current_dir, unique_locs, locs_df_int, name_shortcuts))
    
        #     for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
        #         try:
        #             result = future.result()
        #         except Exception as exc:
        #             print(f'Error in processing: {exc}')
        results = []

        # Iterate over each long_name in name_shortcuts.values() and process sequentially
        for long_name in name_shortcuts.values():
            try:
                result = write_var_loc_to_para(data_source, long_name, raw_dir, current_dir, unique_locs, locs_df_int, name_shortcuts)
                results.append(result)
            except Exception as exc:
                print(f'Error in processing: {exc}')
                    
                    
                    
def merge_cur_hist(data_source, cur_file, current_dir, hist_dir, force_copy_all_current = False):
    cur_file_path = f'{current_dir}{cur_file}' 
    hist_file_path = f'{hist_dir}{cur_file}'
    try:
        cur_file = pl.read_parquet(cur_file_path).select(pl.all().exclude("^__index_level_.*$"))
        if cur_file.is_empty():
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

    hist_col_names = [col for col in hist_file.columns if col != 'time']
    hist_file = hist_file.with_columns(pl.col(hist_col_names).cast(pl.Float32))
    cur_to_hist = cur_to_hist.with_columns(pl.col(hist_col_names).cast(pl.Float32))
    hist_col_order = ['time'] + [col for col in hist_file.columns if col != 'time']
    cur_col_order = ['time'] + [col for col in cur_to_hist.columns if col != 'time']
    hist_file = hist_file.select(hist_col_order)
    cur_to_hist = cur_to_hist.select(cur_col_order)
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
            
            
def merge_current_to_historic(data_source, current_dir, hist_dir, force_copy_all_current = False, max_threads=19):
    print('merginge current files into historic')
    # move into function
    current_files = set(os.listdir(current_dir))
    hist_files = set(os.listdir(hist_dir))

    mismatched_files = list(current_files-hist_files)+list(hist_files-current_files)
    print(mismatched_files)
    # if the historic and current files dont match, do not run merge
    if not mismatched_files:
        for cur_file in current_files:
            merge_cur_hist(data_source, cur_file, current_dir, hist_dir, force_copy_all_current = False)
        
        # futures = []
        # print('merging older current files into the historic files, this may take a while')
        # with concurrent.futures.ProcessPoolExecutor(max_workers=max_threads) as executor:
        #     for cur_file in current_files:
        #         futures.append(executor.submit(merge_cur_hist, data_source, cur_file, current_dir, hist_dir, force_copy_all_current = False))
    
        #     for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
        #         try:
        #             result = future.result()
        #         except Exception as exc:
        #             print(f'Error in processing: {exc}')
    else:
        warnings.warn(f'files in current but not historic: {current_files-hist_files} will not perform merge')
        warnings.warn(f'files in historic but not current: {hist_files-current_files} will not perform merge')