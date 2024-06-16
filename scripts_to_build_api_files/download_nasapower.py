"""
Script to download the raw data from the nasapower api
"""

start_date = "2001-01-01"
end_date = "2023-12-31"

nasapower_coords = pd.read_csv('nz_coords_merra2.csv') # note that this was manaully created
nasapower_dir = './nasapower_raw/'


import pandas as pd
from pandas.api.types import CategoricalDtype
import os
import requests
import json
from tqdm import tqdm

if not os.path.exists(nasapower_dir):
    os.makedirs(nasapower_dir)

# downcast util to make df smaller before saving:
def squeeze_dataframe(df):
    #----- Get columns in dataframe
    cols = dict(df.dtypes)
    #----- Check each column's type downcast or categorize as appropriate
    for col, type in cols.items():
        if type == 'float64':
            df[col] = pd.to_numeric(df[col], downcast='float')
        elif type == 'int64':
            df[col] = pd.to_numeric(df[col], downcast='integer')
        elif type == 'object':
            df[col] = df[col].astype(CategoricalDtype(ordered=True))
    return df

# fucntion to save as a compressed paraquat file:
def save_df(df, file_name):
    df = squeeze_dataframe(df)
    df.to_parquet(f'{file_name}.parquet', compression='brotli')

def to_timestamp(date_string):
    return pd.to_datetime(date_string)

# https://gist.github.com/abelcallejo/d68e70f43ffa1c8c9f6b5e93010704b8 for parameter names
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
    start_date_ts = to_timestamp(start_date)
    end_date_ts = to_timestamp(end_date)

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




# nasapower data download loop
for index, row in tqdm(nasapower_coords.iterrows(), total=len(nasapower_coords)):
   file_path = f"{nasapower_dir}{int(row.loc_id)}.parquet"
   if not os.path.exists(file_path):
        # If the file does not exist, create DataFrame and save it
        df = create_full_nasapower_df(row.latitude, row.longitude, start_date, end_date)
        save_df(df, f"{nasapower_dir}{int(row.loc_id)}")
       