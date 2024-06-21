
import xarray as xr
import os
import pandas as pd
import polars as pl
import warnings
from tqdm import tqdm
nasapower_raw_dir = '/workspaces/weather_data_api/scripts_to_build_api_files/nasapower_raw/'
nasapower_parquet_dir = '/workspaces/weather_data_api/weatherapi/historic_weather_api/static/historic/nasapower/'
nasapower_parquet_current_dir = '/workspaces/weather_data_api/weatherapi/historic_weather_api/static/current/nasapower/'


df = pd.read_parquet(f'{nasapower_parquet_current_dir}precipitation_0.parquet')
print(df)