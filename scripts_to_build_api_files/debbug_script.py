
import xarray as xr
import os
import pandas as pd
import polars as pl
import warnings
from tqdm import tqdm
netcdf4_folder = './scripts_to_build_api_files/era5_raw/'

file_name = 'runoff_2024_4.nc'

df= xr.open_dataset(f'./{netcdf4_folder}/{file_name}')
print(df)