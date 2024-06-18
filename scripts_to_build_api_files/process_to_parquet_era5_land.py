"""
Process era5_land raw data into varaible location parquet files
"""



netcdf4_folder = './scripts_to_build_api_files/era5_land_raw/' 
output_dir = './scripts_to_build_api_files/era5_land/' # note this will place the files in a subdircoty here, you will then have to manually copy them to the static dir in the api, or you could write them directly if you feel like living on the wild side

import zipfile
import xarray as xr
import os
import pandas as pd
import polars as pl
import warnings
from tqdm import tqdm

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

locs_df = pd.read_csv('./scripts_to_build_api_files/nz_coords_era5_land.csv')
locs_df_int = locs_df.copy()
locs_df_int[['longitude_int', 'latitude_int']] = (locs_df[['longitude', 'latitude']]*1000).round().astype(int)
locs_df_int = locs_df_int.drop(columns=['longitude', 'latitude'])
locs_df_int = pl.from_pandas(locs_df_int)

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
    'tp':'precipitation',
}



def get_cur_var_name(col_names):
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

def load_nc_file(file_name, netcdf4_folder, name_shortcuts):
    with zipfile.ZipFile(f'./{netcdf4_folder}/{file_name}', 'r') as zip_ref:
        zip_ref.extractall()  # Extract files to a temporary folder
        df= xr.open_dataset('data.nc').to_dataframe().reset_index().dropna()

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
    
# Define the function to load and merge the DataFrames
def merge_dataframes(long_name, netcdf4_folder, name_shortcuts):
    if long_name == 'dewpoint_temperature_2m':
        long_name_og = '2m_dewpoint_temperature'
    elif long_name == 'temperature_2m':
        long_name_og = '2m_temperature'
    elif long_name == 'precipitation':
        long_name_og = 'total_precipitation'
    else:
        long_name_og = long_name
    merged_df = None
    for file_name in os.listdir(netcdf4_folder):
        
        if file_name.split("_")[:-2]==long_name_og.split("_") and file_name.endswith('.netcdf.zip'):
            df = load_nc_file(file_name, netcdf4_folder, name_shortcuts)
            if merged_df is None:
                merged_df = df
            else:
                merged_df = pl.concat([merged_df, df])
    merged_df = merged_df.sort("time")
    return merged_df

def write_var_loc_to_para(long_name, netcdf4_folder, unique_locs, name_shortcuts):
    
    df = merge_dataframes(long_name, netcdf4_folder, name_shortcuts)

    for loc in unique_locs:
        filtered_df = df.filter(pl.col('loc_id') == loc)
        filtered_df = filtered_df.drop(['loc_id'])
        output_file = f"./{output_dir}/{long_name}_{loc}.parquet"
        filtered_df.write_parquet(output_file)
        filtered_df = None

def loop_over_vars(name_shortcuts, netcdf4_folder, output_dir, locs_df_int, replace_existing=False):
    unique_locs = locs_df_int['loc_id'].unique().to_list()
    unique_fnames = []
    for file_name in os.listdir(output_dir):
        fnames = "_".join(file_name.split("_")[:-1])
        if fnames not in unique_fnames:
            unique_fnames.append(fnames) # get list of already produced files, note this will include any partually saved varables, ie if var_a has 1 location saved, it will pass this test, and the other locations will not be recalced
    for long_name in tqdm(name_shortcuts.values()):
        if long_name not in unique_fnames:
            write_var_loc_to_para(long_name, netcdf4_folder, unique_locs, name_shortcuts)


loop_over_vars(name_shortcuts, netcdf4_folder, output_dir, locs_df_int)