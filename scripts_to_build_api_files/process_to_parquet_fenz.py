"""
Script to take the raw fenz data, create a coords file from the raw data, and then generate the parquet files for the locations and vars
"""


fenz_dir = '/workspaces/weather_data_api/scripts_to_build_api_files/fenz_raw/' # raw data
FENZ_parquet = '/workspaces/weather_data_api/scripts_to_build_api_files/fenz/' # note this will place the files in a subdircoty here, you will then have to manually copy them to the static dir in the api, or you could write them directly if you feel like living on the wild side

import os
from tqdm import tqdm
import pandas as pd
import xarray as xr
import warnings

if not os.path.exists(FENZ_parquet):
    os.makedirs(FENZ_parquet)

fenz_list = [file for file in os.listdir(fenz_dir) if os.path.isfile(os.path.join(fenz_dir, file))]
list_of_rh = []
list_of_vsw = []
list_of_st = []
list_of_ws = []
list_of_p = []
list_of_t = []
list_of_wd = []

# note okato and hawea flat and timaru may cause some issues. 
for file in tqdm(fenz_list):
    ref = file.split('_!_')[-1].split('.')[0]
    var = file.split('_!_')[0]
    file_path = fenz_dir + file
    results = xr.open_dataset(f'{fenz_dir}{file}', engine='h5netcdf') 
    df = results.to_dataframe().reset_index()
    df = df.drop(columns=['geometry','height','externalId','name','station_id','ref'])
    # the timaru stations has messed up coords in the downloaded files, fixing manually
    if file in ['relative_humidity_2m_!_timaru_coastal_hv.h5','wind_speed_2m_!_timaru_coastal_hv.h5','wind_direction_2m_!_timaru_coastal_hv.h5']: 
        df['lat'] = -44.30493
        df['lon'] = 171.22158

    df = df.rename(columns={'lat':'latitude', 'lon':'longitude'})
    df.columns.values[-1] = var
    if var == 'relative_humidity_2m':
        list_of_rh.append(df)
    elif var == 'volumetric_soil_water_layer_2':
        list_of_vsw.append(df)
    elif var == 'soil_temperature_level_2':
        list_of_st.append(df)
    elif var == 'wind_speed_2m':
        list_of_ws.append(df)
    elif var == 'precipitation':
        list_of_p.append(df)
    elif var == 'temperature_2m':
        list_of_t.append(df)
    elif var == 'wind_direction_2m':
        list_of_wd.append(df)
    else:
        print('can not add to list')

rh_df = pd.concat(list_of_rh)
vsw_df = pd.concat(list_of_vsw)
st_df = pd.concat(list_of_st)
ws_df = pd.concat(list_of_ws)
p_df = pd.concat(list_of_p)
t_df = pd.concat(list_of_t)
wd_df = pd.concat(list_of_wd)

rh_coords = rh_df[['latitude','longitude']].drop_duplicates()
vsw_coords = vsw_df[['latitude','longitude']].drop_duplicates()
st_coords = st_df[['latitude','longitude']].drop_duplicates()
ws_coords = ws_df[['latitude','longitude']].drop_duplicates()
p_coords = p_df[['latitude','longitude']].drop_duplicates()
t_coords = t_df[['latitude','longitude']].drop_duplicates()
wd_coords = wd_df[['latitude','longitude']].drop_duplicates()

coords_all = pd.concat([rh_coords,vsw_coords,st_coords,ws_coords,p_coords,t_coords,wd_coords])
coords = coords_all.drop_duplicates()
coords.reset_index(inplace=True, drop=True)

coords_old = pd.read_csv('/workspaces/weather_data_api/weatherapi/historic_weather_api/static/coords/nz_coords_FENZ.csv')
coords_new = coords_old.merge(coords, on=['latitude','longitude'], how='outer')
coords_new = (coords_new.sort_values(by=['loc_id'])).reset_index(drop=True) # add new loc ids

coords_new['loc_id'] = coords_new['loc_id'].fillna(pd.Series(coords_new.index)).astype(int)

# Print the rows with mismatched loc_id values
if not coords_new[coords_new['loc_id'] != coords_new.index].empty:
    warnings.warn("Mismatched loc_id values aborting fenz, your need to check what is going on with the coords")
    ############################################################################################# return in here

#coords_new.to_csv(f'{FENZ_parquet}nz_coords_FENZ.csv', index=False)

coords = coords_new

rh_df = rh_df.merge(coords, on=['latitude','longitude']).drop(columns=['latitude','longitude'])
vsw_df = vsw_df.merge(coords, on=['latitude','longitude']).drop(columns=['latitude','longitude'])
st_df = st_df.merge(coords, on=['latitude','longitude']).drop(columns=['latitude','longitude'])
ws_df = ws_df.merge(coords, on=['latitude','longitude']).drop(columns=['latitude','longitude'])
p_df = p_df.merge(coords, on=['latitude','longitude']).drop(columns=['latitude','longitude'])
t_df = t_df.merge(coords, on=['latitude','longitude']).drop(columns=['latitude','longitude'])
wd_df = wd_df.merge(coords, on=['latitude','longitude']).drop(columns=['latitude','longitude'])


for loc_id, group_df in rh_df.groupby('loc_id'):
    group_df.drop(columns=['loc_id'], inplace=True)
    group_df.to_parquet(f"{FENZ_parquet}/relative_humidity_2m_{loc_id}.parquet")
    
for loc_id, group_df in wd_df.groupby('loc_id'):
    group_df.drop(columns=['loc_id'], inplace=True)
    group_df.to_parquet(f"{FENZ_parquet}/wind_direction_2m_{loc_id}.parquet")

for loc_id, group_df in t_df.groupby('loc_id'):
    group_df.drop(columns=['loc_id'], inplace=True)
    group_df.to_parquet(f"{FENZ_parquet}/temperature_2m_{loc_id}.parquet")

for loc_id, group_df in p_df.groupby('loc_id'):
    group_df.drop(columns=['loc_id'], inplace=True)
    group_df.to_parquet(f"{FENZ_parquet}/precipitation_{loc_id}.parquet")

for loc_id, group_df in ws_df.groupby('loc_id'):
    group_df.drop(columns=['loc_id'], inplace=True)
    group_df.to_parquet(f"{FENZ_parquet}/wind_speed_2m_{loc_id}.parquet")

for loc_id, group_df in st_df.groupby('loc_id'):
    group_df.drop(columns=['loc_id'], inplace=True)
    group_df.to_parquet(f"{FENZ_parquet}/soil_temperature_level_2_{loc_id}.parquet")

for loc_id, group_df in vsw_df.groupby('loc_id'):
    group_df.drop(columns=['loc_id'], inplace=True)
    group_df.to_parquet(f"{FENZ_parquet}/volumetric_soil_water_layer_2_{loc_id}.parquet")