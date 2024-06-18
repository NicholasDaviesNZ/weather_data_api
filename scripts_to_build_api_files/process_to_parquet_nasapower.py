import pandas as pd
import os
from tqdm import tqdm


nasapower_raw_dir = './scripts_to_build_api_files/nasapower_raw/'
nasapower_parquet_dir = './scripts_to_build_api_files/nasapower/'# note this will place the files in a subdircoty here, you will then have to manually copy them to the static dir in the api, or you could write them directly if you feel like living on the wild side


nasapower_coords = pd.read_csv('./scripts_to_build_api_files/nz_coords_merra2.csv') # note that this was manaully created

if not os.path.exists(nasapower_parquet_dir):
    os.makedirs(nasapower_parquet_dir)

# split nasapower to varable_loc.parquet
for index, row in tqdm(nasapower_coords.iterrows()):
    file_path = f"{nasapower_raw_dir}{int(row.loc_id)}.parquet"
    if os.path.exists(file_path):
        df = pd.read_parquet(file_path)
        df = df.rename(columns={'date':'time'})
        var_list = df.columns.to_list()
        var_list.remove('time')
        for var_name in var_list:
            df_out = df[['time',var_name]]
            df_out.to_parquet(f"{nasapower_parquet_dir}{var_name}_{int(row.loc_id)}.parquet")