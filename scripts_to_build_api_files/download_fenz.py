"""
This script is used to download teh fenz data from the tethysts api at UC. Set fenz_dir for where to store the raw downloaded data
"""

# raw data directory to store the fenz data
fenz_dir = './FENZ/' # raw data


from tethysts import Tethys
import os
from tqdm import tqdm

if not os.path.exists(fenz_dir):
    os.makedirs(fenz_dir)

ts = Tethys()
datasets = ts.datasets
fenz_dataset = [d for d in datasets if (d['owner'] == 'FENZ')]

# this is a dict to convert the orgional names to the new ones to make them consistant with other datasets, they are used in the file name for saving the raw data 
name_shortcuts_hv = {
    'atmosphere_wind_direction':'wind_direction_2m',
    'atmosphere_temperature':'temperature_2m',
    'atmosphere_precipitation':'precipitation',
    'atmosphere_wind_speed':'wind_speed_2m',
    'atmosphere_relative_humidity':'relative_humidity_2m',
    'pedosphere_temperature':'soil_temperature_level_2', # 10cm for harvest or 20cm for niwa - layer 2 is 7-28cm
    'pedosphere_volumetric_water_content':'volumetric_soil_water_layer_2', #  ~10cm for harvest and 20 for niwa 
}

#download as raw files - could use asyncio to speed it up, or use get_results direclty on the dataset and then do a post processing to split locations
for data in tqdm(fenz_dataset):
    dataset_id = data['dataset_id']
    var_name = name_shortcuts_hv.get(f"{data['feature']}_{data['parameter']}", None)
    stations = ts.get_stations(dataset_id)
    for stat in stations:
        station_id = stat['station_id']
        output_file = '{}{}_!_{}.h5'.format(fenz_dir, var_name, stat['ref'])
        if not os.path.exists(output_file):
            ts.get_results(dataset_id, station_id, output_path=output_file, compression='lzf')