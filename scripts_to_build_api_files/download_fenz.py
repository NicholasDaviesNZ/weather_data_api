"""
This script is used to download teh fenz data from the tethysts api at UC. Set fenz_dir for where to store the raw downloaded data
"""

# raw data directory to store the fenz data
fenz_dir = './scripts_to_build_api_files/FENZ_raw/' # raw data


from tethysts import Tethys
import os
from tqdm import tqdm
from datetime import datetime
ts = Tethys()
datasets = ts.datasets
fenz_dataset = [d for d in datasets if (d['owner'] == 'FENZ')]


if not os.path.exists(fenz_dir):
    os.makedirs(fenz_dir)

start_date = datetime.strptime("2024-01-01", "%Y-%m-%d")
end_date = datetime.now()

#fenz_dataset = [d for d in datasets if (datetime.strptime(d['time_range']['to_date'], "%Y-%m-%dT%H:%M:%S")>=start_date)]

name_shortcuts_hv = {
    'atmosphere_wind_direction':'wind_direction_2m',
    'atmosphere_temperature':'temperature_2m',
    'atmosphere_precipitation':'precipitation',
    'atmosphere_wind_speed':'wind_speed_2m',
    'atmosphere_relative_humidity':'relative_humidity_2m',
    'pedosphere_temperature':'soil_temperature_level_2', # 10cm for harvest or 20cm for niwa - layer 2 is 7-28cm
    'pedosphere_volumetric_water_content':'volumetric_soil_water_layer_2', #  ~10cm for harvest and 20 for niwa 
}


for data in tqdm(fenz_dataset):
    dataset_id = data['dataset_id']
    var_name = name_shortcuts_hv.get(f"{data['feature']}_{data['parameter']}", None)
    stations = ts.get_stations(dataset_id)
    stations = [s for s in stations if (datetime.strptime(s['time_range']['to_date'], "%Y-%m-%dT%H:%M:%S")>=start_date)]
    for stat in stations:
        station_id = stat['station_id']
        output_file = '{}{}_!_{}.h5'.format(fenz_dir, var_name, stat['ref'])
        if not os.path.exists(output_file):
            ts.get_results(dataset_id, station_id, output_path=output_file, from_date=start_date, to_date=end_date, compression='lzf')