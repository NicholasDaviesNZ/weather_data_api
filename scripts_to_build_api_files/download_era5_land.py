


era5_land_dir = './era5_land_raw/'

start_year = 2023
end_year = 2024


import cdsapi
import itertools
from concurrent.futures import ThreadPoolExecutor
import os
import cfgrib as cfgrib
from dotenv import load_dotenv
load_dotenv() # load the env variables from the .env file, needs to have CDSAPI_URL and CDSAPI_KEY from your cds store account

yearly_list = [str(year) for year in range(start_year, end_year + 1)]
monthly_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
al = [-33.3, 164.64, -47.24, 179.53]
num_threads = 19 # 20 is the max simultainius connections to cds for era5_land, but they may reduce it if its bussy or they decide you are making too many connections

if not os.path.exists(era5_land_dir):
    os.makedirs(era5_land_dir)

var_to_collect = ['evaporation_from_bare_soil', 'evaporation_from_open_water_surfaces_excluding_oceans', 'evaporation_from_the_top_of_canopy',
            'evaporation_from_vegetation_transpiration', 'potential_evaporation', 'runoff',
            'soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
            'sub_surface_runoff', 'surface_runoff', 'total_evaporation',
            'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2', 'volumetric_soil_water_layer_3', 
            '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_dewpoint_temperature',
            '2m_temperature', 'snow_depth', 'snowfall',
            'surface_pressure', 'total_precipitation']


# https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land?tab=overview

def get_era5_1st_jan(year, area, out_dir, var_to_collect):
    c = cdsapi.Client()

    data = c.retrieve(
        'reanalysis-era5-land',
        {
            'variable': var_to_collect,
            'year': str(year),
            'month': '01',
            'day': [
                '01',
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
            'area': area,
            'format': 'netcdf.zip',
        },
        f"{era5_land_dir}{var_to_collect}_{year}_01.netcdf.zip")


def get_era5_data(year, month, area, out_dir, var_to_collect):
    c = cdsapi.Client()

    data = c.retrieve(
        'reanalysis-era5-land',
        {
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
            'area': area,
            'format': 'netcdf.zip',
        },
        f"{era5_land_dir}{var_to_collect}_{year}_{month}.netcdf.zip")


# Assuming you have these lists defined: yearly_list, monthly_list, var_to_collect
all_combinations = list(itertools.product(yearly_list, monthly_list, var_to_collect))
# get the file names for all combo biles
result_list = ["{}_{}_{}.netcdf.zip".format(item[2], item[0], item[1]) for item in all_combinations]

# get a list of files currnetly in the directory
file_list = os.listdir('./era5_land/')

# get a list of the all_combinations indexes
not_files_indexes = [index for index, item in enumerate(result_list) if item not in file_list]

#get only the missing combos
requested_combinations = [all_combinations[index] for index in not_files_indexes]

# run just for jan of 2024, to deal with the difference in time zones
jan24_combinations = list(itertools.product([str(max(yearly_list))], ['01'], var_to_collect))##########################################################################################
jan24_result_list = ["{}_{}_{}.netcdf.zip".format(item[2], item[0], item[1]) for item in jan24_combinations]
file_list = os.listdir('./era5_land/')
jan24_not_files_indexes = [index for index, item in enumerate(jan24_result_list) if item not in file_list]
jan24_requested_combinations = [jan24_combinations[index] for index in jan24_not_files_indexes]

num_threads = 19
all_results = []

# Define a function to process each task and append the results to the all_results list
def process_task(yl, ml, data_name):
    try:
        result = get_era5_data(yl, ml, al, era5_land_dir, data_name)
        return result
    except Exception as exc:
        print(f"Error processing data: {exc}")
        return None
                        
def process_era5():
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for yl, ml, data_name in requested_combinations:
            future = executor.submit(get_era5_data, yl, ml, al, data_name)
            futures.append(future)

        for future in futures:
            result = future.result()
            
def process_era5_1jan():
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for yl, ml, data_name in jan24_requested_combinations:
            future = executor.submit(get_era5_1st_jan, yl, al, data_name)
            futures.append(future)

        for future in futures:
            result = future.result()


process_era5()

process_era5_1jan()