"""
Download the era5 raw nc data fro the area given.
"""
import cdsapi
import itertools
from concurrent.futures import ThreadPoolExecutor
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv() # load the env variables from the .env file, needs to have CDSAPI_URL and CDSAPI_KEY from your cds store account



def get_unique_months_years(start_date, end_date):    
    # Get unique months
    unique_months = set()
    current_date = start_date
    while current_date <= end_date:
        unique_months.add((current_date.month))
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)
    # Get unique years
    unique_years = set()
    current_date = start_date
    while current_date.year <= end_date.year:
        unique_years.add(current_date.year)
        current_date = current_date.replace(year=current_date.year + 1)
    
    # Convert sets to sorted lists
    unique_months = sorted(unique_months)
    unique_years = sorted(unique_years)
    
    return unique_months, unique_years


def get_era5_data(year, month, area, var_to_collect):
    c = cdsapi.Client()

    data = c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'format': 'netcdf',
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
        },
        f"{era5_dir}{var_to_collect}_{year}_{month}.nc")

def process_era5():
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for yl, ml, data_name in requested_combinations:
            future = executor.submit(get_era5_data, yl, ml, al, data_name)
            futures.append(future)

        for future in futures:
            result = future.result()



era5_dir = './scripts_to_build_api_files/era5_raw/'
num_threads = 19

start_date = datetime.strptime("2024-01-01", "%Y-%m-%d")
end_date = datetime.now()

al = [-33.3, 164.64, -47.24, 179.53]

monthly_list, yearly_list = get_unique_months_years(start_date, end_date)

if not os.path.exists(era5_dir):
    os.makedirs(era5_dir)

var_to_collect = ['10m_u_component_of_wind', '10m_v_component_of_wind','2m_dewpoint_temperature','2m_temperature','soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
                 'runoff','soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3','sub_surface_runoff', 'surface_runoff','surface_pressure','total_precipitation',
                  'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2', 'volumetric_soil_water_layer_3', 'cloud_base_height','evaporation','high_cloud_cover','medium_cloud_cover',
                  'low_cloud_cover','potential_evaporation','snow_depth','snowfall','soil_type','total_cloud_cover']

# https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land?tab=overview


# Assuming you have these lists defined: yearly_list, monthly_list, var_to_collect
all_combinations = list(itertools.product(yearly_list, monthly_list, var_to_collect))
# get the file names for all combo biles
result_list = ["{}_{}_{}.nc".format(item[2], item[0], item[1]) for item in all_combinations]

# get a list of files currnetly in the directory
file_list = os.listdir(era5_dir)
#file_list = []
# get a list of the all_combinations indexes
not_files_indexes = [index for index, item in enumerate(result_list) if item not in file_list]

#get only the missing combos
requested_combinations = [all_combinations[index] for index in not_files_indexes]


process_era5()



# def get_era5_1st_jan(year, area, var_to_collect):
#     c = cdsapi.Client(quiet=False,debug=True)

#     data = c.retrieve(
#         'reanalysis-era5-single-levels',
#         {
#             'product_type': 'reanalysis',
#             'format': 'netcdf',
#             'variable': var_to_collect,
#             'year': str(year),
#             'month': '01',
#             'day': [
#                 '01',
#             ],
#             'time': [
#                 '00:00', '01:00', '02:00',
#                 '03:00', '04:00', '05:00',
#                 '06:00', '07:00', '08:00',
#                 '09:00', '10:00', '11:00',
#                 '12:00', '13:00', '14:00',
#                 '15:00', '16:00', '17:00',
#                 '18:00', '19:00', '20:00',
#                 '21:00', '22:00', '23:00',
#             ],
#             'area': area,
#         },
#         f"{era5_dir}{var_to_collect}_{year}_01.nc")
    
    
# def process_era5_1jan():
#     with ThreadPoolExecutor(max_workers=num_threads) as executor:
#         futures = []
#         for yl, ml, data_name in jan24_requested_combinations:
#             future = executor.submit(get_era5_1st_jan, yl, al, data_name)
#             futures.append(future)

#         for future in futures:
#             result = future.result()
            
# # run just for jan of 2024, to deal with the difference in time zones
# jan24_combinations = list(itertools.product([str(max(yearly_list))], ['01'], var_to_collect))
# jan24_result_list = ["{}_{}_{}.nc".format(item[2], item[0], item[1]) for item in jan24_combinations]
# #file_list = os.listdir(od)
# file_list = []
# jan24_not_files_indexes = [index for index, item in enumerate(jan24_result_list) if item not in file_list]
# jan24_requested_combinations = [jan24_combinations[index] for index in jan24_not_files_indexes]

# process_era5_1jan()