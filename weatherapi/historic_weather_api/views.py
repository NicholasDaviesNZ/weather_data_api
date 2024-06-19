from django.shortcuts import render
from rest_framework.response import Response
from rest_framework.decorators import api_view
from datetime import datetime, time
import pandas as pd
from django.conf import settings
import os
from django.templatetags.static import static
from scipy.spatial.distance import cdist
import numpy as np
import polars as pl
from scipy.spatial import cKDTree
import time as timer
from concurrent.futures import ThreadPoolExecutor

coords_df_np = pd.read_csv(os.path.join(settings.BASE_DIR, 'historic_weather_api', 'static', 'coords', 'nz_coords_merra2.csv'))
tree_np = cKDTree(coords_df_np[['latitude', 'longitude']])

coords_df_era5 = pd.read_csv(os.path.join(settings.BASE_DIR, 'historic_weather_api', 'static', 'coords', 'nz_coords_era5_proper.csv'))
tree_era5 = cKDTree(coords_df_era5[['latitude', 'longitude']])

coords_df_era5_land = pd.read_csv(os.path.join(settings.BASE_DIR, 'historic_weather_api', 'static', 'coords', 'nz_coords_era5_land.csv'))
tree_era5_land = cKDTree(coords_df_era5_land[['latitude', 'longitude']])

coords_df_fenz = pd.read_csv(os.path.join(settings.BASE_DIR, 'historic_weather_api', 'static', 'coords', 'nz_coords_FENZ.csv'))
tree_fenz = cKDTree(coords_df_fenz[['latitude', 'longitude']])

def get_date_ranges_on_start(data_source):
    hist_file_name = os.path.join(settings.BASE_DIR,'historic_weather_api','static', 'historic', f'{data_source}',"temperature_2m_0.parquet")
    hist_df = pd.read_parquet(hist_file_name)['time']
    dates_dict = {
        'hist_min':min(hist_df),
        'hist_max':max(hist_df),
    }
    
    cur_file_name = os.path.join(settings.BASE_DIR,'historic_weather_api','static', 'current', f'{data_source}',"temperature_2m_0.parquet")
    if os.path.exists(cur_file_name):
        cur_df = pd.read_parquet(cur_file_name)['time']
    
        dates_dict['cur_min'] = min(cur_df)
        dates_dict['cur_max'] = max(cur_df)

    return(dates_dict)
    
minmax_dates = {
    'nasapower':get_date_ranges_on_start('nasapower'),
    'era5':get_date_ranges_on_start('era5'),
    'era5_land':get_date_ranges_on_start('era5_land'),
    'fenz':get_date_ranges_on_start('fenz'),
    }

# should add a snap IDW option into here to only return 1 or return 4
def get_closest_points_and_weights(coords_df, tree, lat, lon):
    """from the csv file which holds the coordinates and loc_ids for the dataset, coords_url
    get a new dataframe, closest_df with the 4 closest points, and the weight from the inverse distance to those points from 
    the user defined lat and long, retuns dataframe with the locations and the weights for the sum"""
    
    # coords_df['latitude'] = (coords_df['latitude']).astype(float)
    # coords_df['longitude'] = (coords_df['longitude']).astype(float)
    # coords_df['loc_id'] = (coords_df['loc_id']).astype(int)

    # this is a sort tree from scipy spectral to get make finding the distance from the users point to the nearest locations in the coords file
    dist, indices = tree.query([(lat, lon)], k=len(coords_df), workers=-1)
    dist = dist.flatten()
    closest_df = coords_df.iloc[indices.flatten()]
    closest_df.insert(len(closest_df.columns), 'dist', dist)
    closest_df = closest_df[closest_df.dist < 1] # out an uppder bound on how much we can realalisticly keep

    return closest_df 

def process_file(file_name, start_datetime,end_datetime):
    df = pl.read_parquet(file_name).select(pl.all().exclude("^__index_level_.*$"))
    df = df.with_columns(pl.col('time').dt.cast_time_unit('ns'))
    
    df = df.filter((pl.col("time") >= start_datetime) & (pl.col('time') <= end_datetime))
    return df

def get_single_variable_df(data_source, var_name, closest_df, start_datetime, end_datetime, interp_mode):
    """ for a given variable in the weather datastt var_name, load the values for the 4 closest locations
    filter them down to only include the user defined time range, merge them all to a single df, note that this is an inner merge,
    if one location is missing values, the time step will not be in the output. Finanly sum the weighted values (to get the IDW average)
    and reutrn a df which is only the time and the idw value. Note a polars dataframe is returned
    """

    rows_to_remove = []
    for index, row in closest_df.iterrows():
        if not os.path.exists(os.path.join(settings.BASE_DIR, 'historic_weather_api', 'static', 'historic', f'{data_source}', f"{var_name}_{int(row['loc_id'])}.parquet")):
            rows_to_remove.append(index)
            
            
    closest_df = closest_df.drop(rows_to_remove)

    if interp_mode.lower() == 'idw':
        closest_df = closest_df.head(4)
        weights = 1 / closest_df['dist']
        weights_prop = weights/weights.sum()
        closest_df.insert(len(closest_df.columns), 'weights', weights_prop)
    else:
        closest_df = closest_df.head(1)
        closest_df.insert(len(closest_df.columns), 'weights', 1) 
    
    file_names_hist = []
    file_names_cur = []
    if (minmax_dates[data_source].get('hist_max') is not None) and (start_datetime <= minmax_dates[data_source].get('hist_max')):
        for i in range(len(closest_df)):
            file_name = os.path.join(settings.BASE_DIR,'historic_weather_api','static', 'historic', f'{data_source}',f"{var_name}_{int(closest_df.iloc[i]['loc_id'])}.parquet")
            file_names_hist.append(file_name)
        if (minmax_dates[data_source].get('cur_min') is not None) and (end_datetime >= minmax_dates[data_source].get('cur_min')):
            for i in range(len(closest_df)):
                file_name = os.path.join(settings.BASE_DIR,'historic_weather_api','static', 'current', f'{data_source}',f"{var_name}_{int(closest_df.iloc[i]['loc_id'])}.parquet")
                file_names_cur.append(file_name)
    elif (minmax_dates[data_source].get('cur_min') is not None) and (start_datetime >= minmax_dates[data_source].get('cur_min')):
        for i in range(len(closest_df)):
            file_name = os.path.join(settings.BASE_DIR,'historic_weather_api','static', 'current', f'{data_source}',f"{var_name}_{int(closest_df.iloc[i]['loc_id'])}.parquet")
            file_names_cur.append(file_name)
        

    # if there are no files that can be loaded, return an empty dataframe to the calling function
    if not file_names_hist and not file_names_cur:
        return(pl.DataFrame(schema={'time': pl.Datetime, var_name: pl.Float64}))
    
    
    # if there are no files that can be loaded, return an empty dataframe to the calling function
    if file_names_hist:
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_file, file_name, start_datetime, end_datetime) for i, file_name in enumerate(file_names_hist)]
            results_hist = [future.result() for future in futures]

        for i, df in enumerate(results_hist):
            weight = closest_df.iloc[i]['weights']
            results_hist[i] = df.with_columns([(pl.col(var_name) * weight).alias(var_name)])

        merged_hist_df = results_hist[0]
        
        for i, df in enumerate(results_hist[1:], start=1):
            # Join and sum the weighted var_name columns
            merged_hist_df = merged_hist_df.join(df, on='time', how='inner')
            merged_hist_df = merged_hist_df.with_columns([(pl.col(var_name) + pl.col(f'{var_name}_right')).alias(var_name)])
            merged_hist_df = merged_hist_df.drop(f"{var_name}_right")
        if not file_names_cur:
            return(merged_hist_df)
        
    if file_names_cur:
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_file, file_name, start_datetime, end_datetime) for i, file_name in enumerate(file_names_cur)]
            results_cur = [future.result() for future in futures]

        for i, df in enumerate(results_cur):
            weight = closest_df.iloc[i]['weights']
            results_cur[i] = df.with_columns([(pl.col(var_name) * weight).alias(var_name)])

        merged_cur_df = results_cur[0]

        for i, df in enumerate(results_cur[1:], start=1):
            # Join and sum the weighted var_name columns
            merged_cur_df = merged_cur_df.join(df, on='time', how='inner')
            merged_cur_df = merged_cur_df.with_columns([(pl.col(var_name) + pl.col(f'{var_name}_right')).alias(var_name)])
            merged_cur_df = merged_cur_df.drop(f"{var_name}_right")
        if not file_names_hist:
            return(merged_cur_df)
    merged_df = pl.concat([merged_hist_df, merged_cur_df], how="vertical")
    merged_df = merged_df.unique(subset=["time"], keep="first")
    return(merged_df)
        
def build_multi_var_df(var_names_list, data_source, closest_df, start_datetime, end_datetime, interp_mode):
    """ caller to get_single_variable_df for when there is multiple values, returns a polars dataframe
    which containes the timestamp and the values for each variable in a single df. Note that if a single variable
    is missing values, those timestamps will not be returned for any variables. 
    """
    cc = 0
    for var_name in var_names_list:
        df = get_single_variable_df(data_source, var_name, closest_df, start_datetime, end_datetime, interp_mode)
        if cc == 0:
            merged_df = df
        else:
            merged_df = merged_df.join(df, on='time', how='inner')
        cc +=1
        
    return(merged_df)

def run_standard_input_checks(request):
    """ function to run the standard input checks from the users call to the api, 
    returns the user values, or an error, which ever is relevent for the call.  
    """
    data_source = request.query_params.get('data_source', None)
    
    if data_source is not None:
        data_source = data_source.lower()
    else:
        return Response({"error": "data_source is not defined, data_source must be nasapower, era5 or era5_land"}, status=400)
    if data_source not in ['nasapower', 'era5', 'era5_land', 'fenz']:
        return Response({"error": "data_source must be nasapower, era5 or era5_land"}, status=400)
    
    lat = request.query_params.get('lat', None)
    lon = request.query_params.get('lon', None)
    if lat is None or lon is None:
        return Response({"error": "lat or lon parameter is missing."}, status=400)
    try:
        lat = float(lat)
        lon = float(lon)
    except ValueError:
        return Response({"error": "lat and lon must be valid numbers."}, status=400)
    
    # not sure of a better way to do this, but if the function routing is changed in the view func then this needs updateing
    if data_source == 'nasapower':
        valid_var_names = [
            'temperature_2m', 'relative_humidity_2m', 
            'precipitation', 'snowfall', 'snow_depth', 'surface_pressure',
            'cloud_cover', 'wind_speed_10m', 'wind_direction_10m',
            'wind_speed_50m', 'wind_direction_50m'
            ]
    elif data_source == 'era5':
        valid_var_names = [
            '10m_u_component_of_wind', '10m_v_component_of_wind','dewpoint_temperature_2m','temperature_2m','soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
            'runoff','soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3','sub_surface_runoff', 'surface_runoff','surface_pressure','total_precipitation',
            'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2', 'volumetric_soil_water_layer_3', 'cloud_base_height','evaporation','high_cloud_cover','medium_cloud_cover',
            'low_cloud_cover','potential_evaporation','snow_depth','snowfall','soil_type','total_cloud_cover'
            ]
    elif data_source == 'era5_land':
        valid_var_names = [
            'evaporation_from_bare_soil', 'evaporation_from_open_water_surfaces_excluding_oceans', 'evaporation_from_the_top_of_canopy',
            'evaporation_from_vegetation_transpiration', 'potential_evaporation', 'runoff',
            'soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
            'sub_surface_runoff', 'surface_runoff', 'total_evaporation',
            'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2', 'volumetric_soil_water_layer_3', 
            '10m_u_component_of_wind', '10m_v_component_of_wind', 'dewpoint_temperature_2m',
            'temperature_2m', 'snow_depth', 'snowfall',
            'surface_pressure', 'precipitation'
            ]
    elif data_source == 'fenz':
        valid_var_names = [
            'precipitation', 'relative_humidity_2m','soil_temperature_level_2','temperature_2m','volumetric_soil_water_layer_2','wind_direction_2m','wind_speed_2m'
        ]
    else: 
        return Response({"error": "data_source has no valid variable names, check you call is correct"}, status=400)
    
    var_names = request.query_params.get('var_name', None)
    if var_names is None:
        return Response({"error": "var_name parameter is missing."}, status=400)
    var_names_list = var_names.split(',')
    invalid_var_names = [var_name for var_name in var_names_list if var_name not in valid_var_names]
    if invalid_var_names:
        return Response({"error": f"Invalid var_name(s): {', '.join(invalid_var_names)}"}, status=400)
    
    start_date_str = request.query_params.get('start_date', None)
    end_date_str = request.query_params.get('end_date', None)
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        start_datetime = datetime.combine(start_date, time(0,0))
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        end_datetime = datetime.combine(end_date, time(23,0))
    except ValueError:
        return Response({"error": "Invalid date format. Dates must be in YYYY-MM-DD format. Check your date formatting and that you are not requesting a date which does not exist (eg 31st in a month with 30 days)"}, status=400)
    
    interp_mode = request.query_params.get('interp_mode', None)
    if interp_mode is None or interp_mode.lower() != 'snap':
        interp_mode = 'IDW'

    return data_source, lat, lon, var_names_list, start_datetime, end_datetime, interp_mode

def get_nasapower(lat, lon, interp_mode, var_names_list, data_source, start_datetime, end_datetime):    
    # calls a function which gets the closest 4 locations to the users lat and long point, calculates the IDW for each 
    # of those points and reutns the lcoations and weights in a pandas df, closest_df
    closest_df = get_closest_points_and_weights(coords_df_np, tree_np, lat, lon)
   
    # Given the 4 closest points and their weights, for every variable the user input, trim to the users date range and
    # merge into a single polars dataframe for retun to the user
    merged_df = build_multi_var_df(var_names_list, data_source, closest_df, start_datetime, end_datetime, interp_mode)
    return(merged_df)

def get_era5(lat, lon, interp_mode, var_names_list, data_source, start_datetime, end_datetime):
    closest_df = get_closest_points_and_weights(coords_df_era5, tree_era5, lat, lon)
    merged_df = build_multi_var_df(var_names_list, data_source, closest_df, start_datetime, end_datetime, interp_mode)
    return(merged_df)

def get_era5_land(lat, lon, interp_mode, var_names_list, data_source, start_datetime, end_datetime):
    closest_df = get_closest_points_and_weights(coords_df_era5_land, tree_era5_land, lat, lon)
    merged_df = build_multi_var_df(var_names_list, data_source, closest_df, start_datetime, end_datetime, interp_mode)
    return(merged_df)

def get_fenz(lat, lon, interp_mode, var_names_list, data_source, start_datetime, end_datetime):
    closest_df = get_closest_points_and_weights(coords_df_fenz, tree_fenz, lat, lon)
    merged_df = build_multi_var_df(var_names_list, data_source, closest_df, start_datetime, end_datetime, interp_mode)
    return(merged_df)
    

@api_view(['GET'])
def test(request):
    """test function just for the user to check that the api is up"""
    dummy_data = {"message": "This is a test response!"}
    return Response(dummy_data)


@api_view(['GET'])
def get_data(request):
    
    """API function for the dataset assocated with the user input variables, returns a json and a message to the user
    """
    
    # call the function to run the input checks to make sure the request is valid
    input_check_response = run_standard_input_checks(request)
    
    # return errors to the user if it fails the input checks
    if isinstance(input_check_response, Response):
        return input_check_response
    
    # if the input was valid unpack the valid inputs to their required variable names
    data_source, lat, lon, var_names_list, start_datetime, end_datetime, interp_mode = input_check_response

    # if you change this routeing then you need to update the error checking in run_standard_input_checks
    if data_source == 'nasapower':
        merged_df = get_nasapower(lat, lon, interp_mode, var_names_list, data_source, start_datetime, end_datetime)
    elif data_source == 'era5':
        merged_df = get_era5(lat, lon, interp_mode, var_names_list, data_source, start_datetime, end_datetime)
    elif data_source == 'era5_land':
        merged_df = get_era5_land(lat, lon, interp_mode, var_names_list, data_source, start_datetime, end_datetime)
    elif data_source == 'fenz':
        merged_df = get_fenz(lat, lon, interp_mode, var_names_list, data_source, start_datetime, end_datetime)
    else:
        return Response({"error": "data_source not recognised"}, status=400)
    
    # check if what was returned from the data_source specific function was a responce error, and if it was return it to the user
    if isinstance(merged_df, Response):
        return merged_df

    if merged_df.is_empty():
        return Response({"error": "the data_source exists, but there are no or not enough nearby values for your varables within your date range, try using snap, reducing the varables requested or increasing the date range"}, status=400)
    
    merged_df = merged_df.sort("time")
    # convert to json to pass it out
    data_json = merged_df.write_json(row_oriented=True)

    # a message for the user to check what they ahve done, is what they wanted to do
    message = f"This is a {data_source} response for location: {lat},{lon} and the variable(s): {', '.join(var_names_list)} between {start_datetime} and {end_datetime} using mode {interp_mode}"
    return Response({"message": message, 'Data': data_json})