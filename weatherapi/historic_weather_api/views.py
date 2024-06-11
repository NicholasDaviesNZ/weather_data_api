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
import re

# Create your views here.
@api_view(['GET'])
def test(request):
    """test function just for the user to check that the api is up"""
    dummy_data = {"message": "This is a test response!"}
    return Response(dummy_data)

def calculate_distance(x1, y1, x2, y2):
    """returns the distance between two points, is used for getting the 4 closest locations to the point"""
    return np.sqrt((x2 - x1)**2 + (y2 - y1)**2)

def get_closest_points_and_weights(coords_url, lat, lon):
    """from the csv file which holds the coordinates and loc_ids for the dataset, coords_url
    get a new dataframe, closest_df with the 4 closest points, and the weight from the inverse distance to those points from 
    the user defined lat and long, retuns dataframe with the locations and the weights for the sum"""
    coords_df = pd.read_csv(coords_url)
    
    coords_df['latitude'] = (coords_df['latitude']).astype(float)
    coords_df['longitude'] = (coords_df['longitude']).astype(float)

    p_lat = coords_df['latitude'][(coords_df['latitude'] - lat) >= 0].min()
    p_lon = coords_df['longitude'][(coords_df['longitude'] - lon) >= 0].min()
    n_lat = coords_df['latitude'][(coords_df['latitude'] - lat) < 0].max()
    n_lon = coords_df['longitude'][(coords_df['longitude'] - lon) < 0].max()

    points = [
        (p_lat, n_lon),  
        (p_lat, p_lon),  
        (n_lat, n_lon),  
        (n_lat, p_lon)   
    ]
    given_point = (lat, lon)
    closest_df = pd.merge(pd.DataFrame(points, columns=['latitude', 'longitude']), coords_df, on=['latitude', 'longitude'], how='left')
    

    closest_df['distances'] = closest_df.apply(lambda row: calculate_distance(given_point[0], given_point[1], row['latitude'], row['longitude']), axis=1)

    closest_df['weights'] = 1/closest_df['distances']
    closest_df['weights'] = closest_df['weights']/closest_df['weights'].sum()
    return(closest_df)


def get_single_varable_df(dataset_name, var_name, closest_df, start_datetime, end_datetime):
    """ for a given varable in the weather datastt var_name, load the values for the 4 closest locations
    filter them down to only include the user defined time range, merge them all to a single df, note that this is an inner merge,
    if one location is missing values, the time step will not be in the output. Finanly sum the weighted values (to get the IDW average)
    and reutrn a df which is only the time and the idw value. Note a polars dataframe is returned
    """
    
    file_0_name = os.path.join(settings.BASE_DIR, 'historic_weather_api', 'static', f'{dataset_name}', f"{var_name}_{int(closest_df.iloc[0]['loc_id'])}.parquet")
    file_1_name = os.path.join(settings.BASE_DIR, 'historic_weather_api', 'static', f'{dataset_name}', f"{var_name}_{int(closest_df.iloc[1]['loc_id'])}.parquet")
    file_2_name = os.path.join(settings.BASE_DIR, 'historic_weather_api', 'static', f'{dataset_name}', f"{var_name}_{int(closest_df.iloc[2]['loc_id'])}.parquet")
    file_3_name = os.path.join(settings.BASE_DIR, 'historic_weather_api', 'static', f'{dataset_name}', f"{var_name}_{int(closest_df.iloc[3]['loc_id'])}.parquet")
    
    loc_0 = pl.read_parquet(file_0_name)
    loc_1 = pl.read_parquet(file_1_name)
    loc_2 = pl.read_parquet(file_2_name)
    loc_3 = pl.read_parquet(file_3_name)

    # subset by date range
    loc_0 = loc_0.filter((pl.col("time") >= start_datetime) & (pl.col('time') <= end_datetime))
    
    loc_0 = loc_0.with_columns([(pl.col(var_name)*closest_df.iloc[0]['weights'])])
    loc_1 = loc_1.with_columns([(pl.col(var_name)*closest_df.iloc[1]['weights'])])
    loc_2 = loc_2.with_columns([(pl.col(var_name)*closest_df.iloc[2]['weights'])])
    loc_3 = loc_3.with_columns([(pl.col(var_name)*closest_df.iloc[3]['weights'])])

    merged_df = loc_0.join(loc_1, on='time', how='inner', suffix='_loc1') \
                 .join(loc_2, on='time', how='inner', suffix='_loc2') \
                 .join(loc_3, on='time', how='inner', suffix='_loc3')
                 
    merged_df = merged_df.with_columns([
        (pl.col(var_name) + pl.col(f'{var_name}_loc1') + pl.col(f'{var_name}_loc2') + pl.col(f'{var_name}_loc3')).alias('summed'), 
    ])
    merged_df = merged_df.select(['time','summed'])
    merged_df = merged_df.rename({"summed": var_name})
    
    return(merged_df)

def build_multi_var_df(var_names_list, dataset_name, closest_df, start_datetime, end_datetime):
    """ caller to get_single_varable_df for when there is multiple values, returns a polars dataframe
    which containes the timestamp and the values for each varable in a single df. Note that if a single varable
    is missing values, those timestamps will not be returned for any varables. 
    """
    cc = 0
    for var_name in var_names_list:
        df = get_single_varable_df(dataset_name, var_name, closest_df, start_datetime, end_datetime)
        if cc == 0:
            merged_df = df
        else:
            merged_df = merged_df.join(df, on='time', how='inner')
        cc +=1
    return(merged_df)

def run_standard_input_checks(request, valid_var_names):
    """ function to run the standard input checks from the users call to the api, 
    returns the user values, or an error, which ever is relevent for the call.  
    """
    lat = float(request.query_params.get('lat', None))
    lon = float(request.query_params.get('lon', None))
    var_names = request.query_params.get('var_name', None)
    start_date_str = request.query_params.get('start_date', None)
    end_date_str = request.query_params.get('end_date', None)
    
    if lat is None or lon is None:
        return Response({"error": "lat or lon parameter is missing."}, status=400)
    if var_names is None:
        return Response({"error": "var_name parameter is missing."}, status=400)
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        start_datetime = datetime.combine(start_date, time(0,0))
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        end_datetime = datetime.combine(end_date, time(23,0))
    except ValueError:
        return Response({"error": "Invalid date format. Dates must be in YYYY-MM-DD format."}, status=400)
    # Split the comma-separated var_name values into a list
    var_names_list = var_names.split(',')
    
    invalid_var_names = [var_name for var_name in var_names_list if var_name not in valid_var_names]
    if invalid_var_names:
        return Response({"error": f"Invalid var_name(s): {', '.join(invalid_var_names)}"}, status=400)
    return lat, lon, var_names_list, start_datetime, end_datetime

@api_view(['GET'])
def get_nasapower(request):
    """API function for the nasapower dataset, returns a json and a message to the user
    """
    dataset_name = 'nasapower' # the dataset which this function is resoncable for

    # the list of valid varable names for this dataset
    valid_var_names = [
        'temperature_2m', 'relative_humidity_2m', 
        'precipitation', 'snowfall', 'snow_depth', 'surface_pressure',
        'cloud_cover', 'wind_speed_10m', 'wind_direction_10m',
        'wind_speed_50m', 'wind_direction_50m'
    ]
    
    # call the function to run the input checks to make sure the request is valid
    input_check_response = run_standard_input_checks(request, valid_var_names)
    # return errors to the user if it fails the input checks
    if isinstance(input_check_response, Response):
        return input_check_response
    
    # if the input was valid unpack the valid inputs to their required varable names
    lat, lon, var_names_list, start_datetime, end_datetime = input_check_response
    
    # the url to the coordinates file for this dataset - holds the lat, long and loc_id values for this dataset
    coords_url = os.path.join(settings.BASE_DIR, 'historic_weather_api', 'static', 'coords', 'nz_coords_merra2.csv')
    # calls a function which gets the closest 4 locations to the users lat and long point, calculates the IDW for each 
    # of those points and reutns the lcoations and weights in a pandas df, closest_df
    closest_df = get_closest_points_and_weights(coords_url, lat, lon)
    # Given the 4 closest points and their weights, for every varable the user input, trim to the users date range and
    # merge into a single polars dataframe for retun to the user
    merged_df = build_multi_var_df(var_names_list, dataset_name, closest_df, start_datetime, end_datetime)
    # convert to json to pass it out
    data_json = merged_df.write_json(row_oriented=True)
    # a message for the user to check what they ahve done, is what they wanted to do
    message = f"This is a nasapower response for location: {lat},{lon} and the variable(s): {', '.join(var_names_list)} between {start_datetime} and {end_datetime}"
    return Response({"message": message, 'Data': data_json})