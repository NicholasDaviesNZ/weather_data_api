from django.shortcuts import render
from rest_framework.response import Response
from rest_framework.decorators import api_view
from datetime import datetime
import pandas as pd
from django.conf import settings
import os
from django.templatetags.static import static
from scipy.spatial.distance import cdist
import numpy as np
import polars as pl

# Create your views here.
@api_view(['GET'])
def test(request):
    dummy_data = {"message": "This is a test response!"}
    return Response(dummy_data)


# need generic functions to check taht lat and long are valid
# to check that all var_names are in the accseptable list for that datasourse/view
# to check that start and end date are valid

def calculate_distance(x1, y1, x2, y2):
    return np.sqrt((x2 - x1)**2 + (y2 - y1)**2)

@api_view(['GET'])
def get_nasapower(request):
    # Get the location, var_name, start_date, and end_date query parameters from the request
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
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    except ValueError:
        return Response({"error": "Invalid date format. Dates must be in YYYY-MM-DD format."}, status=400)
    # Split the comma-separated var_name values into a list
    var_names_list = var_names.split(',')
    
    # Validate each var_name in the list
    valid_var_names = [
        'temperature_2m', 'relative_humidity_2m', 'dewpoint_temperature_2m',
        'precipitation', 'snowfall', 'snow_depth', 'surface_pressure',
        'cloud_cover', 'wind_speed_10m', 'wind_direction_10m',
        'wind_speed_50m', 'wind_direction_50m'
    ]
    
    invalid_var_names = [var_name for var_name in var_names_list if var_name not in valid_var_names]
    if invalid_var_names:
        return Response({"error": f"Invalid var_name(s): {', '.join(invalid_var_names)}"}, status=400)
    
    # Error checking around start and end date...
    
    # Create a response message including the location and var_names
    message = f"This is a nasapower response for location: {lat},{lon} and the variable(s): {', '.join(var_names_list)} between {start_date} and {end_date}"
    coords_url = os.path.join(settings.BASE_DIR, 'historic_weather_api', 'static', 'coords', 'nz_coords_merra2.csv')
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
    

    distances = closest_df.apply(lambda row: calculate_distance(given_point[0], given_point[1], row['latitude'], row['longitude']), axis=1)

    weights = 1 / distances
    weights = weights / weights.sum()
    print("Weights:", weights)
    
    file_0_name = os.path.join(settings.BASE_DIR, 'historic_weather_api', 'static', 'nasapower', f"{var_names}_{int(closest_df.iloc[0]['loc_id'])}.parquet")
    file_1_name = os.path.join(settings.BASE_DIR, 'historic_weather_api', 'static', 'nasapower', f"{var_names}_{int(closest_df.iloc[1]['loc_id'])}.parquet")
    file_2_name = os.path.join(settings.BASE_DIR, 'historic_weather_api', 'static', 'nasapower', f"{var_names}_{int(closest_df.iloc[2]['loc_id'])}.parquet")
    file_3_name = os.path.join(settings.BASE_DIR, 'historic_weather_api', 'static', 'nasapower', f"{var_names}_{int(closest_df.iloc[3]['loc_id'])}.parquet")
    
    loc_0 = pl.read_parquet(file_0_name)
    loc_1 = pl.read_parquet(file_1_name)
    loc_2 = pl.read_parquet(file_2_name)
    loc_3 = pl.read_parquet(file_3_name)

    # subset by date range
    
    loc_0 = loc_0.with_columns([(pl.col(var_names)*weights[0])])
    loc_1 = loc_1.with_columns([(pl.col(var_names)*weights[1])])
    loc_2 = loc_2.with_columns([(pl.col(var_names)*weights[2])])
    loc_3 = loc_3.with_columns([(pl.col(var_names)*weights[3])])

    merged_df = loc_0.join(loc_1, on='time', how='inner', suffix='_loc1') \
                 .join(loc_2, on='time', how='inner', suffix='_loc2') \
                 .join(loc_3, on='time', how='inner', suffix='_loc3')
                 
    merged_df = merged_df.with_columns([
        (pl.col('temperature_2m') + pl.col('temperature_2m_loc1') + pl.col('temperature_2m_loc2') + pl.col('temperature_2m_loc3')).alias('summed'), 
    ])
    merged_df = merged_df.select(['time','summed'])
    merged_df.rename({"summed": var_names})

    print(merged_df)

    # convert to json
    # pass out to user
    
    return Response({"message": message})