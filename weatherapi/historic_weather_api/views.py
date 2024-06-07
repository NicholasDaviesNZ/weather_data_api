from django.shortcuts import render
from rest_framework.response import Response
from rest_framework.decorators import api_view
from datetime import datetime


# Create your views here.
@api_view(['GET'])
def test(request):
    dummy_data = {"message": "This is a test response!"}
    return Response(dummy_data)


# need generic functions to check taht lat and long are valid
# to check that all var_names are in the accseptable list for that datasourse/view
# to check that start and end date are valid


@api_view(['GET'])
def get_nasapower(request):
    # Get the location, var_name, start_date, and end_date query parameters from the request
    lat = request.query_params.get('lat', None)
    lon = request.query_params.get('lon', None)
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
    
    # Return the response
    return Response({"message": message})