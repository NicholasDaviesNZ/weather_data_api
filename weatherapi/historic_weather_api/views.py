from django.shortcuts import render
from rest_framework.response import Response
from rest_framework.decorators import api_view

# Create your views here.
@api_view(['GET'])
def test(request):
    dummy_data = {"message": "This is a test response!"}
    return Response(dummy_data)