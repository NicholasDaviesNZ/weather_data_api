from django.urls import path
from . import views
from django.conf import settings

urlpatterns = [
    #path('historic/', views.getData),
    #path('post/', views.postData),
    path('test/', views.test),
]