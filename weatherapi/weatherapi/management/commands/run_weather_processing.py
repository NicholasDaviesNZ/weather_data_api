from django.core.management.base import BaseCommand
from scripts_to_build_api_files.download_process_update import process_weather_data_task

class Command(BaseCommand):
    help = 'Run weather data processing tasks'

    def handle(self, *args, **kwargs):
        process_weather_data_task('era5_land')
        process_weather_data_task('era5')
        process_weather_data_task('nasapower')
        self.stdout.write(self.style.SUCCESS('Weather data processing tasks completed successfully.'))
