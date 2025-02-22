import datetime
import pandas as pd
import arrow
import requests

import yaml
from pathlib import Path


class WeatherWrangler:
    
    FORECAST_PARAMETERS = [
            "airTemperature",
            "pressure",
            "windSpeed",
            "windDirection",
            "currentSpeed",
            "currentDirection",
            "waveDirection",
            "waveHeight",
            "wavePeriod",
        ]
    
    HISTORICAL_PARAMETERS = [
            "airTemperature",
            "pressure",
            "windSpeed",
            "windDirection",
        ]
    
    CONFIG_FILE = "config.yaml"
    
    def __init__(self):

        # Check config file exists
        if not Path(self.CONFIG_FILE).is_file():
            raise FileNotFoundError("Config file not found. Please create a config file with the api_key")
        
        # Store api_key in a local config.yaml file and load it from their during initialization
        with open(self.CONFIG_FILE, "r") as file:
            self.config = yaml.safe_load(file)
        
        self.api_key = (
            self.config["stormglass_weather"]["api_key"]
        )

    def get_historical_weather_data(self, latitude, longitude, start_date, end_date):
        
        # Convert start and end date string in format "YYYY-MM-DD" to datetime object
        start_time = arrow.get(start_date, "YYYY-MM-DD")
        end_time = arrow.get(end_date, "YYYY-MM-DD")
        
        response = self.make_request(latitude, longitude, start_time, end_time)
                     
        parsed_response = self.parse_stormglass_response(response, self.HISTORICAL_PARAMETERS)
        
        return pd.DataFrame.from_records(parsed_response)

    def make_request(self, latitude, longitude, start_time, end_time):
        
        response = requests.get(
            "https://api.stormglass.io/v2/weather/point",
            params={
                "lat": latitude,
                "lng": longitude,
                "params": ",".join(self.HISTORICAL_PARAMETERS),
                "start": start_time.to(
                    "UTC"
                ).timestamp(),  # Convert to UTC timestamp
                "end": end_time.to(
                    "UTC"
                ).timestamp(),  # Convert to UTC timestamp
            },
            headers={"Authorization": self.api_key},
        )

        # Do something with response data.
        json_data = response.json()

        return json_data


    def parse_stormglass_response(self, response, parameters):

        stormglass_metrics = []

        for hour in response["hours"]:

            hour_metrics = {}
            
            hour_metrics["time"] = datetime.datetime.strptime(
                hour["time"], "%Y-%m-%dT%H:%M:%S+00:00"
            )

            for key in parameters:

                if key in hour:
                    hour_metrics[key] = hour[key]["sg"]
                else:
                    raise ValueError("Key %s is missing from the ")

            hour_metrics["timestamp"] = datetime.datetime.now()
            stormglass_metrics.append(hour_metrics)

        return stormglass_metrics

    def get_weather_forecast(self, latitude, longitude, hours_in_future):

        datetime_hours_in_future = arrow.utcnow().shift(hours=+hours_in_future)

        return self.get_weather_forecast_between_times(latitude, longitude, datetime_hours_in_future, datetime_hours_in_future)

    def get_weather_forecast_between_times(self, latitude, longitude, start_time, end_time):

        response = requests.get(
            "https://api.stormglass.io/v2/weather/point",
            params={
                "lat": latitude,
                "lng": longitude,
                "params": ",".join(self.FORECAST_PARAMETERS),
                "start": start_time.to(
                    "UTC"
                ).timestamp(),  # Convert to UTC timestamp
                "end": end_time.to(
                    "UTC"
                ).timestamp(),  # Convert to UTC timestamp
                "source": "sg",
            },
            headers={"Authorization": self.api_key},
        )

        # Do something with response data.
        json_data = response.json()

        return json_data

