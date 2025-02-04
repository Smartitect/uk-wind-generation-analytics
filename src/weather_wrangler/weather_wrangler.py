import datetime
import pandas as pd
import arrow
import requests


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
    
    def __init__(self):

        self.api_key = (
            r"f0ad46d0-e431-11ec-a296-0242ac130002-f0ad473e-e431-11ec-a296-0242ac130002"
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

