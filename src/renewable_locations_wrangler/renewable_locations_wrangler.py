import polars as pl
import requests
import io
import plotly.express as px

import polars as pl
from datetime import datetime, timedelta

import logging
# Set logging level to info
logging.basicConfig(level=logging.INFO)

from pyproj import Transformer

class RenewableLocationsWrangler:
    
    def __init__(self, gov_uk_url=None):
        if gov_uk_url is None:
            # "https://assets.publishing.service.gov.uk/media/673b215249ce28002166a93e/repd-q3-oct-2024.csv"
            self.gov_uk_url = "https://assets.publishing.service.gov.uk/media/673b218149ce28002166a940/repd-q3-oct-2024.xlsx"
        else:
            self.gov_uk_url = gov_uk_url
            
    def get_renewable_locations(self):
    
        # Using piping to chain the methods together
        return (
            self.download_data() \
            .pipe(self.prune_and_rename_columns) \
            .pipe(self.select_only_operational_sites) \
            .pipe(self.select_only_wind_sites) \
            .pipe(self.convert_coordinates) \
            .pipe(self.drop_nulls_in_coordinates) \
            .pipe(self.drop_nulls_in_date_operational) \
            .pipe(self.fill_na_in_installed_capacity_mw)
        )
            
    def download_data(self):
        
        # Set up stream to download data
        response = requests.get(self.gov_uk_url)
        
        # Check if the request was successful
        if response.status_code != 200:
            raise ValueError(f"Failed to download data from {self.gov_uk_url}")
               
        # Convert string content to file-like object
        stream = io.BytesIO(response.content)
        
        # Read the CSV data into a Polars DataFrame
        df = pl.read_excel(stream, sheet_name="REPD")
        
        logging.info(f"Downloaded {len(df)} rows of data with the following column names:\n {df.columns}")
        
        return df

    def prune_and_rename_columns(self, df):
        
        # Column renaming dictionary:
        columns_to_rename = {
            'Ref ID': 'ref_id',
            'Operator (or Applicant)': 'operator',
            'Site Name': 'site_name',
            'Technology Type': 'technology_type',
            'Storage Type': 'storage_type',
            'Installed Capacity (MWelec)': 'installed_capacity_mw',
            'Turbine Capacity (MW)': 'turbine_capacity_mw',
            'No. of Turbines': 'number_of_turbines',
            'Height of Turbines (m)': 'turbine_height_m',
            'Development Status': 'development_status',
            'County': 'county',
            'Region': 'region',
            'Country': 'country',
            'Post Code': 'post_code',
            'X-coordinate': 'x_coordinate',
            'Y-coordinate': 'y_coordinate',
            'Operational': 'date_operational',
        }
        
        # Select only the columns we need
        df = df.select(columns_to_rename.keys())
        
        # Rename columns
        df = df.rename(columns_to_rename)
        
        return df
    
    def select_only_operational_sites(self, df):
        return df.filter(pl.col("development_status") == "Operational")
    
    def select_only_wind_sites(self, df):
        return df.filter(pl.col("technology_type").str.starts_with("Wind"))

    def convert_coordinates(self, df):
        
        transformer = Transformer.from_crs("epsg:27700", "epsg:4326")
        
        def transform_coords(x, y):
            lat, lon = transformer.transform(x, y)
            return lat, lon
        
        # Apply the transformation to each row
        coords = df.select(["x_coordinate", "y_coordinate"]).to_numpy()
        latitudes, longitudes = zip(*[transform_coords(x, y) for x, y in coords])
        
        # Add the new columns to the DataFrame
        df = df.with_columns([
            pl.Series("latitude", latitudes),
            pl.Series("longitude", longitudes)
        ])
        
        return df

    def drop_nulls_in_coordinates(self, df):
        return df.drop_nulls(subset=["x_coordinate", "y_coordinate"])

    def drop_nulls_in_date_operational(self, df):
        return df.drop_nulls(subset=["date_operational"])
   
    def fill_na_in_installed_capacity_mw(self, df: pl.DataFrame):
        df = df.with_columns([pl.col("installed_capacity_mw").fill_null(0)])
        return df
    
    def plot_locations(self, df, color_column="technology_type"):
    
        # Calculate the mean latitude and longitude for centering the map
        center_lat = df.select(pl.col("latitude").mean()).item()
        center_lon = df.select(pl.col("longitude").mean()).item()
        print(center_lat, center_lon)
        
        fig = px.scatter_mapbox(
            df,
            lat="latitude",
            lon="longitude",
            color=color_column,
            hover_name="site_name",
            mapbox_style="carto-positron",
            zoom=4,
            title="Renewable Energy Sites",
            center={"lat": center_lat, "lon": center_lon},
            size="installed_capacity_mw",
        )
        
        fig.update_layout(
            autosize=True,
            width=1200,
            height=800
        )
        
        return fig

    def compute_cumulative_installed_capacity(self, df):

        # Group by date_operational and sum the installed_capacity_mw
        df = df.group_by('date_operational').agg(pl.sum('installed_capacity_mw').alias('daily_installed_capacity_mw'))
        
        # Rename the date_operational column to date
        df = df.rename({'date_operational': 'date'})

        # Sort the DataFrame by the date_operational column
        df = df.sort('date')
        
        # Compute the cumulative sum of the daily_installed_capacity_mw column
        df = df.with_columns([pl.col('daily_installed_capacity_mw').cum_sum().alias('cumulative_installed_capacity_mw')])
        
        # Generate a date range from the earliest date_operational to today's date
        start_date = df['date'].min()
        end_date = datetime.today().date()
        date_range = pl.date_range(start_date, end_date, interval='1d', eager=True)
        date_range_dataframe = pl.DataFrame({'date': date_range})
        
        # Join the date range with the cumulative capacity DataFrame
        result_df = date_range_dataframe.join(df, on='date', how='left')
        
        # Fill forward the cumulative_installed_capacity_mw column to fill missing values
        result_df = result_df.with_columns([
            pl.col('cumulative_installed_capacity_mw').fill_null(strategy='forward')
        ])
        
        # Drop the daily_installed_capacity_mw column
        result_df = result_df.drop('daily_installed_capacity_mw')
        
        return result_df

    def plot_cumulative_installed_capacity(self, df):
    
        fig = px.line(
            df,
            x='date',
            y='cumulative_installed_capacity_mw',
            title='Cumulative Installed Capacity (MW) Over Time'
        )
        
        fig.update_layout(
            autosize=True,
            width=1200,
            height=800
        )
        
        return fig