import requests
from datetime import datetime, timedelta, timezone

import plotly.express as px
import plotly.graph_objects as go

import polars as pl

import logging
logging.basicConfig(level=logging.INFO)

class HistoricGenerationWrangler:
    
    fuel_type_mapping = pl.DataFrame(
        [
            {"fuelType": 'INTGRNL', "group": "Interconnector", "name": "greenlink"},
            {"fuelType": 'INTEW', "group": "Interconnector", "name": "ewic"},
            {"fuelType": 'BIOMASS', "group": "Renewable", "name": "Biomass"},
            {"fuelType": 'INTELEC', "group": "Interconnector", "name": "eleclink"},
            {"fuelType": 'CCGT', "group": "Fossil Fuel", "name": "CCGT"},
            {"fuelType": 'INTNSL', "group": "Interconnector", "name": "nsl"},
            {"fuelType": 'INTIFA2', "group": "Interconnector", "name": "ifa2"},
            {"fuelType": 'WIND', "group": "Renewable", "name": "Wind"},
            {"fuelType": 'OCGT', "group": "Fossil Fuel", "name": "OCGT"},
            {"fuelType": 'INTNEM', "group": "Interconnector", "name": "nemo"},
            {"fuelType": 'NUCLEAR', "group": "Nuclear", "name": "Nuclear"},
            {"fuelType": 'OIL', "group": "Fossil Fuel", "name": "Oil"},
            {"fuelType": 'INTIRL', "group": "Interconnector", "name": "moyle"},
            {"fuelType": 'PS', "group": "Renewable", "name": "Pumped Storage"},
            {"fuelType": 'OTHER', "group": "Other", "name": "Other"},
            {"fuelType": 'NPSHYD', "group": "Renewable", "name": "Hydro"},
            {"fuelType": 'INTVKL', "group": "Interconnector", "name": "viking"},
            {"fuelType": 'INTNED', "group": "Interconnector", "name": "britned"},
            {"fuelType": 'COAL', "group": "Fossil Fuel", "name": "Coal"},
            {"fuelType": 'INTFR', "group": "Interconnector", "name": "france"},
        ]
    )
    
    def __init__(self, elexon_url=None):
        if elexon_url is None:
            self.elexon_url = 'https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELINST/stream'
        else:
            self.elexon_url = url
    
    def get_generation_data(self, number_of_days=7):
        return (
            self.download_data(number_of_days) \
            .pipe(self.join_fuel_type, self.fuel_type_mapping) \
            .pipe(self.calculate_percentage_of_total_generation) \
            .pipe(self.convert_settlement_date_to_date) \
            .pipe(self.exclude_interconnectors)
            .pipe(self.exclude_pump_storage)
        )
    
    def download_data(self, number_of_days):
        publish_date_time_from = (datetime.now(timezone.utc) - timedelta(days=number_of_days)).strftime('%Y-%m-%dT%H:%M:%SZ')
        publish_date_time_to = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        request_url = f'{self.elexon_url}?publishDateTimeFrom={publish_date_time_from}&publishDateTimeTo={publish_date_time_to}'
        logging.info(f"Downloading data from {request_url}")
        response = requests.get(request_url)

        if response.status_code != 200:
            raise Exception(f"Failed to download data: {response.status_code} - {response.text}")
        return pl.DataFrame(response.json())

    def join_fuel_type(self, generation_data, fuel_type_mapping):
        return generation_data.join(fuel_type_mapping, on='fuelType')
    
    def calculate_percentage_of_total_generation(self, generation_data):
        # Apply a window function to calculate the total generation for each group of startTime, settlementDate, settlementPeriod, group and name
        generation_data = generation_data.with_columns(
            [
                pl.col('generation').sum().over(['publishTime', 'startTime', 'settlementDate', 'settlementPeriod']).alias('total_generation')
            ]
            
        )
        
        # Calculate the percentage of total generation
        generation_data = generation_data.with_columns(
            [
                (pl.col('generation') / pl.col('total_generation')).alias('percentage_of_total_generation')
            ]
        )
    
        return generation_data
    
    def convert_settlement_date_to_date(self, generation_data):
        return generation_data.with_columns(
            [
                pl.col('settlementDate').str.strptime(pl.Date, "%Y-%m-%d")
            ]
        )
    
    def exclude_interconnectors(self, generation_data):
        return generation_data.filter(pl.col('group') != 'Interconnector')

    def exclude_pump_storage(self, generation_data):
        return generation_data.filter(pl.col('name') != 'Pumped Storage')
    
    def plot_generation_data(self, generation_data):
        
        # Convert startTime to datetime
        generation_data = generation_data.with_columns(
            [
                pl.col("startTime").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ")
            ]
        )

        # Create the plot
        # Define the color sequence and category order
        
        color_sequence = [
            '#808080',  # grey (Nuclear)
            '#0000FF',  # blue (Hydro)
            '#ADD8E6',  # light blue (Pumped Storage)
            '#FFA500',  # orange (Biomass)
            '#FFFF00',  # yellow (Other)
            '#FFC0CB',  # pink (OCGT)
            '#FFA07A',  # light red (Coal)
            '#8B0000',  # dark red (Oil)
            '#FF0000',  # red (CCGT)
            '#008000'   # green (Wind)
        ]

        category_order = [
            'Nuclear',
            'Hydro', 'Pumped Storage',
            'Biomass', 'Other',
            'OCGT', 'Coal', 'Oil', 'CCGT',
            'Wind'
        ]

        fig = px.area(
            generation_data,
            x='startTime',
            y='generation',
            color='name',
            title='Generation Data by Fuel Type',
            line_group='group',
            groupnorm='fraction',
            category_orders={'name': category_order},
            color_discrete_sequence=color_sequence
        )
        
        fig.update_layout(
            autosize=True,
            width=1200,
            height=800
        )
        
        return fig
    
    def aggregate_generation_data_by_settlement_date_and_fuel_type(self, generation_data):
            
        # Group by group and name, return max, median and min of generation
        return generation_data.group_by(['settlementDate', 'group', 'name']).agg([
            pl.col('generation').max().alias('max_generation'),
            pl.col('generation').median().alias('median_generation'),
            pl.col('generation').min().alias('min_generation'),
            pl.col('percentage_of_total_generation').min().alias('min_percentage_of_total_generation'),
            pl.col('percentage_of_total_generation').max().alias('max_percentage_of_total_generation'),
            pl.col('percentage_of_total_generation').median().alias('median_percentage_of_total_generation'),
            pl.len().alias('row_count')
        ])

    def plot_aggregated_generation_data(self, aggregated_generation_data):
        
        # Filter to just wind, CCGT and nuclear
        aggregated_generation_data = aggregated_generation_data.filter(pl.col('name').is_in(['Wind', 'CCGT', 'Nuclear']))
        
        aggregated_generation_data = aggregated_generation_data.sort('settlementDate')
        
        # Plot a line for max, median and min generation for each fuel type, make the max a thick sold line, the median a dashed line and the min a thin solid line.
        # Make the line colour different for each fuel type.

        fig = go.Figure()

        fuel_types = ['Wind', 'CCGT', 'Nuclear']
        colors = {'Wind': 'green', 'CCGT': 'red', 'Nuclear': 'blue'}
        line_styles = {'max_generation': 'solid', 'median_generation': 'dash', 'min_generation': 'solid'}
        line_widths = {'max_generation': 4, 'median_generation': 2, 'min_generation': 1}

        for fuel in fuel_types:
            for gen_type in ['max_generation', 'median_generation', 'min_generation']:
                fig.add_trace(go.Scatter(
                    x=aggregated_generation_data.filter(pl.col('name') == fuel)['settlementDate'],
                    y=aggregated_generation_data.filter(pl.col('name') == fuel)[gen_type],
                    mode='lines',
                    name=f'{fuel} {gen_type}',
                    line=dict(color=colors[fuel], width=line_widths[gen_type], dash=line_styles[gen_type])
                ))

        fig.update_layout(
            title='Aggregated Generation Data by Fuel Type',
            xaxis_title='Settlement Date',
            yaxis_title='Generation',
            legend_title='Fuel Type and Generation Type',
            autosize=True,
            width=1200,
            height=800
        )
        
        return fig