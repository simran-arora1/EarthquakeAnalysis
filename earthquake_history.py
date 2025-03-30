import time
import requests
import pandas as pd
import numpy as np
import io
import boto3
import base64
import random
import json
import awswrangler as wr
from decimal import Decimal
from datetime import datetime
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
from botocore.exceptions import ClientError

# USGS Earthquake API Endpoint
USGS_API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

# DynamoDB config
REGION = "us-east-1"
TABLE_NAME = "earthquakes"

dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)

# Function to fetch historical Earthquake Data from USGS API
def fetch_historical_earthquake_data(start_time="2024-01-01", end_time="2024-12-31", additional_params=None):
    params = {
        "starttime": start_time,
        "endtime": end_time,
    }
    if additional_params != None: params.update(additional_params)
    params['format'] = "geojson"
    print(params)
    response = requests.get(USGS_API_URL, params=params)
    
    if response.status_code == 200:
        print(f" Successfully retrieved data: {response.status_code}")

        return response.json()
    else:
        print(f" Failed to retrieve data: {response.status_code}")
        return None


def clean_data(json_data):
    df = pd.json_normalize(json_data["features"])

    # Extract longitude, latitude, depth
    df["longitude"] = df["geometry.coordinates"].apply(lambda x: x[0] if isinstance(x, list) else None)
    df["latitude"] = df["geometry.coordinates"].apply(lambda x: x[1] if isinstance(x, list) else None)
    df["depth_km"] = df["geometry.coordinates"].apply(lambda x: x[2] if isinstance(x, list) else None)
         
    # Rename Columns
    df = df.rename(columns={
        "properties.mag": "magnitude",
        "properties.place": "location",
        "properties.time": "time_epoch",
        "properties.updated": "updated_time_epoch",
        "properties.tz": "timezone",
        "properties.url": "detail_url",
        "properties.detail": "detail_api",
        "properties.felt": "felt_reports",
        "properties.cdi": "cdi_intensity",
        "properties.mmi": "mmi_intensity",
        "properties.alert": "alert_level",
        "properties.status": "review_status",
        "properties.tsunami": "tsunami_warning",
        "properties.sig": "significance",
        "properties.net": "network",
        "properties.code": "event_code",
        "properties.ids": "event_ids",
        "properties.sources": "data_sources",
        "properties.types": "event_types",
        "properties.nst": "station_count",
        "properties.dmin": "distance_to_nearest_station",
        "properties.rms": "rms_amplitude",
        "properties.gap": "azimuthal_gap",
        "properties.magType": "magnitude_type",
        "properties.type": "event_type",
        "properties.title": "event_title",
        "geometry.type": "geometry_type",
        "geometry.coordinates": "coordinates",
    })

    # Processing Missing Values
    df["alert_level"] = df["alert_level"].fillna("unknown")
    df["location"] = df["location"].fillna("unknown")
    df["magnitude_type"] = df["magnitude_type"].fillna("unknown")
    df["event_type"] = df["event_type"].fillna("unknown")
    # df["alert_level", "location", "magnitude_type", "event_type"] = df[["alert_level", "location", "magnitude_type", "event_type"]].fillna("unknown")
    df["felt_reports"] = df["felt_reports"].fillna(np.nan)
    df["cdi_intensity"] = df["cdi_intensity"].fillna(np.nan)
    df["mmi_intensity"] = df["mmi_intensity"].fillna(np.nan)
    df["significance"] = df["significance"].fillna(0)
    df["tsunami_warning"] = df["tsunami_warning"].fillna(0)
    df["station_count"] = df["station_count"].fillna(0)
    df["distance_to_nearest_station"] = df["distance_to_nearest_station"].fillna(0.0)
    df["rms_amplitude"] = df["rms_amplitude"].fillna(0.0)
    df["azimuthal_gap"] = df["azimuthal_gap"].fillna(0.0)

    # Drop Rows with Essential Data Missing
    df.dropna(subset=["magnitude", "latitude", "longitude", "depth_km"])

    df = df.drop(['coordinates'], axis=1)

    return df

def data_processing_transformation(df):
    # breaking down time components for easy analysis
    df["time_readable"] = pd.to_datetime(df["time_epoch"], unit="ms")
    df["year"] = df["time_readable"].dt.year
    df["month"] = df["time_readable"].dt.month
    df["day"] = df["time_readable"].dt.day
    df["hour"] = df["time_readable"].dt.hour
    df["day_of_week"] = df["time_readable"].dt.dayofweek
    df["quarter"] = df["time_readable"].dt.quarter
    
    # extracting accuate region name
    df["region_name"] = df["location"].str.extract(r",\s*(.*)$")
    df["region_name"] = df["region_name"].fillna("Unknown")

    # display of important location info
    df["location_info_display"] = df["location"] + " (Magnitude " + df["magnitude"].astype(str) + ")" 

    # expanded alert classification 
    def expanded_alert(row):
        if row["tsunami_warning"] == 1 and row["magnitude"] >= 6.5:
            return "Severe Tsunami Risk"
        elif row["tsunami_warning"] == 1:
            return "Tsunami Warning"
        elif row["magnitude"] >= 7.0:
            return "Major Earthquake"
        elif row["magnitude"] >= 6.0:
            return "Strong Earthquake"
        elif row["alert_level"] in ["orange", "red"]:
            return "Significant Alert"
        elif row["alert_level"] in ["yellow", "green"]:
            return "Moderate Alert"
        else:
            return "No Alert"
            
        df["full_alert_level"] = df.apply(expanded_alert, axis=1)

    return df

# Converts data types for dynamodb
def process_data_for_dynamodb(df):
    float_columns =  df.select_dtypes(include=['float','int'])
    for c in float_columns:
        df[c] = df[c].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else x)

    df['time_readable']= df['time_readable'].astype(str)

    return df

# writes data to dynamodb
def save_to_dynamodb(df):
    boto3.setup_default_session(region_name='us-east-1')
    wr.dynamodb.put_df(df=df, table_name='earthquakes')

# cleans, transforms and writes data to dynamodb
def clean_transform_write(json_data):
    df = clean_data(json_data)
    df = data_processing_transformation(df)
    df = process_data_for_dynamodb(df)
    save_to_dynamodb(df)


# NOTE: There is a limit on how much data can be fetched at once
# So the data is fetched using monthly increments
def get_last_year_data(min_magnitude=4):

    start = datetime.now(timezone.utc) - relativedelta(months=13)
    end = datetime.now(timezone.utc)
    # Creating range of dates that increments monthly
    monthly_dates = pd.date_range(start=start, end=end, freq='MS') 

    record_num = 0

    #Retrieving yearly data using monthly increments
    for i in range(len(monthly_dates)-1):
        start = monthly_dates[i].strftime('%Y-%m-%d')
        end = ((monthly_dates[i+1].replace(hour=0, minute=0, second=0, microsecond=0)) - relativedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%S')

        json_data = fetch_historical_earthquake_data(start, end, params)
        clean_transform_write(json_data)

    # Get remaining data from the current month
    start = monthly_dates[-1].strftime('%Y-%m-%d')
    end = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    json_data = fetch_historical_earthquake_data(start, end, params)
    clean_transform_write(json_data)

    #print('Total number of records retrieved:', record_num)

params = {'minmagnitude':4}
get_last_year_data(params)
