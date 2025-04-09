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
from boto3.dynamodb.conditions import Key, Attr
import pycountry

# USGS Earthquake API Endpoint
USGS_API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

countries = {}

US_STATE_ABBR = {
    "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "IA",
    "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO",
    "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI",
    "WV", "WY", "DC", "AS", "GU", "MP", "PR", "VI"}

US_STATE_NAMES = {
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", 
    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", 
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", 
    "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", 
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", 
    "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", 
    "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", 
    "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", 
    "Washington", "West Virginia", "Wisconsin", "Wyoming"}

def get_latest_datetimestamp_db():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table("earthquakes")
    year = datetime.now(timezone.utc).year
    month = datetime.now(timezone.utc).month
    print(month)
    
    response = table.scan(
        FilterExpression=Attr("year").eq(year) and Attr("year").eq(year), 
        ProjectionExpression='time_epoch')

    if len(response['Items']) != 0:
        epoch_time = []
        for item in response['Items']:
            epoch_time.append(int(item['time_epoch']))
        # find the latest time
        time_epoch = max(epoch_time)
        latest_datetime = datetime.utcfromtimestamp(time_epoch/1000).strftime('%Y-%m-%dT%H:%M:%S')
    else:
        latest_datetime = (datetime.now(timezone.utc) - relativedelta(hours=3)).strftime('%Y-%m-%dT%H:%M:%S')

    return latest_datetime

def fetch_daily_earthquake_data(starttime, additional_params=None):
    params = {"starttime": starttime}
    if additional_params != None: params.update(additional_params)
    params['format'] = "geojson"
    print("params:", params)
    response = requests.get(USGS_API_URL, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f" Failed to retrieve data: {response.status_code}")
        return None

def clean_data(json_data):
    df = pd.json_normalize(json_data["features"])

    # Extract longitude, latitude, depth
    coords_df = pd.DataFrame(df["geometry.coordinates"].tolist(), columns=["longitude", "latitude", "depth_km"])
    df = pd.concat([df, coords_df], axis=1)

    # Drop irrelevant columns
    df = df.drop([
        'type',
        'properties.detail',
        'properties.net', 
        'geometry.type',  
        'properties.tz', 
        'geometry.coordinates', 
        'properties.code', 
        'properties.nst', 
        'properties.dmin', 
        'properties.ids'], axis=1)

    # Rename Columns
    df = df.rename(columns={
        "properties.mag": "magnitude",
        "properties.place": "location",
        "properties.time": "time_epoch",
        "properties.updated": "updated_time_epoch",
        "properties.url": "detail_url",
        "properties.felt": "felt_reports",
        "properties.cdi": "cdi_intensity",
        "properties.mmi": "mmi_intensity",
        "properties.alert": "alert_level",
        "properties.status": "review_status",
        "properties.tsunami": "tsunami_warning",
        "properties.sig": "significance",
        "properties.type": "event_type",
        "properties.sources": "data_sources",
        "properties.types": "event_types",
        "properties.rms": "rms_amplitude",
        "properties.gap": "azimuthal_gap",
        "properties.magType": "magnitude_type",
        "properties.title": "event_title"
    })

    # Processing Missing Values
    df["alert_level"] = df["alert_level"].fillna("unknown")
    df["location"] = df["location"].fillna("unknown")
    df["magnitude_type"] = df["magnitude_type"].fillna("unknown")
    df["event_type"] = df["event_type"].fillna("unknown")
    df["felt_reports"] = df["felt_reports"].astype(float).fillna(np.nan)
    df["cdi_intensity"] = df["cdi_intensity"].astype(float).fillna(np.nan)
    df["mmi_intensity"] = df["mmi_intensity"].astype(float).fillna(np.nan)
    df["significance"] = df["significance"].fillna(np.nan)
    df["tsunami_warning"] = df["tsunami_warning"].fillna(np.nan)
    df["rms_amplitude"] = df["rms_amplitude"].fillna(np.nan)
    df["azimuthal_gap"] = df["azimuthal_gap"].fillna(np.nan)

    # Drop Rows with Essential Data Missing
    df.dropna(subset=["magnitude", "latitude", "longitude", "depth_km"])

    return df

# Geo lookup
def latlon_to_country(lat, lon):
    try:
        result = rg.search((lat, lon), mode=1)[0]
        return pycountry.countries.get(alpha_2=result['cc']).name
    except:
        return "Unknown"

# Converts country code to continent
def country_to_continent(country_code):
    try:
        return pc.country_alpha2_to_continent_code(country_code)
    except:
        return "Unknown"

# Get corresponding country and continent of earthquake
def get_country_continent(location, lat, lon):
    try:
        country_name = ""
        # use regex to extract region name
        region = re.search(r",\s*(.*)$", location)
        # region extracted is country
        if region in countries:
            country_name = region
        # region extracted is US state
        elif region in US_STATE_ABBR or region in US_STATE_NAMES:
            country_name = "United States"
        else:
             # use coordiates to extract country
            country_name = latlon_to_country(lat, lon)

        return country_name, country_to_continent(countries[country_name])
    except:
        return "Unknown", "Unknown"

# Transform data for analysis
def data_processing_transformation(df):
    # breaking down time components for easy analysis
    df["time_readable"] = pd.to_datetime(df["time_epoch"], unit="ms")
    df["year"] = df["time_readable"].dt.year
    df["month"] = df["time_readable"].dt.month
    df["day"] = df["time_readable"].dt.day
    df["quarter"] = df["time_readable"].dt.quarter

    # breaking down updated time component
    df['updated_time_readable'] = pd.to_datetime(df["updated_time_epoch"], unit="ms")
    df["updated_year"] = df["updated_time_readable"].dt.year
    df["updated_month"] = df["updated_time_readable"].dt.month
    
    # extracting region information (country and continent)
    df[["country", "continent"]] = df.apply(lambda row: get_country_continent(row["location"], row["latitude"], row["longitude"]), axis=1, result_type="expand")

    # expanded alert classification 
    conditions = [
    (df["tsunami_warning"] == 1) & (df["magnitude"] >= 6.5),
    (df["tsunami_warning"] == 1),
    (df["magnitude"] >= 7.0),
    (df["magnitude"] >= 6.0),
    df["alert_level"].isin(["orange", "red"]),
    df["alert_level"].isin(["yellow", "green"])]

    choices = [
    "Severe Tsunami Risk",
    "Tsunami Warning",
    "Major Earthquake",
    "Strong Earthquake",
    "Significant Alert",
    "Moderate Alert"]

    df["full_alert_level"] = np.select(conditions, choices, default="No Alert")

# Converts data types for dynamodb
def process_data_for_dynamodb(df):
    float_columns =  df.select_dtypes(include=['float','int'])
    for c in float_columns:
        str_vals = df[c].astype(str)
        mask = df[c].notnull()
        df[c] = np.where(mask, str_vals, None)
        df[c] = df[c].apply(lambda x: Decimal(x) if x is not None else None)

    df['time_readable']= df['time_readable'].astype(str)
    df['updated_time_readable']= df['updated_time_readable'].astype(str)

    return df

# writes data to dynamodb
def save_to_dynamodb(df):
    boto3.setup_default_session(region_name='us-east-1')
    wr.dynamodb.put_df(df=df, table_name='earthquakes')
    print("Stored:", df.shape[0], 'records')

def clean_transform_write_latest_data(params=None):
    starttime = get_latest_datetimestamp_db()
    json_data = fetch_daily_earthquake_data(starttime)
    print("Cleaning and transforming data....")
    df = clean_data(json_data)
    df = data_processing_transformation(df)
    print("Pocessing and saving data to DynamoDB...")
    df = process_data_for_dynamodb(df)
    save_to_dynamodb(df)

def lambda_handler(event, context):
    for country in list(pycountry.countries):
        countries[country.name] = country.alpha_2

    clean_transform_write_latest_data()
