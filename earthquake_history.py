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
import reverse_geocoder as rg
import pycountry_convert as pc
import pycountry
import re

# USGS Earthquake API Endpoint
USGS_API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

# DynamoDB config
REGION = "us-east-1"
TABLE_NAME = "earthquakes"

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


# Fetch historical Earthquake Data from USGS API
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

# Clean data
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
    df["felt_reports"] = df["felt_reports"].fillna(np.nan)
    df["cdi_intensity"] = df["cdi_intensity"].fillna(np.nan)
    df["mmi_intensity"] = df["mmi_intensity"].fillna(np.nan)
    df["significance"] = df["significance"].fillna(np.nan)
    df["tsunami_warning"] = df["tsunami_warning"].fillna(np.nan)
    df["rms_amplitude"] = df["rms_amplitude"].fillna(np.nan)
    df["azimuthal_gap"] = df["azimuthal_gap"].fillna(np.nan)

    # Drop Rows with Essential Data Missing
    df.dropna(subset=["magnitude", "latitude", "longitude", "depth_km"])

    return df

# Geo lookup
def latlon_to_country(lat, lon):
    result = rg.search((lat, lon), mode=1)[0]
    return pycountry.countries.get(alpha_2=result['cc']).name

# Converts country code to continent
def country_to_continent(country_code):
    return pc.country_alpha2_to_continent_code(country_code)

# Get corresponding country and continent of earthquake
def get_country_continent(location, lat, lon):
    try:
        country_name = ""
        # use regex to extract region name
        region = re.search(r",\s*(.*)$", location).group(1)
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

# Transform data
def data_processing_transformation(df):
    # breaking down time components for easy analysis
    df["time_readable"] = pd.to_datetime(df["time_epoch"], unit="ms")
    df["date"] = df["time_readable"].dt.date
    df["year"] = df["time_readable"].dt.year
    df["month"] = df["time_readable"].dt.month
    df["day"] = df["time_readable"].dt.day

    # breaking down updated time components
    df['updated_time_readable'] = pd.to_datetime(df["updated_time_epoch"], unit="ms")
    df["updated_year"] = df["updated_time_readable"].dt.year
    df["updated_month"] = df["updated_time_readable"].dt.month
    
    # extracting region information (country and continent)
    df[["country", "continent"]] = df.apply(lambda row: get_country_continent(row["location"], row["latitude"], row["longitude"]), axis=1, result_type="expand")

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
        str_vals = df[c].astype(str)
        mask = df[c].notnull()
        df[c] = np.where(mask, str_vals, None)
        df[c] = df[c].apply(lambda x: Decimal(x) if x is not None else None)

    df['date']= df['date'].astype(str)
    df['time_readable']= df['time_readable'].astype(str)
    df['updated_time_readable']= df['updated_time_readable'].astype(str)
    
    return df

# Writes data to dynamodb
def save_to_dynamodb(df):
    boto3.setup_default_session(region_name='us-east-1')
    wr.dynamodb.put_df(df=df, table_name='earthquakes')

# Cleans, transforms and writes data to dynamodb
def clean_transform_write(json_data):
    df = clean_data(json_data)
    df = data_processing_transformation(df)
    df = process_data_for_dynamodb(df)
    save_to_dynamodb(df)
    return df.shape[0]

# Retrieves data from the past year using monthly increments
def get_last_year_data(min_magnitude=4):
# NOTE: There is a limit on how much data can be fetched at once
# So the data is fetched using monthly increments

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
        record_num = record_num + clean_transform_write(json_data)

    # Get remaining data from the current month
    start = monthly_dates[-1].strftime('%Y-%m-%d')
    end = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    json_data = fetch_historical_earthquake_data(start, end, params)
    record_num = record_num + clean_transform_write(json_data)

    print('Total number of records retrieved:', record_num)

# Create a global map of countries and it's corresponding country code
for country in list(pycountry.countries):
    countries[country.name] = country.alpha_2

# Get earthquakes with magnitude greater than 4
params = {'minmagnitude':4}
get_last_year_data(params)
