import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta

def additional_transoformations(df):
    # categorizing earthquake depth
    df["depth_category"] = pd.cut(df["depth_km"], bins=[0, 70, 300, 700], labels=["Shallow", "Intermediate", "Deep"])

    # categorizing earthquake magnitude 
    df["mag_category"] = pd.cut(
    df["magnitude"],
    bins=[-float("inf"), 2.0, 3.9, 4.9, 5.9, 6.9, 7.9, float("inf")],
    labels=["Micro", "Minor", "Light", "Moderate", "Strong", "Major", "Great"],
    right=True)  

    # calculating time since the event (in hours)
    df["time_since_event"] = (pd.Timestamp.now() - df["datetime"]).dt.total_seconds() / 3600

    return df


def get_geosummary(df):
    # summary by geographic bins
    geo_summary = df.groupby(["lat_bin", "lon_bin"]).agg(
        event_count=("magnitude", "size"),
        avg_magnitude=("magnitude", "mean"),
        max_magnitude=("magnitude", "max"),
        tsunami_events=("tsunami_warning", "sum")
    ).reset_index().sort_values("event_count", ascending=False)

    return geo_summary