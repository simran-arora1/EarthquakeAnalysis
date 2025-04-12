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

# Deletes earthquakes that have a magnitude less than 4 from 2 days ago
def lambda_handler(event, context):
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table("earthquakes")

    datetime_obj = (datetime.now(timezone.utc)) - relativedelta(days=2)
    year = datetime_obj.year
    month = datetime_obj.month
    day = datetime_obj.day

    print(datetime_obj)

    # Get data from DynamoDB
    response = table.scan(
        FilterExpression=Attr("year").eq(year) and Attr("month").eq(month) and Attr("day").eq(day) and Attr("magnitude").lt(4), 
        ProjectionExpression='id')

    print(len(response['Items']))
    for item in response['Items']:
        try:
            # delete item
            table.delete_item(
                Key={
                    'id': item['id']
                    }
            )
        except:
            print("Error in deleting item")
