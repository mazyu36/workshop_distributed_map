import json
import os
import csv
import io

from datetime import datetime
from decimal import Decimal
from typing import Dict, List

import boto3


S3_CLIENT = boto3.client("s3")


def lambda_handler(event: dict, context):
    """Handler that will find the weather station that has the highest average temperature by month.

    Returns a dictionary with "year-month" as the key and dictionary (weather station info) as value.

    """
    print(event)
    input_bucket_name = os.environ["INPUT_BUCKET_NAME"]
    output_table_name = os.environ["RESULTS_DYNAMODB_TABLE_NAME"]

    high_by_station: Dict[str, float] = {}

    for item in event["Items"]:
        csv_data = get_file_from_s3(input_bucket_name, item["Key"])
        dict_data = get_csv_dict_from_string(csv_data)
        station = None
        high_prcp = 0
        for row in dict_data:
            if row["ELEMENT"] == "PRCP":
                if not station:
                    station = row["ID"]
                prcp = float(row["DATA_VALUE"])
                if prcp > high_prcp:
                    high_prcp = prcp
        high_by_station[station] = high_prcp
    print(high_by_station)
    _write_results_to_ddb(high_by_station, output_table_name)

    response = {
        "input_bucket": input_bucket_name,
        "output_table_name": output_table_name,
    }
    return {"statucCode": 200, "body": json.dumps(response)}


def _write_results_to_ddb(high_by_station: Dict[str, Dict], table_name):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    for station, prcp in high_by_station.items():
        if station is not None:
            row = {}
            row["pk"] = station
            row["PRCP"] = round(Decimal(prcp))
            table.put_item(Item=row)


def get_file_from_s3(input_bucket_name: str, key: str) -> str:
    resp = S3_CLIENT.get_object(Bucket=input_bucket_name, Key=key)
    return resp["Body"].read().decode("utf-8")


def get_csv_dict_from_string(csv_string: str) -> dict:
    return csv.DictReader(io.StringIO(csv_string))
