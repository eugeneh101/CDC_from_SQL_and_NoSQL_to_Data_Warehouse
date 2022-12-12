import json
import os
from decimal import Decimal

import boto3

table = boto3.resource("dynamodb").Table(os.environ["DYNAMODB_TABLE_NAME"])
JSON_FILENAME = os.environ["JSON_FILENAME"]


def lambda_handler(event, context):
    with table.batch_writer() as writer, open(JSON_FILENAME) as f:
        trades = json.load(f, parse_float=Decimal)["data"]
        for trade in trades:
            writer.put_item(Item=trade)
    return
