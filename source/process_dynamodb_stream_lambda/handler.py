import json
import os
import uuid
from datetime import datetime
from decimal import Decimal

import boto3
from boto3.dynamodb.types import TypeDeserializer


s3_bucket = boto3.resource("s3").Bucket(os.environ["S3_FOR_DYNAMODB_STREAM_TO_REDSHIFT"])


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)


def lambda_handler(event, context):
    s3_file_contents = []
    print(event["Records"])
    for record in event["Records"]:
        if record["eventName"] in ["INSERT", "MODIFY"]:
            s3_file_contents.append(TypeDeserializer().deserialize({"M": record["dynamodb"]["NewImage"]}))
        elif record["eventName"] in ["REMOVE"]:
            pass
        else:
            raise ValueError(f'Did not expect `eventName` to be "{record["eventName"]}"')
    print(s3_file_contents)
    s3_file_contents_in_redshift_json_string = "\n".join(json.dumps(record, cls=DecimalEncoder) for record in s3_file_contents)
    if s3_file_contents_in_redshift_json_string:
        s3_bucket.put_object(
            Key=f"{datetime.utcnow().strftime('%Y-%d-%m %H.%M.%S')}__{uuid.uuid4()}.json",
            Body=s3_file_contents_in_redshift_json_string.encode(),
            
        )
    else:
        s3_bucket.put_object(Key=f"{datetime.utcnow().strftime('%Y-%d-%m %H.%M.%S')}__no_inserted_or_modified_records.txt")
    return