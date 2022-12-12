import os
import time

import boto3

s3_bucket = boto3.resource("s3").Bucket(
    os.environ["S3_FOR_DYNAMODB_STREAM_TO_REDSHIFT"]
)
redshift_data_client = boto3.client("redshift-data")

REDSHIFT_CLUSTER_NAME = os.environ["REDSHIFT_ENDPOINT_ADDRESS"].split(".")[0]
REDSHIFT_ROLE_ARN = os.environ["REDSHIFT_ROLE_ARN"]
REDSHIFT_USER = os.environ["REDSHIFT_USER"]
REDSHIFT_DATABASE_NAME = os.environ["REDSHIFT_DATABASE_NAME"]
REDSHIFT_SCHEMA_NAME = os.environ["REDSHIFT_SCHEMA_NAME"]
REDSHIFT_TABLE_NAME = os.environ["REDSHIFT_TABLE_NAME"]
AWS_REGION = os.environ["AWSREGION"]


def execute_sql_statement(sql_statement):
    response = redshift_data_client.execute_statement(
        ClusterIdentifier=REDSHIFT_CLUSTER_NAME,
        Database=REDSHIFT_DATABASE_NAME,
        DbUser=REDSHIFT_USER,
        Sql=sql_statement,
    )
    time.sleep(1)
    while True:
        response = redshift_data_client.describe_statement(Id=response["Id"])
        status = response["Status"]
        if status == "FINISHED":
            print(f"Finished executing the following SQL statement: {sql_statement}")
            return
        elif status in ["SUBMITTED", "PICKED", "STARTED"]:
            time.sleep(1)
        elif status == "FAILED":
            print(response)
            raise  ### figure out useful messge in exception
        else:
            print(response)
            raise  ### figure out useful messge in exception


def lambda_handler(event, context):
    dynamodb_stream_s3_files = list(s3_bucket.objects.all())
    if dynamodb_stream_s3_files:
        sql_statements = [
            f"CREATE SCHEMA IF NOT EXISTS {REDSHIFT_SCHEMA_NAME};",
            f"""CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA_NAME}.{REDSHIFT_TABLE_NAME} (
                id varchar(30) UNIQUE NOT NULL,
                details super,
                price float,
                shares integer,
                ticker varchar(10),
                ticket varchar(10),
                time super
            );""",
        ]
        for sql_statement in sql_statements:
            execute_sql_statement(sql_statement=sql_statement)
        for s3_file in dynamodb_stream_s3_files:
            if "__inserted_or_modified_records__" in s3_file.key:
                sql_statement = f"""
                    COPY {REDSHIFT_DATABASE_NAME}.{REDSHIFT_SCHEMA_NAME}.{REDSHIFT_TABLE_NAME}
                    FROM 's3://{s3_bucket.name}/{s3_file.key}'
                    REGION '{AWS_REGION}'
                    iam_role '{REDSHIFT_ROLE_ARN}'
                    format as json 'auto';
                """
                execute_sql_statement(sql_statement=sql_statement)
                print(f"Deleting s3://{s3_bucket.name}/{s3_file.key}")
                s3_file.delete()
            elif "__no_inserted_or_modified_records__" in s3_file.key:
                print(f"Deleting s3://{s3_bucket.name}/{s3_file.key}")
                s3_file.delete()
            else:
                raise  ### figure out useful messge in exception
    else:
        print("No DynamoDB stream files")
