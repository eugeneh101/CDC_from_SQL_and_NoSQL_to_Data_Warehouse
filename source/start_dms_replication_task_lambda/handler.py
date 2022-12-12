import os

import boto3
from botocore.exceptions import ClientError

client = boto3.client("dms")
DMS_REPLICATION_TASK_ARN = os.environ["DMS_REPLICATION_TASK_ARN"]


def lambda_handler(event, context):
    response = client.describe_replication_tasks(
        Filters=[{"Name": "replication-task-arn", "Values": [DMS_REPLICATION_TASK_ARN]}]
    )["ReplicationTasks"]
    assert len(response) == 1, "There should be exactly 1 replication task ARN"
    status = response[0]["Status"]
    assert status in ["ready", "stopped", "running"], f"Unexpected status: {status}"
    if status in ["ready", "stopped"]:
        response = client.start_replication_task(
            ReplicationTaskArn=DMS_REPLICATION_TASK_ARN,
            StartReplicationTaskType="start-replication",
        )
        print(f"Started DMS Replication Task. Here is the response: {response}")
    elif status == "running":
        print("DMS Replication Task is already running, so do no extra action.")
    else:
        raise
