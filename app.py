import boto3
import aws_cdk as cdk

from cdk_infrastructure import CDCStack

app = cdk.App()
environment = app.node.try_get_context("environment")
account = boto3.client("sts").get_caller_identity()["Account"]
stack = CDCStack(
    app,
    "CDCStack",
    env=cdk.Environment(account=account, region=environment["AWS_REGION"]),
    environment=environment,
)
app.synth()
