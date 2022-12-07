import aws_cdk as cdk

from cdk_infrastructure import CDCStack


app = cdk.App()
environment = app.node.try_get_context("environment")
stack = CDCStack(
    app,
    "CDCStack",
    env=cdk.Environment(region=environment["AWS_REGION"]),
    environment=environment,
)
app.synth()
