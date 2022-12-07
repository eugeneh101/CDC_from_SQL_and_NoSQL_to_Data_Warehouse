from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_lambda as _lambda,
    aws_lambda_event_sources as event_sources,
    aws_s3 as s3,
)
from constructs import Construct




class CDCStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.dynamodb_table = dynamodb.Table(
            self,
            "DynamoDBToRedshift",
            partition_key=dynamodb.Attribute(
                name="id", type=dynamodb.AttributeType.STRING
            ),
            stream=dynamodb.StreamViewType.NEW_IMAGE,
            # CDK wil not automatically deleted DynamoDB during `cdk destroy`
            # (as DynamoDB is a stateful resource) unless explicitly specified by the following line
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.process_dynamodb_stream_lambda = _lambda.Function(
            self,
            "ProcessDynamoDBStreamLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "source/process_dynamodb_stream_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(1),  # should be effectively instantaneous
            memory_size=128,  # in MB
            environment={  # apparently "AWS_REGION" is not allowed as a Lambda env variable
                "AWSREGION": environment["AWS_REGION"],
            },
        )

        self.dynamodb_to_redshift_s3_bucket = s3.Bucket(
            self, "DynamoDBToRedshiftS3Bucket", removal_policy=RemovalPolicy.DESTROY,

        )

        # connect the AWS resources
        self.process_dynamodb_stream_lambda.add_event_source(
            event_sources.DynamoEventSource(self.dynamodb_table,
            starting_position=_lambda.StartingPosition.LATEST,
            # filters=[{"event_name": _lambda.FilterRule.is_equal("INSERT")}]
        ))
        self.dynamodb_to_redshift_s3_bucket.grant_write(self.process_dynamodb_stream_lambda)