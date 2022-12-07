from aws_cdk import (
    BundlingOptions,
    Duration,
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_lambda_event_sources as event_sources,
    aws_redshift as redshift,
    aws_s3 as s3,
)
from constructs import Construct




class CDCStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # stateful resources
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

        self.cdc_from_dynamodb_to_redshift_s3_bucket = s3.Bucket(
            self, "DynamoDBToRedshiftS3Bucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        self.redshift_cluster_role = iam.Role(
            self,
            "RedshiftClusterRole",
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRedshiftAllCommandsFullAccess")  # later principle of least privileges
                ],
        )
        self.redshift_cluster = redshift.CfnCluster(  # refactor as its own Construct
            self,
            "RedshiftClsuter",
            cluster_type="single-node",  # for demo purposes
            number_of_nodes=1,  # for demo purposes
            node_type="dc2.large",  # for demo purposes
            db_name="redshift_database",  ### hard coded
            master_username="admin",  ### hard coded
            master_user_password="Password1",  ### hard coded
            iam_roles=[self.redshift_cluster_role.role_arn],
            # cluster_subnet_group_name=demo_cluster_subnet_group.ref,
            # vpc_security_group_ids=[
            #     quicksight_to_redshift_sg.security_group_id]
        )

        # stateless resources
        # self.load_data_to_rds_lambda = _lambda.Function(
        #     self,
        #     "LoadDataToRDSLambda",
        #     runtime=_lambda.Runtime.PYTHON_3_9,
        #     code=_lambda.Code.from_asset(
        #         "source/load_data_to_rds_lambda",
        #         exclude=[".venv/*"],
        #     ),
        #     handler="handler.lambda_handler",
        #     timeout=Duration.seconds(3),  # should be fairly quick
        #     memory_size=128,  # in MB
        # )


        self.load_data_to_dynamodb_lambda = _lambda.Function(
            self,
            "LoadDataToDynamoDBLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "source/load_data_to_dynamodb_lambda",
                exclude=[".venv/*", "tests/*"],  # seems to no longer do anything if use BundlingOptions
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(3),  # should be fairly quick
            memory_size=128,  # in MB
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
            timeout=Duration.seconds(3),  # should be fairly quick
            memory_size=128,  # in MB
            environment={  # apparently "AWS_REGION" is not allowed as a Lambda env variable
                "AWSREGION": environment["AWS_REGION"],
            },
        )
        self.load_s3_files_from_dynamodb_stream_to_redshift_lambda = _lambda.Function(
            self,
            "LoadS3FilesFromDynamoDBStreamToRedshiftLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "source/load_s3_files_from_dynamodb_stream_to_redshift_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(3),  # should be fairly quick
            memory_size=128,  # in MB
            environment={  # apparently "AWS_REGION" is not allowed as a Lambda env variable
                "AWSREGION": environment["AWS_REGION"],
            },
        )

        self.scheduled_eventbridge_event = events.Rule(
            self,
            "RunEvery5Minutes",
            event_bus=None,  # scheduled events must be on "default" bus
            schedule=events.Schedule.rate(Duration.minutes(5)),
        )



        # connect the AWS resources
        self.scheduled_eventbridge_event.add_target(
            target=events_targets.LambdaFunction(
                handler=self.load_data_to_dynamodb_lambda, retry_attempts=3,
                ### then put in DLQ
            ),
        )
        self.dynamodb_table.grant_write_data(self.load_data_to_dynamodb_lambda)
        self.load_data_to_dynamodb_lambda.add_environment(
            key="DYNAMODB_TABLE_NAME", value=self.dynamodb_table.table_name
        )
        self.process_dynamodb_stream_lambda.add_event_source(
            event_sources.DynamoEventSource(self.dynamodb_table,
            starting_position=_lambda.StartingPosition.LATEST,
            # filters=[{"event_name": _lambda.FilterRule.is_equal("INSERT")}]
        ))
        self.process_dynamodb_stream_lambda.add_environment(
            key="S3_FOR_DYNAMODB_STREAM_TO_REDSHIFT",
            value=self.cdc_from_dynamodb_to_redshift_s3_bucket.bucket_name,
        )
        self.cdc_from_dynamodb_to_redshift_s3_bucket.grant_write(self.process_dynamodb_stream_lambda)

        self.load_s3_files_from_dynamodb_stream_to_redshift_lambda.add_environment(
            key="REDSHIFT_CLUSTER_NAME",
            value=self.redshift_cluster.attr_id,
        )
        self.load_s3_files_from_dynamodb_stream_to_redshift_lambda.add_environment(
            key="S3_FOR_DYNAMODB_STREAM_TO_REDSHIFT",
            value=self.cdc_from_dynamodb_to_redshift_s3_bucket.bucket_name,
        )
        self.cdc_from_dynamodb_to_redshift_s3_bucket.grant_read(
            self.load_s3_files_from_dynamodb_stream_to_redshift_lambda
        )


        # self.scheduled_eventbridge_event.add_target(
        #     target=events_targets.LambdaFunction(
        #         handler=self.vrf_request_lambda, retry_attempts=3,
        #         ### then put in DLQ
        #     ),
        # )