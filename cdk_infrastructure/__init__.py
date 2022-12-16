import json

from aws_cdk import (
    BundlingOptions,
    Duration,
    RemovalPolicy,
    SecretValue,
    Stack,
    aws_dms as dms,
    aws_dynamodb as dynamodb,
    aws_ec2 as ec2,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_lambda_event_sources as event_sources,
    aws_rds as rds,
    aws_redshift as redshift,
    aws_s3 as s3,
)
from constructs import Construct


class RedshiftService(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
        security_group: ec2.SecurityGroup,
    ) -> None:
        super().__init__(scope, construct_id)  # required
        self.redshift_full_commands_full_access_role = iam.Role(
            self,
            "RedshiftClusterRole",
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonRedshiftAllCommandsFullAccess"
                ),  ### later principle of least privileges
            ],
        )
        self.redshift_cluster = redshift.CfnCluster(  ### refactor as its own Construct
            self,
            "RedshiftCluster",
            cluster_type="single-node",  # for demo purposes
            number_of_nodes=1,  # for demo purposes
            node_type="dc2.large",  # for demo purposes
            db_name=environment["REDSHIFT_DATABASE_NAME"],
            master_username=environment["REDSHIFT_USER"],
            master_user_password=environment["REDSHIFT_PASSWORD"],
            iam_roles=[self.redshift_full_commands_full_access_role.role_arn],
            # cluster_subnet_group_name=demo_cluster_subnet_group.ref,
            vpc_security_group_ids=[security_group.security_group_id],
        )


class RDSService(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
        vpc: ec2.Vpc,
        security_group: ec2.SecurityGroup,
    ) -> None:
        super().__init__(scope, construct_id)  # required
        self.rds_instance = rds.DatabaseInstance(
            self,
            "RDSToCDCToRedshift",
            engine=rds.DatabaseInstanceEngine.mysql(
                version=rds.MysqlEngineVersion.VER_8_0_28
            ),
            instance_type=ec2.InstanceType(
                "t3.micro"
            ),  # for demo purposes; otherwise defaults to m5.large
            credentials=rds.Credentials.from_username(
                username=environment["RDS_USER"],
                password=SecretValue.unsafe_plain_text(environment["RDS_PASSWORD"]),
            ),
            database_name=environment["RDS_DATABASE_NAME"],
            port=environment["RDS_PORT"],
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC
            ),  ### will have to figure out VPC
            security_groups=[security_group],
            parameters={"binlog_format": "ROW"},
            publicly_accessible=True,  ### will have to figure out VPC
            removal_policy=RemovalPolicy.DESTROY,
            delete_automated_backups=True,
        )

        self.load_data_to_rds_lambda = _lambda.Function(
            self,
            "LoadDataToRDSLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "source/load_data_to_rds_lambda",
                # exclude=[".venv/*"],  # seems to no longer do anything if use BundlingOptions
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        " && ".join(
                            [
                                "pip install -r requirements.txt -t /asset-output",
                                "cp handler.py txns.csv /asset-output",  # need to cp instead of mv
                            ]
                        ),
                    ],
                ),
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(3),  # should be fairly quick
            memory_size=128,  # in MB
            environment={
                "RDS_USER": environment["RDS_USER"],
                "RDS_PASSWORD": environment["RDS_PASSWORD"],
                "RDS_DATABASE_NAME": environment["RDS_DATABASE_NAME"],
                "RDS_TABLE_NAME": environment["RDS_TABLE_NAME"],
                "CSV_FILENAME": environment["CSV_FILENAME"],
            },
        )

        # connect the AWS resources
        self.load_data_to_rds_lambda.add_environment(
            key="RDS_HOST", value=self.rds_instance.db_instance_endpoint_address
        )


class CDCFromRDSToRedshiftService(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
        rds_endpoint_address: str,
        redshift_endpoint_address: str,
        security_group_id: str,
    ) -> None:
        super().__init__(scope, construct_id)  # required
        self.dms_rds_source_endpoint = dms.CfnEndpoint(
            self,
            "RDSSourceEndpoint",
            endpoint_type="source",
            engine_name="mysql",
            server_name=rds_endpoint_address,
            port=environment["RDS_PORT"],
            username=environment["RDS_USER"],
            password=environment["RDS_PASSWORD"],
        )
        self.dms_redshift_target_endpoint = dms.CfnEndpoint(
            self,
            "RedshiftTargetEndpoint",
            endpoint_type="target",
            engine_name="redshift",
            database_name=environment["REDSHIFT_DATABASE_NAME"],
            server_name=redshift_endpoint_address,
            port=5439,
            username=environment["REDSHIFT_USER"],
            password=environment["REDSHIFT_PASSWORD"],
        )
        self.dms_replication_instance = dms.CfnReplicationInstance(
            self,
            "DMSReplicationInstance",
            replication_instance_class="dms.t3.micro",  # for demo purposes
            vpc_security_group_ids=[security_group_id],
        )
        self.dms_replication_task = dms.CfnReplicationTask(
            self,
            "DMSReplicationTask",
            migration_type="cdc",
            replication_instance_arn=self.dms_replication_instance.ref,  # appears that
            source_endpoint_arn=self.dms_rds_source_endpoint.ref,  # `ref` means
            target_endpoint_arn=self.dms_redshift_target_endpoint.ref,  # arn
            table_mappings=json.dumps(
                {
                    "rules": [
                        {
                            "rule-type": "selection",
                            "rule-id": "1",
                            "rule-name": "1",
                            "object-locator": {
                                "schema-name": "%",
                                "table-name": environment["RDS_TABLE_NAME"],
                            },
                            "rule-action": "include",
                            "filters": [],
                        }
                    ]
                }
            ),
            replication_task_settings=json.dumps({"Logging": {"EnableLogging": True}}),
        )

        env_vars = {
            "PRINT_RDS_AND_REDSHIFT_NUM_ROWS": json.dumps(environment["PRINT_RDS_AND_REDSHIFT_NUM_ROWS"])
        }
        if environment["PRINT_RDS_AND_REDSHIFT_NUM_ROWS"]:
             env_vars.update({
                "RDS_HOST": rds_endpoint_address,
                "RDS_USER": environment["RDS_USER"],
                "RDS_PASSWORD": environment["RDS_PASSWORD"],
                "RDS_DATABASE_NAME": environment["RDS_DATABASE_NAME"],
                "RDS_TABLE_NAME": environment["RDS_TABLE_NAME"],
                "REDSHIFT_ENDPOINT_ADDRESS": redshift_endpoint_address,
                "REDSHIFT_USER": environment["REDSHIFT_USER"],
                "REDSHIFT_DATABASE_NAME": environment["REDSHIFT_DATABASE_NAME"],
            })
        self.start_dms_replication_task_lambda = _lambda.Function(
            self,
            "StartDMSReplicationTaskLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "source/start_dms_replication_task_lambda",
                # exclude=[".venv/*"],  # seems to no longer do anything if use BundlingOptions
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        " && ".join(
                            [
                                "pip install -r requirements.txt -t /asset-output",
                                "cp handler.py /asset-output",  # need to cp instead of mv
                            ]
                        ),
                    ],
                ),
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(3),  # should be fairly quick
            memory_size=128,  # in MB
            environment=env_vars,
        )
        self.start_dms_replication_task_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["dms:StartReplicationTask", "dms:DescribeReplicationTasks"],
                resources=["*"],
            )
        )
        if environment["PRINT_RDS_AND_REDSHIFT_NUM_ROWS"]:
            self.start_dms_replication_task_lambda.add_to_role_policy(
                iam.PolicyStatement(
                    actions=[
                        "redshift-data:ExecuteStatement",
                        "redshift-data:DescribeStatement",
                        "redshift-data:GetStatementResult",
                        "redshift:GetClusterCredentials",
                    ],
                    resources=["*"],
                )
            )

        # connect the AWS resources
        self.start_dms_replication_task_lambda.add_environment(
            key="DMS_REPLICATION_TASK_ARN",
            value=self.dms_replication_task.ref,  # appears `ref` means arn
        )


class DynamoDBService(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
    ) -> None:
        super().__init__(scope, construct_id)  # required
        self.dynamodb_table = dynamodb.Table(
            self,
            "DynamoDBTableToCDCToRedshift",
            partition_key=dynamodb.Attribute(
                name="id", type=dynamodb.AttributeType.STRING
            ),
            stream=dynamodb.StreamViewType.NEW_IMAGE,
            # CDK wil not automatically deleted DynamoDB during `cdk destroy`
            # (as DynamoDB is a stateful resource) unless explicitly specified by the following line
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.cdc_from_dynamodb_to_redshift_s3_bucket = s3.Bucket(
            self,
            "DynamoDBStreamToRedshiftS3Bucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        self.load_data_to_dynamodb_lambda = _lambda.Function(
            self,
            "LoadDataToDynamoDBLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "source/load_data_to_dynamodb_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(3),  # should be fairly quick
            memory_size=128,  # in MB
            environment={"JSON_FILENAME": environment["JSON_FILENAME"]},
        )
        self.write_dynamodb_stream_to_s3_lambda = _lambda.Function(
            self,
            "WriteDynamoDBStreamToS3Lambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "source/write_dynamodb_stream_to_s3_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(3),  # should be fairly quick
            memory_size=128,  # in MB
            environment={  # apparently "AWS_REGION" is not allowed as a Lambda env variable
                "AWSREGION": environment["AWS_REGION"],
            },
        )

        # connect the AWS resources
        self.load_data_to_dynamodb_lambda.add_environment(
            key="DYNAMODB_TABLE_NAME", value=self.dynamodb_table.table_name
        )
        self.dynamodb_table.grant_write_data(self.load_data_to_dynamodb_lambda)
        self.write_dynamodb_stream_to_s3_lambda.add_environment(
            key="S3_FOR_DYNAMODB_STREAM_TO_REDSHIFT",
            value=self.cdc_from_dynamodb_to_redshift_s3_bucket.bucket_name,
        )
        self.write_dynamodb_stream_to_s3_lambda.add_event_source(
            event_sources.DynamoEventSource(
                self.dynamodb_table,
                starting_position=_lambda.StartingPosition.LATEST,
                # filters=[{"event_name": _lambda.FilterRule.is_equal("INSERT")}]
            )
        )
        self.cdc_from_dynamodb_to_redshift_s3_bucket.grant_write(
            self.write_dynamodb_stream_to_s3_lambda
        )


class CDCFromDynamoDBToRedshiftService(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
        cdc_from_dynamodb_to_redshift_s3_bucket: s3.Bucket,
        redshift_endpoint_address: str,
        redshift_role_arn: str,
    ) -> None:
        super().__init__(scope, construct_id)  # required
        self.lambda_redshift_full_access_role = iam.Role(
            self,
            "LambdaRedshiftFullAccessRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonRedshiftFullAccess"
                ),  ### later principle of least privileges
            ],
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
            timeout=Duration.seconds(10),  # may take some time if many files
            memory_size=128,  # in MB
            environment={
                "REDSHIFT_USER": environment["REDSHIFT_USER"],
                "REDSHIFT_DATABASE_NAME": environment["REDSHIFT_DATABASE_NAME"],
                "REDSHIFT_SCHEMA_NAME_FOR_DYNAMODB_CDC": environment["REDSHIFT_SCHEMA_NAME_FOR_DYNAMODB_CDC"],
                "REDSHIFT_TABLE_NAME_FOR_DYNAMODB_CDC": environment["REDSHIFT_TABLE_NAME_FOR_DYNAMODB_CDC"],
                "AWSREGION": environment[
                    "AWS_REGION"
                ],  # apparently "AWS_REGION" is not allowed as a Lambda env variable
            },
            role=self.lambda_redshift_full_access_role,
        )

        # connect the AWS resources
        lambda_environment_variables = {
            "S3_FOR_DYNAMODB_STREAM_TO_REDSHIFT": cdc_from_dynamodb_to_redshift_s3_bucket.bucket_name,
            "REDSHIFT_ENDPOINT_ADDRESS": redshift_endpoint_address,
            "REDSHIFT_ROLE_ARN": redshift_role_arn,
        }
        for key, value in lambda_environment_variables.items():
            self.load_s3_files_from_dynamodb_stream_to_redshift_lambda.add_environment(
                key=key, value=value
            )
        cdc_from_dynamodb_to_redshift_s3_bucket.grant_read_write(
            self.load_s3_files_from_dynamodb_stream_to_redshift_lambda
        )


class CDCStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.default_vpc = ec2.Vpc.from_lookup(self, "DefaultVPC", is_default=True)
        self.security_group_for_rds_redshift_dms = ec2.SecurityGroup(
            self,
            "SecurityGroupForRDSRedshiftDMS",
            vpc=self.default_vpc,
            allow_all_outbound=True,
        )
        self.security_group_for_rds_redshift_dms.add_ingress_rule(  # for RDS + DMS
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(environment["RDS_PORT"]),
        )
        self.security_group_for_rds_redshift_dms.add_ingress_rule(  # for Redshift + DMS
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(environment["REDSHIFT_PORT"]),
        )

        self.redshift_service = RedshiftService(
            self,
            "RedshiftService",
            environment=environment,
            security_group=self.security_group_for_rds_redshift_dms,
        )
        self.rds_service = RDSService(
            self,
            "RDSService",
            environment=environment,
            vpc=self.default_vpc,
            security_group=self.security_group_for_rds_redshift_dms,
        )
        self.cdc_from_rds_to_redshift_service = CDCFromRDSToRedshiftService(
            self,
            "CDCFromRDSToRedshiftService",
            environment=environment,
            rds_endpoint_address=self.rds_service.rds_instance.db_instance_endpoint_address,
            redshift_endpoint_address=self.redshift_service.redshift_cluster.attr_endpoint_address,
            security_group_id=self.security_group_for_rds_redshift_dms.security_group_id,
        )
        self.dynamodb_service = DynamoDBService(
            self, "DynamoDBService", environment=environment
        )
        self.cdc_from_dynamodb_to_redshift_service = CDCFromDynamoDBToRedshiftService(
            self,
            "CDCFromDynamoDBToRedshiftService",
            environment=environment,
            cdc_from_dynamodb_to_redshift_s3_bucket=self.dynamodb_service.cdc_from_dynamodb_to_redshift_s3_bucket,
            redshift_endpoint_address=self.redshift_service.redshift_cluster.attr_endpoint_address,  # appears redshift_cluster.attr_id is broken,
            redshift_role_arn=self.redshift_service.redshift_full_commands_full_access_role.role_arn,
        )

        # schedule Lambdas to run
        self.scheduled_eventbridge_event = events.Rule(
            self,
            "RunEvery5Minutes",
            event_bus=None,  # scheduled events must be on "default" bus
            schedule=events.Schedule.rate(Duration.minutes(5)),
        )
        lambda_functions = [
            self.rds_service.load_data_to_rds_lambda,
            self.cdc_from_rds_to_redshift_service.start_dms_replication_task_lambda,
            self.dynamodb_service.load_data_to_dynamodb_lambda,
            self.cdc_from_dynamodb_to_redshift_service.load_s3_files_from_dynamodb_stream_to_redshift_lambda,
        ]
        for lambda_function in lambda_functions:
            self.scheduled_eventbridge_event.add_target(
                target=events_targets.LambdaFunction(
                    handler=lambda_function,
                    retry_attempts=3,
                    ### then put in DLQ
                ),
            )
