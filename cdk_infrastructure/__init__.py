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




class CDCStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # stateful resources
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
            self, "DynamoDBStreamToRedshiftS3Bucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        self.default_vpc = ec2.Vpc.from_lookup(self, "DefaultVPC", is_default=True)
        # self.default_security_group = ec2.SecurityGroup.from_lookup_by_name(
        #     self, "DefaultSecurityGroup", security_group_name="default", vpc=self.default_vpc
        # )
        self.security_group_for_rds = ec2.SecurityGroup(
            self,
            "SecurityGroupForRDS",
            vpc=self.default_vpc,
            allow_all_outbound=True,
            )
        self.security_group_for_rds.add_ingress_rule(
            peer=ec2.Peer.any_ipv4(),
            connection=ec2.Port.tcp(3306),
        )
        self.rds_instance = rds.DatabaseInstance(
            self,
            "RDSToCDCToRedshift",
            engine=rds.DatabaseInstanceEngine.mysql(version=rds.MysqlEngineVersion.VER_8_0_28),
            # optional, defaults to m5.large
            instance_type=ec2.InstanceType("t3.micro"),  # for demo purposes
            # ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.MICRO),
            credentials=rds.Credentials.from_username(
                username="admin", password=SecretValue.unsafe_plain_text("password")  ### hard coded
            ),
            database_name="rds_to_redshift_database",  ### hard coded
            port=3306,  ### hard coded
            vpc=self.default_vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),  # will have to figure out VPC
            security_groups=[
                self.security_group_for_rds,
                # self.default_security_group,
                # ec2.SecurityGroup.from_security_group_id(
                #     self, "DefaultSecurityGroup", security_group_id="sg-3e224941"  ### replace with code
                # ),
            ],
            parameters={"binlog_format": "ROW"},
            publicly_accessible=True,  # will have to figure out VPC
            removal_policy=RemovalPolicy.DESTROY,
            delete_automated_backups=True,
        )


        self.redshift_full_commands_full_access_role = iam.Role(
            self,
            "RedshiftClusterRole",
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRedshiftAllCommandsFullAccess"),  ### later principle of least privileges
            ],
        )
        self.redshift_cluster = redshift.CfnCluster(  ### refactor as its own Construct
            self,
            "RedshiftCluster",
            cluster_type="single-node",  # for demo purposes
            number_of_nodes=1,  # for demo purposes
            node_type="dc2.large",  # for demo purposes
            db_name="redshift_database",  ### hard coded
            master_username="admin",  ### hard coded
            master_user_password="Password1",  ### hard coded
            iam_roles=[self.redshift_full_commands_full_access_role.role_arn],
            # cluster_subnet_group_name=demo_cluster_subnet_group.ref,
            # vpc_security_group_ids=[
            #     quicksight_to_redshift_sg.security_group_id]
        )



        self.dms_rds_source_endpoint = dms.CfnEndpoint(
            self,
            "RDSSourceEndpoint",
            endpoint_type="source",
            engine_name="mysql",
            server_name=self.rds_instance.db_instance_endpoint_address,
            port=3306,
            username="admin",
            password="password",
        )
        self.dms_redshift_target_endpoint = dms.CfnEndpoint(
            self,
            "RedshiftTargetEndpoint",
            endpoint_type="target",
            engine_name="redshift",
            database_name="redshift_database",
            server_name=self.redshift_cluster.attr_endpoint_address,
            port=5439,
            username="admin",
            password="Password1",
        )
        self.dms_replication_instance = dms.CfnReplicationInstance(
            self,
            "DMSReplicationInstance",
            replication_instance_class="dms.t3.micro",  # for demo purposes
            vpc_security_group_ids=["sg-3e224941"],
        )
        self.dms_replication_task = dms.CfnReplicationTask(self, "DMSReplicationTask",
            migration_type="cdc",
            replication_instance_arn=self.dms_replication_instance.ref, # appears that
            source_endpoint_arn=self.dms_rds_source_endpoint.ref,  # `ref` means
            target_endpoint_arn=self.dms_redshift_target_endpoint.ref,  # arn
            table_mappings=json.dumps({
                "rules": [
                    {
                        "rule-type": "selection",
                        "rule-id": "1",
                        "rule-name": "1",
                        "object-locator": {
                            "schema-name": "%",
                            "table-name": "rds_cdc_table"  ### hard coded
                        },
                        "rule-action": "include",
                        "filters": [],
                    }
                ]
            }),
            replication_task_settings=json.dumps({"Logging": {"EnableLogging": True}}),
        )


        # stateless resources
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
        self.lambda_redshift_full_access_role = iam.Role(
            self,
            "LambdaRedshiftFullAccessRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRedshiftFullAccess"),  ### later principle of least privileges
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
            environment={  # apparently "AWS_REGION" is not allowed as a Lambda env variable
                "AWSREGION": environment["AWS_REGION"],
            },
            role=self.lambda_redshift_full_access_role,
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
        )


        self.start_dms_role = iam.Role(
            self,
            "StartDMSRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
            ],
        )
        self.start_dms_role.add_to_policy(
            iam.PolicyStatement(
                actions=["dms:StartReplicationTask",  "dms:DescribeReplicationTasks"], resources=["*"]
            )
        )
        self.start_dms_replication_task_lambda = _lambda.Function(
            self,
            "StartDMSReplicationTaskLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "source/start_dms_replication_task_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(1),  # should be instantaneous
            memory_size=128,  # in MB
            role=self.start_dms_role,
        )
        # self.start_dms_replication_task_lambda.add_permission(
        #     "start_dms_replication_task_permission",
        #     principal=iam.ServicePrincipal("lambda.amazonaws.com"),
        #     action="dms:StartReplicationTask",
        # )


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
        self.write_dynamodb_stream_to_s3_lambda.add_event_source(
            event_sources.DynamoEventSource(self.dynamodb_table,
            starting_position=_lambda.StartingPosition.LATEST,
            # filters=[{"event_name": _lambda.FilterRule.is_equal("INSERT")}]
        ))
        self.write_dynamodb_stream_to_s3_lambda.add_environment(
            key="S3_FOR_DYNAMODB_STREAM_TO_REDSHIFT",
            value=self.cdc_from_dynamodb_to_redshift_s3_bucket.bucket_name,
        )
        self.cdc_from_dynamodb_to_redshift_s3_bucket.grant_write(self.write_dynamodb_stream_to_s3_lambda)

        self.scheduled_eventbridge_event.add_target(
            target=events_targets.LambdaFunction(
                handler=self.load_s3_files_from_dynamodb_stream_to_redshift_lambda, retry_attempts=3,
                ### then put in DLQ
            ),
        )
        self.load_s3_files_from_dynamodb_stream_to_redshift_lambda.add_environment(
            key="REDSHIFT_ENDPOINT_ADDRESS",
            value=self.redshift_cluster.attr_endpoint_address,  # appears self.redshift_cluster.attr_id is broken
        )
        self.load_s3_files_from_dynamodb_stream_to_redshift_lambda.add_environment(
            key="REDSHIFT_ROLE_ARN",
            value=self.redshift_full_commands_full_access_role.role_arn,
        )
        self.load_s3_files_from_dynamodb_stream_to_redshift_lambda.add_environment(
            key="S3_FOR_DYNAMODB_STREAM_TO_REDSHIFT",
            value=self.cdc_from_dynamodb_to_redshift_s3_bucket.bucket_name,
        )
        self.cdc_from_dynamodb_to_redshift_s3_bucket.grant_read_write(
            self.load_s3_files_from_dynamodb_stream_to_redshift_lambda
        )

        self.scheduled_eventbridge_event.add_target(
            target=events_targets.LambdaFunction(
                handler=self.load_data_to_rds_lambda, retry_attempts=3,
                ### then put in DLQ
            ),
        )
        self.load_data_to_rds_lambda.add_environment(
            key="RDS_HOST", value=self.rds_instance.db_instance_endpoint_address
        )


        self.scheduled_eventbridge_event.add_target(
            target=events_targets.LambdaFunction(
                handler=self.start_dms_replication_task_lambda, retry_attempts=3,
                ### then put in DLQ
            ),
        )
        self.start_dms_replication_task_lambda.add_environment(
            key="DMS_REPLICATION_TASK_ARN", value=self.dms_replication_task.ref  # appears `ref` means arn
        )
