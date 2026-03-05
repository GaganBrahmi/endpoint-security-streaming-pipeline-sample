"""
CDK Stack: Amazon Managed Service for Apache Flink (MSF) for Endpoint Security Streaming.

Two-phase deployment (orchestrated by deploy.sh):
  Phase 1 (cdk deploy without -c deploy_app=true):
    Creates S3 artifact bucket, S3 warehouse bucket, IAM role,
    security group, CloudWatch log group.
    The MSF application is NOT created yet (no ZIP uploaded).

  Phase 2 (deploy.sh uploads ZIP, then cdk deploy -c deploy_app=true):
    Creates the MSF application pointing to the uploaded ZIP,
    with all runtime configuration properties.

Context variables:
    vpc_id (required): VPC ID
    subnet_ids (optional): Comma-separated subnet IDs
    msk_security_group_id (required): MSK cluster security group ID
    kafka_bootstrap_servers (required): MSK bootstrap servers
    deploy_app (optional): "true" to include the MSF application (Phase 2)
    kafka_topic (optional): Kafka topic name (default: endpoint_logs)
    database_name (optional): Glue database name (default: endpoint_security)
    table_name (optional): Iceberg table name (default: endpoint_data_flink)
    warehouse_path (optional): S3 warehouse path for Iceberg
"""
from aws_cdk import (
    Stack,
    CfnOutput,
    RemovalPolicy,
    aws_s3 as s3,
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_kinesisanalyticsv2 as kda,
    aws_logs as logs,
)
from constructs import Construct


class FlinkStreamingStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- Context ---
        vpc_id = self.node.try_get_context("vpc_id")
        if not vpc_id:
            raise ValueError("Missing required CDK context 'vpc_id'.")
        vpc = ec2.Vpc.from_lookup(self, "VPC", vpc_id=vpc_id)

        subnet_ids_ctx = self.node.try_get_context("subnet_ids")
        if subnet_ids_ctx:
            subnet_ids = subnet_ids_ctx.split(",")
        else:
            subnet_ids = [s.subnet_id for s in vpc.public_subnets]

        msk_sg_id = self.node.try_get_context("msk_security_group_id")
        if not msk_sg_id:
            raise ValueError(
                "Missing required CDK context 'msk_security_group_id'. "
                "Deploy MSK first via common/scripts/deploy_msk.sh"
            )

        kafka_bootstrap_servers = self.node.try_get_context("kafka_bootstrap_servers")
        if not kafka_bootstrap_servers:
            raise ValueError("Missing required CDK context 'kafka_bootstrap_servers'.")

        deploy_app = self.node.try_get_context("deploy_app") == "true"

        kafka_topic = self.node.try_get_context("kafka_topic") or "endpoint_logs"
        database_name = self.node.try_get_context("database_name") or "endpoint_security"
        table_name = self.node.try_get_context("table_name") or "endpoint_data_flink"

        # --- S3 Artifact Bucket (Flink app ZIP) ---
        artifact_bucket = s3.Bucket(
            self, "ArtifactBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # --- S3 Warehouse Bucket (Iceberg table data) ---
        warehouse_bucket = s3.Bucket(
            self, "WarehouseBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        warehouse_path = self.node.try_get_context("warehouse_path") or \
            f"s3://{warehouse_bucket.bucket_name}/iceberg/"

        # --- Security Group ---
        flink_sg = ec2.SecurityGroup(
            self, "FlinkSecurityGroup",
            vpc=vpc,
            description="Amazon MSF Flink Streaming",
            allow_all_outbound=True,
        )

        # --- CloudWatch Log Group ---
        log_group = logs.LogGroup(
            self, "FlinkLogGroup",
            removal_policy=RemovalPolicy.DESTROY,
        )
        log_stream = logs.LogStream(
            self, "FlinkLogStream",
            log_group=log_group,
            log_stream_name="flink-streaming",
        )

        # --- IAM Role ---
        flink_role = iam.Role(
            self, "FlinkServiceRole",
            assumed_by=iam.ServicePrincipal("kinesisanalytics.amazonaws.com"),
            description="Execution role for Flink Streaming on Amazon MSF",
        )

        # S3 permissions
        artifact_bucket.grant_read(flink_role)
        warehouse_bucket.grant_read_write(flink_role)

        # Glue Data Catalog — Iceberg database + Flink's default catalog
        flink_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase",
                "glue:CreateTable", "glue:GetTable", "glue:GetTables",
                "glue:UpdateTable", "glue:DeleteTable",
                "glue:GetPartition", "glue:GetPartitions",
                "glue:CreatePartition", "glue:UpdatePartition", "glue:DeletePartition",
                "glue:BatchCreatePartition", "glue:BatchDeletePartition", "glue:BatchGetPartition",
            ],
            resources=[
                f"arn:aws:glue:{self.region}:{self.account}:catalog",
                # Iceberg target database
                f"arn:aws:glue:{self.region}:{self.account}:database/{database_name}",
                f"arn:aws:glue:{self.region}:{self.account}:table/{database_name}/*",
                # Flink's default catalog (used for Kafka source table references)
                f"arn:aws:glue:{self.region}:{self.account}:database/default_catalog",
                f"arn:aws:glue:{self.region}:{self.account}:table/default_catalog/*",
                f"arn:aws:glue:{self.region}:{self.account}:database/default_database",
                f"arn:aws:glue:{self.region}:{self.account}:table/default_database/*",
            ],
        ))

        # MSK / Kafka (IAM auth)
        flink_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "kafka-cluster:Connect", "kafka-cluster:DescribeCluster",
                "kafka-cluster:DescribeTopic", "kafka-cluster:ReadData",
                "kafka-cluster:DescribeGroup", "kafka-cluster:AlterGroup",
            ],
            resources=[
                f"arn:aws:kafka:{self.region}:{self.account}:cluster/streaming-demo-msk/*",
                f"arn:aws:kafka:{self.region}:{self.account}:topic/streaming-demo-msk/*",
                f"arn:aws:kafka:{self.region}:{self.account}:group/streaming-demo-msk/*",
            ],
        ))

        # CloudWatch Logs
        flink_role.add_to_policy(iam.PolicyStatement(
            actions=["logs:PutLogEvents", "logs:DescribeLogGroups", "logs:DescribeLogStreams"],
            resources=[log_group.log_group_arn, f"{log_group.log_group_arn}:*"],
        ))

        # VPC permissions (ENI management)
        flink_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "ec2:DescribeVpcs", "ec2:DescribeSubnets", "ec2:DescribeSecurityGroups",
                "ec2:DescribeNetworkInterfaces", "ec2:CreateNetworkInterface",
                "ec2:CreateNetworkInterfacePermission", "ec2:DeleteNetworkInterface",
                "ec2:DescribeDhcpOptions",
            ],
            resources=["*"],
        ))

        # CloudWatch Metrics
        flink_role.add_to_policy(iam.PolicyStatement(
            actions=["cloudwatch:PutMetricData"],
            resources=["*"],
        ))

        # --- MSF Application (Phase 2 only — after ZIP is uploaded) ---
        app_s3_key = "flink-app/flink-application.zip"

        if deploy_app:
            msf_app = kda.CfnApplication(
                self, "FlinkApplication",
                runtime_environment="FLINK-1_20",
                service_execution_role=flink_role.role_arn,
                application_configuration=kda.CfnApplication.ApplicationConfigurationProperty(
                    application_code_configuration=kda.CfnApplication.ApplicationCodeConfigurationProperty(
                        code_content=kda.CfnApplication.CodeContentProperty(
                            s3_content_location=kda.CfnApplication.S3ContentLocationProperty(
                                bucket_arn=artifact_bucket.bucket_arn,
                                file_key=app_s3_key,
                            )
                        ),
                        code_content_type="ZIPFILE",
                    ),
                    environment_properties=kda.CfnApplication.EnvironmentPropertiesProperty(
                        property_groups=[
                            kda.CfnApplication.PropertyGroupProperty(
                                property_group_id="kinesis.analytics.flink.run.options",
                                property_map={
                                    "python": "flink_consumer_with_aggregations.py",
                                    "jarfile": "lib/pyflink-dependencies.jar",
                                    "pyFiles": "aggregation_config.py,table_creators.py,aggregation_jobs.py",
                                },
                            ),
                            kda.CfnApplication.PropertyGroupProperty(
                                property_group_id="consumer.config.0",
                                property_map={
                                    "kafka.bootstrap.servers": kafka_bootstrap_servers,
                                    "kafka.topic.name": kafka_topic,
                                    "kafka.consumer.group.id": "flink-endpoint-consumer",
                                    "warehouse.path": warehouse_path,
                                    "catalog.name": "glue_catalog",
                                    "database.name": database_name,
                                    "table.name": table_name,
                                    "aws.region": self.region,
                                    "enable.raw.events": "true",
                                    "enable.tumbling.windows": "true",
                                    "enable.sliding.windows": "true",
                                    "enable.session.windows": "true",
                                    "tumbling.window.minutes": "5,15,30,60",
                                    "sliding.window.size.minutes": "5",
                                    "sliding.window.slide.minutes": "1",
                                    "session.gap.minutes": "5",
                                },
                            ),
                        ]
                    ),
                    flink_application_configuration=kda.CfnApplication.FlinkApplicationConfigurationProperty(
                        checkpoint_configuration=kda.CfnApplication.CheckpointConfigurationProperty(
                            configuration_type="CUSTOM",
                            checkpointing_enabled=True,
                            checkpoint_interval=60000,
                            min_pause_between_checkpoints=5000,
                        ),
                        parallelism_configuration=kda.CfnApplication.ParallelismConfigurationProperty(
                            configuration_type="CUSTOM",
                            parallelism=1,
                            parallelism_per_kpu=1,
                            auto_scaling_enabled=False,
                        ),
                        monitoring_configuration=kda.CfnApplication.MonitoringConfigurationProperty(
                            configuration_type="CUSTOM",
                            metrics_level="APPLICATION",
                            log_level="INFO",
                        ),
                    ),
                    vpc_configurations=[
                        kda.CfnApplication.VpcConfigurationProperty(
                            subnet_ids=subnet_ids,
                            security_group_ids=[flink_sg.security_group_id, msk_sg_id],
                        )
                    ],
                ),
            )

            # CloudWatch logging
            kda.CfnApplicationCloudWatchLoggingOption(
                self, "FlinkLogging",
                application_name=msf_app.ref,
                cloud_watch_logging_option=kda.CfnApplicationCloudWatchLoggingOption.CloudWatchLoggingOptionProperty(
                    log_stream_arn=f"arn:aws:logs:{self.region}:{self.account}:"
                                   f"log-group:{log_group.log_group_name}:"
                                   f"log-stream:{log_stream.log_stream_name}",
                ),
            )

            CfnOutput(self, "ApplicationName", value=msf_app.ref)

        # --- Outputs (always available) ---
        CfnOutput(self, "ArtifactBucketName", value=artifact_bucket.bucket_name)
        CfnOutput(self, "WarehouseBucketName", value=warehouse_bucket.bucket_name)
        CfnOutput(self, "AppS3Key", value=app_s3_key)
        CfnOutput(self, "WarehousePath", value=warehouse_path)
        CfnOutput(self, "DatabaseName", value=database_name)
        CfnOutput(self, "FlinkRoleArn", value=flink_role.role_arn)
        CfnOutput(self, "LogGroupName", value=log_group.log_group_name)
        CfnOutput(self, "SecurityGroupId", value=flink_sg.security_group_id)
        CfnOutput(self, "SubnetIds", value=",".join(subnet_ids))
