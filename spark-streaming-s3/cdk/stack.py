"""
CDK Stack: EMR Serverless for Spark Streaming to Standard S3 (Iceberg).

Creates infrastructure for writing Iceberg tables to a standard S3 bucket
using GlueCatalog for metadata management:
- Standard S3 bucket (Iceberg warehouse)
- EMR Serverless application (emr-7.12.0)
- IAM role with S3, Glue, MSK permissions
- Security group with MSK connectivity

No S3 Tables, no Lake Formation, no REST catalog needed.
MSK and Lambda data generator are deployed separately via common/scripts/.

Context variables:
    vpc_id (required): VPC ID
    subnet_ids (optional): Comma-separated subnet IDs
    msk_security_group_id (required): MSK cluster security group ID
    namespace (optional): Glue database name (default: endpoint_security)
"""
from aws_cdk import (
    Stack,
    CfnOutput,
    RemovalPolicy,
    aws_s3 as s3,
    aws_emrserverless as emr,
    aws_iam as iam,
    aws_ec2 as ec2,
)
from constructs import Construct
import time


class SparkStreamingS3Stack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- Context ---
        namespace_name = self.node.try_get_context("namespace") or "endpoint_security"

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

        # --- S3 Bucket (Iceberg warehouse) ---
        timestamp = str(int(time.time()))
        warehouse_bucket = s3.Bucket(
            self,
            "WarehouseBucket",
            bucket_name=f"spark-streaming-iceberg-{self.account}-{self.region}-{timestamp}",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # --- Security Groups ---
        emr_sg = ec2.SecurityGroup(
            self, "EMRSecurityGroup",
            vpc=vpc,
            description="EMR Serverless Spark Streaming to S3",
            allow_all_outbound=True,
        )

        msk_sg = ec2.SecurityGroup.from_security_group_id(
            self, "MSKSecurityGroup", msk_sg_id
        )

        # --- IAM Role ---
        emr_role = iam.Role(
            self, "EMRServerlessRole",
            assumed_by=iam.ServicePrincipal("emr-serverless.amazonaws.com"),
            description="Execution role for Spark Streaming to S3 on EMR Serverless",
        )

        # S3 permissions for Iceberg warehouse bucket
        warehouse_bucket.grant_read_write(emr_role)

        # S3 permissions for scripts bucket
        emr_role.add_to_policy(iam.PolicyStatement(
            actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
            resources=[
                f"arn:aws:s3:::spark-streaming-s3-scripts-{self.region}-*",
                f"arn:aws:s3:::spark-streaming-s3-scripts-{self.region}-*/*",
            ],
        ))

        # S3 permissions for checkpoints bucket
        emr_role.add_to_policy(iam.PolicyStatement(
            actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
            resources=[
                f"arn:aws:s3:::spark-streaming-s3-checkpoints-{self.region}-*",
                f"arn:aws:s3:::spark-streaming-s3-checkpoints-{self.region}-*/*",
            ],
        ))

        # Glue Data Catalog permissions (standard catalog, no S3 Tables)
        emr_role.add_to_policy(iam.PolicyStatement(
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
                f"arn:aws:glue:{self.region}:{self.account}:database/{namespace_name}",
                f"arn:aws:glue:{self.region}:{self.account}:table/{namespace_name}/*",
            ],
        ))

        # MSK / Kafka (IAM auth)
        emr_role.add_to_policy(iam.PolicyStatement(
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

        # --- EMR Serverless Application ---
        emr_app = emr.CfnApplication(
            self, "EMRServerlessApp",
            name="spark-streaming-s3",
            release_label="emr-7.12.0",
            type="Spark",
            maximum_capacity=emr.CfnApplication.MaximumAllowedResourcesProperty(
                cpu="8 vCPU", memory="32 GB",
            ),
            auto_start_configuration=emr.CfnApplication.AutoStartConfigurationProperty(
                enabled=True,
            ),
            auto_stop_configuration=emr.CfnApplication.AutoStopConfigurationProperty(
                enabled=True, idle_timeout_minutes=30,
            ),
            network_configuration=emr.CfnApplication.NetworkConfigurationProperty(
                subnet_ids=subnet_ids,
                security_group_ids=[emr_sg.security_group_id, msk_sg_id],
            ),
        )

        # --- Outputs ---
        CfnOutput(self, "WarehouseBucketName", value=warehouse_bucket.bucket_name)
        CfnOutput(self, "WarehouseBucketArn", value=warehouse_bucket.bucket_arn)
        CfnOutput(self, "WarehouseLocation",
                  value=f"s3://{warehouse_bucket.bucket_name}/iceberg/",
                  description="Iceberg warehouse location for GlueCatalog")
        CfnOutput(self, "DatabaseName", value=namespace_name)
        CfnOutput(self, "EMRApplicationId", value=emr_app.attr_application_id)
        CfnOutput(self, "EMRRoleArn", value=emr_role.role_arn)
        CfnOutput(self, "SecurityGroupId", value=emr_sg.security_group_id)
        CfnOutput(self, "SubnetIds", value=",".join(subnet_ids))
