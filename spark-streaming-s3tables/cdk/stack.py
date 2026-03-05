"""
CDK Stack: EMR Serverless for Spark Streaming to S3 Tables.

Creates the full infrastructure matching the working s3TableBucketDemo stack:
- S3 Table Bucket with resource policy
- EMR Serverless application (emr-7.12.0)
- IAM role with S3 Tables, Glue Iceberg REST, Lake Formation, MSK permissions
- Security group with MSK connectivity

MSK and Lambda data generator are deployed separately via common/scripts/.
Lake Formation catalog permissions are granted via deploy.sh (AWS CLI).

Context variables:
    vpc_id (required): VPC ID
    subnet_ids (optional): Comma-separated subnet IDs
    msk_security_group_id (required): MSK cluster security group ID
    namespace (optional): S3 Tables namespace (default: endpoint_security)
"""
from aws_cdk import (
    Stack,
    CfnOutput,
    aws_s3tables as s3tables,
    aws_emrserverless as emr,
    aws_iam as iam,
    aws_ec2 as ec2,
)
from constructs import Construct
import time


class SparkStreamingStack(Stack):
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

        # --- S3 Table Bucket (matching working s3TableBucketDemo) ---
        timestamp = str(int(time.time()))
        table_bucket = s3tables.CfnTableBucket(
            self,
            "TableBucket",
            table_bucket_name=f"spark-streaming-tables-{self.account}-{self.region}-{timestamp}",
        )

        table_bucket_name = table_bucket.table_bucket_name
        table_bucket_arn = (
            f"arn:aws:s3tables:{self.region}:{self.account}:bucket/{table_bucket_name}"
        )

        # Table Bucket Policy — grants s3tables actions to the account root
        # (identical to working s3TableBucketDemo stack)
        table_bucket_policy = s3tables.CfnTableBucketPolicy(
            self,
            "TableBucketPolicy",
            table_bucket_arn=table_bucket_arn,
            resource_policy={
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": f"arn:aws:iam::{self.account}:root"},
                        "Action": [
                            "s3tables:GetTable",
                            "s3tables:GetTableMetadataLocation",
                            "s3tables:GetTableBucket",
                            "s3tables:CreateTable",
                            "s3tables:DeleteTable",
                            "s3tables:UpdateTableMetadataLocation",
                            "s3tables:PutTableData",
                            "s3tables:GetTableData",
                            "s3tables:CreateNamespace",
                            "s3tables:GetNamespace",
                        ],
                        "Resource": [
                            table_bucket_arn,
                            f"{table_bucket_arn}/*",
                        ],
                    }
                ]
            },
        )
        table_bucket_policy.add_dependency(table_bucket)

        # --- Security Groups ---
        emr_sg = ec2.SecurityGroup(
            self, "EMRSecurityGroup",
            vpc=vpc,
            description="EMR Serverless Spark Streaming",
            allow_all_outbound=True,
        )

        msk_sg = ec2.SecurityGroup.from_security_group_id(
            self, "MSKSecurityGroup", msk_sg_id
        )

        # --- IAM Role ---
        emr_role = iam.Role(
            self, "EMRServerlessRole",
            assumed_by=iam.ServicePrincipal("emr-serverless.amazonaws.com"),
            description="Execution role for Spark Streaming on EMR Serverless",
        )

        # S3 Tables permissions
        emr_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "s3tables:GetTable", "s3tables:GetTableMetadataLocation",
                "s3tables:GetTableBucket", "s3tables:CreateTable",
                "s3tables:DeleteTable", "s3tables:UpdateTableMetadataLocation",
                "s3tables:PutTableData", "s3tables:GetTableData",
                "s3tables:CreateNamespace", "s3tables:GetNamespace",
                "s3tables:ListTables", "s3tables:ListNamespaces",
            ],
            resources=[table_bucket_arn, f"{table_bucket_arn}/*"],
        ))

        # S3 permissions for underlying data access (matching working stack)
        emr_role.add_to_policy(iam.PolicyStatement(
            actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
            resources=[
                f"arn:aws:s3:::{table_bucket_name}",
                f"arn:aws:s3:::{table_bucket_name}/*",
            ],
        ))

        # S3 permissions for scripts bucket
        emr_role.add_to_policy(iam.PolicyStatement(
            actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
            resources=[
                f"arn:aws:s3:::spark-streaming-scripts-{self.region}-*",
                f"arn:aws:s3:::spark-streaming-scripts-{self.region}-*/*",
            ],
        ))

        # S3 permissions for checkpoints bucket
        emr_role.add_to_policy(iam.PolicyStatement(
            actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
            resources=[
                f"arn:aws:s3:::spark-streaming-checkpoints-{self.region}-*",
                f"arn:aws:s3:::spark-streaming-checkpoints-{self.region}-*/*",
            ],
        ))

        # Glue Catalog permissions (matching working stack exactly)
        emr_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "glue:GetCatalog",
                "glue:GetDatabase", "glue:GetDatabases",
                "glue:CreateDatabase",
                "glue:CreateTable", "glue:GetTable", "glue:GetTables",
                "glue:UpdateTable", "glue:DeleteTable",
                "glue:GetPartition", "glue:GetPartitions",
                "glue:CreatePartition", "glue:UpdatePartition", "glue:DeletePartition",
                "glue:BatchCreatePartition", "glue:BatchDeletePartition", "glue:BatchGetPartition",
            ],
            resources=[
                f"arn:aws:glue:{self.region}:{self.account}:catalog",
                f"arn:aws:glue:{self.region}:{self.account}:catalog/s3tablescatalog",
                f"arn:aws:glue:{self.region}:{self.account}:catalog/s3tablescatalog/*",
                f"arn:aws:glue:{self.region}:{self.account}:database/s3tablescatalog/*",
                f"arn:aws:glue:{self.region}:{self.account}:table/s3tablescatalog/*/*",
            ],
        ))

        # Lake Formation permissions
        emr_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "lakeformation:GetDataAccess",
                "lakeformation:GrantPermissions",
                "lakeformation:RevokePermissions",
                "lakeformation:ListPermissions",
            ],
            resources=["*"],
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
            name="spark-streaming-s3tables",
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
        CfnOutput(self, "TableBucketName", value=table_bucket_name)
        CfnOutput(self, "TableBucketArn", value=table_bucket_arn)
        CfnOutput(self, "NamespaceName", value=namespace_name)
        CfnOutput(self, "EMRApplicationId", value=emr_app.attr_application_id)
        CfnOutput(self, "EMRRoleArn", value=emr_role.role_arn)
        CfnOutput(self, "SecurityGroupId", value=emr_sg.security_group_id)
        CfnOutput(self, "SubnetIds", value=",".join(subnet_ids))
        CfnOutput(
            self, "S3TablesCatalogId",
            value=f"{self.account}:s3tablescatalog/{table_bucket_name}",
            description="S3 Tables catalog ID for Lake Formation grants",
        )
