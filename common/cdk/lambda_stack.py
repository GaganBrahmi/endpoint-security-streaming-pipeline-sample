"""
CDK Stack: Lambda data generator for MSK.

Deploys a Lambda function that generates fake endpoint security events
and publishes them to MSK using IAM auth.

Depends on MSKStack outputs (reads them from CloudFormation exports or context).

Context variables:
    vpc_id (required): VPC ID
    subnet_ids (optional): Comma-separated subnet IDs
    msk_security_group_id (required): MSK cluster security group ID
    kafka_topic (optional): Kafka topic name (default: endpoint_logs)
"""
from aws_cdk import (
    Stack,
    CfnOutput,
    Duration,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_logs as logs,
)
from constructs import Construct
import os


class LambdaDataGenStack(Stack):
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
                "Deploy MSKStack first, then pass the security group ID."
            )

        kafka_topic = self.node.try_get_context("kafka_topic") or "endpoint_logs"

        # Import MSK security group
        msk_sg = ec2.SecurityGroup.from_security_group_id(
            self, "MSKSecurityGroup", msk_sg_id
        )

        # --- Lambda Security Group ---
        lambda_sg = ec2.SecurityGroup(
            self, "LambdaSG",
            vpc=vpc,
            description="Lambda data generator",
            allow_all_outbound=True,
        )

        # --- IAM Role ---
        role = iam.Role(
            self, "LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaVPCAccessExecutionRole"
                ),
            ],
        )

        # MSK IAM permissions
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kafka-cluster:Connect",
                    "kafka-cluster:DescribeCluster",
                    "kafka-cluster:DescribeTopic",
                    "kafka-cluster:CreateTopic",
                    "kafka-cluster:WriteData",
                    "kafka-cluster:ReadData",
                    "kafka-cluster:DescribeGroup",
                    "kafka-cluster:AlterGroup",
                    "kafka-cluster:AlterTopic",
                ],
                resources=[
                    f"arn:aws:kafka:{self.region}:{self.account}:cluster/streaming-demo-msk/*",
                    f"arn:aws:kafka:{self.region}:{self.account}:topic/streaming-demo-msk/*",
                    f"arn:aws:kafka:{self.region}:{self.account}:group/streaming-demo-msk/*",
                ],
            )
        )

        # --- Lambda Layer ---
        layer_path = os.path.join(os.path.dirname(__file__), "..", "lambda_layer")
        layer = _lambda.LayerVersion(
            self, "KafkaLayer",
            code=_lambda.Code.from_asset(layer_path),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            description="kafka-python-ng + aws-msk-iam-sasl-signer",
        )

        # --- Lambda Function ---
        lambda_code_path = os.path.join(os.path.dirname(__file__), "..", "lambda_function")
        fn = _lambda.Function(
            self, "DataGenerator",
            function_name="streaming-demo-data-generator",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="msk_data_generator.lambda_handler",
            code=_lambda.Code.from_asset(lambda_code_path),
            layers=[layer],
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnets=[
                    ec2.Subnet.from_subnet_id(self, f"Subnet{i}", sid)
                    for i, sid in enumerate(subnet_ids)
                ]
            ),
            # Attach both Lambda SG and MSK SG so Lambda is a member of the MSK SG
            security_groups=[lambda_sg, msk_sg],
            role=role,
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "KAFKA_TOPIC": kafka_topic,
                "NUM_EVENTS": "100",
                "NUM_CUSTOMERS": "5",
                "NUM_TENANTS": "3",
                # KAFKA_BOOTSTRAP_SERVERS set post-deploy by deploy script
            },
            log_retention=logs.RetentionDays.ONE_WEEK,
        )

        # --- Outputs ---
        CfnOutput(self, "LambdaFunctionName", value=fn.function_name)
        CfnOutput(self, "LambdaSecurityGroupId", value=lambda_sg.security_group_id)
        CfnOutput(self, "KafkaTopic", value=kafka_topic)
