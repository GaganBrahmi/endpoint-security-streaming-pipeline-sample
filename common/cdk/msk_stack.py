"""
CDK Stack: MSK Serverless cluster with IAM auth.

Shared infrastructure used by both Spark Streaming and Flink consumers.

Context variables:
    vpc_id (required): VPC ID
    subnet_ids (optional): Comma-separated subnet IDs (defaults to public subnets)
"""
from aws_cdk import (
    Stack,
    CfnOutput,
    aws_ec2 as ec2,
    aws_msk as msk,
)
from constructs import Construct


class MSKStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- VPC ---
        vpc_id = self.node.try_get_context("vpc_id")
        if not vpc_id:
            raise ValueError("Missing required CDK context 'vpc_id'.")
        vpc = ec2.Vpc.from_lookup(self, "VPC", vpc_id=vpc_id)

        subnet_ids_ctx = self.node.try_get_context("subnet_ids")
        if subnet_ids_ctx:
            subnet_ids = subnet_ids_ctx.split(",")
        else:
            subnet_ids = [s.subnet_id for s in vpc.public_subnets]

        # MSK Serverless requires at least 2 subnets in different AZs
        if len(subnet_ids) < 2:
            raise ValueError(
                f"MSK Serverless requires at least 2 subnets (got {len(subnet_ids)}). "
                "Pass 2+ subnets in different AZs: -c subnet_ids=subnet-aaa,subnet-bbb"
            )

        # --- Security Group ---
        self.msk_sg = ec2.SecurityGroup(
            self, "MSKSecurityGroup",
            vpc=vpc,
            description="MSK Serverless cluster",
            allow_all_outbound=True,
        )

        # Allow self-referencing traffic on port 9098 (IAM auth).
        # All consumers (EMR, Lambda, Flink) attach this SG so they
        # can reach MSK without extra ingress rules.
        self.msk_sg.add_ingress_rule(
            self.msk_sg,
            ec2.Port.tcp(9098),
            "Self-referencing: MSK SG members can access Kafka IAM endpoint",
        )

        # --- MSK Serverless ---
        cluster = msk.CfnServerlessCluster(
            self, "MSKServerless",
            cluster_name="streaming-demo-msk",
            client_authentication=msk.CfnServerlessCluster.ClientAuthenticationProperty(
                sasl=msk.CfnServerlessCluster.SaslProperty(
                    iam=msk.CfnServerlessCluster.IamProperty(enabled=True)
                )
            ),
            vpc_configs=[
                msk.CfnServerlessCluster.VpcConfigProperty(
                    subnet_ids=subnet_ids,
                    security_groups=[self.msk_sg.security_group_id],
                )
            ],
        )

        # --- Outputs ---
        CfnOutput(self, "MSKClusterArn", value=cluster.attr_arn)
        CfnOutput(self, "MSKSecurityGroupId", value=self.msk_sg.security_group_id)
        CfnOutput(self, "VPCId", value=vpc.vpc_id)
        CfnOutput(self, "SubnetIds", value=",".join(subnet_ids))
