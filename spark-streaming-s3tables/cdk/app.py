#!/usr/bin/env python3
import os
import aws_cdk as cdk
from stack import SparkStreamingStack

app = cdk.App()

SparkStreamingStack(
    app,
    "SparkStreamingStack",
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT", os.environ.get("AWS_ACCOUNT_ID")),
        region=os.environ.get("CDK_DEFAULT_REGION", os.environ.get("AWS_REGION", "us-east-1")),
    ),
    description="Spark Streaming to S3 Tables via EMR Serverless with Lake Formation governance",
)

app.synth()
