#!/usr/bin/env python3
import os
import aws_cdk as cdk
from stack import SparkStreamingS3Stack

app = cdk.App()

SparkStreamingS3Stack(
    app,
    "SparkStreamingS3Stack",
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT", os.environ.get("AWS_ACCOUNT_ID")),
        region=os.environ.get("CDK_DEFAULT_REGION", os.environ.get("AWS_REGION", "us-east-1")),
    ),
    description="Spark Streaming to Standard S3 (Iceberg) via EMR Serverless with Glue Catalog",
)

app.synth()
