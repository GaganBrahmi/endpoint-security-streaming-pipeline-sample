#!/usr/bin/env python3
import os
import aws_cdk as cdk
from stack import FlinkStreamingStack

app = cdk.App()

FlinkStreamingStack(
    app,
    "FlinkStreamingStack",
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT", os.environ.get("AWS_ACCOUNT_ID")),
        region=os.environ.get("CDK_DEFAULT_REGION", os.environ.get("AWS_REGION", "us-east-1")),
    ),
    description="PyFlink Streaming to Iceberg via Amazon MSF with Glue Catalog",
)

app.synth()
