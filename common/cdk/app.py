#!/usr/bin/env python3
import os
import aws_cdk as cdk

app = cdk.App()

# Determine which stacks to deploy based on context
deploy_msk = app.node.try_get_context("deploy_msk") or "true"
deploy_lambda = app.node.try_get_context("deploy_lambda") or "true"

env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT", os.environ.get("AWS_ACCOUNT_ID")),
    region=os.environ.get("CDK_DEFAULT_REGION", os.environ.get("AWS_REGION", "us-east-1")),
)

if deploy_msk == "true":
    from msk_stack import MSKStack
    MSKStack(app, "MSKStack", env=env, description="MSK Serverless cluster for streaming demos")

if deploy_lambda == "true":
    from lambda_stack import LambdaDataGenStack
    LambdaDataGenStack(app, "LambdaDataGenStack", env=env, description="Lambda data generator for MSK")

app.synth()
