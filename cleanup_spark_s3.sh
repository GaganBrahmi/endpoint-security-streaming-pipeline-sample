#!/bin/bash
set -e

echo "============================================================"
echo "  Cleanup: MSK + Lambda + Spark Streaming (Standard S3)"
echo "============================================================"
echo ""
echo "Teardown order: Spark → Lambda → MSK"
echo ""

export AWS_PROFILE=${AWS_PROFILE:-default}
export AWS_REGION=${AWS_REGION:-us-east-1}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Step 1: Cleanup Spark Streaming (Standard S3)
echo "━━━ Step 1/3: Spark Streaming (Standard S3) ━━━"
cd "$SCRIPT_DIR/spark-streaming-s3"
./scripts/cleanup.sh
echo ""

# Step 2: Cleanup Lambda
echo "━━━ Step 2/3: Lambda Data Generator ━━━"
"$SCRIPT_DIR/common/scripts/cleanup_lambda.sh"
echo ""

# Step 3: Cleanup MSK
echo "━━━ Step 3/3: MSK Serverless ━━━"
"$SCRIPT_DIR/common/scripts/cleanup_msk.sh"
echo ""

echo "============================================================"
echo "  ✅ Full Stack Cleaned Up"
echo "============================================================"
echo ""
