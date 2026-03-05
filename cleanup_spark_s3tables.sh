#!/bin/bash
set -e

echo "============================================================"
echo "  Cleanup: MSK + Lambda + Spark Streaming (S3 Tables)"
echo "============================================================"
echo ""
echo "Teardown order: Spark → Lambda → MSK"
echo ""

export AWS_PROFILE=${AWS_PROFILE:-default}
export AWS_REGION=${AWS_REGION:-us-east-1}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Step 1: Cleanup Spark Streaming (S3 Tables)
echo "━━━ Step 1/3: Spark Streaming (S3 Tables) ━━━"
cd "$SCRIPT_DIR/spark-streaming-s3tables"
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
