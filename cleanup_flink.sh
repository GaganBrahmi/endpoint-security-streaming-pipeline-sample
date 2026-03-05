#!/bin/bash
set -e

echo "============================================================"
echo "  Cleanup: MSK + Lambda + Flink Streaming (Amazon MSF)"
echo "============================================================"
echo ""
echo "Teardown order: Flink → Lambda → MSK"
echo ""

export AWS_PROFILE=${AWS_PROFILE:-default}
export AWS_REGION=${AWS_REGION:-us-east-1}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Step 1: Cleanup Flink Streaming
echo "━━━ Step 1/3: Flink Streaming (Amazon MSF) ━━━"
cd "$SCRIPT_DIR/flink-streaming"
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
