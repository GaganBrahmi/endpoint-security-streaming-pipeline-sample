#!/bin/bash
set -e

echo "============================================================"
echo "  Deploy: MSK + Lambda + Flink Streaming (Amazon MSF)"
echo "============================================================"
echo ""

export AWS_PROFILE=${AWS_PROFILE:-default}
export AWS_REGION=${AWS_REGION:-us-east-1}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMMON_DIR="$SCRIPT_DIR/common"

# ── Step 1: MSK ──
echo "━━━ Step 1/3: MSK Serverless ━━━"
if [ -f "$COMMON_DIR/.env.msk" ]; then
    source "$COMMON_DIR/.env.msk"
    MSK_STATE=$(aws kafka describe-cluster-v2 \
        --cluster-arn "$MSK_CLUSTER_ARN" \
        --region "$AWS_REGION" \
        --query 'ClusterInfo.State' \
        --output text 2>/dev/null || echo "NOT_FOUND")
    if [ "$MSK_STATE" = "ACTIVE" ]; then
        echo "  ✅ MSK already deployed and ACTIVE — skipping"
        echo "     Cluster: $MSK_CLUSTER_ARN"
        echo ""
    else
        echo "  ⚠️  MSK env exists but cluster state is $MSK_STATE — redeploying"
        "$SCRIPT_DIR/common/scripts/deploy_msk.sh"
        echo ""
    fi
else
    "$SCRIPT_DIR/common/scripts/deploy_msk.sh"
    echo ""
fi

# ── Step 2: Lambda ──
echo "━━━ Step 2/3: Lambda Data Generator ━━━"
if [ -f "$COMMON_DIR/.env.lambda" ]; then
    source "$COMMON_DIR/.env.lambda"
    LAMBDA_STATE=$(aws lambda get-function \
        --function-name "$LAMBDA_FUNCTION" \
        --region "$AWS_REGION" \
        --query 'Configuration.State' \
        --output text 2>/dev/null || echo "NOT_FOUND")
    LAMBDA_STATE_UPPER=$(echo "$LAMBDA_STATE" | tr '[:lower:]' '[:upper:]')
    if [ "$LAMBDA_STATE_UPPER" = "ACTIVE" ]; then
        echo "  ✅ Lambda already deployed and Active — skipping"
        echo "     Function: $LAMBDA_FUNCTION"
        echo ""
    else
        echo "  ⚠️  Lambda env exists but function state is $LAMBDA_STATE — redeploying"
        "$SCRIPT_DIR/common/scripts/deploy_lambda.sh"
        echo ""
    fi
else
    "$SCRIPT_DIR/common/scripts/deploy_lambda.sh"
    echo ""
fi

# ── Step 3: Flink Streaming ──
echo "━━━ Step 3/3: Flink Streaming (Amazon MSF) ━━━"
cd "$SCRIPT_DIR/flink-streaming"
./scripts/deploy.sh
echo ""

echo "============================================================"
echo "  ✅ Full Stack Deployed: MSK + Lambda + Flink (Amazon MSF)"
echo "============================================================"
echo ""
echo "Database:       endpoint_security_flink"
echo "Start app:      cd flink-streaming && ./scripts/start_app.sh"
echo "Generate data:  ./common/scripts/generate_data.sh"
echo "Cleanup:        ./cleanup_flink.sh"
echo ""
