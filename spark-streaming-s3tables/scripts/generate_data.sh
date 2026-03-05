#!/bin/bash
# Convenience wrapper — delegates to the shared data generator
COMMON_DIR="$(dirname "$0")/../../common"
exec "$COMMON_DIR/scripts/generate_data.sh" "$@"
