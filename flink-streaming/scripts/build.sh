#!/bin/bash
set -e

echo "=== Building PyFlink Application ==="
echo ""

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FLINK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$FLINK_DIR"

# Check prerequisites
echo "Checking prerequisites..."

if ! command -v java &> /dev/null; then
    echo "ERROR: Java not found. Install JDK 11+."
    exit 1
fi
JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
echo "  Java $JAVA_VERSION"

if ! command -v mvn &> /dev/null; then
    echo "ERROR: Maven not found. Install Apache Maven."
    exit 1
fi
MVN_VERSION=$(mvn -version 2>&1 | head -n 1 | awk '{print $3}')
echo "  Maven $MVN_VERSION"

if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 not found."
    exit 1
fi
PYTHON_VERSION=$(python3 --version | awk '{print $2}')
echo "  Python $PYTHON_VERSION"
echo ""

# Clean
echo "Cleaning previous builds..."
rm -rf target/ lib/ build/ *.zip

# Maven build (fat-jar + assembly zip)
echo "Building Maven package (fat-jar + zip)..."
mvn clean package -q

if [ ! -f "target/pyflink-dependencies.jar" ]; then
    echo "ERROR: JAR file not found after build."
    exit 1
fi

# The Maven assembly plugin creates the zip with Python files + lib/jar
MAVEN_ZIP="target/endpoint-security-flink-consumer-1.0.0.zip"
if [ ! -f "$MAVEN_ZIP" ]; then
    echo "ERROR: ZIP file not created by Maven assembly."
    exit 1
fi

# Copy to a known location
cp "$MAVEN_ZIP" flink-application.zip

ZIP_SIZE=$(du -h flink-application.zip | cut -f1)
echo ""
echo "✅ Build complete: flink-application.zip ($ZIP_SIZE)"
echo ""
echo "Contents:"
unzip -l flink-application.zip | grep -E '^\s+[0-9]' | awk '{print $NF}'
echo ""
echo "Next steps:"
echo "  1. Deploy:  ./scripts/deploy.sh   (builds + deploys infra + uploads code)"
echo "  2. Or if already deployed, update code only:  ./scripts/update_app.sh"
echo ""
