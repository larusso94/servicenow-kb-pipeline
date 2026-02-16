#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$SCRIPT_DIR/functions-kb"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
OUTPUT_ZIP="cu2-kb-sync-deployment-${TIMESTAMP}.zip"
TEMP_BUILD_DIR="/tmp/cu2-build-$$"

if [ ! -d "$SOURCE_DIR" ]; then
    echo "ERROR: Source directory not found: $SOURCE_DIR"
    exit 1
fi

mkdir -p "$TEMP_BUILD_DIR"
trap "rm -rf '$TEMP_BUILD_DIR'" EXIT

cd "$SOURCE_DIR"

INCLUDE_PATTERNS=(
    "host.json"
    "requirements.txt"
    "kb_sync_timer/"
    "shared/"
)

EXCLUDE_PATTERNS=(
    "local.settings.json"
    "*.pyc"
    "__pycache__/"
    "*.log"
    "*.tmp"
    ".git/"
    ".gitignore"
    ".vscode/"
    "*.zip"
)

for pattern in "${INCLUDE_PATTERNS[@]}"; do
    if [ -e "$pattern" ] || [ -d "$pattern" ]; then
        cp -r "$pattern" "$TEMP_BUILD_DIR/"
    fi
done

for pattern in "${EXCLUDE_PATTERNS[@]}"; do
    find "$TEMP_BUILD_DIR" -name "$pattern" -type f -delete 2>/dev/null || true
    find "$TEMP_BUILD_DIR" -name "$pattern" -type d -exec rm -rf {} + 2>/dev/null || true
done

find "$TEMP_BUILD_DIR" -name "*.pyc" -delete 2>/dev/null || true
find "$TEMP_BUILD_DIR" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

REQUIRED_FILES=(
    "host.json"
    "requirements.txt"
    "kb_sync_timer/__init__.py"
    "kb_sync_timer/function.json"
    "shared/config.py"
    "shared/cosmos_client.py"
    "shared/servicenow_client.py"
    "shared/embeddings.py"
    "shared/chunking.py"
    "shared/logging_client.py"
)

MISSING_FILES=()
for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$TEMP_BUILD_DIR/$file" ]; then
        MISSING_FILES+=("$file")
    fi
done

if [ ${#MISSING_FILES[@]} -ne 0 ]; then
    echo "ERROR: Missing required files:"
    for file in "${MISSING_FILES[@]}"; do
        echo "  - $file"
    done
    exit 1
fi

cd "$TEMP_BUILD_DIR"
zip -r "$SCRIPT_DIR/$OUTPUT_ZIP" . -x "*.DS_Store" "*.git*"

cd "$SCRIPT_DIR"
echo "Package created: ${OUTPUT_ZIP}"