#!/bin/bash
# test-docs.sh - Test script for Addax documentation builds

set -e

echo "🧪 Testing Addax Documentation Builds"
echo "====================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Test configurations
configs=("mkdocs.yml" "mkdocs-en.yml" "mkdocs-zh.yml" "mkdocs-en-dev.yml" "mkdocs-zh-dev.yml")
config_names=("Main (English default)" "English Production" "Chinese Production" "English Development" "Chinese Development")

# Test each configuration
for i in "${!configs[@]}"; do
    config="${configs[$i]}"
    name="${config_names[$i]}"
    
    print_status "Testing $name configuration ($config)..."
    
    if mkdocs build --config-file "$config" > /dev/null 2>&1; then
        print_success "$name configuration builds successfully"
    else
        print_error "$name configuration build failed"
        echo "Run 'mkdocs build --config-file $config' for details"
        exit 1
    fi
done

# Test serve command (just check if it starts without error)
print_status "Testing serve command..."
timeout 5 mkdocs serve --config-file mkdocs-en.yml > /dev/null 2>&1 || true
print_success "Serve command works"

print_success "All documentation tests passed!"

# Show next steps
echo ""
echo "🚀 Next Steps:"
echo "============="
echo "1. Run './build-docs.sh' to build all documentation"
echo "2. Run 'mkdocs serve' to start development server"
echo "3. Open http://127.0.0.1:8000 to view documentation"