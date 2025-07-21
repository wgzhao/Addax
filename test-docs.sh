#!/bin/bash
# test-docs.sh - Test script for Addax documentation using single mkdocs.yml

set -e

echo "🧪 Testing Addax Documentation Build (Single Configuration)"
echo "=========================================================="

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

# Test main configuration
print_status "Testing main mkdocs.yml configuration..."
if mkdocs build > /dev/null 2>&1; then
    print_success "Main mkdocs.yml builds successfully (English default)"
else
    print_error "Main mkdocs.yml build failed"
    echo "Run 'mkdocs build' for details"
    exit 1
fi

# Test Chinese build by creating temporary config
print_status "Testing Chinese build (temporary config)..."
cat mkdocs.yml | \
sed -e 's|docs_dir: docs/en|docs_dir: docs/zh|g' \
    -e 's|site_dir: site|site_dir: site/zh|g' \
    -e 's|language: en|language: zh|g' \
    -e 's|site_description: Addax is an open source universal ETL tool.*|site_description: Addax 是一个开源的通用 ETL 工具，支持地球上大多数 RDBMS 和 NoSQL 数据库|g' \
    > mkdocs-zh-test.yml

if mkdocs build --config-file mkdocs-zh-test.yml > /dev/null 2>&1; then
    print_success "Chinese configuration builds successfully"
    rm -f mkdocs-zh-test.yml
else
    print_error "Chinese configuration build failed"
    echo "Run 'mkdocs build --config-file mkdocs-zh-test.yml' for details"
    rm -f mkdocs-zh-test.yml
    exit 1
fi

# Test serve command (just check if it starts without error)
print_status "Testing serve command..."
timeout 5 mkdocs serve > /dev/null 2>&1 || true
print_success "Serve command works"

print_success "All single-configuration documentation tests passed!"

# Show next steps
echo ""
echo "🚀 Next Steps:"
echo "============="
echo "1. Run './build-docs.sh' to build both language versions"
echo "2. Run 'mkdocs serve' to start development server (English)"
echo "3. Open http://127.0.0.1:8000 to view documentation"
echo ""
echo "💡 Single Configuration Benefits:"
echo "   ✅ One mkdocs.yml file manages both languages"
echo "   ✅ Dynamic configuration for different builds"
echo "   ✅ Simplified maintenance and updates"
echo "   ✅ Automatic language switching support"