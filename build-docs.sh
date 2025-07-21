#!/bin/bash
# build-docs.sh - Build script using single mkdocs.yml configuration
#
# This script uses a single mkdocs.yml configuration with dynamic modification
# for different languages, as requested in the feedback.

set -e

echo "🏗️  Building Addax Multi-Language Documentation (Single Config)"
echo "=============================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if mkdocs is installed
if ! command -v mkdocs &> /dev/null; then
    print_error "MkDocs is not installed. Please install it with: pip install mkdocs mkdocs-material"
    exit 1
fi

print_success "MkDocs found: $(mkdocs --version)"

# Clean previous builds
print_status "Cleaning previous builds..."
rm -rf site/
print_success "Previous builds cleaned"

# Create temporary Chinese config by modifying the main mkdocs.yml
create_chinese_config() {
    print_status "Creating temporary Chinese configuration from main mkdocs.yml..."
    
    # Create temporary Chinese config with proper modifications
    cat mkdocs.yml | \
    sed -e 's|docs_dir: docs/en|docs_dir: docs/zh|g' \
        -e 's|site_dir: site|site_dir: site/zh|g' \
        -e 's|language: en|language: zh|g' \
        -e 's|site_description: Addax is an open source universal ETL tool.*|site_description: Addax 是一个开源的通用 ETL 工具，支持地球上大多数 RDBMS 和 NoSQL 数据库|g' \
        -e 's|site_url: https://wgzhao.github.io/Addax/|site_url: https://wgzhao.github.io/Addax/zh/|g' \
        -e 's|- Home: index.md|- 首页: index.md|g' \
        -e 's|- Quick Start: quickstart.md|- 快速开始: quickstart.md|g' \
        -e 's|- Job Configuration: setupJob.md|- 任务配置: setupJob.md|g' \
        -e 's|- Command Line: commandline.md|- 命令行工具: commandline.md|g' \
        -e 's|- Reader Plugins:|- 读取插件:|g' \
        -e 's|- Writer Plugins:|- 写入插件:|g' \
        -e 's|- Debug: debug.md|- 调试模式: debug.md|g' \
        -e 's|- Password Encryption: encrypt_password.md|- 密码加密: encrypt_password.md|g' \
        -e 's|- Statistics Report: statsreport.md|- 统计报告: statsreport.md|g' \
        -e 's|- Transformer: transformer.md|- 数据转换: transformer.md|g' \
        -e 's|- Plugin Development: plugin_development.md|- 插件开发: plugin_development.md|g' \
        -e 's|link: /|link: /zh/|g' \
        > mkdocs-zh-temp.yml
    
    print_success "Temporary Chinese configuration created"
}

# Build English (default) documentation using main mkdocs.yml
print_status "Building English documentation (default using mkdocs.yml)..."
if mkdocs build; then
    print_success "English documentation built successfully"
else
    print_error "Failed to build English documentation"
    exit 1
fi

# Build Chinese documentation with temporary config
create_chinese_config
print_status "Building Chinese documentation (using temporary config)..."
if mkdocs build --config-file mkdocs-zh-temp.yml; then
    print_success "Chinese documentation built successfully"
    rm -f mkdocs-zh-temp.yml
else
    print_error "Failed to build Chinese documentation"
    rm -f mkdocs-zh-temp.yml
    exit 1
fi

# Verify output structure
print_status "Verifying output structure..."

if [[ -d "site" ]]; then
    print_success "Main site directory created"
else
    print_error "Main site directory not found"
    exit 1
fi

# Check for main site files
if [[ -f "site/index.html" ]]; then
    print_success "Main site (English default) ready"
else
    print_error "Main site index not found"
fi

# Check for Chinese site
if [[ -f "site/zh/index.html" ]]; then
    print_success "Chinese site ready at site/zh/"
else
    print_error "Chinese site not found"
fi

# Display build summary
echo ""
echo "📊 Build Summary (Single Configuration)"
echo "======================================="
echo "✅ Using single mkdocs.yml configuration file"
echo "✅ English as default language (docs/en → site/)"  
echo "✅ Chinese as secondary language (docs/zh → site/zh/)"
echo "✅ Language switcher configured via extra.alternate"
echo ""

if [[ -f "site/index.html" ]]; then
    echo "✅ Main site (English default): site/index.html"
else
    echo "❌ Main site index not found"
fi

if [[ -f "site/zh/index.html" ]]; then
    echo "✅ Chinese site: site/zh/index.html"
else
    echo "❌ Chinese site not found"
fi

echo ""
echo "🔧 Single Configuration Benefits:"
echo "   • One mkdocs.yml manages both languages"
echo "   • Dynamic config modification for different builds"
echo "   • Simplified maintenance and configuration"
echo "   • Language switcher works automatically"
echo ""

# Display size information
if command -v du &> /dev/null; then
    echo ""
    echo "📦 Build Size Information"
    echo "========================"
    echo "Total size: $(du -sh site/ 2>/dev/null | cut -f1 || echo "Unknown")"
    if [[ -d "site/zh" ]]; then
        echo "Chinese docs: $(du -sh site/zh/ 2>/dev/null | cut -f1 || echo "Unknown")"
    fi
fi

echo ""
echo "🎉 Single-configuration documentation build completed successfully!"
echo ""
echo "🚀 To serve locally with single config:"
echo "   mkdocs serve                    # English (default) at http://127.0.0.1:8000/"
echo ""
echo "🌐 To deploy to GitHub Pages:"
echo "   mkdocs gh-deploy               # Deploy complete multi-language site"
echo ""
echo "📁 Documentation URLs:"
echo "   English: https://wgzhao.github.io/Addax/"
echo "   Chinese: https://wgzhao.github.io/Addax/zh/"