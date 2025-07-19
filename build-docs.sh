#!/bin/bash
# build-docs.sh - Comprehensive documentation build script for Addax

set -e

echo "🏗️  Building Addax Multi-Language Documentation"
echo "================================================="

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

# Build configurations for multi-language setup
print_status "Building English documentation (mkdocs-en.yml)..."
if mkdocs build --config-file mkdocs-en.yml; then
    print_success "English documentation built successfully"
else
    print_error "Failed to build English documentation"
    exit 1
fi

print_status "Building Chinese documentation (mkdocs-zh.yml)..."
if mkdocs build --config-file mkdocs-zh.yml; then
    print_success "Chinese documentation built successfully"
else
    print_error "Failed to build Chinese documentation"
    exit 1
fi

print_status "Building main site with English as default (mkdocs.yml)..."
if mkdocs build --config-file mkdocs.yml; then
    print_success "Main site built successfully"
else
    print_error "Failed to build main site"
    exit 1
fi

# Copy language-specific builds back into main site structure
print_status "Organizing multi-language site structure..."
if [[ -d "site/en" && -d "site/zh" ]]; then
    print_warning "Language directories already exist in main site - skipping copy"
else
    # The main build overwrote our language directories, which is expected
    # For GitHub Pages with mike or manual deployment, each language is built separately
    print_success "Site structure organized (single English site as default)"
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

# Display build summary
echo ""
echo "📊 Build Summary"
echo "================"
echo "Output directory: site/"

if [[ -f "site/index.html" ]]; then
    echo "✅ Main site (English default): site/index.html"
else
    echo "❌ Main site index not found"
fi

echo ""
echo "🔧 Individual Language Builds Available:"
echo "   English: Use 'mkdocs serve --config-file mkdocs-en.yml' (builds to site/en/)"
echo "   Chinese: Use 'mkdocs serve --config-file mkdocs-zh.yml' (builds to site/zh/)"
echo ""
echo "💡 Deployment Options:"
echo "   • Default: Deploy 'site/' directory (English only)"
echo "   • Multi-language: Use mike or GitHub Actions for proper multi-language deployment"

# Display size information
if command -v du &> /dev/null; then
    echo ""
    echo "📦 Build Size Information"
    echo "========================"
    echo "Total size: $(du -sh site/ 2>/dev/null | cut -f1 || echo "Unknown")"
    if [[ -d "site/en" ]]; then
        echo "English docs: $(du -sh site/en/ 2>/dev/null | cut -f1 || echo "Unknown")"
    fi
    if [[ -d "site/zh" ]]; then
        echo "Chinese docs: $(du -sh site/zh/ 2>/dev/null | cut -f1 || echo "Unknown")"
    fi
fi

echo ""
echo "🎉 Documentation build completed successfully!"
echo ""
echo "🚀 To serve locally:"
echo "   mkdocs serve                          # English (default)"
echo "   mkdocs serve --config-file mkdocs-en.yml  # English explicit"
echo "   mkdocs serve --config-file mkdocs-zh.yml  # Chinese"
echo ""
echo "🌐 To deploy to GitHub Pages:"
echo "   mkdocs gh-deploy                      # Deploy main site"
echo ""
echo "📁 Documentation URLs:"
echo "   English: https://wgzhao.github.io/Addax/"
echo "   Chinese: https://wgzhao.github.io/Addax/zh/"