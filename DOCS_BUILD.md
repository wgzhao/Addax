# Addax Documentation Build Guide (Single Configuration)

This guide provides comprehensive instructions for building and deploying the Addax multi-language documentation using a **single `mkdocs.yml` configuration file**.

## 🌟 Single Configuration Approach

As requested in the project feedback, we now use **one unified `mkdocs.yml`** file that:
- Defaults to English (`docs/en/` → `site/`)
- Supports Chinese through dynamic configuration (`docs/zh/` → `site/zh/`)
- Maintains language switching via `extra.alternate`
- Eliminates multiple config file maintenance

## Table of Contents

- [Prerequisites](#prerequisites)
- [Directory Structure](#directory-structure)
- [Quick Start](#quick-start)
- [Detailed Build Commands](#detailed-build-commands)
- [Deployment Strategies](#deployment-strategies)
- [Configuration Details](#configuration-details)
- [Testing and Validation](#testing--validation)
- [Troubleshooting](#troubleshooting)
- [Development Workflow](#development-workflow)
- [Key Advantages](#key-advantages)

## Prerequisites

### Required Software
```bash
# Install Python 3.7+
python3 --version

# Install MkDocs and Material theme
pip install mkdocs mkdocs-material

# Verify installation
mkdocs --version
```

### Optional Dependencies
```bash
# For enhanced build automation
pip install -r requirements-docs.txt
```

### 2. Install Documentation Dependencies
```bash
# Using pip
pip install -r requirements-docs.txt

## 📁 Directory Structure

```
Addax/
├── mkdocs.yml              # Single configuration file (English default)
├── build-docs.sh           # Automated build script
├── test-docs.sh           # Testing script
├── requirements-docs.txt   # Documentation dependencies
├── docs/
│   ├── en/                # English documentation (default)
│   │   ├── index.md
│   │   ├── quickstart.md
│   │   ├── reader/        # Reader plugins
│   │   └── writer/        # Writer plugins
│   ├── zh/                # Chinese documentation
│   │   ├── index.md
│   │   ├── quickstart.md
│   │   ├── reader/        # 读取插件
│   │   └── writer/        # 写入插件
│   ├── assets/            # Shared assets
│   └── images/            # Shared images
└── site/                  # Generated documentation
    ├── index.html         # English site (default)
    └── zh/                # Chinese site subdirectory
        └── index.html
```

## 🚀 Quick Start

### 1. Build All Documentation
```bash
# Automated build (recommended)
./build-docs.sh

# Manual build
mkdocs build                    # English (default)
```

### 2. Development Server
```bash
# Serve English documentation (default)
mkdocs serve
# Access: http://127.0.0.1:8000/

# The single config serves English by default
# Language switching happens via the UI
```

### 3. Test All Builds
```bash
./test-docs.sh
```

## 🔧 Detailed Build Commands

### Using Automated Scripts

#### Build Script (`./build-docs.sh`)
- Uses single `mkdocs.yml` configuration
- Builds English as default language
- Creates Chinese version through dynamic config modification
- Outputs to `site/` (English) and `site/zh/` (Chinese)

```bash
./build-docs.sh
```

#### Test Script (`./test-docs.sh`)
- Validates single configuration works for both languages
- Tests build processes
- Verifies serve functionality

```bash
./test-docs.sh
```

### Manual Build Process

#### English (Default)
```bash
# Uses mkdocs.yml as-is
mkdocs build

# Output: site/
# URL: https://wgzhao.github.io/Addax/
```

#### Chinese
```bash
# The build script automatically creates temporary Chinese config
# and builds to site/zh/

# For manual Chinese build, use the build script:
./build-docs.sh
```

## 🌐 Deployment Strategies

### Strategy 1: Single Language (English Only)
```bash
mkdocs build
mkdocs gh-deploy
```

### Strategy 2: Multi-Language (Recommended)
```bash
# Build both languages
./build-docs.sh

# Deploy with GitHub Pages
mkdocs gh-deploy

# Access:
# English: https://wgzhao.github.io/Addax/
# Chinese: https://wgzhao.github.io/Addax/zh/
```

### Strategy 3: GitHub Actions (Automated)
Create `.github/workflows/docs.yml`:
```yaml
name: Deploy Documentation
on:
  push:
    branches: [ main ]
    paths: [ 'docs/**', 'mkdocs.yml' ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: 3.x
      - name: Install dependencies
        run: pip install mkdocs mkdocs-material
      - name: Build documentation
        run: ./build-docs.sh
      - name: Deploy to GitHub Pages
        run: mkdocs gh-deploy --force
```

## ⚙️ Configuration Details

### Single Configuration Benefits

The unified `mkdocs.yml` provides:

1. **Simplified Maintenance**: One file to manage
2. **Dynamic Language Support**: Automatic config modification for Chinese
3. **Default English**: International-first approach
4. **Language Switching**: Built-in UI language toggle
5. **Shared Resources**: Assets and images shared between languages

### Language-Specific Settings

#### English (Default)
- **Source**: `docs/en/`
- **Output**: `site/`
- **Language**: `en`
- **URL**: Root path

#### Chinese (Dynamic)
- **Source**: `docs/zh/`
- **Output**: `site/zh/`
- **Language**: `zh`
- **URL**: `/zh/` subpath

### Navigation Structure

The single config includes navigation for both languages:
- English: Clear descriptive names
- Chinese: Localized section names (dynamically applied)

## 🔍 Testing and Validation

### Build Tests
```bash
# Test single configuration
./test-docs.sh

# Manual validation
mkdocs build --strict       # English with strict mode
```

### Serve Tests
```bash
# Test development server
mkdocs serve --dev-addr localhost:8000
# Visit: http://localhost:8000/

# Test language switching in browser
# Click language toggle in top navigation
```

### Content Validation
```bash
# Check English content
ls -la site/
curl -s http://127.0.0.1:8000/ | grep -i "addax"

# Check Chinese content (after build)
ls -la site/zh/
curl -s http://127.0.0.1:8000/zh/ | grep -i "addax"
```

## 🚨 Troubleshooting

### Common Issues

#### 1. Build Fails
```bash
# Check configuration syntax
mkdocs build --strict

# Verify file paths
ls -la docs/en/
ls -la docs/zh/
```

#### 2. Language Switching Not Working
- Verify `extra.alternate` configuration in `mkdocs.yml`
- Check that both `site/` and `site/zh/` exist after build
- Ensure proper URL configuration

#### 3. Missing Content
```bash
# Verify source directories
find docs/en -name "*.md" | wc -l
find docs/zh -name "*.md" | wc -l

# Check for translation gaps
diff -r docs/en docs/zh --brief | grep "Only in"
```

#### 4. Assets Not Loading
- Assets are shared from `docs/assets/` and `docs/images/`
- Verify paths in Markdown files use relative references
- Check `pymdownx.snippets` base_path configuration

### Debug Commands
```bash
# Verbose build
mkdocs build --verbose

# Configuration validation
mkdocs config-check

# Serve with debug
mkdocs serve --verbose --dev-addr 0.0.0.0:8000
```

## 📊 Performance and Optimization

### Build Performance
- Single config reduces build complexity
- Shared assets eliminate duplication
- Dynamic config generation is fast

### Size Optimization
- Shared images and assets
- Compressed output
- Optimized theme resources

### Monitoring
```bash
# Check build sizes
du -sh site/
du -sh site/zh/

# Monitor build time
time ./build-docs.sh
```

## 🔄 Development Workflow

### 1. Content Updates
```bash
# Edit English content
vim docs/en/index.md

# Edit Chinese content  
vim docs/zh/index.md

# Test changes
mkdocs serve
# Visit: http://127.0.0.1:8000/
```

### 2. Configuration Updates
```bash
# Edit single configuration
vim mkdocs.yml

# Test configuration
./test-docs.sh

# Build and verify
./build-docs.sh
```

### 3. New Plugin Documentation
```bash
# Add to English
cp template.md docs/en/reader/newreader.md

# Add to Chinese
cp template.md docs/zh/reader/newreader.md

# Update navigation in mkdocs.yml
# Both languages handled by build script
```

## 🎯 Key Advantages

### Single Configuration Approach
1. **Unified Management**: One `mkdocs.yml` for all languages
2. **Reduced Complexity**: No separate config files to maintain
3. **Dynamic Generation**: Chinese config created automatically
4. **Consistent Styling**: Shared theme and settings
5. **Language Toggle**: Built-in switching via Material theme

### Multi-Language Support
1. **English Default**: International-first approach
2. **Chinese Secondary**: Proper localization
3. **SEO Friendly**: Language-specific URLs
4. **Accessible**: Proper language attributes
5. **Maintainable**: Clear separation of content

## 📚 Additional Resources

- [MkDocs Documentation](https://www.mkdocs.org/)
- [Material Theme Documentation](https://squidfunk.github.io/mkdocs-material/)
- [Multi-language Setup Guide](https://squidfunk.github.io/mkdocs-material/setup/changing-the-language/)

---

This guide ensures you can successfully build and deploy bilingual Addax documentation using the simplified single-configuration approach.