# Comprehensive Guide: Building Addax Multi-Language Documentation

This repository supports bilingual documentation (English and Chinese) using MkDocs Material's native multi-language approach. This guide provides complete instructions for setting up, building, and maintaining the documentation.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation & Setup](#installation--setup)
- [Directory Structure](#directory-structure)
- [Configuration Files](#configuration-files)
- [Building Documentation](#building-documentation)
- [Development Workflow](#development-workflow)
- [Deployment](#deployment)
- [Language Switching](#language-switching)
- [Asset Management](#asset-management)
- [Testing & Validation](#testing--validation)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## Prerequisites

Before building the documentation, ensure you have the following installed:

### Required Software
- **Python 3.8+** - Check with `python --version`
- **pip** - Python package manager
- **Git** - For version control

### Install MkDocs and Dependencies
```bash
# Install MkDocs and Material theme
pip install mkdocs mkdocs-material

# Install additional dependencies
pip install pymdown-extensions mkdocs-minify-plugin
```

### Verify Installation
```bash
mkdocs --version
```

## Installation & Setup

### 1. Clone the Repository
```bash
git clone https://github.com/wgzhao/Addax.git
cd Addax
```

### 2. Install Documentation Dependencies
```bash
# Using pip
pip install -r requirements-docs.txt

# Or install manually
pip install mkdocs mkdocs-material pymdown-extensions
```

### 3. Verify Setup
```bash
# Test English documentation
mkdocs serve --config-file mkdocs-en.yml

# Test Chinese documentation  
mkdocs serve --config-file mkdocs-zh.yml
```

## Directory Structure

The documentation follows this organized structure:

```
Addax/
├── docs/
│   ├── en/                    # English documentation
│   │   ├── index.md          # English homepage
│   │   ├── quickstart.md     # English quick start
│   │   ├── reader/           # English reader plugins
│   │   └── writer/           # English writer plugins
│   ├── zh/                    # Chinese documentation (中文文档)
│   │   ├── index.md          # Chinese homepage
│   │   ├── quickstart.md     # Chinese quick start
│   │   ├── reader/           # Chinese reader plugins
│   │   └── writer/           # Chinese writer plugins
│   ├── assets/               # Shared assets (JSON configs, etc.)
│   │   └── jobs/            # Job configuration examples
│   └── images/              # Shared images and media
│       ├── logo.png         # Project logo
│       └── favicon.ico      # Site favicon
├── mkdocs.yml               # Main config (defaults to English)
├── mkdocs-en.yml           # English-specific configuration
├── mkdocs-zh.yml           # Chinese-specific configuration
└── site/                   # Generated documentation output
    ├── index.html          # English homepage (root)
    ├── en/                 # English version
    └── zh/                 # Chinese version
```

## Configuration Files

### Development Configurations (Recommended for Local Development)

#### English Development (`mkdocs-en-dev.yml`)
- **Purpose**: Local English documentation development
- **Docs Directory**: `docs/en`
- **Output Directory**: `site/` (root level)
- **URL**: Serves at `http://localhost:8000/`
- **Features**: No versioning complexity, simplified for development

#### Chinese Development (`mkdocs-zh-dev.yml`)
- **Purpose**: Local Chinese documentation development
- **Docs Directory**: `docs/zh`
- **Output Directory**: `site/` (root level) 
- **URL**: Serves at `http://localhost:8000/`
- **Features**: No versioning complexity, simplified for development

### Production Configurations

#### Main Configuration (`mkdocs.yml`)
- **Purpose**: Default configuration serving English documentation
- **Docs Directory**: `docs/en`
- **Output Directory**: `site/`
- **Language**: English (`en`)

#### English Production (`mkdocs-en.yml`)
- **Purpose**: Production English documentation build
- **Docs Directory**: `docs/en`
- **Output Directory**: `site/en/`
- **Features**: English navigation, metadata, versioning support

#### Chinese Production (`mkdocs-zh.yml`)
- **Purpose**: Production Chinese documentation build
- **Docs Directory**: `docs/zh`
- **Output Directory**: `site/zh/`
- **Features**: Chinese navigation (中文导航), metadata, versioning support

## Building Documentation

### Understanding the Multi-Language Setup

The Addax documentation uses three separate MkDocs configurations:

- **`mkdocs.yml`**: Main configuration (builds English to root: `site/`)
- **`mkdocs-en.yml`**: Explicit English build (builds to: `site/en/`)  
- **`mkdocs-zh.yml`**: Chinese build (builds to: `site/zh/`)

### Quick Build Commands

```bash
# Build main site (English in root directory)
mkdocs build

# Build individual language versions
mkdocs build --config-file mkdocs-en.yml  # → site/en/
mkdocs build --config-file mkdocs-zh.yml  # → site/zh/

# Build all versions (automated script)
./build-docs.sh
```

### Deployment Strategies

#### Option 1: Single Language (English Only)
```bash
# Build main English site
mkdocs build

# Deploy site/ directory
# Results in: https://yoursite.com/
```

#### Option 2: Multi-Language with GitHub Actions
For proper multi-language deployment, use GitHub Actions:

```yaml
# .github/workflows/docs.yml
name: Deploy Multi-Language Docs
on:
  push:
    branches: [ main ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    - uses: actions/setup-python@v3
      with:
        python-version: '3.x'
    
    - name: Install dependencies
      run: pip install mkdocs mkdocs-material pymdown-extensions
    
    - name: Deploy English (default)
      run: mkdocs gh-deploy --config-file mkdocs.yml --remote-branch gh-pages
    
    - name: Deploy Chinese
      run: mkdocs gh-deploy --config-file mkdocs-zh.yml --remote-branch gh-pages
```

#### Option 3: Manual Multi-Language Deployment
```bash
# Build and deploy each language separately
mkdocs build --config-file mkdocs-en.yml
mkdocs build --config-file mkdocs-zh.yml  
mkdocs build --config-file mkdocs.yml

# Use rsync or similar to deploy to different paths on your server
rsync -av site/ user@server:/var/www/docs/
rsync -av site/en/ user@server:/var/www/docs/en/  
rsync -av site/zh/ user@server:/var/www/docs/zh/
```

## Development Workflow

### Local Development Server

#### 🚀 Recommended: Use Development Configurations

For **English documentation development**:
```bash
# Development config - serves at root level
mkdocs serve --config-file mkdocs-en-dev.yml
# Visit: http://localhost:8000/
```

For **Chinese documentation development**:
```bash
# Development config - serves at root level
mkdocs serve --config-file mkdocs-zh-dev.yml  
# Visit: http://localhost:8000/
```

#### Production-like Testing

For **English documentation** (production-like):
```bash
# Production config - serves with subdirectory
mkdocs serve --config-file mkdocs-en.yml
# Visit: http://localhost:8000/Addax/en/
```

For **Chinese documentation** (production-like):
```bash
# Production config - serves with subdirectory  
mkdocs serve --config-file mkdocs-zh.yml
# Visit: http://localhost:8000/Addax/zh/
```

#### ⚠️ Important Note about `/latest` URLs

- **Development configs** serve at root level - no `/latest` path needed
- **Production configs** serve with subdirectories - `/latest` is Mike versioning concept
- When using `mkdocs-en-dev.yml`, visit `http://localhost:8000/` directly
- When using `mkdocs-en.yml`, visit `http://localhost:8000/Addax/en/`

#### Custom Port/Host
```bash
# Serve on custom port
mkdocs serve --config-file mkdocs-en.yml --dev-addr 127.0.0.1:8080

# Serve on all interfaces
mkdocs serve --config-file mkdocs-zh.yml --dev-addr 0.0.0.0:8000
```

### Live Reload Features
- **Auto-rebuild**: Files are automatically rebuilt on changes
- **Live reload**: Browser refreshes automatically
- **Error reporting**: Build errors shown in terminal and browser

## Deployment

### Understanding Multi-Language Deployment

The current setup supports two deployment approaches:

#### Single Language Deployment (Current Default)
- Builds English documentation to root directory
- Simple deployment suitable for English-only users
- Uses `mkdocs.yml` configuration

#### True Multi-Language Deployment (Recommended for Bilingual Sites)
- Requires advanced setup with tools like `mike` or custom GitHub Actions
- Each language gets its own URL path (`/en/`, `/zh/`)
- Users can switch languages via UI

### GitHub Pages Deployment

#### Simple English-Only Deployment
```bash
# Deploy main English site to GitHub Pages
mkdocs gh-deploy

# Or explicitly
mkdocs gh-deploy --config-file mkdocs.yml
```

#### Advanced Multi-Language GitHub Actions
Create `.github/workflows/docs.yml`:

```yaml
name: Deploy Documentation
on:
  push:
    branches: [ main ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
      with:
        fetch-depth: 0
    
    - name: Setup Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: |
        pip install mkdocs mkdocs-material pymdown-extensions mike
    
    - name: Configure Git
      run: |
        git config user.name "GitHub Actions"
        git config user.email "actions@github.com"
    
    - name: Deploy English (default)
      run: |
        mike deploy --config-file mkdocs.yml --push --update-aliases main en
        mike set-default --config-file mkdocs.yml --push main
    
    - name: Deploy Chinese
      run: |
        mike deploy --config-file mkdocs-zh.yml --push zh
```

### Custom Server Deployment

#### Single Site Deployment
```bash
# Build and deploy main site
mkdocs build
scp -r site/ user@server:/var/www/addax-docs/
```

#### Multi-Language Server Deployment
```bash
# Build all versions
mkdocs build --config-file mkdocs-en.yml
mkdocs build --config-file mkdocs-zh.yml
mkdocs build --config-file mkdocs.yml

# Deploy with proper structure
scp -r site/ user@server:/var/www/addax-docs/

# If you have separate builds, organize them:
# English: /var/www/addax-docs/en/
# Chinese: /var/www/addax-docs/zh/
# Default (English): /var/www/addax-docs/
```

### Docker Deployment
```dockerfile
FROM nginx:alpine
COPY site/ /usr/share/nginx/html/
EXPOSE 80
```

```bash
# Build and deploy with Docker
./build-docs.sh
docker build -t addax-docs .
docker run -p 80:80 addax-docs
```

## Language Switching

The documentation includes automatic language switching via the `extra.alternate` configuration:

### Configuration
```yaml
# In each config file
extra:
  alternate:
    - name: English
      link: /en/
      lang: en
    - name: 中文
      link: /zh/
      lang: zh
```

### URL Structure
- **English**: `https://wgzhao.github.io/Addax/` (root)
- **English**: `https://wgzhao.github.io/Addax/en/` (explicit)
- **Chinese**: `https://wgzhao.github.io/Addax/zh/`

### Language Detection
- Default language is English
- Users can manually switch via the language selector
- Language preference is maintained in browser session

## Asset Management

### Shared Assets Strategy
Assets are shared between languages to avoid duplication:

```
docs/
├── assets/           # Shared across all languages
│   ├── jobs/        # JSON configuration examples
│   └── stylesheets/ # Custom CSS (if any)
└── images/          # Shared images and media
    ├── logo.png
    ├── favicon.ico
    └── screenshots/
```

### Asset Referencing
In Markdown files, reference shared assets:

```markdown
<!-- Images -->
![Logo](../images/logo.png)

<!-- Job configurations -->
--8<-- "jobs/mysql-example.json"
```

### Adding New Assets
1. Place shared assets in `docs/assets/` or `docs/images/`
2. Update references in both language versions
3. Test links in both builds

## Testing & Validation

### Build Testing
```bash
# Test all configurations build successfully
mkdocs build --config-file mkdocs.yml --strict
mkdocs build --config-file mkdocs-en.yml --strict
mkdocs build --config-file mkdocs-zh.yml --strict
```

### Link Validation
```bash
# Install link checker
pip install mkdocs-linkcheck

# Check for broken links
mkdocs build --config-file mkdocs-en.yml --strict
mkdocs build --config-file mkdocs-zh.yml --strict
```

### Content Validation Checklist
- [ ] All navigation links work
- [ ] Images load correctly
- [ ] Asset references resolve
- [ ] Language switcher functions
- [ ] Search works in both languages
- [ ] Mobile responsive design
- [ ] No console errors

### Automated Testing
```bash
#!/bin/bash
# test-docs.sh

echo "Testing documentation builds..."

configs=("mkdocs.yml" "mkdocs-en.yml" "mkdocs-zh.yml")

for config in "${configs[@]}"; do
    echo "Testing $config..."
    if mkdocs build --config-file "$config" --strict; then
        echo "✅ $config build successful"
    else
        echo "❌ $config build failed"
        exit 1
    fi
done

echo "All documentation builds passed!"
```

## Troubleshooting

### Common Issues

#### Development Server Issues

**Problem**: Visiting `/latest` shows Chinese documentation instead of English
```bash
# ❌ Wrong - Using production config with versioning paths
mkdocs serve --config-file mkdocs-en.yml
# Visit http://localhost:8000/latest ← Shows wrong content

# ✅ Solution - Use development config  
mkdocs serve --config-file mkdocs-en-dev.yml
# Visit http://localhost:8000/ ← Shows correct English content
```

**Problem**: Server serves content at subdirectory path
```bash
# Issue: http://localhost:8000/Addax/en/ instead of http://localhost:8000/
# Solution: Use development configs (-dev.yml) for local development
```

**Problem**: `/latest` URL returns 404 in development
```bash
# Expected behavior with development configs
# Use root URL instead: http://localhost:8000/
```

#### Build Errors
```bash
# Issue: Module not found
pip install mkdocs mkdocs-material

# Issue: Permission denied
sudo chmod +x build-docs.sh

# Issue: Port already in use
mkdocs serve --dev-addr 127.0.0.1:8001
```

#### Content Issues
```bash
# Issue: Images not loading
# Check: Relative paths from docs/en/ and docs/zh/
# Use: ../images/filename.png

# Issue: Navigation not working
# Check: File paths in nav section of mkdocs.yml
# Ensure: Files exist in respective language directories
```

#### Language Switching
```bash
# Issue: Language switcher not working
# Check: extra.alternate configuration in all config files
# Verify: Consistent URL structure
```

### Debug Mode
```bash
# Enable verbose output
mkdocs serve --config-file mkdocs-en.yml --verbose

# Check configuration
mkdocs config --config-file mkdocs-en.yml
```

## Contributing

### Adding New Documentation

#### 1. English Documentation
1. Create file in `docs/en/`
2. Update `mkdocs-en.yml` navigation
3. Test build: `mkdocs serve --config-file mkdocs-en.yml`

#### 2. Chinese Documentation
1. Create translated file in `docs/zh/`
2. Update `mkdocs-zh.yml` navigation
3. Test build: `mkdocs serve --config-file mkdocs-zh.yml`

#### 3. Update Main Configuration
1. Update `mkdocs.yml` navigation if needed
2. Test complete build: `./build-docs.sh`

### Translation Workflow
1. **Create English version** in `docs/en/`
2. **Translate to Chinese** in `docs/zh/`
3. **Update navigation** in both config files
4. **Test both versions** work correctly
5. **Submit pull request** with both language versions

### Style Guidelines
- Use clear, concise language
- Include code examples
- Add relevant images/screenshots
- Maintain consistent formatting
- Follow existing documentation patterns

---

## Quick Reference Commands

```bash
# Development
mkdocs serve --config-file mkdocs-en.yml    # English dev server
mkdocs serve --config-file mkdocs-zh.yml    # Chinese dev server

# Building
mkdocs build --config-file mkdocs-en.yml    # Build English
mkdocs build --config-file mkdocs-zh.yml    # Build Chinese
./build-docs.sh                              # Build all languages

# Testing
mkdocs build --strict                        # Test default build
mkdocs build --config-file mkdocs-en.yml --strict  # Test English
mkdocs build --config-file mkdocs-zh.yml --strict  # Test Chinese

# Deployment
mkdocs gh-deploy                             # Deploy to GitHub Pages
```

For additional help, refer to:
- [MkDocs Documentation](https://www.mkdocs.org/)
- [Material Theme Documentation](https://squidfunk.github.io/mkdocs-material/)
- [Project Issues](https://github.com/wgzhao/Addax/issues)