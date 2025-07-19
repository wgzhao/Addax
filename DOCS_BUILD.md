# Multi-Language Documentation Build

This repository now supports multi-language documentation using MkDocs Material's recommended approach.

## Configuration Files

- `mkdocs.yml` - Main configuration (English by default)
- `mkdocs-en.yml` - English documentation configuration
- `mkdocs-zh.yml` - Chinese documentation configuration

## Directory Structure

```
docs/
├── en/          # English documentation
├── zh/          # Chinese documentation
├── assets/      # Shared assets
└── images/      # Shared images
```

## Building Documentation

### Build Default (English)
```bash
mkdocs build
```

### Build English Version
```bash
mkdocs build --config-file mkdocs-en.yml
```

### Build Chinese Version  
```bash
mkdocs build --config-file mkdocs-zh.yml
```

### Build All Languages
```bash
# Build English
mkdocs build --config-file mkdocs-en.yml
# Build Chinese  
mkdocs build --config-file mkdocs-zh.yml
```

## Output Structure

After building, the site structure will be:
```
site/
├── (root - English content)
├── en/         # English version
└── zh/         # Chinese version
```

## Language Switcher

The documentation includes a language switcher in the header that allows users to switch between English and Chinese versions.

## Development

For development with live reload:

### English
```bash
mkdocs serve --config-file mkdocs-en.yml
```

### Chinese
```bash
mkdocs serve --config-file mkdocs-zh.yml
```

## Migration from mkdocs-static-i18n

This setup replaces the previous `mkdocs-static-i18n` plugin approach with MkDocs Material's native multi-language support, which provides better compatibility and performance.