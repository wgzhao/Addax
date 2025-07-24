# Password Encryption

Addax supports password encryption to enhance security when storing database credentials in configuration files.

## Overview

Instead of storing passwords in plain text in job configuration files, you can use encrypted passwords. This feature helps protect sensitive credentials, especially in shared environments or version control systems.

## Generating Encrypted Passwords

Use the provided script to encrypt passwords:

```bash
bin/encrypt.sh <password>
```

Example:

```bash
bin/encrypt.sh mypassword123
```

Output:
```
Encrypted password: addax:enc:AES:7kMgvpYVGh2kH5tZ1AxyHQ==
```

## Using Encrypted Passwords

### In Job Configuration

Replace plain text passwords with encrypted ones in your job configuration:

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "dbuser",
            "password": "addax:enc:AES:7kMgvpYVGh2kH5tZ1AxyHQ==",
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://localhost:3306/testdb",
                "table": ["users"]
              }
            ]
          }
        }
      }
    ]
  }
}
```

### Environment Variables

You can also store encrypted passwords in environment variables:

```bash
export DB_PASSWORD="addax:enc:AES:7kMgvpYVGh2kH5tZ1AxyHQ=="
```

Then reference it in your configuration:

```json
{
  "parameter": {
    "password": "${DB_PASSWORD}"
  }
}
```

## Encryption Algorithm

Addax uses AES (Advanced Encryption Standard) for password encryption:

- **Algorithm**: AES-128
- **Mode**: CBC (Cipher Block Chaining)
- **Padding**: PKCS5Padding
- **Key**: Generated based on system properties

## Security Considerations

### Key Management

The encryption key is derived from system properties. For enhanced security:

1. **Set custom encryption key**:
   ```bash
   export ADDAX_ENCRYPT_KEY="your-custom-key-here"
   ```

2. **Use different keys per environment**:
   ```bash
   # Development
   export ADDAX_ENCRYPT_KEY="dev-key-2024"
   
   # Production  
   export ADDAX_ENCRYPT_KEY="prod-key-2024"
   ```

### Best Practices

1. **Rotate encryption keys regularly**
2. **Use different keys for different environments**
3. **Store keys securely (not in source code)**
4. **Limit access to encryption keys**
5. **Use encrypted passwords for all sensitive data**

## Advanced Usage

### Custom Encryption Provider

You can implement a custom encryption provider by implementing the `PasswordEncryptor` interface:

```java
public class CustomPasswordEncryptor implements PasswordEncryptor {
    @Override
    public String encrypt(String plainPassword) {
        // Your custom encryption logic
        return "custom:enc:" + encryptedPassword;
    }
    
    @Override
    public String decrypt(String encryptedPassword) {
        // Your custom decryption logic
        return decryptedPassword;
    }
}
```

### Batch Encryption

For multiple passwords, create a script:

```bash
#!/bin/bash
passwords=("password1" "password2" "password3")

for pwd in "${passwords[@]}"; do
    echo "Encrypting: $pwd"
    bin/encrypt.sh "$pwd"
    echo "---"
done
```

## Configuration Examples

### MySQL with Encrypted Password

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "readonly_user",
            "password": "addax:enc:AES:7kMgvpYVGh2kH5tZ1AxyHQ==",
            "column": ["*"],
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://prod-db:3306/analytics",
                "table": ["user_events"]
              }
            ]
          }
        },
        "writer": {
          "name": "postgresqlwriter",
          "parameter": {
            "username": "analytics_user",
            "password": "addax:enc:AES:9nPsrKlMN8xR2vY5aBcDfG==",
            "column": ["*"],
            "connection": [
              {
                "jdbcUrl": "jdbc:postgresql://warehouse:5432/analytics",
                "table": ["user_events"]
              }
            ]
          }
        }
      }
    ]
  }
}
```

### Multiple Environments

**Development (dev.json)**:
```json
{
  "parameter": {
    "password": "addax:enc:AES:devKeyEncryptedPassword=="
  }
}
```

**Production (prod.json)**:
```json
{
  "parameter": {
    "password": "addax:enc:AES:prodKeyEncryptedPassword=="
  }
}
```

## Troubleshooting

### Decryption Errors

If you encounter decryption errors:

1. **Verify encryption key**: Ensure the same key is used for encryption and decryption
2. **Check password format**: Ensure the encrypted password starts with `addax:enc:AES:`
3. **Validate environment**: Confirm environment variables are set correctly

### Password Not Recognized

```bash
# Test decryption
bin/decrypt.sh "addax:enc:AES:7kMgvpYVGh2kH5tZ1AxyHQ=="
```

### Key Management Issues

```bash
# Check current encryption key
echo $ADDAX_ENCRYPT_KEY

# Set temporary key for testing
export ADDAX_ENCRYPT_KEY="test-key-123"
```

## Migration Guide

### From Plain Text to Encrypted

1. **Identify all passwords** in configuration files
2. **Encrypt each password** using the encrypt script
3. **Replace plain text** with encrypted values
4. **Test the configuration** to ensure it works
5. **Update documentation** with new security procedures

### Example Migration Script

```bash
#!/bin/bash

# Backup original files
cp config/job.json config/job.json.backup

# Replace passwords (adjust patterns as needed)
sed -i 's/"password": "plainpassword"/"password": "addax:enc:AES:encryptedvalue"/g' config/job.json

echo "Migration complete. Test the configuration before deploying."
```

This encryption feature significantly improves the security posture of your Addax deployments while maintaining ease of use.