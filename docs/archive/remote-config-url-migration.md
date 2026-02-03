# Remote Storage Configuration: URL-Based Migration

## Summary

The `RemoteConfig` struct has been refactored to use URL-based configuration that aligns with `object_store`'s standard `parse_url_opts()` API. This provides a more consistent and idiomatic interface.

## Changes

### Old Structure (Fields-Based)
```yaml
storage_type: "s3"
endpoint: "https://s3.us-east-1.amazonaws.com"
path: ""  # unused for S3
bucket: "my-backup-bucket"
region: "us-east-1"
key: "AKIAIOSFODNN7EXAMPLE"
secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
compression_level: 3
```

### New Structure (URL-Based)
```yaml
url: "s3://my-backup-bucket"
# bucket: ""  # Optional, can be in URL instead
region: "us-east-1"
key: "AKIAIOSFODNN7EXAMPLE"
secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
endpoint: ""  # Optional: for S3-compatible services like MinIO
compression_level: 3
```

## Configuration Examples

### Standard AWS S3
```yaml
url: "s3://my-backup-bucket"
region: "us-east-1"
key: "AKIAIOSFODNN7EXAMPLE"
secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
compression_level: 3
```

### Local Filesystem
```yaml
url: "file:///path/to/backup/directory"
compression_level: 3
```

### S3-Compatible Service (MinIO, DigitalOcean Spaces, etc.)
```yaml
url: "s3://my-bucket"
region: "us-east-1"
key: "minioadmin"
secret: "minioadmin"
endpoint: "http://localhost:9000"
compression_level: 3
```

### Alternative: Bucket in Separate Field
```yaml
url: "s3://"
bucket: "my-backup-bucket"
region: "us-east-1"
key: "AKIAIOSFODNN7EXAMPLE"
secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
compression_level: 3
```

## Field Reference

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `url` | `ServiceEndpoint<String>` | **Yes** | Storage URL with scheme (`file://`, `s3://`) |
| `bucket` | `String` | No* | S3 bucket name (if not in URL) |
| `region` | `String` | No** | AWS region (can be inferred) |
| `key` | `ApiKey<String>` | Yes*** | Access key ID for S3 |
| `secret` | `ApiSecret<String>` | Yes*** | Secret access key for S3 |
| `endpoint` | `ServiceEndpoint<String>` | No | Custom endpoint for S3-compatible services |
| `compression_level` | `i32` | No | Compression level (0-21, default: 3) |

\* Required if not specified in the URL  
\** Required for S3, but can often be inferred from credentials  
\*** Only required for `s3://` URLs

## Supported URL Schemes

- **`file://`** - Local filesystem storage
  - Example: `file:///var/backups/pond`
  
- **`s3://`** - S3-compatible object storage (AWS S3, MinIO, DigitalOcean Spaces, etc.)
  - Example: `s3://my-bucket` or `s3://my-bucket/path/prefix`

## Migration Guide

### For AWS S3 Configurations

**Before:**
```yaml
storage_type: "s3"
bucket: "my-backups"
region: "us-west-2"
key: "AKIA..."
secret: "wJal..."
```

**After:**
```yaml
url: "s3://my-backups"
region: "us-west-2"
key: "AKIA..."
secret: "wJal..."
```

### For Local Filesystem Configurations

**Before:**
```yaml
storage_type: "local"
path: "/var/backups/pond"
```

**After:**
```yaml
url: "file:///var/backups/pond"
```

### For S3-Compatible Services (MinIO)

**Before:**
```yaml
storage_type: "s3"
bucket: "my-backups"
region: "us-east-1"
endpoint: "http://minio:9000"
key: "minioadmin"
secret: "minioadmin"
```

**After:**
```yaml
url: "s3://my-backups"
region: "us-east-1"
endpoint: "http://minio:9000"
key: "minioadmin"
secret: "minioadmin"
```

## Benefits

1. **Standards-Aligned**: Uses `object_store::parse_url_opts()` which is the idiomatic way to configure object stores
2. **Cleaner API**: URL scheme determines storage type, eliminating the `storage_type` field
3. **Future-Proof**: Easy to add support for other schemes (`gs://`, `az://`, etc.)
4. **Flexible**: Bucket can be in URL or separate field
5. **Consistent**: Matches patterns used throughout the object_store ecosystem

## Implementation Details

The `build_object_store()` function now:

1. Parses the URL to determine the scheme
2. For `file://`: Extracts the path and creates a `LocalFileSystem`
3. For `s3://`: 
   - Builds an options map with credentials and configuration
   - Uses `object_store::parse_url_opts()` to create the store
   - Converts the returned `Box<dyn ObjectStore>` to `Arc<dyn ObjectStore>`

## Validation

The validation function checks:

- **URL presence**: The `url` field must not be empty
- **URL format**: Must be a valid URL with a supported scheme
- **Scheme-specific requirements**:
  - `file://`: Must have a non-empty path
  - `s3://`: Must have credentials (`key` and `secret`)
  - `s3://`: Must have bucket either in URL or `bucket` field

## Testing

All existing tests have been updated to work with the new URL-based configuration. The test suite covers:

- Scanning remote versions
- Downloading bundles
- Extracting bundle contents
- Applying files to Delta tables
