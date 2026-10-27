# Configuration

Credentials are read from the environment the first time a backend is used; the
client never takes them as constructor arguments. The bucket or container always
comes from the URL host, so a single process can talk to several buckets across
several providers at once. That is what makes cross-provider copy and move
work: export the variables for every provider you touch, and a single client
can shuttle objects between them. The local filesystem needs no variables.

## AWS S3 (`s3://`)

Also covers S3-compatible stores such as Garage and SeaweedFS.

```bash
export S3_ACCESS_KEY_ID="AKIA..."
export S3_SECRET_ACCESS_KEY="..."
export AWS_REGION="eu-north-1"
# Optional: temporary credentials
export AWS_SESSION_TOKEN="..."
# Optional: custom endpoint for S3-compatible stores (e.g. Garage)
export AWS_ENDPOINT_URL_S3="http://localhost:3900"

# Convenience overrides honoured by this client (take precedence when set):
#   S3_REGION, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY
# Allow plain HTTP (e.g. a local Garage):
#   export S3_ALLOW_HTTP=true
# (It never applies to the AWS default endpoint: bucket operations against
#  AWS always use https.)
```

## Google Cloud Storage (`gs://` / `gcs://`)

```bash
# Path to a service-account JSON key file...
export GOOGLE_SERVICE_ACCOUNT="/path/to/service-account.json"
# (GOOGLE_SERVICE_ACCOUNT_PATH and the standard
#  GOOGLE_APPLICATION_CREDENTIALS are also recognised.)

# ...or the service-account JSON supplied inline instead of a path:
#   export GOOGLE_SERVICE_ACCOUNT_KEY='{"type":"service_account", ...}'

# Optional: JSON API origin for bucket operations (create, exists), for
# example an emulator. Default https://storage.googleapis.com; the OAuth
# token is only sent when the origin is https.
#   export GCS_BASE_URL="http://localhost:4443"
```

## Azure Blob Storage (`az://`, `wasb(s)://`, `abfs(s)://`)

```bash
export AZURE_STORAGE_ACCOUNT_NAME="mystorageaccount"

# Pick ONE authentication method:
# 1. Shared account key
export AZURE_STORAGE_ACCOUNT_KEY="..."
# 2. Shared Access Signature (SAS) token
#   export AZURE_STORAGE_SAS_KEY="?sv=..."
# 3. Service principal (Azure AD)
#   export AZURE_STORAGE_CLIENT_ID="..."
#   export AZURE_STORAGE_CLIENT_SECRET="..."
#   export AZURE_STORAGE_TENANT_ID="..."

# A custom http:// AZURE_STORAGE_ENDPOINT (e.g. Azurite) also needs:
#   export AZURE_ALLOW_HTTP=true
```

## Local filesystem (`file://`)

No environment variables; no credentials are required.

## Client settings

| Variable              | Used by | Effect                                                                                                                                                 |
|-----------------------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| `OSC_KEYRING_FILE`    | `osc`   | Keyring file, the same as `--keyring`. Enables encryption.                                                                                             |
| `OSC_KEYRING_COMMAND` | `osc`   | A shell command that prints a keyring, as an alternative to the file; setting both is an error. See [Encryption](ENCRYPTION.md#cli).                  |
| `OSC_WORKER_THREADS`  | Python  | Worker threads of the async runtime, default `min(CPU count, 4)`. Set it before the first call.                                                       |
