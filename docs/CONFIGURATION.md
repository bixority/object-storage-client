# Configuration

Credentials come from the environment the first time a backend is used or,
for `s3://`, from a named AWS profile; the client never takes them as
constructor arguments. The bucket or container always comes from the URL host,
so a single process can talk to several buckets across several providers at
once. That is what makes cross-provider copy and move work: export the
variables for every provider you touch, and a single client can shuttle objects
between them. The local filesystem needs no variables.

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
#   S3_REGION, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_ENDPOINT, S3_ADDRESSING_STYLE
# Allow plain HTTP (e.g. a local Garage):
#   export S3_ALLOW_HTTP=true
# (It never applies to the AWS default endpoint: bucket operations against
#  AWS always use https.)
```

Without a named profile, `s3://` uses these variables: static keys, then web
identity, container credentials and, on AWS only, the instance-metadata
service (IMDS). No AWS config or credentials file and no SSO cache is read,
and `AWS_PROFILE` is never read (see [AWS profiles and SSO](#aws-profiles-and-sso)).

### Endpoint and plain HTTP

- The endpoint is the first of `S3_ENDPOINT`, `AWS_ENDPOINT_URL_S3`,
  `AWS_ENDPOINT` and `AWS_ENDPOINT_URL` that is set and not empty, then, with a
  profile, the profile's S3 `endpoint_url`. Without any, requests go to AWS,
  and bucket calls to `https://s3.<region>.amazonaws.com`, always over https.
- Addressing: `S3_ADDRESSING_STYLE=path` (the default) puts the bucket in the
  path, `S3_ADDRESSING_STYLE=virtual` puts it in the host. The value is
  case-insensitive, and any other value is an error. When it is unset,
  `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` decides. Object and bucket requests use
  the same style. With a custom endpoint, `virtual` sends requests to the
  endpoint as is, so the endpoint must already be scoped to the bucket.
- An `http://` endpoint enables plain HTTP by itself. Plain HTTP exposes every
  request header to the network, including `x-amz-security-token`, so use it
  only for a local or otherwise trusted store.
- `S3_ALLOW_HTTP` is read only as the exact value `true`. It can switch plain
  HTTP on but never off, and is kept for compatibility. It does not apply to
  the AWS default endpoint.
- A custom endpoint with no credential source runs unsigned (anonymous),
  because the only thing left to try would be IMDS, which hangs against a
  non-AWS store. `AWS_SKIP_SIGNATURE=true` or `false` overrides this either
  way. These count as credential sources: `S3_ACCESS_KEY_ID`,
  `AWS_ACCESS_KEY_ID`, `AWS_WEB_IDENTITY_TOKEN_FILE` together with
  `AWS_ROLE_ARN`, `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, and
  `AWS_CONTAINER_CREDENTIALS_FULL_URI` together with
  `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`; one variable of a pair does not
  count. A named profile counts too.
- Region order: `S3_REGION`, `AWS_REGION`, `AWS_DEFAULT_REGION`, then the
  profile's `region`, else `us-east-1`. Empty values are ignored.

### AWS profiles and SSO

A named profile from `~/.aws/config` / `~/.aws/credentials` can supply the
credentials. Select it with `osc --profile NAME`,
`ObjectStorageClient(profile="NAME")` in Python or `Client::with_s3_profile` in
Rust. The kinds supported are:

- static keys;
- `sso_session` and the legacy `sso_*` keys;
- `role_arn` with `source_profile` or `credential_source`;
- `role_arn` with `web_identity_token_file`;
- `credential_process`.

`login_session` profiles (`aws login`) are not supported. Export their
credentials instead, for example with
`aws configure export-credentials --profile NAME --format process` as the
`credential_process` of another profile, or with `aws-vault exec NAME -- osc ...`
or `eval "$(aws configure export-credentials --profile NAME --format env)"`,
then run without `--profile`.

An explicit profile is the only S3 credential source:

- `S3_ACCESS_KEY_ID`, `AWS_ACCESS_KEY_ID` and the other static-key variables
  are not used with it.
- `AWS_PROFILE` is never read. A profile is used only when it is named with
  `--profile`, `profile=` or `with_s3_profile`. Other profiles (`[default]`
  included) are read only through the named profile's `source_profile`.
- There is no fallback to IMDS or any other source when the profile cannot be
  resolved (unknown name, SSO login missing or expired): that is an error. A
  `credential_source` named in the profile itself is honoured.

For client-side encryption with a profile, use
`ObjectStorageClient(profile="NAME").with_encryption(keyring)`, which shares
the plain client's stores and profile; `EncryptedObjectStorageClient(keyring,
profile="NAME")` constructs the encrypted client directly. In Rust it is
`ObjectStorageClient::new().with_s3_profile(p)?.with_encryption(cfg)`
(`Client::<Envelope>::new` takes no profile).

Role and web-identity profiles (and SSO-then-role) call STS, which needs a
region: `AWS_REGION`, `AWS_DEFAULT_REGION`, the profile's `region` (through
`source_profile`), else `us-east-1`. `S3_REGION` applies to S3 only.

The profile files are read on first use and again after a failed lookup, so
fixing the file or running `aws sso login` needs no restart. A credential
lookup gives up after 30 seconds. Resolved credentials are cached and renewed
when less than 5 minutes remain.

Endpoints. A global `AWS_ENDPOINT_URL` is refused with a profile, because the
AWS SDK would send the profile's SSO and STS tokens to it; set the S3 endpoint
with `S3_ENDPOINT` or `AWS_ENDPOINT_URL_S3`. The SDK's own `AWS_ENDPOINT_URL_SSO`,
`AWS_ENDPOINT_URL_SSO_OIDC` and `AWS_ENDPOINT_URL_STS`, and `endpoint_url` or
`services` entries in the profile, redirect the SSO token and the STS calls, so
treat them as part of the credential configuration.

The profile's S3 endpoint is the `services` entry for S3, else the profile's
own `endpoint_url`.
`AWS_IGNORE_CONFIGURED_ENDPOINT_URLS=true` or
`ignore_configured_endpoint_urls = true` in the profile ignores it.

SSO. `sso-session` and legacy SSO profiles read the AWS CLI token cache under
`~/.aws/sso/cache` (`HOME`) and rewrite it when they refresh the token, so run
`aws sso login --profile NAME` first. The SSO, SSO-OIDC and STS requests use
the AWS SDK's own HTTP client (see [Development](DEVELOPMENT.md#cargo-features)).
The SDK also reads the process environment for its own settings (the SSO and
STS endpoint variables above, FIPS and dual-stack settings).

`credential_process`. The helper command from `~/.aws/config` is run by osc,
by the library and by the Python service, with that process's privileges. Anyone who can edit the profile files can therefore make those
processes execute commands, so keep them as private as the keyring. The helper's
standard error is never shown. The AWS SDK logs a failing helper's error chain
at WARN through `tracing`; this crate suppresses those events for credential
lookups, so no host application log receives them.

Errors. A profile that does not exist, or whose SSO or role settings are
incomplete, fails with a message that names the profile, the section and the
file problem. Any other failure (a helper that exits non-zero, an expired SSO
token, a rejected role assumption) shows only a fixed text that names the
profile, with the hint to run `aws sso login --profile NAME` for SSO profiles,
because helper output and provider responses can contain secrets. A
`login_session` profile is reported as unsupported.

```ini
# ~/.aws/config
[profile dev]
sso_session = corp
sso_account_id = 123456789012
sso_role_name = Developer
region = eu-north-1

[sso-session corp]
sso_start_url = https://example.awsapps.com/start
sso_region = eu-north-1
sso_registration_scopes = sso:account:access
```

```bash
aws sso login --profile dev
osc --profile dev ls s3://bucket/prefix/
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

### Request timeouts

Object requests (get, put, list, multipart parts, `http(s)://` stores) connect
within 5 seconds, have no total deadline shorter than 24 hours, and fail when
nothing arrives for 5 minutes: the read timeout bounds the time to the response
headers (including sending the request body) and every gap between body reads.
Set `AWS_TIMEOUT` / `AWS_READ_TIMEOUT`, `GOOGLE_TIMEOUT` / `GOOGLE_READ_TIMEOUT`
or `AZURE_TIMEOUT` / `AZURE_READ_TIMEOUT` (for example `90s`, `10m`) to change
them for S3, GCS and Azure; a value you set is kept. `http(s)://` stores take
no settings. Bucket operations keep their own limits (5 s to connect, 30 s per
request), as does the profile credential lookup (30 s).
