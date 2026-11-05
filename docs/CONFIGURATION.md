# Configuration

Credentials come from the environment the first time a backend is used or,
for `s3://`, from a named AWS profile; the client never takes them as
constructor arguments. The bucket or container always comes from the URL host,
so a single process can talk to several buckets across several providers at
once. That is what makes cross-provider copy and move work: export the
variables for every provider you touch, and a single client can shuttle objects
between them. The local filesystem needs no variables. `osc-gui` finds its
connections on the machine instead, see
[`osc-gui` connections](#osc-gui-connections).

## AWS S3 (`s3://`)

Also covers S3-compatible stores such as Garage and SeaweedFS.

```bash
export S3_ACCESS_KEY_ID="AKIA..."
export S3_SECRET_ACCESS_KEY="..."
export AWS_REGION="eu-north-1"
# Temporary credentials (STS, SSO) do not go with the S3_* keys: use the
# AWS_* variables instead, or a profile.
#   export AWS_ACCESS_KEY_ID="ASIA..."
#   export AWS_SECRET_ACCESS_KEY="..."
#   export AWS_SESSION_TOKEN="..."
# Optional: custom endpoint for S3-compatible stores (e.g. Garage)
export AWS_ENDPOINT_URL_S3="http://localhost:3900"

# Convenience overrides honoured by this client (take precedence when set):
#   S3_REGION, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_ENDPOINT, S3_ADDRESSING_STYLE
# Allow plain HTTP (e.g. a local Garage):
#   export S3_ALLOW_HTTP=true
# (It never applies to the AWS default endpoint: bucket operations against
#  AWS always use https.)
```

`S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY` are long-lived keys. They are
never sent with a session token, and `AWS_SESSION_TOKEN` is ignored while
`S3_ACCESS_KEY_ID` is set, because that token belongs to the ambient `AWS_*`
identity of another account. `AWS_SESSION_TOKEN` pairs only with
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

Without a named profile, `s3://` uses these variables: static keys, then web
identity, container credentials and, on AWS only, the instance-metadata
service (IMDS). No AWS config or credentials file and no SSO cache is read,
and `AWS_PROFILE` is never read (see [AWS profiles and SSO](#aws-profiles-and-sso)).

### Endpoint and plain HTTP

- The endpoint is the first of `S3_ENDPOINT`, `AWS_ENDPOINT_URL_S3`,
  `AWS_ENDPOINT` and `AWS_ENDPOINT_URL` that is set and not empty, then, with a
  profile, the profile's S3 `endpoint_url`. Without any, requests go to AWS,
  and bucket calls to `https://s3.<region>.amazonaws.com` (virtual-hosted:
  `https://<bucket>.s3.<region>.amazonaws.com`), always over https.
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

A named profile from `~/.aws/config` / `~/.aws/credentials` (or the files
`AWS_CONFIG_FILE` and `AWS_SHARED_CREDENTIALS_FILE` name) can supply the
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
  `--profile`, `profile=` or `with_s3_profile`, or picked as a connection in
  `osc-gui`. Other profiles (`[default]` included) are read only through the
  named profile's `source_profile`.
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
when less than 5 minutes remain. While the cached credential has at least 1
minute left, a refresh that is running, slow or failed does not hold requests
back: they use the cached credential, and a failed refresh is retried after 30
seconds. Its error is not shown until less than 1 minute of the credential is
left. Credentials without an expiry (static keys in
the profile, a `credential_process` that prints no `Expiration`,
`credential_source = Environment`) are kept until the client is recreated, as
in the AWS SDKs. After rotating such a key, create a new client or restart the
process.

A lookup that times out is not cancelled in the helper: a `credential_process`
program keeps running and is not killed, and it may outlive the process.
Without a usable cached credential, the next call joins that lookup instead of
starting another helper, for at most 5 minutes; after that it starts a new one.
With one, the lookup is given up and a new one starts after the 30-second
back-off, even if the first helper is still running.

Endpoints. A global `AWS_ENDPOINT_URL` is refused with a profile, in `osc` and
in `osc-gui` alike, because the AWS SDK would send the profile's SSO and STS
tokens to it; set the S3 endpoint with `S3_ENDPOINT` or `AWS_ENDPOINT_URL_S3`. The SDK's own `AWS_ENDPOINT_URL_SSO`,
`AWS_ENDPOINT_URL_SSO_OIDC` and `AWS_ENDPOINT_URL_STS`, and `endpoint_url` or
`services` entries in the profile, redirect the SSO token and the STS calls, so
treat them as part of the credential configuration.

The profile's S3 endpoint is the `services` entry for S3, else the profile's
own `endpoint_url`.
`AWS_IGNORE_CONFIGURED_ENDPOINT_URLS=true` or
`ignore_configured_endpoint_urls = true` in the profile ignores it.

SSO. `sso-session` and legacy SSO profiles read the AWS CLI token cache under
`~/.aws/sso/cache` (`HOME`) and rewrite it when they refresh the token, so run
`aws sso login --profile NAME` first (in `osc-gui`, "Log in" on the error
banner writes the same cache). The SSO, SSO-OIDC and STS requests use
the AWS SDK's own HTTP client (see [Development](DEVELOPMENT.md#cargo-features)).
The SDK also reads the process environment for its own settings (the SSO and
STS endpoint variables above, FIPS and dual-stack settings).

`credential_process`. The helper command from `~/.aws/config` is run by osc,
by osc-gui, by the library and by the Python service, with that process's
privileges. Anyone who can edit the profile files can therefore make those
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
# token is only sent when the origin is https. Without it, bucket operations
# use GOOGLE_BASE_URL. Object operations need GOOGLE_BASE_URL (or the key's
# gcs_base_url) to reach an emulator.
#   export GCS_BASE_URL="http://localhost:4443"

# Bucket creation, and the bucket list in osc-gui, need the project
# (GCS_PROJECT_ID is also recognised):
#   export GOOGLE_CLOUD_PROJECT="my-project"
```

`create_bucket` on an existing name succeeds only if `GOOGLE_CLOUD_PROJECT`
lists the bucket. The credentials therefore need `storage.buckets.create` and
`storage.buckets.list` on the project (`roles/storage.admin` has both). A
bucket owned by another project is an error, even one you can write to.

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
# 4. The Azure CLI login (`az login`)
#   export AZURE_USE_AZURE_CLI=true

# A custom http:// AZURE_STORAGE_ENDPOINT (e.g. Azurite) also needs:
#   export AZURE_ALLOW_HTTP=true
```

Bucket operations (`create_bucket`, `bucket_exists`) use the account name and
endpoint of the store, so `AZURE_ENDPOINT` works like `AZURE_STORAGE_ENDPOINT`.
They refuse `AZURE_STORAGE_USE_EMULATOR` and `AZURE_USE_FABRIC_ENDPOINT` unless
an endpoint is set. With `AZURE_SKIP_SIGNATURE=true` the requests are unsigned,
and a 404 from `bucket_exists` is an error, because Azure answers an anonymous
request for a private container with 404.

## Local filesystem (`file://`)

No environment variables; no credentials are required.

## `osc-gui` connections

The library and `osc` read the variables above, and a profile only through
`--profile`, `profile=` or `with_s3_profile`. `osc-gui` instead lists the
connections it finds on the machine and gives each one a client of its own
(see [GUI](GUI.md#using-it)):

- Local and the "environment" connections of AWS, GCS and Azure read the
  process environment, as `osc` does.
- An AWS profile connection reads that profile from the files the AWS CLI reads
  (`AWS_CONFIG_FILE` and `AWS_SHARED_CREDENTIALS_FILE` honoured), as
  `--profile` does. A profile without credential settings (keys, SSO, a role,
  `credential_process`, ...) is not listed; a note names it.
- A gcloud connection uses the configuration's `credential_file_override`,
  else the application default credentials (an `authorized_user` or
  `service_account` file), and the configuration's `project`.
- An Azure CLI connection is one storage account that Azure Resource Manager
  lists for a subscription, signed in through `az` (`AZURE_USE_AZURE_CLI`).

A profile, gcloud or Azure CLI connection takes nothing else from the
environment: `S3_ENDPOINT`, `AWS_REGION` and the timeout and transport
variables do not apply to it, and an AWS profile's region and S3
endpoint come from the profile. The system proxy settings still apply, and the
AWS SDK's own SSO and STS requests still read the process environment (see
[AWS profiles and SSO](#aws-profiles-and-sso)). A global `AWS_ENDPOINT_URL` is
refused for a profile connection, as described under Endpoints: a panel on that
profile shows the error. Unset the variable to use the profile.

## Client settings

| Variable                 | Used by          | Effect                                                                                                                                 |
|--------------------------|------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| `OSC_KEYRING_FILE`       | `osc`, `osc-gui` | Keyring file, the same as `--keyring`. Enables encryption.                                                                             |
| `OSC_KEYRING_COMMAND`    | `osc`, `osc-gui` | A shell command that prints a keyring, as an alternative to the file; setting both is an error. See [Encryption](ENCRYPTION.md#cli).   |
| `OSC_REQUIRE_ENCRYPTION` | `osc`, `osc-gui` | The same as `--require-encryption` (`1`, `true`, `yes`, `on`): fail instead of reading or writing plaintext when no key source is set. |
| `OSC_WORKER_THREADS`     | Python           | Worker threads of the async runtime, default `min(CPU count, 4)`. Set it before the first call.                                        |

Set environment variables before the first client call. Do not modify
`os.environ` while client calls are in flight: backends read the environment
on worker threads without the GIL.

One client shares HTTP connections and credentials across the buckets of a
provider, so these variables apply to all of them alike and tokens are not
fetched again for each bucket. See [Behavior](BEHAVIOR.md).

### Payload signing (S3)

S3 requests sign the SHA-256 of every body, which costs one hashing pass over
each upload and part. Over HTTPS this can be skipped with
`AWS_UNSIGNED_PAYLOAD=true` (the AWS name for sending `UNSIGNED-PAYLOAD`),
which saves CPU on fast links and on CPUs without SHA extensions. TLS then
protects the body in transit, but no application-level checksum is sent, so
integrity rests on TLS alone (encrypted objects are authenticated by their
own frames either way). Do not use it with a plain `http://` endpoint. The
default stays signed.

### Request timeouts

Object requests (get, put, list, multipart parts, `http(s)://` stores) connect
within 5 seconds, have no total deadline shorter than 24 hours, and fail when
nothing arrives for 5 minutes: the read timeout bounds the time to the response
headers and every gap between body reads. The time to the headers includes
sending the request body, so each upload request (one put, or one multipart
part) must be fully sent and answered within the read timeout, even while bytes
flow steadily. With the default 4 parts of 8 MiB in flight that takes about
0.9 Mbit/s of uplink for one upload; a single `http(s)://` put of 1 GiB takes
about 28.6 Mbit/s. A slower link fails with a timeout, and the upload is
aborted. Raise the read timeout for the upload.
Set `AWS_TIMEOUT` / `AWS_READ_TIMEOUT`, `GOOGLE_TIMEOUT` / `GOOGLE_READ_TIMEOUT`
or `AZURE_TIMEOUT` / `AZURE_READ_TIMEOUT` (for example `90s`, `10m`) to change
them for S3, GCS and Azure; a value you set is kept. `http(s)://` stores take
no settings, so their read timeout cannot be raised. Bucket operations and the
requests of a server-side copy keep their own limits (5 s to connect, 30 s per
request, answer included), as does the profile credential lookup (30 s). Apart
from the limits, bucket operations and copy requests follow the client settings
of the store (proxy, certificates, user agent, HTTP version), see
[Behaviour](BEHAVIOR.md).
