#[path = "integration/azure_storage_ops.rs"]
mod azure_storage_ops;
#[path = "integration/common/mod.rs"]
mod common;
#[cfg(feature = "encryption")]
#[path = "integration/encryption.rs"]
mod encryption;
#[path = "integration/file_roundtrip.rs"]
mod file_roundtrip;
#[path = "integration/gcs_storage_ops.rs"]
mod gcs_storage_ops;
#[cfg(feature = "encryption")]
#[path = "integration/reencrypt.rs"]
mod reencrypt;
#[path = "integration/s3_storage_ops.rs"]
mod s3_storage_ops;
#[path = "integration/storage_ops.rs"]
mod storage_ops;
#[path = "support/mod.rs"]
mod support;
