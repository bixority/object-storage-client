//! Provider-parameterised tests: the writable emulators (Garage for S3,
//! Azurite for Azure; fake-gcs-server rejects the client's writes).

use super::{unique_container, unique_prefix};
use object_storage_client::{ObjectStorageClient, Result};

#[derive(Clone, Copy)]
pub enum Provider {
    Garage,
    Azure,
}

/// A fresh, empty location (`s3://default/<prefix>/` or a new container).
pub async fn base(plain: &ObjectStorageClient, p: Provider, test: &str) -> Result<String> {
    match p {
        Provider::Garage => Ok(unique_prefix(test)),
        Provider::Azure => {
            let url = unique_container();
            plain.create_bucket(&url).await?;
            Ok(url)
        }
    }
}

/// Registers `<name>::garage` and `<name>::azure` tests running `$f`.
macro_rules! per_provider {
    ($name:ident, $f:ident) => {
        mod $name {
            use super::{Provider, TestResult};

            #[tokio::test]
            #[ignore = "needs a container runtime (docker or podman)"]
            async fn garage() -> TestResult {
                super::$f(Provider::Garage).await
            }

            #[tokio::test]
            #[ignore = "needs a container runtime (docker or podman)"]
            async fn azure() -> TestResult {
                super::$f(Provider::Azure).await
            }
        }
    };
}
pub(crate) use per_provider;
