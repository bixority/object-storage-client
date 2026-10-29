#[path = "functional/client.rs"]
mod client;
#[path = "support/mod.rs"]
mod support;
#[macro_use]
#[path = "functional/suite.rs"]
mod suite;
#[cfg(feature = "cli")]
#[path = "functional/aws_profile.rs"]
mod aws_profile;
#[cfg(all(feature = "encryption", feature = "cli"))]
#[path = "functional/cli.rs"]
mod cli;
#[cfg(feature = "encryption")]
#[path = "functional/encryption.rs"]
mod encryption;
#[cfg(feature = "encryption")]
#[path = "functional/golden.rs"]
mod golden;
