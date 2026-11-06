//! The storage library shared by `osc` and `osc-gui`: clients, backends,
//! encryption, the registry and the Python bindings.

#[cfg(test)]
#[macro_use]
mod forward;

pub mod backend;
pub mod client;
pub mod codec;
#[cfg(feature = "encryption")]
pub mod encryption;
pub mod fsmode;
pub mod hex;
#[cfg(feature = "python")]
pub mod python;
pub mod registry;
pub mod sign;
pub mod text;
pub mod upload;
