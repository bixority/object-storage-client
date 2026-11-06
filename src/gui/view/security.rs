//! The security indicators: whether the client encrypts, and whether a
//! connection uses TLS. Both are decided from configuration, before any
//! request.

use crate::backend::{Vars, tls};
use crate::gui::connection::{Connection, Provider, Source};
use crate::gui::model::Encryption;

/// What a connection's configuration says about TLS.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TlsState {
    /// An `https` endpoint, certificates verified.
    Verified,
    /// An `https` endpoint with certificate verification switched off.
    Unverified,
    /// No TLS: a plain `http` endpoint, or the local file system.
    Plain,
}

/// The TLS state of `connection`, read from the variables its client uses.
pub(super) fn tls_state(connection: &Connection) -> TlsState {
    let scheme = match connection.provider {
        Provider::Local => return TlsState::Plain,
        Provider::S3 => "s3",
        Provider::Gcs => "gs",
        Provider::Azure => "az",
    };
    let vars = match (connection.vars(), &connection.source) {
        // The profile's endpoint, read at discovery, counts as the endpoint
        // unless the connection's own variables name one (S3 ranks those first).
        (
            Some(mut vars),
            Source::AwsProfile {
                endpoint: Some(endpoint),
                ..
            },
        ) => {
            const ENDPOINT_KEYS: [&str; 4] = [
                "S3_ENDPOINT",
                "AWS_ENDPOINT_URL_S3",
                "AWS_ENDPOINT",
                "AWS_ENDPOINT_URL",
            ];
            if ENDPOINT_KEYS
                .iter()
                .all(|key| vars.get(*key).is_none_or(String::is_empty))
            {
                vars.insert("AWS_ENDPOINT_URL_S3".to_owned(), endpoint.clone());
            }
            Vars::map(vars)
        }
        (vars, _) => vars.map_or(Vars::Env, Vars::map),
    };
    state_of(scheme, &vars)
}

fn state_of(scheme: &str, vars: &Vars) -> TlsState {
    match tls(scheme, vars) {
        Some(t) if !t.https => TlsState::Plain,
        Some(t) if t.allow_invalid_certificates => TlsState::Unverified,
        Some(_) => TlsState::Verified,
        None => TlsState::Plain,
    }
}

pub(super) const fn tls_icon(state: TlsState) -> &'static str {
    match state {
        TlsState::Verified => "🛡️",
        TlsState::Unverified => "⚠️",
        TlsState::Plain => "◌",
    }
}

pub(super) const fn tls_hover(state: TlsState) -> &'static str {
    match state {
        TlsState::Verified => "TLS: certificate verified",
        TlsState::Unverified => "TLS: certificate not verified",
        TlsState::Plain => "No TLS: plain connection",
    }
}

pub(super) const fn encryption_icon(encryption: Encryption) -> &'static str {
    match encryption {
        Encryption::Off => "⊘",
        Encryption::File | Encryption::Command => "🔑",
    }
}

pub(super) const fn encryption_hover(encryption: Encryption) -> &'static str {
    match encryption {
        Encryption::Off => "Client-side encryption off",
        Encryption::File => "Client-side encryption on · keyring file",
        Encryption::Command => "Client-side encryption on · keyring command",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gui::connection::Source;
    use std::path::PathBuf;

    fn connection(provider: Provider, source: Source) -> Connection {
        Connection {
            id: "t".into(),
            label: "t".into(),
            provider,
            source,
            login: None,
        }
    }

    /// The state of an S3 store configured by exactly `pairs`.
    fn s3(pairs: &[(&str, &str)]) -> TlsState {
        let vars = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        state_of("s3", &Vars::map(vars))
    }

    #[test]
    fn local_is_plain() {
        let c = Connection::local();
        assert_eq!(tls_state(&c), TlsState::Plain);
    }

    #[test]
    fn default_s3_is_verified() {
        assert_eq!(s3(&[]), TlsState::Verified);
    }

    #[test]
    fn s3_with_an_http_endpoint_is_plain() {
        assert_eq!(
            s3(&[("S3_ENDPOINT", "http://127.0.0.1:9000")]),
            TlsState::Plain
        );
        // Allowing invalid certificates changes nothing without TLS.
        assert_eq!(
            s3(&[
                ("S3_ENDPOINT", "http://127.0.0.1:9000"),
                ("AWS_ALLOW_INVALID_CERTIFICATES", "true"),
            ]),
            TlsState::Plain
        );
    }

    #[test]
    fn s3_with_an_https_endpoint_is_verified() {
        assert_eq!(
            s3(&[("S3_ENDPOINT", "https://minio.example")]),
            TlsState::Verified
        );
    }

    #[test]
    fn s3_with_invalid_certificates_allowed_is_unverified() {
        assert_eq!(
            s3(&[
                ("S3_ENDPOINT", "https://minio.example"),
                ("AWS_ALLOW_INVALID_CERTIFICATES", "true"),
            ]),
            TlsState::Unverified
        );
        assert_eq!(
            s3(&[("AWS_ALLOW_INVALID_CERTIFICATES", "true")]),
            TlsState::Unverified
        );
    }

    /// An AWS profile connection whose config file names `endpoint`.
    fn profile_with_endpoint(endpoint: Option<&str>) -> Connection {
        connection(
            Provider::S3,
            Source::AwsProfile {
                name: "dev".into(),
                config: None,
                credentials: None,
                process_endpoint: false,
                regions: Vec::new(),
                endpoint: endpoint.map(str::to_owned),
            },
        )
    }

    #[test]
    fn profile_endpoint_decides_the_tls_state() {
        assert_eq!(
            tls_state(&profile_with_endpoint(Some("http://minio.local:9000"))),
            TlsState::Plain
        );
        assert_eq!(
            tls_state(&profile_with_endpoint(Some("https://minio.example"))),
            TlsState::Verified
        );
        // No endpoint in the profile: the default AWS https endpoint.
        assert_eq!(tls_state(&profile_with_endpoint(None)), TlsState::Verified);
    }

    #[test]
    fn azure_cli_is_verified() {
        let c = connection(
            Provider::Azure,
            Source::AzureCli {
                account: "acct".into(),
                endpoint: "https://acct.blob.core.windows.net".into(),
            },
        );
        assert_eq!(tls_state(&c), TlsState::Verified);
    }

    #[test]
    fn azure_cli_with_an_http_endpoint_is_plain() {
        let c = connection(
            Provider::Azure,
            Source::AzureCli {
                account: "acct".into(),
                endpoint: "http://127.0.0.1:10000/acct".into(),
            },
        );
        assert_eq!(tls_state(&c), TlsState::Plain);
    }

    #[test]
    fn gcs_defaults_to_verified() {
        let c = connection(
            Provider::Gcs,
            Source::Gcs {
                credentials: PathBuf::from("/nonexistent/creds.json"),
                project: None,
            },
        );
        assert_eq!(tls_state(&c), TlsState::Verified);
    }

    #[test]
    fn tls_text_names_each_state() {
        assert_eq!(tls_icon(TlsState::Verified), "🛡️");
        assert_eq!(tls_hover(TlsState::Verified), "TLS: certificate verified");
        assert_eq!(tls_icon(TlsState::Unverified), "⚠️");
        assert_eq!(
            tls_hover(TlsState::Unverified),
            "TLS: certificate not verified"
        );
        assert_eq!(tls_icon(TlsState::Plain), "◌");
        assert_eq!(tls_hover(TlsState::Plain), "No TLS: plain connection");
    }

    #[test]
    fn encryption_text_and_glyph() {
        assert_eq!(encryption_icon(Encryption::Off), "⊘");
        assert_eq!(
            encryption_hover(Encryption::Off),
            "Client-side encryption off"
        );
        assert_eq!(encryption_icon(Encryption::File), "🔑");
        assert_eq!(
            encryption_hover(Encryption::File),
            "Client-side encryption on · keyring file"
        );
        assert_eq!(encryption_icon(Encryption::Command), "🔑");
        assert_eq!(
            encryption_hover(Encryption::Command),
            "Client-side encryption on · keyring command"
        );
    }
}
