use crate::core::MtopError;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls_platform_verifier::BuilderVerifierExt;
use std::path::PathBuf;
use tokio::fs;
use tokio_rustls::rustls::client::WantsClientCert;
use tokio_rustls::rustls::{ClientConfig, ConfigBuilder, RootCertStore};

/// Configuration for establishing a TLS connection to server with optional mTLS.
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// Path to a custom certificate authority. If not supplied, default root certificates
    /// from the `webpki_roots` crate are used.
    pub ca_path: Option<PathBuf>,

    /// Path to a PEM format client certificate for mTLS. If not supplied, no client authentication
    /// is used when connecting to the server.
    pub cert_path: Option<PathBuf>,

    /// Path to a PEM format client key for mTLS. If not supplied, no client authentication is used
    /// when connecting to the server.
    pub key_path: Option<PathBuf>,

    /// Name of the server for validating its certificate. If not supplied, the hostname
    /// of the server is used instead.
    pub server_name: Option<ServerName<'static>>,
}

pub(crate) async fn tls_client_config(config: TlsConfig) -> Result<ClientConfig, MtopError> {
    let client_cert = if let Some(p) = &config.cert_path {
        Some(load_cert(p).await?)
    } else {
        None
    };

    let client_key = if let Some(p) = &config.key_path {
        Some(load_key(p).await?)
    } else {
        None
    };

    let builder = if let Some(p) = &config.ca_path {
        tracing::debug!(message = "using custom TLS CA for server certificate verification", cert = ?p);
        builder_for_custom_root_store(p).await?
    } else {
        tracing::debug!(message = "using platform TLS CA for server certificate verification");
        builder_for_platform_roots()?
    };

    let client_config = match (client_cert, client_key, config.cert_path, config.key_path) {
        (Some(cert), Some(key), Some(cert_path), Some(key_path)) => {
            tracing::debug!(message = "using TLS key and cert for client authentication", key = ?key_path, cert = ?cert_path);
            builder
                .with_client_auth_cert(cert, key)
                .map_err(|e| MtopError::configuration_cause("unable to use client cert or key", e))?
        }
        _ => {
            tracing::debug!(message = "not using any TLS client authentication");
            builder.with_no_client_auth()
        }
    };

    Ok(client_config)
}

async fn load_cert(path: &PathBuf) -> Result<Vec<CertificateDer<'static>>, MtopError> {
    let contents = fs::read(path)
        .await
        .map_err(|e| MtopError::configuration_cause(format!("unable to load cert {}", path.display()), e))?;

    let mut out = Vec::new();
    for res in CertificateDer::pem_slice_iter(&contents) {
        out.push(
            res.map_err(|e| MtopError::configuration_cause(format!("unable to parse cert {}", path.display()), e))?,
        );
    }

    Ok(out)
}

async fn load_key(path: &PathBuf) -> Result<PrivateKeyDer<'static>, MtopError> {
    let contents = fs::read(path)
        .await
        .map_err(|e| MtopError::configuration_cause(format!("unable to load key {}", path.display()), e))?;

    PrivateKeyDer::from_pem_slice(&contents)
        .map_err(|e| MtopError::configuration_cause(format!("unable to parse key {}", path.display()), e))
}

async fn builder_for_custom_root_store(
    path: &PathBuf,
) -> Result<ConfigBuilder<ClientConfig, WantsClientCert>, MtopError> {
    let ca_certs = load_cert(path).await?;
    let mut store = RootCertStore::empty();
    for cert in ca_certs {
        store
            .add(cert)
            .map_err(|e| MtopError::configuration_cause("unable to parse TLS CA cert", e))?;
    }

    Ok(ClientConfig::builder().with_root_certificates(store))
}

fn builder_for_platform_roots() -> Result<ConfigBuilder<ClientConfig, WantsClientCert>, MtopError> {
    ClientConfig::builder()
        .with_platform_verifier()
        .map_err(|e| MtopError::configuration_cause("unable to load platform TLS CA certs", e))
}
