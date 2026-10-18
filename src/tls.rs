//! Mutual TLS between nodes (feature `tls`, rustls 0.23 with the ring provider).
//!
//! Identity model:
//! * every node certificate carries the DNS SAN `node-<id>` (see [`node_name`]);
//! * a dialing node verifies the server against `node-<to>`;
//! * the accepting node verifies the client chain against the CA and then
//!   requires the certificate to be valid for `node-<id>` of the id announced in
//!   `PeerHello`. A hello whose id does not match the certificate is refused,
//!   so the id comes from the certificate, not from the peer's word.
//! * CLI clients may connect without a certificate (client path only); they
//!   verify the server against [`CLIENT_SERVER_NAME`], which every node
//!   certificate must also carry.

use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::rpc::NodeId;

/// SAN every node certificate carries so CLI clients can verify any node.
pub const CLIENT_SERVER_NAME: &str = "raft-kv";

/// DNS SAN that identifies node `id`.
pub fn node_name(id: NodeId) -> String {
    format!("node-{id}")
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Loaded certificates and the rustls configs built from them.
pub struct TlsMaterial {
    server: Option<Arc<ServerConfig>>,
    client: Arc<ClientConfig>,
}

impl std::fmt::Debug for TlsMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsMaterial")
            .field("server", &self.server.is_some())
            .finish()
    }
}

impl TlsMaterial {
    /// Node material: certificate chain + key (PEM) and the cluster CA (PEM).
    pub fn from_pem_files(cert: &Path, key: &Path, ca: &Path) -> anyhow::Result<Self> {
        let read = |p: &Path| std::fs::read(p).with_context(|| format!("read {}", p.display()));
        Self::from_pem(&read(cert)?, &read(key)?, &read(ca)?)
    }

    pub fn from_pem(cert_pem: &[u8], key_pem: &[u8], ca_pem: &[u8]) -> anyhow::Result<Self> {
        let roots = Arc::new(roots(ca_pem)?);
        let certs = certs(cert_pem)?;
        let key = PrivateKeyDer::from_pem_slice(key_pem).context("parse private key")?;

        let verifier = WebPkiClientVerifier::builder_with_provider(roots.clone(), provider())
            // Anonymous CLI clients are allowed; peer hellos require a cert.
            .allow_unauthenticated()
            .build()
            .context("client verifier")?;
        let server = ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()?
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs.clone(), key.clone_key())
            .context("server cert")?;
        let client = ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_client_auth_cert(certs, key)
            .context("client cert")?;
        Ok(Self {
            server: Some(Arc::new(server)),
            client: Arc::new(client),
        })
    }

    /// Client-only material (CLI): verifies servers, presents no certificate.
    pub fn client_only(ca_pem: &[u8]) -> anyhow::Result<Self> {
        let client = ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots(ca_pem)?)
            .with_no_client_auth();
        Ok(Self {
            server: None,
            client: Arc::new(client),
        })
    }

    pub fn acceptor(&self) -> anyhow::Result<TlsAcceptor> {
        self.server
            .clone()
            .map(TlsAcceptor::from)
            .ok_or_else(|| anyhow!("client-only TLS material cannot accept"))
    }

    pub fn connector(&self) -> TlsConnector {
        TlsConnector::from(self.client.clone())
    }
}

fn roots(ca_pem: &[u8]) -> anyhow::Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for c in certs(ca_pem)? {
        roots.add(c).context("add CA")?;
    }
    Ok(roots)
}

fn certs(pem: &[u8]) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let v = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .context("parse certificates")?;
    if v.is_empty() {
        return Err(anyhow!("no certificate in PEM"));
    }
    Ok(v)
}

/// True if the (already chain-verified) end-entity certificate names node `id`.
pub fn cert_names_node(cert: &CertificateDer<'_>, id: NodeId) -> bool {
    let Ok(name) = ServerName::try_from(node_name(id)) else {
        return false;
    };
    webpki::EndEntityCert::try_from(cert)
        .and_then(|c| c.verify_is_valid_for_subject_name(&name))
        .is_ok()
}

/// Server name a dialing node expects from peer `to`.
pub fn peer_server_name(to: NodeId) -> ServerName<'static> {
    ServerName::try_from(node_name(to)).expect("node-<id> is a valid DNS name")
}

pub fn client_server_name() -> ServerName<'static> {
    ServerName::try_from(CLIENT_SERVER_NAME).expect("valid DNS name")
}
