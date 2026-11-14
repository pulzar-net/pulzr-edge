//! The one MQTT 5 session to the platform broker: TLS with a client certificate,
//! commands in, queued messages out one at a time until each is acknowledged.

use crate::{config::Broker, modbus::bus::Router, outbox::Outbox};
use anyhow::{Context, Result};
use rumqttc::{
    tokio_rustls::rustls::{
        self,
        pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
    },
    v5::{mqttbytes::QoS, AsyncClient, Event, Incoming, MqttOptions},
    TlsConfiguration, Transport,
};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::sync::{watch, Notify};

fn tls(b: &Broker) -> Result<TlsConfiguration> {
    let ca = b.ca.as_ref().unwrap();
    let mut roots = rustls::RootCertStore::empty();
    for c in
        CertificateDer::pem_file_iter(ca).with_context(|| format!("reading {}", ca.display()))?
    {
        roots.add(c?)?;
    }
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots);
    let cfg = match (&b.cert, &b.key) {
        (Some(cert), Some(key)) => {
            let chain = CertificateDer::pem_file_iter(cert)
                .with_context(|| format!("reading {}", cert.display()))?
                .collect::<Result<Vec<_>, _>>()?;
            let key = PrivateKeyDer::from_pem_file(key)
                .with_context(|| format!("reading {}", key.display()))?;
            builder.with_client_auth_cert(chain, key)?
        }
        _ => builder.with_no_client_auth(),
    };
    Ok(TlsConfiguration::Rustls(Arc::new(cfg)))
}

/// The password in `file`, without its trailing newline.
fn read_password(file: &Path) -> Result<String> {
    let text =
        std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
    Ok(text.trim_end_matches(['\r', '\n']).to_owned())
}

/// Put the broker login into `o`, reading the password file now. `run` calls it
/// again before every reconnect, so a rotated token is picked up.
pub fn set_login(o: &mut MqttOptions, b: &Broker) -> Result<()> {
    if let (Some(user), Some(file)) = (&b.username, &b.password_file) {
        o.set_credentials(user, read_password(file)?);
    }
    Ok(())
}

pub fn options(b: &Broker, client_id: &str) -> Result<MqttOptions> {
    let mut o = MqttOptions::new(client_id, &b.host, b.port);
    o.set_keep_alive(Duration::from_secs(b.keep_alive_secs));
    o.set_clean_start(false);
    o.set_session_expiry_interval(Some(3600));
    set_login(&mut o, b)?;
    if b.tls {
        o.set_transport(Transport::tls_with_config(tls(b)?));
    }
    Ok(o)
}

/// Run until `shutdown`. Reconnects with a backoff of 1 s doubling to 30 s.
pub async fn run(
    opts: MqttOptions,
    broker: Broker,
    out: Outbox,
    router: Arc<Router>,
    mut shutdown: watch::Receiver<bool>,
) {
    let (client, mut ev) = AsyncClient::new(opts, 32);
    let (conn_tx, conn_rx) = watch::channel(false);
    let acked = Arc::new(Notify::new());
    let sender = tokio::spawn(send_loop(
        client.clone(),
        out.clone(),
        conn_rx,
        acked.clone(),
        shutdown.clone(),
    ));
    let cmd_filter = format!("pulzr/a/{}/d/+/cmd", out.adapter());
    let mut backoff = Duration::from_secs(1);
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            e = ev.poll() => match e {
                Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                    backoff = Duration::from_secs(1);
                    tracing::info!("connected to the broker");
                    if let Err(e) = client.subscribe(cmd_filter.clone(), QoS::AtLeastOnce).await {
                        tracing::error!(error = %e, "subscribe failed");
                    }
                    let _ = conn_tx.send(true);
                }
                Ok(Event::Incoming(Incoming::PubAck(_))) => acked.notify_one(),
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    let topic = String::from_utf8_lossy(&p.topic).into_owned();
                    router.dispatch(&out, &topic, &p.payload);
                }
                Ok(_) => {}
                Err(e) => {
                    let _ = conn_tx.send(false);
                    tracing::warn!(error = %e, retry_in = ?backoff, "broker connection lost");
                    tokio::select! { _ = tokio::time::sleep(backoff) => {}, _ = shutdown.changed() => break }
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                    if let Err(e) = set_login(&mut ev.options, &broker) {
                        tracing::error!(error = %e, "password file unreadable, keeping the last password");
                    }
                }
            }
        }
    }
    let _ = client.disconnect().await;
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        while ev.poll().await.is_ok() {}
    })
    .await;
    sender.abort();
}

/// Publish the oldest queued message at QoS 1, wait for its PubAck, then drop it from
/// the buffer. One message in flight keeps replay order exact; an unacknowledged one
/// is sent again (the platform drops the repeated `seq`).
async fn send_loop(
    client: AsyncClient,
    out: Outbox,
    mut conn: watch::Receiver<bool>,
    acked: Arc<Notify>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        while !*conn.borrow() {
            tokio::select! { r = conn.changed() => if r.is_err() { return }, _ = shutdown.changed() => return }
        }
        let Some(entry) = out.peek() else {
            tokio::select! { () = out.wait() => {}, _ = shutdown.changed() => return, _ = tokio::time::sleep(Duration::from_secs(5)) => {} }
            continue;
        };
        // forget an ack that arrived late for an earlier attempt
        let _ = tokio::time::timeout(Duration::ZERO, acked.notified()).await;
        if client
            .publish(
                entry.topic.clone(),
                QoS::AtLeastOnce,
                false,
                entry.payload.clone(),
            )
            .await
            .is_err()
        {
            return;
        }
        match tokio::time::timeout(Duration::from_secs(15), acked.notified()).await {
            Ok(()) => out.ack(&entry),
            Err(_) => tracing::warn!(topic = %entry.topic, "no PubAck in 15 s, sending again"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker(file: &Path) -> Broker {
        Broker {
            host: "mqtt.example.net".into(),
            port: 8883,
            tls: false,
            ca: None,
            cert: None,
            key: None,
            username: Some("aabbccddeeff0011".into()),
            password_file: Some(file.to_path_buf()),
            keep_alive_secs: 30,
        }
    }

    #[test]
    fn password_file_is_read_trimmed_and_reread() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("password");
        std::fs::write(&file, "s3cret\n").unwrap();
        let b = broker(&file);
        let mut o = options(&b, "edge-aabbccddeeff0011").unwrap();
        let login = |o: &MqttOptions| {
            let l = o.credentials().unwrap();
            (l.username, l.password)
        };
        assert_eq!(
            login(&o),
            ("aabbccddeeff0011".to_string(), "s3cret".to_string())
        );
        std::fs::write(&file, "rotated\r\n").unwrap();
        set_login(&mut o, &b).unwrap();
        assert_eq!(login(&o).1, "rotated".to_string());
        std::fs::remove_file(&file).unwrap();
        assert!(set_login(&mut o, &b).is_err());
    }
}
