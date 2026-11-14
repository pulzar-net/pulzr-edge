use anyhow::{Context, Result};
use pulzr_edge::{
    config::{BusKind, Config},
    modbus::{bus, regmap::RegisterMap},
    mqtt,
    outbox::Outbox,
};
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{mpsc, watch};

#[tokio::main]
async fn main() -> Result<()> {
    let path: PathBuf = match std::env::args().nth(1).as_deref() {
        Some("--version" | "-V") => {
            println!("pulzr-edge {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        Some("--help" | "-h") | None => {
            eprintln!("usage: pulzr-edge <config.toml>");
            std::process::exit(2);
        }
        Some(p) => p.into(),
    };
    let cfg = Config::load(&path)?;

    let filter =
        tracing_subscriber::EnvFilter::try_new(&cfg.log.level).unwrap_or_else(|_| "info".into());
    let sub = tracing_subscriber::fmt().with_env_filter(filter);
    if cfg.log.json {
        sub.json().init();
    } else {
        sub.init();
    }

    let adapter = cfg.adapter();
    let out = Outbox::open(
        &cfg.state_dir,
        &adapter,
        &cfg.gateway_eui,
        cfg.buffer.max_bytes,
    )?;
    tracing::info!(version = env!("CARGO_PKG_VERSION"), %adapter, queued = out.pending(), "pulzr-edge starting");

    let (stop_tx, stop_rx) = watch::channel(false);
    let base = path.parent().map(PathBuf::from).unwrap_or_default();
    let mut router = bus::Router::default();
    let mut tasks = Vec::new();
    let mut maps: HashMap<PathBuf, Arc<RegisterMap>> = HashMap::new();
    for b in &cfg.buses {
        let (tx, rx) = mpsc::channel(32);
        let mut units = Vec::new();
        for u in &b.units {
            let file = base.join(&u.map);
            let map = match maps.get(&file) {
                Some(m) => m.clone(),
                None => {
                    let m = Arc::new(RegisterMap::load(&file)?);
                    maps.insert(file, m.clone());
                    m
                }
            };
            let key = cfg.device_key(&b.id, u.id);
            router.add(&key, map.clone(), tx.clone());
            units.push(bus::UnitSpec {
                key,
                unit: u.id,
                map,
                offline_after: u.offline_after,
            });
        }
        let kind = match &b.kind {
            BusKind::Rtu {
                serial,
                baud,
                parity,
                data_bits,
                stop_bits,
            } => BusKind::Rtu {
                serial: serial.clone(),
                baud: *baud,
                parity: parity.clone(),
                data_bits: *data_bits,
                stop_bits: *stop_bits,
            },
            BusKind::Tcp { address } => BusKind::Tcp {
                address: address.clone(),
            },
        };
        let s = bus::BusSettings {
            id: b.id.clone(),
            kind,
            timeout: Duration::from_millis(b.timeout_ms),
            retries: b.retries,
            backoff: Duration::from_millis(b.backoff_ms),
            turnaround: Duration::from_millis(b.turnaround_ms),
        };
        tasks.push(tokio::spawn(bus::run(
            s,
            units,
            out.clone(),
            rx,
            stop_rx.clone(),
        )));
    }

    let opts = mqtt::options(&cfg.broker, &adapter).context("broker options")?;
    let session = tokio::spawn(mqtt::run(
        opts,
        cfg.broker.clone(),
        out.clone(),
        Arc::new(router),
        stop_rx.clone(),
    ));

    shutdown_signal().await;
    tracing::info!("shutting down");
    let _ = stop_tx.send(true);
    for t in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(10), t).await;
    }
    let _ = tokio::time::timeout(Duration::from_secs(5), session).await;
    out.flush();
    tracing::info!(left_in_buffer = out.pending(), "stopped");
    Ok(())
}

async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
