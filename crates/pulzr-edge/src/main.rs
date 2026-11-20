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
    let mut args = std::env::args().skip(1);
    let first = args.next();
    let (ble, path): (bool, PathBuf) = match first.as_deref() {
        Some("--version" | "-V") => {
            println!("pulzr-edge {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        Some("--help" | "-h") | None => usage(),
        Some("ble") => match args.next() {
            Some(p) => (true, p.into()),
            None => usage(),
        },
        Some(p) => (false, p.into()),
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

    if ble {
        return run_ble(cfg).await;
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

fn usage() -> ! {
    eprintln!("usage: pulzr-edge <config.toml>       Modbus\n       pulzr-edge ble <config.toml>   Bluetooth LE ([ble] in the config)");
    std::process::exit(2);
}

/// `pulzr-edge ble`: the gateway's Bluetooth controller, as its own process
/// with its own buffer and MQTT session (client id `<adapter>-ble`) under
/// the same adapter and broker login.
async fn run_ble(cfg: Config) -> Result<()> {
    let Some(ble) = cfg.ble.clone().filter(|b| b.enabled) else {
        tracing::warn!("Bluetooth is off ([ble] enabled = false); nothing to do");
        std::process::exit(3);
    };
    let adapter = cfg.adapter();
    let dir = cfg.state_dir.join("ble");
    let out = Outbox::open(
        &dir.join("outbox"),
        &adapter,
        &cfg.gateway_eui,
        cfg.buffer.max_bytes,
    )?;
    tracing::info!(version = env!("CARGO_PKG_VERSION"), %adapter, transport = %ble.transport, queued = out.pending(), "pulzr-edge ble starting");
    let (stop_tx, stop_rx) = watch::channel(false);
    let router = Arc::new(pulzr_edge::ble::commands::Router::new(ble.commands));
    let opts = mqtt::options(&cfg.broker, &format!("{adapter}-ble")).context("broker options")?;
    let session = tokio::spawn(mqtt::run(
        opts,
        cfg.broker.clone(),
        out.clone(),
        Arc::new(router.clone()),
        stop_rx.clone(),
    ));
    let radio = tokio::spawn(pulzr_edge::ble::run(
        ble,
        dir.join("keys"),
        out.clone(),
        router,
        stop_rx,
    ));
    shutdown_signal().await;
    tracing::info!("shutting down");
    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), radio).await;
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
