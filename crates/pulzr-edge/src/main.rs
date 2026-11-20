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
        Some("radio") => return radio(args.collect()).await,
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
    eprintln!(
        "usage: pulzr-edge <config.toml>                       Modbus
       pulzr-edge ble <config.toml>                   Bluetooth LE ([ble] in the config)
       pulzr-edge radio serve <radio id> <config.toml>  a radio module (docs/radio-modules.md in PulzrOS)
       pulzr-edge radio <radio id> <op> < params.json   one op, as the console API sends it"
    );
    std::process::exit(2);
}

/// `pulzr-edge radio`: run a radio's module, or send it one op. The op's
/// params come on stdin, never the command line, because they may carry a
/// network key. Prints the result; on a refusal prints the reason on stderr
/// and exits 2 (bad request), 1 (refused or busy) or 3 (no module running).
async fn radio(args: Vec<String>) -> Result<()> {
    use pulzr_edge::radio::{self, control, Paths};
    let mut paths = Paths::default();
    if let Ok(run) = std::env::var("PULZR_EDGE_RUN") {
        paths.run = run.into();
    }
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["serve", id, config] => {
            let cfg = Config::load(std::path::Path::new(config))?;
            let filter = tracing_subscriber::EnvFilter::try_new(&cfg.log.level)
                .unwrap_or_else(|_| "info".into());
            tracing_subscriber::fmt().with_env_filter(filter).init();
            radio::serve(cfg, id, paths).await
        }
        [id, op] if radio::valid_radio_id(id) => {
            use std::io::Read;
            let mut input = String::new();
            std::io::stdin().read_to_string(&mut input)?;
            let params: serde_json::Value = if input.trim().is_empty() {
                serde_json::json!({})
            } else {
                match serde_json::from_str(&input) {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!("The request is not JSON.");
                        std::process::exit(2);
                    }
                }
            };
            let answer = control::ask(&paths.socket(id), id, op, params).await;
            if answer["ok"] == true {
                println!("{}", answer["result"]);
                return Ok(());
            }
            eprintln!(
                "{}",
                answer["error"]
                    .as_str()
                    .unwrap_or("The radio module refused.")
            );
            std::process::exit(match answer["code"].as_str() {
                Some("bad-request") => 2,
                Some("no-module") => 3,
                _ => 1,
            });
        }
        _ => usage(),
    }
}

/// `pulzr-edge ble`: the gateway's Bluetooth controller, as its own process
/// with its own buffer and MQTT session (client id `<adapter>-ble`) under
/// the same adapter and broker login.
#[cfg(not(target_endian = "little"))]
async fn run_ble(_cfg: Config) -> Result<()> {
    anyhow::bail!("Bluetooth is not supported on big-endian machines")
}

#[cfg(target_endian = "little")]
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
