//! Modbus TCP device simulator, for testing the daemon without hardware.
//!
//! pulzr-edge-sim --listen 127.0.0.1:5020 --unit 17 --map meter.toml [--unit 18 --map other.toml]...

use anyhow::{bail, Context, Result};
use pulzr_edge::{modbus::regmap::RegisterMap, sim};
use std::{path::PathBuf, time::Duration};

const USAGE: &str =
    "usage: pulzr-edge-sim [--listen ADDR] --unit ID --map FILE [--unit ID --map FILE]...";

#[tokio::main]
async fn main() -> Result<()> {
    let mut listen = "127.0.0.1:5020".to_string();
    let (mut ids, mut maps) = (vec![], vec![]);
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut value = || args.next().with_context(|| format!("{a} needs a value"));
        match a.as_str() {
            "--version" | "-V" => {
                println!("pulzr-edge-sim {}", env!("CARGO_PKG_VERSION"));
                return Ok(());
            }
            "--listen" => listen = value()?,
            "--unit" => ids.push(value()?.parse::<u8>().context("--unit is 0-255")?),
            "--map" => maps.push(PathBuf::from(value()?)),
            _ => {
                eprintln!("{USAGE}");
                std::process::exit(2);
            }
        }
    }
    if ids.is_empty() || ids.len() != maps.len() {
        eprintln!("{USAGE}");
        std::process::exit(2);
    }
    let mut units = vec![];
    for (id, path) in ids.into_iter().zip(maps) {
        if units.iter().any(|(u, _)| *u == id) {
            bail!("unit {id} given twice");
        }
        units.push((id, RegisterMap::load(&path)?));
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .with_context(|| format!("listen on {listen}"))?;
    for (id, m) in &units {
        tracing::info!(unit = id, model = %m.model, fields = m.fields.len(), "serving");
    }
    tracing::info!("listening on {}", listener.local_addr()?);
    let sim = sim::Sim::new(units);
    tokio::spawn(sim::run_ticker(sim.clone(), Duration::from_secs(1)));
    sim::serve(listener, sim).await?;
    Ok(())
}
