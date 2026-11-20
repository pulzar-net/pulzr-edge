//! Bluetooth LE: the gateway's own Bluetooth controller, through the
//! `bluetooth-host` stack (pulzar-net/bluetooth-bridge), speaking protocol v1.
//!
//! - Each device heard is `ble:<EUI-64>` (its address, or the identity its
//!   private address resolved to, by the IEEE MAC-48 rule: `fffe` in the
//!   middle). One `up` per device per interval: the latest advertising data
//!   (and scan response) as `raw`, the best RSSI of the interval in `rx`, and
//!   `link.ble`. What the bytes mean is the platform's BLE stage, the same as
//!   for every adapter.
//! - The controller itself is the device `ble:<its EUI-64>` (as a Zigbee
//!   coordinator is): `state` online, and the advertising intents.
//! - GATT intents go to a device's key: the gateway connects to it, does the
//!   operation and disconnects.
//!
//! Runs as its own process, `pulzr-edge ble <config.toml>`, and only when
//! `[ble] enabled = true`: turning it on for a gateway whose beacons are live
//! is a cutover, not a default.

pub mod commands;
pub mod window;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use base64::Engine;
use bluetooth_host::{Address, Host, HostConfig, HostEvent, ScanConfig, TransportSpec};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::watch;

use crate::outbox::{now, Outbox};
use window::{Heard, Window};

/// `[ble]` in the configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BleCfg {
    /// The switch. Nothing runs unless it is on.
    #[serde(default)]
    pub enabled: bool,
    /// `hci0` (the kernel's driver loads the firmware, then the user channel),
    /// `serial:/dev/ttyAMA0:115200`, or `tcp:host:port`.
    #[serde(default = "default_transport")]
    pub transport: String,
    /// One `up` per device per interval, seconds.
    #[serde(default = "default_interval")]
    pub interval_secs: u64,
    /// Only devices whose address (or identity) starts with one of these hex
    /// prefixes, such as a beacon maker's OUI (`BC5729`). A car park is full
    /// of phones and watches, none of them ours to upload.
    #[serde(default)]
    pub admit: Vec<String>,
    /// Admit every device, with `admit` empty (for a site that wants them all).
    #[serde(default)]
    pub admit_all: bool,
    /// Ignore weaker signals, dBm.
    #[serde(default = "default_min_rssi")]
    pub min_rssi: i8,
    /// Active scanning (scan requests; more airtime, scan responses).
    #[serde(default)]
    pub active: bool,
    /// Put what the gateway decoded (AD fields, beacon formats) in
    /// `link.ble.parsed`. Costs bytes on every message; off by default.
    #[serde(default)]
    pub parsed: bool,
    /// Answer GATT and advertising intents.
    #[serde(default = "yes")]
    pub commands: bool,
    /// Run on a static random address even when the controller has a public one.
    #[serde(default)]
    pub random_address: bool,
}

fn default_transport() -> String {
    "hci0".into()
}
fn default_interval() -> u64 {
    10
}
fn default_min_rssi() -> i8 {
    -127
}
fn yes() -> bool {
    true
}

impl BleCfg {
    pub fn validate(&self) -> Result<()> {
        if self.transport.parse::<TransportSpec>().is_err() {
            bail!(
                "ble.transport {:?}: want hciN, serial:PATH:BAUD or tcp:HOST:PORT",
                self.transport
            );
        }
        if !(1..=3600).contains(&self.interval_secs) {
            bail!("ble.interval_secs is 1 to 3600");
        }
        if self.admit.is_empty() && !self.admit_all {
            bail!(
                "ble.admit is empty: list the address prefixes to publish, or set admit_all = true"
            );
        }
        for p in &self.admit {
            if prefix(p).is_none() {
                bail!("ble.admit {p:?} is not a hex address prefix");
            }
        }
        Ok(())
    }

    fn prefixes(&self) -> Vec<Vec<u8>> {
        self.admit.iter().filter_map(|p| prefix(p)).collect()
    }
}

/// `BC:57:29` or `bc5729` as bytes, most significant first.
fn prefix(p: &str) -> Option<Vec<u8>> {
    let hex: String = p.chars().filter(|c| *c != ':' && *c != '-').collect();
    if hex.is_empty() || hex.len() > 12 || !hex.len().is_multiple_of(2) {
        return None;
    }
    hex::decode(hex).ok()
}

/// `ble:<EUI-64>` for an address: the MAC-48 with `fffe` in the middle.
pub fn key_for(a: &Address) -> String {
    let b = a.bytes;
    format!(
        "ble:{:02x}{:02x}{:02x}fffe{:02x}{:02x}{:02x}",
        b[5], b[4], b[3], b[2], b[1], b[0]
    )
}

/// The `up` body for one device's interval (without `v` and `seq`).
pub fn up_body(h: &Heard, via: &str, parsed: bool) -> Value {
    let mut data = h.data.clone();
    if let Some(r) = &h.scan_response {
        data.extend_from_slice(r);
    }
    let mut ble = serde_json::Map::new();
    // The PDU in `adv`, never in `adv_type`: the protocol's `adv_type` is the
    // class a gateway reported (a KGateway's), which the platform passes to
    // the device type's codec as fPort. A gateway that only hears the radio
    // has no class to report, as PulzrOS's shell scanner never did.
    ble.insert("adv".into(), json!(h.kind));
    if let Some(p) = h.tx_power {
        ble.insert("tx_power".into(), json!(p));
    }
    if parsed {
        let adv = bluetooth_adv::Advertisement::parse(&data);
        let mut p = serde_json::to_value(&adv).unwrap_or_else(|_| json!({}));
        let ctx = bluetooth_adv::beacon::Context::for_address(h.address.bytes);
        let beacons = bluetooth_adv::beacon::decode(&adv, &ctx);
        if !beacons.is_empty() {
            p["beacons"] = serde_json::to_value(&beacons).unwrap_or(Value::Null);
        }
        if let Some(id) = h.identity {
            p["identity"] = json!(id.to_string());
        }
        ble.insert("parsed".into(), p);
    }
    json!({
        "raw": { "ts": h.ts, "b64": base64::engine::general_purpose::STANDARD.encode(&data) },
        "rx": [ { "via": via, "rssi": h.rssi, "ts": h.ts, "time": "received" } ],
        "link": { "ble": Value::Object(ble) },
    })
}

/// Opens the controller, retrying while it is absent or still loading firmware.
async fn open(cfg: &BleCfg, dir: PathBuf, stop: &mut watch::Receiver<bool>) -> Option<Host> {
    let spec: TransportSpec = cfg.transport.parse().ok()?;
    let mut delay = Duration::from_secs(1);
    loop {
        let host_cfg = HostConfig {
            data_dir: Some(dir.clone()),
            device_name: "gateway".into(),
            use_public_address: !cfg.random_address,
            ..HostConfig::default()
        };
        match Host::open(&spec, host_cfg).await {
            Ok(h) => return Some(h),
            Err(e) => {
                tracing::warn!(error = %e, retry_in = ?delay, transport = %spec, "Bluetooth controller");
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = stop.changed() => return None,
                }
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        }
    }
}

/// Runs the module until `stop`: scanning and publishing, and (when the
/// commands are on) the intents through `router`.
pub async fn run(
    cfg: BleCfg,
    data_dir: PathBuf,
    out: Outbox,
    router: Arc<commands::Router>,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        let Some(host) = open(&cfg, data_dir.clone(), &mut stop).await else {
            return Ok(());
        };
        let controller = key_for(&host.address());
        tracing::info!(%controller, features = ?host.capabilities().le_feature_names(), "Bluetooth controller up");
        out.state(&controller, true, Some(now()), None);
        router.attach(host.clone(), controller.clone());
        let reason = session(&cfg, &host, &out, &router, &mut stop).await;
        router.detach();
        out.state(&controller, false, Some(now()), None);
        match reason {
            None => return Ok(()),
            Some(r) => {
                tracing::warn!(reason = %r, "Bluetooth host stopped; reopening");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

/// One host's life: `None` when told to stop, else why the host went away.
async fn session(
    cfg: &BleCfg,
    host: &Host,
    out: &Outbox,
    router: &commands::Router,
    stop: &mut watch::Receiver<bool>,
) -> Option<String> {
    let mut events = host.events();
    let scan = ScanConfig {
        active: cfg.active,
        dedup: bluetooth_host::Dedup::Off,
        ..ScanConfig::default()
    };
    if let Err(e) = host.start_scan(scan).await {
        return Some(format!("starting the scan: {e}"));
    }
    let mut window = Window::new(cfg.prefixes(), cfg.min_rssi);
    let mut tick = tokio::time::interval(Duration::from_secs(cfg.interval_secs));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let via = out.via();
    loop {
        tokio::select! {
            _ = stop.changed() => {
                let _ = host.stop_scan().await;
                flush(&mut window, out, &via, cfg.parsed);
                return None;
            }
            _ = tick.tick() => flush(&mut window, out, &via, cfg.parsed),
            e = events.recv() => match e {
                Ok(HostEvent::Advertisement(r)) => {
                    router.heard(&r);
                    window.add(&r, now());
                }
                Ok(HostEvent::Stopped(reason)) => return Some(reason),
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(dropped = n, "advertisements dropped (the process is too slow)");
                }
                Err(_) => return Some("host event channel closed".into()),
            },
        }
    }
}

fn flush(window: &mut Window, out: &Outbox, via: &str, parsed: bool) {
    for h in window.drain() {
        out.up_body(&key_for(&h.device()), up_body(&h, via, parsed));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_and_prefixes() {
        let a: Address = "BC:57:29:00:00:01".parse().unwrap();
        assert_eq!(key_for(&a), "ble:bc5729fffe000001");
        assert_eq!(prefix("BC:57:29"), Some(vec![0xbc, 0x57, 0x29]));
        assert_eq!(prefix("bc5"), None);
        let base = BleCfg {
            enabled: true,
            transport: "hci0".into(),
            interval_secs: 10,
            admit: vec![],
            admit_all: false,
            min_rssi: -127,
            active: false,
            parsed: false,
            commands: true,
            random_address: false,
        };
        assert!(
            base.validate().is_err(),
            "an empty admit list must be deliberate"
        );
        assert!(BleCfg {
            admit_all: true,
            ..base.clone()
        }
        .validate()
        .is_ok());
        assert!(BleCfg {
            admit: vec!["BC5729".into()],
            ..base.clone()
        }
        .validate()
        .is_ok());
        assert!(BleCfg {
            transport: "usb:1".into(),
            admit_all: true,
            ..base
        }
        .validate()
        .is_err());
    }
}
