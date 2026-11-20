//! Radio modules: one coordinator each (a Zigbee network on an EmberZNet or
//! Z-Stack dongle, a Z-Wave network on a Serial API controller), driven by the
//! contract in PulzrOS's `docs/radio-modules.md`.
//!
//! Each radio runs in a process of its own, `pulzr-edge radio serve <radio id>
//! <config>`, so one stack failing never takes another network down. The
//! process reads the radio's file (`/etc/pulzr-edge/radios/<radio id>.json`,
//! written by PulzrOS's `pulzr-radios use`), finds the radio's port in
//! pulzr-probe's report, starts the module for its protocol, and answers the
//! radio's control socket. Everything that changes a network comes in on that
//! socket from the gateway's console API, as a person's session: never over
//! MQTT, whose commands reach a gateway from API keys and agents too.
//!
//! A module implements [`RadioModule`]: `call` takes an op and its JSON
//! params and answers within [`OP_TIMEOUT`]. What it learns afterwards (a
//! device joined, an S2 inclusion wants its PIN) it publishes as protocol
//! `event`s on the coordinator's key, through [`RadioCtx`].

pub mod control;

use crate::config::Config;
use crate::outbox::Outbox;
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::future::Future;
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

/// The longest an op may take before the console is told the radio is busy.
/// Anything longer answers once it has started and reports the rest as events.
pub const OP_TIMEOUT: Duration = Duration::from_secs(30);

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Why an op was not done, in words a person can act on.
#[derive(Debug, Clone, PartialEq)]
pub enum RadioError {
    /// The request itself is wrong (a missing or malformed param): 400.
    BadRequest(String),
    /// Not now or not on this radio (no network yet, a backup of another
    /// network, a dongle that cannot take the old IEEE address): 409.
    Refused(String),
    /// Another op or an inclusion is running.
    Busy(String),
}

impl RadioError {
    pub fn code(&self) -> &'static str {
        match self {
            RadioError::BadRequest(_) => "bad-request",
            RadioError::Refused(_) => "refused",
            RadioError::Busy(_) => "busy",
        }
    }
    pub fn message(&self) -> &str {
        match self {
            RadioError::BadRequest(m) | RadioError::Refused(m) | RadioError::Busy(m) => m,
        }
    }
}

/// One coordinator's stack.
pub trait RadioModule: Send + Sync {
    /// `zigbee` or `zwave`.
    fn protocol(&self) -> &'static str;

    /// One op of the contract (`status`, `form`, `permit`, ...) with its
    /// params, which may carry keys: never log them. `use` and `release` are
    /// the gateway's, not the module's, and never arrive here.
    fn call<'a>(&'a self, op: &'a str, params: Value) -> BoxFuture<'a, Result<Value, RadioError>>;

    /// A protocol `cmd` for one of the module's devices (a ZCL write, a
    /// Z-Wave Basic Set). Every process of an adapter receives every command,
    /// so ignore keys outside your scheme. Never a `network.*` op: those come
    /// only through [`RadioModule::call`].
    fn dispatch(&self, _out: &Outbox, _topic: &str, _payload: &[u8]) {}
}

/// Starts a module on its radio. It may open the port; it must not write to
/// the radio's network before `form` or `restore`.
pub type Factory = fn(RadioCtx) -> Result<Arc<dyn RadioModule>>;

/// The module for a protocol in this build, if any. A stack adds its line.
pub fn factory(protocol: &str) -> Option<Factory> {
    match protocol {
        #[cfg(feature = "zigbee")]
        "zigbee" => Some(crate::zigbee::start),
        #[cfg(feature = "zwave")]
        "zwave" => Some(crate::zwave::start),
        #[cfg(feature = "matter")]
        "matter" => Some(crate::matter::start),
        _ => None,
    }
}

/// Where a radio process keeps and finds things. Fixed on a gateway; tests
/// point it into a temporary directory.
#[derive(Debug, Clone)]
pub struct Paths {
    /// `<radio id>.json`: the radio's protocol, network and keys (0600).
    pub radios: PathBuf,
    /// `<radio id>.sock` control sockets and `backup/<radio id>.json`.
    pub run: PathBuf,
    /// pulzr-probe's report, for the radio's port.
    pub probe_report: PathBuf,
    /// Run with the radio id once a backup is written; posts it to the
    /// platform with the gateway's claim proof and removes it.
    pub backup_hook: PathBuf,
}

impl Default for Paths {
    fn default() -> Self {
        Paths {
            radios: "/etc/pulzr-edge/radios".into(),
            run: "/var/run/pulzr-edge".into(),
            probe_report: "/var/run/pulzr-probe/radios.json".into(),
            backup_hook: "/usr/sbin/pulzr-radios".into(),
        }
    }
}

impl Paths {
    pub fn socket(&self, radio: &str) -> PathBuf {
        self.run.join("radio").join(format!("{radio}.sock"))
    }
    pub fn state_file(&self, radio: &str) -> PathBuf {
        self.radios.join(format!("{radio}.json"))
    }
    pub fn backup_file(&self, radio: &str) -> PathBuf {
        self.run.join("backup").join(format!("{radio}.json"))
    }
}

/// The radio id of a Matter fabric's controller: one per gateway, on no
/// port of its own (Matter rides on IP, and on Thread through the border
/// router), but run, driven and kept like any radio's module.
pub const MATTER: &str = "matter";

/// pulzr-probe's radio id (`usb-…`, `uart-…`, `lan-…`), or [`MATTER`].
pub fn valid_radio_id(id: &str) -> bool {
    if id == MATTER {
        return true;
    }
    let rest = ["usb-", "uart-", "lan-"]
        .iter()
        .find_map(|p| id.strip_prefix(p));
    matches!(rest, Some(r) if (1..=96).contains(&r.len())
        && r.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b)))
}

/// What a module is given.
#[derive(Clone)]
pub struct RadioCtx {
    pub radio: String,
    /// `/dev/ttyUSB0`, or `tcp://host:port` for a coordinator on the LAN.
    pub port: String,
    pub out: Outbox,
    paths: Paths,
    /// Where device firmware may be fetched from (`[firmware]`).
    pub firmware: crate::config::FirmwareCfg,
}

impl RadioCtx {
    pub fn new(radio: &str, port: &str, out: Outbox, paths: Paths) -> Self {
        RadioCtx {
            radio: radio.into(),
            port: port.into(),
            out,
            paths,
            firmware: Default::default(),
        }
    }

    /// The `[firmware]` hosts a module may fetch images from.
    pub fn with_firmware(mut self, firmware: crate::config::FirmwareCfg) -> Self {
        self.firmware = firmware;
        self
    }

    /// The radio's file: what `pulzr-radios use` wrote (`radio`, `protocol`)
    /// and whatever the module added (its network, its keys).
    pub fn load(&self) -> Result<Value> {
        let f = self.paths.state_file(&self.radio);
        let text =
            std::fs::read_to_string(&f).with_context(|| format!("reading {}", f.display()))?;
        serde_json::from_str(&text).with_context(|| format!("{} is not JSON", f.display()))
    }

    /// A file of the module's own beside the radio's file
    /// (`<radio id>.<name>`), kept across upgrades with it: a device store.
    /// A path of the module's own in the run directory (tmpfs, gone at
    /// reboot): `<run>/radio/<radio id>.<name>`, for downloads.
    pub fn scratch(&self, name: &str) -> PathBuf {
        self.paths
            .run
            .join("radio")
            .join(format!("{}.{name}", self.radio))
    }

    pub fn file(&self, name: &str) -> PathBuf {
        self.paths.radios.join(format!("{}.{name}", self.radio))
    }

    /// Replaces the radio's file, root-only, atomically. Keep `radio` and
    /// `protocol` in it.
    pub fn save(&self, state: &Value) -> Result<()> {
        write_private(&self.paths.state_file(&self.radio), state)
    }

    /// A protocol `event` on `key` (the coordinator's, usually). No key in
    /// `data`.
    pub fn event(&self, key: &str, name: &str, data: Value) {
        self.out.event(key, name, data, None);
    }

    /// The coordinator's `state` with its `network` (protocol v1's
    /// `network`: no `unformed`, no keys).
    pub fn network_state(&self, key: &str, network: Value) {
        self.out
            .state_body(key, json!({ "online": true, "network": network }));
    }

    /// Hands a backup to the platform: written root-only where the hook
    /// finds it, then the hook (`pulzr-radios backup <radio id>`) posts it
    /// over HTTPS with the gateway's claim proof and removes it. Never MQTT:
    /// a backup carries the network's keys. Returns once the file is written;
    /// the post runs in the background.
    pub fn backup(&self, backup: &Value) -> Result<()> {
        write_private(&self.paths.backup_file(&self.radio), backup)?;
        let hook = &self.paths.backup_hook;
        if hook.exists() {
            let mut cmd = std::process::Command::new(hook);
            cmd.arg("backup").arg(&self.radio);
            cmd.stdin(std::process::Stdio::null());
            cmd.stdout(std::process::Stdio::null());
            cmd.stderr(std::process::Stdio::null());
            match cmd.spawn() {
                // Reaped on its own thread so it never lingers as a zombie.
                Ok(mut child) => drop(std::thread::spawn(move || child.wait())),
                Err(e) => tracing::warn!(error = %e, "backup hook did not start"),
            }
        }
        self.event(
            &self.coordinator_hint(),
            "network.backup",
            json!({ "at": crate::outbox::now(), "bytes": serde_json::to_vec(backup).map_or(0, |b| b.len()) }),
        );
        Ok(())
    }

    /// The coordinator's key as the module last saved it (`coordinator` in
    /// its file), for events sent before the module names one itself.
    fn coordinator_hint(&self) -> String {
        self.load()
            .ok()
            .and_then(|s| {
                s.get("coordinator")
                    .and_then(Value::as_str)
                    .map(String::from)
            })
            .unwrap_or_else(|| self.out.via())
    }
}

/// Writes `value` as JSON to `path`, mode 0600, via a temporary file in the
/// same directory (0700), so a crash never leaves half a key file.
pub fn write_private(path: &Path, value: &Value) -> Result<()> {
    let dir = path.parent().context("no directory")?;
    std::fs::create_dir_all(dir)?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    let tmp = path.with_extension("json.tmp");
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)
            .with_context(|| format!("writing {}", tmp.display()))?;
        f.write_all(&serde_json::to_vec(value)?)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// The radio's port from pulzr-probe's report: its `port` for a USB or UART
/// radio, `tcp://address:port` for one on the LAN.
pub fn port_of(report: &Path, radio: &str) -> Result<String> {
    let text =
        std::fs::read_to_string(report).with_context(|| format!("reading {}", report.display()))?;
    let report: Value = serde_json::from_str(&text)?;
    let found = report["detected"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|r| r["id"] == radio)
        .with_context(|| format!("{radio} is not plugged in"))?;
    match found["port"].as_str() {
        Some(p) => Ok(p.to_string()),
        None => bail!("{radio} has no port in pulzr-probe's report"),
    }
}

/// `pulzr-edge radio serve <radio id> <config>`: the module for the radio's
/// protocol, its control socket and its MQTT session, until SIGTERM.
pub async fn serve(cfg: Config, radio: &str, paths: Paths) -> Result<()> {
    if !valid_radio_id(radio) {
        bail!("{radio} is not a radio id");
    }
    let file = paths.state_file(radio);
    let state: Value = serde_json::from_str(
        &std::fs::read_to_string(&file).with_context(|| format!("reading {}", file.display()))?,
    )?;
    let protocol = state["protocol"].as_str().unwrap_or_default().to_string();
    let Some(start) = factory(&protocol) else {
        tracing::error!(%radio, %protocol, "this pulzr-edge has no module for that protocol");
        // procd's respawn gives up; the console says why.
        std::process::exit(3);
    };
    let port = if radio == MATTER {
        String::new()
    } else {
        port_of(&paths.probe_report, radio)?
    };
    let adapter = cfg.adapter();
    let out = Outbox::open(
        &cfg.state_dir.join("radio").join(radio),
        &adapter,
        &cfg.gateway_eui,
        cfg.buffer.max_bytes,
    )?;
    tracing::info!(version = env!("CARGO_PKG_VERSION"), %radio, %protocol, %port, "pulzr-edge radio starting");
    let module = start(
        RadioCtx::new(radio, &port, out.clone(), paths.clone()).with_firmware(cfg.firmware.clone()),
    )?;

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let opts = crate::mqtt::options(&cfg.broker, &format!("{adapter}-{radio}"))?;
    struct Commands(Arc<dyn RadioModule>);
    impl crate::mqtt::Dispatch for Commands {
        fn dispatch(&self, out: &Outbox, topic: &str, payload: &[u8]) {
            self.0.dispatch(out, topic, payload)
        }
    }
    let session = tokio::spawn(crate::mqtt::run(
        opts,
        cfg.broker.clone(),
        out.clone(),
        Arc::new(Commands(module.clone())),
        stop_rx.clone(),
    ));
    let control = tokio::spawn(control::listen(
        paths.socket(radio),
        radio.to_string(),
        module,
        stop_rx,
    ));
    shutdown_signal().await;
    tracing::info!("shutting down");
    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), control).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), session).await;
    out.flush();
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
