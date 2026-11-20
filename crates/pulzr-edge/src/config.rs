//! The daemon's TOML configuration.

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
pub struct Config {
    /// The gateway's EUI-64, 16 lower case hex digits; part of every Modbus device key.
    pub gateway_eui: String,
    /// Adapter id in topics; `edge-<gateway_eui>` when absent. Must match the
    /// certificate's name on the broker.
    pub adapter: Option<String>,
    /// Where the buffer and the `seq` counters live. Put it on storage that survives
    /// a reboot and tolerates writes (not a small flash overlay).
    #[serde(default = "default_state_dir")]
    pub state_dir: PathBuf,
    pub broker: Broker,
    #[serde(default)]
    pub buffer: BufferCfg,
    #[serde(default)]
    pub log: LogCfg,
    #[serde(default, rename = "bus")]
    pub buses: Vec<BusCfg>,
    /// Bluetooth LE (`pulzr-edge ble`); absent or off, nothing runs.
    #[cfg(target_endian = "little")]
    #[serde(default)]
    pub ble: Option<crate::ble::BleCfg>,
    /// Read and ignored where there is no Bluetooth support (big-endian).
    #[cfg(not(target_endian = "little"))]
    #[serde(default)]
    pub ble: Option<toml::Value>,
    /// IP cameras on the LAN (`pulzr-edge camera`).
    #[serde(default, rename = "camera")]
    pub cameras: Vec<crate::camera::CameraCfg>,
    /// Where device firmware may be fetched from.
    #[serde(default)]
    pub firmware: FirmwareCfg,
}

/// `[firmware]`: the hosts a module fetches device firmware images from
/// (the platform's asset host). A command naming any other host is
/// refused; with none listed, no firmware is fetched.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FirmwareCfg {
    #[serde(default)]
    pub hosts: Vec<String>,
}

impl FirmwareCfg {
    /// Whether `url` is `https://` on one of the hosts (any port; no user
    /// info).
    pub fn allows(&self, url: &str) -> bool {
        let Some(rest) = url.strip_prefix("https://") else {
            return false;
        };
        let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
        if authority.contains('@') {
            return false;
        }
        let host = match authority.rsplit_once(':') {
            Some((h, port)) if port.bytes().all(|b| b.is_ascii_digit()) => h,
            _ => authority,
        };
        !host.is_empty() && self.hosts.iter().any(|h| h.eq_ignore_ascii_case(host))
    }
}

fn default_state_dir() -> PathBuf {
    "/var/lib/pulzr-edge".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct Broker {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    /// TLS (default). `false` only for a local test broker.
    #[serde(default = "yes")]
    pub tls: bool,
    /// PEM file with the CA that signed the broker's certificate.
    pub ca: Option<PathBuf>,
    /// PEM client certificate chain and private key naming this adapter.
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    /// Or a username and a file holding its password (the gateway's own broker
    /// login). The file is read at every connect, so a rotated token is picked up
    /// on the next reconnect; a trailing newline is trimmed.
    pub username: Option<String>,
    pub password_file: Option<PathBuf>,
    #[serde(default = "default_keep_alive")]
    pub keep_alive_secs: u64,
}

fn default_port() -> u16 {
    8883
}
fn yes() -> bool {
    true
}
fn default_keep_alive() -> u64 {
    30
}

#[derive(Debug, Deserialize)]
pub struct BufferCfg {
    /// Disk budget for messages waiting to be sent. Oldest are dropped past it.
    #[serde(default = "default_max_bytes")]
    pub max_bytes: u64,
}

impl Default for BufferCfg {
    fn default() -> Self {
        Self {
            max_bytes: default_max_bytes(),
        }
    }
}

fn default_max_bytes() -> u64 {
    8 * 1024 * 1024
}

#[derive(Debug, Deserialize)]
pub struct LogCfg {
    /// A tracing filter, e.g. `info` or `pulzr_edge=debug`.
    #[serde(default = "default_level")]
    pub level: String,
    /// One JSON object per line (default); `false` for human readable.
    #[serde(default = "yes")]
    pub json: bool,
}

impl Default for LogCfg {
    fn default() -> Self {
        Self {
            level: default_level(),
            json: true,
        }
    }
}

fn default_level() -> String {
    "info".into()
}

#[derive(Debug, Deserialize)]
pub struct BusCfg {
    /// Bus name in device keys: lower case letters, digits and `-`, up to 32.
    pub id: String,
    #[serde(flatten)]
    pub kind: BusKind,
    /// Per request.
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
    /// Extra attempts after a timeout or I/O error.
    #[serde(default = "default_retries")]
    pub retries: u32,
    /// Wait before the first retry; doubles each time.
    #[serde(default = "default_backoff")]
    pub backoff_ms: u64,
    /// Quiet time after every request (slow RS-485 devices).
    #[serde(default)]
    pub turnaround_ms: u64,
    #[serde(default, rename = "unit")]
    pub units: Vec<UnitCfg>,
}

fn default_timeout() -> u64 {
    1000
}
fn default_retries() -> u32 {
    2
}
fn default_backoff() -> u64 {
    200
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum BusKind {
    /// Modbus RTU on a serial port.
    Rtu {
        serial: String,
        #[serde(default = "default_baud")]
        baud: u32,
        /// `none`, `even` or `odd`.
        #[serde(default = "default_parity")]
        parity: String,
        #[serde(default = "default_data_bits")]
        data_bits: u8,
        #[serde(default = "default_stop_bits")]
        stop_bits: u8,
    },
    /// Modbus TCP: `host:port`.
    Tcp { address: String },
}

fn default_baud() -> u32 {
    9600
}
fn default_parity() -> String {
    "none".into()
}
fn default_data_bits() -> u8 {
    8
}
fn default_stop_bits() -> u8 {
    1
}

#[derive(Debug, Deserialize)]
pub struct UnitCfg {
    /// Modbus unit (slave) id, 1-247.
    pub id: u8,
    /// Register map file, relative to this config file.
    pub map: PathBuf,
    /// Consecutive failed polls before the unit is published offline.
    #[serde(default = "default_offline_after")]
    pub offline_after: u32,
}

fn default_offline_after() -> u32 {
    3
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let cfg: Config =
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn adapter(&self) -> String {
        self.adapter
            .clone()
            .unwrap_or_else(|| format!("edge-{}", self.gateway_eui))
    }

    /// `modbus:<gateway eui>.<bus>.<unit>`
    pub fn device_key(&self, bus: &str, unit: u8) -> String {
        format!("modbus:{}.{bus}.{unit}", self.gateway_eui)
    }

    pub fn validate(&self) -> Result<()> {
        if self.gateway_eui.len() != 16
            || !self
                .gateway_eui
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        {
            bail!("gateway_eui must be 16 lower case hex digits");
        }
        let adapter = self.adapter();
        if adapter.is_empty() || adapter.contains(['/', '+', '#', '\0']) {
            bail!("adapter {adapter:?} cannot be a topic level");
        }
        let b = &self.broker;
        let cert_auth = match (&b.cert, &b.key) {
            (Some(_), Some(_)) => true,
            (None, None) => false,
            _ => bail!("broker.cert and broker.key go together"),
        };
        let pass_auth = match (&b.username, &b.password_file) {
            (Some(_), Some(_)) => true,
            (None, None) => false,
            _ => bail!("broker.username and broker.password_file go together"),
        };
        if cert_auth && pass_auth {
            bail!("broker authenticates with cert and key or with username and password_file, not both");
        }
        if b.tls && !(cert_auth || pass_auth) {
            bail!("broker.tls needs cert and key, or username and password_file (or tls = false for a local test broker)");
        }
        if b.tls && b.ca.is_none() {
            bail!("broker.tls needs ca (or tls = false for a local test broker)");
        }
        #[cfg(target_endian = "little")]
        if let Some(b) = &self.ble {
            b.validate()?;
        }
        let mut keys = std::collections::HashSet::new();
        for c in &self.cameras {
            c.validate()?;
            if !keys.insert(c.key()) {
                bail!("camera {} is listed twice", c.mac);
            }
        }
        let mut seen = std::collections::HashSet::new();
        for b in &self.buses {
            if !seen.insert(&b.id) {
                bail!("duplicate bus {}", b.id);
            }
            if b.units.is_empty() {
                bail!("bus {} has no units", b.id);
            }
            for u in &b.units {
                let key = self.device_key(&b.id, u.id);
                // the protocol's own pattern decides what a valid key is
                pulzr_protocol::DeviceKey::try_from(key.as_str())
                    .map_err(|e| anyhow::anyhow!("bus {} unit {}: {e}", b.id, u.id))?;
            }
            let ids: std::collections::HashSet<_> = b.units.iter().map(|u| u.id).collect();
            if ids.len() != b.units.len() {
                bail!("bus {} repeats a unit id", b.id);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOOD: &str = r#"
gateway_eui = "aabbccddeeff0011"
[broker]
host = "mqtt.example.net"
ca = "ca.pem"
cert = "c.pem"
key = "k.pem"
[[bus]]
id = "rs485-0"
kind = "rtu"
serial = "/dev/ttyUSB0"
[[bus.unit]]
id = 17
map = "maps/meter.toml"
[[bus]]
id = "lan-0"
kind = "tcp"
address = "192.168.1.50:502"
[[bus.unit]]
id = 1
map = "maps/meter.toml"
"#;

    #[test]
    fn parses_and_derives_keys() {
        let c: Config = toml::from_str(GOOD).unwrap();
        c.validate().unwrap();
        assert_eq!(c.adapter(), "edge-aabbccddeeff0011");
        assert_eq!(
            c.device_key("rs485-0", 17),
            "modbus:aabbccddeeff0011.rs485-0.17"
        );
        assert!(matches!(c.buses[0].kind, BusKind::Rtu { baud: 9600, .. }));
    }

    #[test]
    fn rejects_bad_eui_unit_and_missing_tls() {
        let bad = GOOD.replace("aabbccddeeff0011", "AABBCCDDEEFF0011");
        assert!(toml::from_str::<Config>(&bad).unwrap().validate().is_err());
        let bad = GOOD.replace("id = 17", "id = 0");
        assert!(toml::from_str::<Config>(&bad).unwrap().validate().is_err());
        let bad = GOOD.replace("key = \"k.pem\"\n", "");
        assert!(toml::from_str::<Config>(&bad).unwrap().validate().is_err());
    }

    fn check(text: &str) -> Result<()> {
        toml::from_str::<Config>(text).unwrap().validate()
    }

    #[test]
    fn broker_auth_is_cert_or_password() {
        let pass = GOOD.replace(
            "cert = \"c.pem\"\nkey = \"k.pem\"\n",
            "username = \"gw\"\npassword_file = \"/run/pw\"\n",
        );
        check(&pass).unwrap();
        // both, or neither
        assert!(check(&GOOD.replace(
            "[[bus]]",
            "username = \"gw\"\npassword_file = \"p\"\n[[bus]]"
        ))
        .is_err());
        assert!(
            check(&pass.replace("username = \"gw\"\npassword_file = \"/run/pw\"\n", "")).is_err()
        );
        // half a pair
        assert!(check(&pass.replace("password_file = \"/run/pw\"\n", "")).is_err());
        // password auth still verifies the server, so it needs a CA
        assert!(check(&pass.replace("ca = \"ca.pem\"\n", "")).is_err());
        // a local test broker needs neither
        let local = pass
            .replace(
                "username = \"gw\"\npassword_file = \"/run/pw\"\n",
                "tls = false\n",
            )
            .replace("ca = \"ca.pem\"\n", "");
        check(&local).unwrap();
    }
}
