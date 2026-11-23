//! From what is plugged in to `radios.detected`: name it, decide whether it
//! may be asked, ask it, and say what it is.

use crate::probe::{first_answer, Answer, Outcome, Port, Prober};
use crate::sx130x::SpiTarget;
use crate::sysfs::{self, Usb};
use crate::table::{ProbeKind, Protocol, Table, UsbIdentity};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// One entry of `radios.detected`, as pulzr-protocol's `detectedRadio`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Detected {
    pub id: String,
    pub bus: Bus,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub port: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub path: Option<String>,
    pub adapter: Option<String>,
    pub label: String,
    pub protocol: Option<Protocol>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub protocols: Vec<Protocol>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub ncp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub firmware: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub ezsp: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub chip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub usb: Option<UsbOut>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub probe: Option<ProbeState>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    #[serde(rename = "use")]
    pub use_: Option<Use>,
    pub state: State,
    pub since: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Bus {
    Usb,
    Uart,
    Lan,
    Spi,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum State {
    Free,
    Unidentified,
    InUse,
    Missing,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProbeState {
    Ok,
    NoAnswer,
    Unavailable,
    NotAllowed,
    Error,
}

/// What a person said a port is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Use {
    Modbus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsbOut {
    pub vid: String,
    pub pid: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub interface: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub manufacturer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub product: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub serial: Option<String>,
}

/// The file `pulzr-probe` writes, and the claim report and MQTT state read.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Report {
    pub detected: Vec<Detected>,
}

/// What a person decided, kept across reboots: which ports are Modbus
/// adapters, and what Identify found on ports the table could not name.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Decisions {
    #[serde(default)]
    pub assigned: BTreeMap<String, Use>,
    #[serde(default)]
    pub identified: BTreeMap<String, Identified>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Identified {
    pub protocol: Protocol,
    pub ncp: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub firmware: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub ezsp: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub chip: Option<String>,
}

/// A coordinator mDNS found on the LAN.
#[derive(Debug, Clone, PartialEq)]
pub struct LanHost {
    pub service: String,
    pub instance: String,
    pub host: String,
    pub port: u16,
}

pub struct Context<'a> {
    pub root: &'a Path,
    pub table: &'a Table,
    pub prober: &'a dyn Prober,
    pub decisions: &'a Decisions,
    /// The last report: a radio still in the same place keeps its `since` and
    /// what its probe learned, and is not asked again.
    pub previous: &'a Report,
    pub lan: &'a [LanHost],
    /// The concentrator slots the board names on SPI, to read this scan.
    /// Empty on a scan that is not to touch them (hotplug: an SPI part does
    /// not come and go), which keeps what the last one found.
    pub spi: &'a [SpiTarget],
    /// Protocols whose stack is installed and owns its radios (PulzrOS's
    /// pulzr-lorawan, whose concentratord resets and talks to the
    /// concentrator itself). Such a radio is named and never asked: a probe
    /// in the middle of its stack's own exchange would break it.
    pub claimed: &'a [Protocol],
    pub now: u64,
}

/// `[a-z0-9._-]`, lower case, for ids.
fn slug(s: &str) -> String {
    s.chars()
        .map(|c| c.to_ascii_lowercase())
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '-'
            }
        })
        .take(64)
        .collect()
}

/// Stable across unplug and replug: the serial number where the adapter has
/// one (so it is the same radio in any port), else the port it is in. The
/// interface is added for a device with a radio per interface.
pub fn usb_id(usb: &Usb) -> String {
    let at = usb
        .serial
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(&usb.port);
    let mut id = format!("usb-{}_{}-{}", usb.vid, usb.pid, slug(at));
    if let Some(i) = usb.interface.filter(|&i| i > 0) {
        id.push_str(&format!(".{i}"));
    }
    id
}

fn usb_out(usb: &Usb) -> UsbOut {
    UsbOut {
        vid: usb.vid.clone(),
        pid: usb.pid.clone(),
        interface: usb.interface,
        manufacturer: usb.manufacturer.clone(),
        product: usb.product.clone(),
        serial: usb.serial.clone(),
    }
}

fn apply(d: &mut Detected, a: &Answer) {
    d.protocol = a.protocol;
    d.ncp = a.ncp.map(str::to_owned);
    d.firmware = a.firmware.clone();
    d.ezsp = a.ezsp;
    d.chip = a.chip.clone();
    d.probe = Some(ProbeState::Ok);
}

/// Free: every protocol a radio can be named for has a stack to run it now
/// (Thread's is OpenThread's border router, the pulzr-thread add-on).
fn readiness(_protocol: Option<Protocol>) -> State {
    State::Free
}

impl Context<'_> {
    fn since(&self, id: &str, path: Option<&str>) -> (u64, Option<&Detected>) {
        match self
            .previous
            .detected
            .iter()
            .find(|p| p.id == id && p.path.as_deref() == path)
        {
            Some(p) => (p.since, Some(p)),
            None => (self.now, None),
        }
    }

    fn serial(&self, port: &sysfs::SerialPort) -> Detected {
        let id = usb_id(&port.usb);
        let dev = format!("/dev/{}", port.name);
        let (since, before) = self.since(&id, Some(&dev));
        let identity = UsbIdentity {
            vid: &port.usb.vid,
            pid: &port.usb.pid,
            product: port.usb.product.as_deref(),
            interface: port.usb.interface,
        };
        let adapter = self.table.adapter(&identity);
        let label = match (adapter, self.table.bridge(&port.usb.vid, &port.usb.pid)) {
            (Some(a), _) => a.label.clone(),
            (None, Some(b)) => format!("USB serial adapter ({})", b.chip),
            (None, None) => match &port.usb.product {
                Some(p) => format!("USB serial adapter ({p})"),
                None => format!("USB serial adapter ({}:{})", port.usb.vid, port.usb.pid),
            },
        };
        let mut d = Detected {
            id: id.clone(),
            bus: Bus::Usb,
            port: Some(port.usb.port.clone()),
            path: Some(dev.clone()),
            adapter: adapter.map(|a| a.id.clone()),
            label,
            protocol: None,
            protocols: adapter.map(|a| a.protocols.clone()).unwrap_or_default(),
            ncp: None,
            firmware: None,
            ezsp: None,
            chip: None,
            address: None,
            usb: Some(usb_out(&port.usb)),
            probe: None,
            use_: None,
            state: State::Unidentified,
            since,
        };

        // A person said it is a Modbus adapter: it is the Modbus module's,
        // and nothing here ever writes to it again.
        if let Some(&u) = self.decisions.assigned.get(&id) {
            d.use_ = Some(u);
            d.state = State::InUse;
            return d;
        }

        let Some(adapter) = adapter else {
            // Nobody could name it. Only what Identify found, a person's
            // click, says what it is; otherwise it is never written to.
            match self.decisions.identified.get(&id) {
                Some(found) => {
                    d.protocol = Some(found.protocol);
                    d.protocols = vec![found.protocol];
                    d.ncp = Some(found.ncp.clone());
                    d.firmware = found.firmware.clone();
                    d.ezsp = found.ezsp;
                    d.chip = found.chip.clone();
                    d.probe = Some(ProbeState::Ok);
                    d.state = readiness(d.protocol);
                }
                None => d.probe = Some(ProbeState::NotAllowed),
            }
            return d;
        };

        // Until something answers, it is what it ships as.
        d.protocol = adapter.protocols.first().copied();
        d.ncp = adapter.probes.first().map(|k| k.ncp().to_owned());
        d.state = readiness(d.protocol);

        if adapter.protocols.iter().any(|p| self.claimed.contains(p)) {
            if let Some(b) = before {
                carry(&mut d, b);
            }
            d.state = State::InUse;
            return d;
        }

        let dev_path = self.root.join(dev.trim_start_matches('/'));
        if sysfs::open_elsewhere(self.root, &dev_path) {
            // Somebody is talking to it. What it was when it was last asked
            // still holds; asking now would interrupt them.
            if let Some(b) = before {
                carry(&mut d, b);
            }
            d.state = State::InUse;
            return d;
        }
        if let Some(b) = before.filter(|b| b.probe == Some(ProbeState::Ok)) {
            carry(&mut d, b);
            return d;
        }
        match first_answer(self.prober, &adapter.probes, Port::Serial(&dev_path)) {
            Outcome::Answered(a) => {
                apply(&mut d, &a);
                d.state = readiness(d.protocol);
            }
            Outcome::NoAnswer => d.probe = Some(ProbeState::NoAnswer),
            Outcome::Unavailable => d.probe = Some(ProbeState::Unavailable),
            Outcome::Error(_) => d.probe = Some(ProbeState::Error),
        }
        d
    }

    fn hci(&self, hci: &sysfs::Hci) -> Detected {
        let id = usb_id(&hci.usb);
        let (since, _) = self.since(&id, None);
        let identity = UsbIdentity {
            vid: &hci.usb.vid,
            pid: &hci.usb.pid,
            product: hci.usb.product.as_deref(),
            interface: None,
        };
        let adapter = self
            .table
            .adapter(&identity)
            .filter(|a| a.protocols.contains(&Protocol::Ble));
        // The kernel's driver claimed it as a Bluetooth controller, which is
        // what naming it means here: there is nothing to probe.
        Detected {
            id,
            bus: Bus::Usb,
            port: Some(hci.usb.port.clone()),
            path: None,
            adapter: adapter.map(|a| a.id.clone()),
            label: adapter
                .map(|a| a.label.clone())
                .unwrap_or_else(|| match &hci.usb.product {
                    Some(p) => format!("Bluetooth adapter ({p})"),
                    None => "Bluetooth adapter".to_owned(),
                }),
            protocol: Some(Protocol::Ble),
            protocols: vec![Protocol::Ble],
            ncp: Some("hci".to_owned()),
            firmware: None,
            ezsp: None,
            chip: None,
            address: hci.address.clone(),
            usb: Some(usb_out(&hci.usb)),
            probe: None,
            use_: None,
            state: State::Free,
            since,
        }
    }

    fn lan_host(&self, host: &LanHost) -> Option<Detected> {
        let service = self.table.lan(&host.service)?;
        let id = format!("lan-{}", slug(host.host.trim_end_matches('.')));
        let path = format!("{}:{}", host.host.trim_end_matches('.'), host.port);
        let (since, before) = self.since(&id, Some(&path));
        let mut d = Detected {
            id,
            bus: Bus::Lan,
            port: None,
            path: Some(path),
            adapter: Some(service.id.clone()),
            label: service.label.clone(),
            protocol: service.protocols.first().copied(),
            protocols: service.protocols.clone(),
            ncp: None,
            firmware: None,
            ezsp: None,
            chip: None,
            address: None,
            usb: None,
            probe: None,
            use_: None,
            state: State::Free,
            since,
        };
        if let Some(b) = before.filter(|b| b.probe == Some(ProbeState::Ok)) {
            carry(&mut d, b);
            return Some(d);
        }
        match first_answer(
            self.prober,
            &service.probes,
            Port::Tcp(host.host.trim_end_matches('.'), host.port),
        ) {
            Outcome::Answered(a) => {
                apply(&mut d, &a);
                d.state = readiness(d.protocol);
            }
            Outcome::NoAnswer => d.probe = Some(ProbeState::NoAnswer),
            Outcome::Unavailable => d.probe = Some(ProbeState::Unavailable),
            Outcome::Error(_) => d.probe = Some(ProbeState::Error),
        }
        Some(d)
    }

    /// A concentrator on an SPI slot, if one answers there. SPI cannot say
    /// a part is there without it answering, so a slot with none is not
    /// listed at all.
    fn spi_slot(&self, target: &SpiTarget) -> Option<Detected> {
        let id = target.id();
        let path = target.dev.to_string_lossy().into_owned();
        let (since, _) = self.since(&id, Some(&path));
        let a = match first_answer(self.prober, &[ProbeKind::Sx130xSpi], Port::Spi(target)) {
            Outcome::Answered(a) => a,
            _ => return None,
        };
        let chip = a.chip.clone();
        let mut d = Detected {
            id,
            bus: Bus::Spi,
            port: None,
            path: Some(path),
            adapter: None,
            label: match &chip {
                Some(c) => format!("LoRaWAN concentrator ({c})"),
                None => "LoRaWAN concentrator".to_owned(),
            },
            protocol: Some(Protocol::Lorawan),
            protocols: vec![Protocol::Lorawan],
            ncp: None,
            firmware: None,
            ezsp: None,
            chip: None,
            address: None,
            usb: None,
            probe: None,
            use_: None,
            state: State::Free,
            since,
        };
        apply(&mut d, &a);
        d.state = readiness(d.protocol);
        Some(d)
    }

    /// Everything plugged in or on the LAN now. A radio that went away is
    /// simply not listed; the platform notices it is gone by comparing.
    pub fn scan(&self) -> Report {
        let mut detected: Vec<Detected> = sysfs::serial_ports(self.root)
            .iter()
            .map(|p| self.serial(p))
            .collect();
        detected.extend(sysfs::bluetooth(self.root).iter().map(|h| self.hci(h)));
        let lorawan_claimed = self.claimed.contains(&Protocol::Lorawan);
        if self.spi.is_empty() || lorawan_claimed {
            // Not read this scan: what the last one found stands, its stack's
            // once LoRaWAN owns it.
            detected.extend(
                self.previous
                    .detected
                    .iter()
                    .filter(|d| d.bus == Bus::Spi)
                    .map(|d| {
                        let mut d = d.clone();
                        if lorawan_claimed {
                            d.state = State::InUse;
                        }
                        d
                    }),
            );
        } else {
            detected.extend(self.spi.iter().filter_map(|t| self.spi_slot(t)));
        }
        let mut seen = Vec::new();
        for host in self.lan {
            if let Some(d) = self.lan_host(host) {
                if !seen.contains(&d.id) {
                    seen.push(d.id.clone());
                    detected.push(d);
                }
            }
        }
        detected.truncate(32);
        Report { detected }
    }

    /// A person's Identify: every kind, on a port the table could not name.
    /// Refused for a port that is a Modbus adapter or that something has
    /// open. Returns what was learned, to be kept in `Decisions::identified`.
    pub fn identify(&self, id: &str) -> Result<Option<Identified>, String> {
        let port = sysfs::serial_ports(self.root)
            .into_iter()
            .find(|p| usb_id(&p.usb) == id)
            .ok_or_else(|| format!("no serial adapter {id} is plugged in"))?;
        if self.decisions.assigned.contains_key(id) {
            return Err(format!("{id} is a Modbus adapter; it is never probed"));
        }
        let dev_path = self.root.join("dev").join(&port.name);
        if sysfs::open_elsewhere(self.root, &dev_path) {
            return Err(format!("{id} is open by another program"));
        }
        let kinds: &[ProbeKind] = &ProbeKind::IDENTIFY;
        match first_answer(self.prober, kinds, Port::Serial(&dev_path)) {
            Outcome::Answered(a) => Ok(Some(Identified {
                protocol: a.protocol.expect("first_answer fills it"),
                ncp: a.ncp.expect("first_answer fills it").to_owned(),
                firmware: a.firmware,
                ezsp: a.ezsp,
                chip: a.chip,
            })),
            Outcome::NoAnswer => Ok(None),
            Outcome::Unavailable => Err("this PulzrOS cannot probe radios yet".to_owned()),
            Outcome::Error(e) => Err(e),
        }
    }
}

fn carry(d: &mut Detected, before: &Detected) {
    d.protocol = before.protocol.or(d.protocol);
    d.ncp = before.ncp.clone().or(d.ncp.take());
    d.firmware = before.firmware.clone();
    d.ezsp = before.ezsp;
    d.chip = before.chip.clone();
    d.probe = before.probe;
    d.state = readiness(d.protocol);
}

/// The default places on a gateway.
pub struct Paths {
    pub report: PathBuf,
    pub decisions: PathBuf,
    pub lan: PathBuf,
}

impl Default for Paths {
    fn default() -> Self {
        Paths {
            report: PathBuf::from("/var/run/pulzr-probe/radios.json"),
            decisions: PathBuf::from("/etc/pulzr-probe/decisions.json"),
            lan: PathBuf::from("/var/run/pulzr-probe/mdns.json"),
        }
    }
}

/// Coordinators from `ubus call umdns browse`, as PulzrOS's wrapper saves it:
/// `{"<service>": {"<instance>": {"host": ..., "port": ..., "ipv4": ...}}}`.
pub fn parse_umdns(text: &str) -> Vec<LanHost> {
    let Ok(serde_json::Value::Object(services)) = serde_json::from_str::<serde_json::Value>(text)
    else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (service, instances) in services {
        let Some(instances) = instances.as_object() else {
            continue;
        };
        for (instance, info) in instances {
            let host = info
                .get("host")
                .and_then(|h| h.as_str())
                .or_else(|| info.get("ipv4").and_then(|h| h.as_str()));
            let port = info
                .get("port")
                .and_then(|p| p.as_u64())
                .and_then(|p| u16::try_from(p).ok());
            if let (Some(host), Some(port)) = (host, port) {
                out.push(LanHost {
                    service: service.clone(),
                    instance: instance.clone(),
                    host: host.to_owned(),
                    port,
                });
            }
        }
    }
    out
}
