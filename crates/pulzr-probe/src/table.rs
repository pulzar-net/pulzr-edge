//! `radios.json`: the adapters pulzr-probe can name, and the bridge chips it
//! must never name on their own.
//!
//! A CP2102 or a CH9102 sits inside Zigbee dongles, Z-Wave sticks and the
//! RS-485 adapters Modbus sites use. Writing a probe frame to an RS-485
//! adapter puts garbage on a live fieldbus, so an adapter whose USB id is one
//! of `bridges` is named only by a product string that names a radio. The
//! table grows by adding product strings, never by widening a match:
//! [`Table::check`] refuses a bridge id with no product string, and CI runs it
//! over the table that ships.

use serde::{Deserialize, Serialize};

/// The table every image carries.
pub const BUILTIN: &str = include_str!("../radios.json");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Zigbee,
    Zwave,
    Thread,
    Ble,
    Lorawan,
}

/// The one harmless exchange each protocol answers, by the co-processor's
/// serial protocol. Which of these a port may be sent is the table's to say.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProbeKind {
    /// Silicon Labs EmberZNet: ASH reset, then EZSP `version`.
    Ezsp,
    /// TI Z-Stack: `SYS_PING`, `SYS_VERSION`.
    Znp,
    /// dresden elektronik: `VERSION`.
    Deconz,
    /// Z-Wave Serial API: ACK, `GetCapabilities`, `GetControllerCapabilities`.
    ZwaveSerial,
    /// OpenThread RCP: Spinel `PROP_VALUE_GET NCP_VERSION`.
    Spinel,
    /// A LoRaWAN concentrator behind Semtech's USB reference MCU: its
    /// `REQ_PING` ([`crate::sx130x`]).
    Sx130xUsb,
    /// A LoRaWAN concentrator on SPI: one reset with the board's lines, and
    /// its version register. Never in the table: a board names its SPI slots.
    Sx130xSpi,
}

impl ProbeKind {
    pub fn protocol(self) -> Protocol {
        match self {
            ProbeKind::Ezsp | ProbeKind::Znp | ProbeKind::Deconz => Protocol::Zigbee,
            ProbeKind::ZwaveSerial => Protocol::Zwave,
            ProbeKind::Spinel => Protocol::Thread,
            ProbeKind::Sx130xUsb | ProbeKind::Sx130xSpi => Protocol::Lorawan,
        }
    }

    /// The `ncp` value a radio running this reports.
    pub fn ncp(self) -> &'static str {
        match self {
            ProbeKind::Ezsp => "ezsp",
            ProbeKind::Znp => "znp",
            ProbeKind::Deconz => "deconz",
            ProbeKind::ZwaveSerial => "zwave-serial",
            ProbeKind::Spinel => "spinel",
            ProbeKind::Sx130xUsb | ProbeKind::Sx130xSpi => "sx130x",
        }
    }

    /// What Identify tries on a port nobody could name, in this order. Every
    /// one of them is a request that changes nothing on a radio that
    /// understands it; on a fieldbus any of them is noise, which is why only a
    /// person may start it.
    pub const IDENTIFY: [ProbeKind; 5] = [
        ProbeKind::Ezsp,
        ProbeKind::Znp,
        ProbeKind::ZwaveSerial,
        ProbeKind::Spinel,
        ProbeKind::Deconz,
    ];
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    pub schema: u32,
    pub bridges: Vec<Bridge>,
    pub adapters: Vec<Adapter>,
    #[serde(default)]
    pub lan: Vec<LanService>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bridge {
    pub vid: String,
    pub pid: String,
    pub chip: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Adapter {
    pub id: String,
    pub label: String,
    /// What it can carry; the first is what it ships running.
    pub protocols: Vec<Protocol>,
    #[serde(default)]
    pub probes: Vec<ProbeKind>,
    #[serde(rename = "match")]
    pub usb: UsbMatch,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsbMatch {
    pub vid: String,
    pub pid: String,
    /// Any one of these, case-insensitive, inside the USB product string.
    #[serde(default)]
    pub product: Vec<String>,
    /// The USB interface number, for a device with one port per radio.
    #[serde(default)]
    pub interface: Option<u8>,
}

/// A coordinator on the LAN that serves its serial port over TCP, by the
/// mDNS service type it announces.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LanService {
    pub service: String,
    pub id: String,
    pub label: String,
    pub protocols: Vec<Protocol>,
    #[serde(default)]
    pub probes: Vec<ProbeKind>,
}

/// What a USB device says about itself, as the matcher needs it.
#[derive(Debug, Clone, Default)]
pub struct UsbIdentity<'a> {
    pub vid: &'a str,
    pub pid: &'a str,
    pub product: Option<&'a str>,
    pub interface: Option<u8>,
}

/// The shortest product string the table accepts: anything shorter matches
/// too much to name a radio.
const MIN_PRODUCT: usize = 4;

impl Table {
    pub fn builtin() -> Table {
        let table: Table = serde_json::from_str(BUILTIN).expect("radios.json parses");
        debug_assert!(table.check().is_empty());
        table
    }

    pub fn parse(text: &str) -> Result<Table, String> {
        let table: Table = serde_json::from_str(text).map_err(|e| e.to_string())?;
        let problems = table.check();
        if problems.is_empty() {
            Ok(table)
        } else {
            Err(problems.join("; "))
        }
    }

    pub fn bridge(&self, vid: &str, pid: &str) -> Option<&Bridge> {
        self.bridges.iter().find(|b| b.vid == vid && b.pid == pid)
    }

    /// The adapter this USB device is, if the table can name it.
    ///
    /// A match on a product string wins over a match on the id alone, so a
    /// unique id can have a generic entry and a more specific one beside it.
    pub fn adapter(&self, usb: &UsbIdentity) -> Option<&Adapter> {
        let product = usb.product.map(str::to_lowercase);
        let candidates = self.adapters.iter().filter(|a| {
            a.usb.vid == usb.vid
                && a.usb.pid == usb.pid
                && a.usb.interface.is_none_or(|i| Some(i) == usb.interface)
        });
        let mut by_id = None;
        for a in candidates {
            if a.usb.product.is_empty() {
                by_id.get_or_insert(a);
            } else if let Some(p) = &product {
                if a.usb.product.iter().any(|s| p.contains(&s.to_lowercase())) {
                    return Some(a);
                }
            }
        }
        // A bridge chip is never named by its id, whatever an entry says;
        // check() refuses such an entry, and this holds even for a table that
        // skipped it.
        if self.bridge(usb.vid, usb.pid).is_some() {
            return None;
        }
        by_id
    }

    pub fn lan(&self, service: &str) -> Option<&LanService> {
        let service = service.trim_end_matches('.').trim_end_matches(".local");
        self.lan.iter().find(|l| l.service == service)
    }

    /// Everything wrong with this table; empty when it may ship.
    pub fn check(&self) -> Vec<String> {
        let mut problems = Vec::new();
        let hex4 = |s: &str| {
            s.len() == 4
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        let id_ok = |s: &str| {
            (2..=63).contains(&s.len())
                && s.bytes()
                    .next()
                    .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
                && s.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        };
        if self.schema != 1 {
            problems.push(format!("schema {} is not 1", self.schema));
        }
        for b in &self.bridges {
            if !hex4(&b.vid) || !hex4(&b.pid) {
                problems.push(format!(
                    "bridge {}:{}: ids are four lower case hex digits",
                    b.vid, b.pid
                ));
            }
        }
        let mut ids: Vec<&str> = Vec::new();
        for a in &self.adapters {
            if !id_ok(&a.id) {
                problems.push(format!("{}: not an adapter id", a.id));
            }
            if ids.contains(&a.id.as_str()) {
                problems.push(format!("{}: listed twice", a.id));
            }
            ids.push(&a.id);
            if !hex4(&a.usb.vid) || !hex4(&a.usb.pid) {
                problems.push(format!("{}: ids are four lower case hex digits", a.id));
            }
            if a.protocols.is_empty() {
                problems.push(format!("{}: carries no protocol", a.id));
            }
            for p in &a.probes {
                if !a.protocols.contains(&p.protocol()) {
                    problems.push(format!(
                        "{}: probe {} is for a protocol it does not carry",
                        a.id,
                        p.ncp()
                    ));
                }
            }
            if a.protocols.iter().any(|p| *p != Protocol::Ble) && a.probes.is_empty() {
                problems.push(format!("{}: a serial radio with no probe", a.id));
            }
            if let Some(b) = self.bridge(&a.usb.vid, &a.usb.pid) {
                if a.usb.product.is_empty() {
                    problems.push(format!(
                        "{}: {} ({}:{}) is a bridge chip; name it by a product string",
                        a.id, b.chip, b.vid, b.pid
                    ));
                }
            }
            for s in &a.usb.product {
                if s.trim().len() < MIN_PRODUCT {
                    problems.push(format!(
                        "{}: product string {s:?} is too short to name a radio",
                        a.id
                    ));
                }
            }
        }
        // Two entries for one id must not both claim a product string, or
        // which one a device is depends on the order of the file.
        for (i, a) in self.adapters.iter().enumerate() {
            for b in &self.adapters[i + 1..] {
                let same_port = a.usb.vid == b.usb.vid
                    && a.usb.pid == b.usb.pid
                    && (a.usb.interface.is_none()
                        || b.usb.interface.is_none()
                        || a.usb.interface == b.usb.interface);
                if !same_port {
                    continue;
                }
                if a.usb.product.is_empty() && b.usb.product.is_empty() {
                    problems.push(format!(
                        "{} and {}: both match {}:{} by id alone",
                        a.id, b.id, a.usb.vid, a.usb.pid
                    ));
                }
                for x in &a.usb.product {
                    for y in &b.usb.product {
                        let (x, y) = (x.to_lowercase(), y.to_lowercase());
                        if x.contains(&y) || y.contains(&x) {
                            problems.push(format!(
                                "{} and {}: product strings {x:?} and {y:?} overlap",
                                a.id, b.id
                            ));
                        }
                    }
                }
            }
        }
        for l in &self.lan {
            if !id_ok(&l.id) || ids.contains(&l.id.as_str()) {
                problems.push(format!("{}: not a unique adapter id", l.id));
            }
            ids.push(&l.id);
            if !(l.service.starts_with('_') && l.service.ends_with("._tcp")) {
                problems.push(format!(
                    "{}: {} is not an mDNS service type",
                    l.id, l.service
                ));
            }
            if l.probes.is_empty() {
                problems.push(format!("{}: a LAN coordinator with no probe", l.id));
            }
        }
        problems
    }
}
