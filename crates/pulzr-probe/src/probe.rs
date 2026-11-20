//! Asking a radio what it runs.
//!
//! The frames themselves belong to the host stacks (`zigbee-host::probe` in
//! zigbee-bridge, `zwave-serial::probe` in zwave-bridge), which know their
//! protocols' framing and timing; pulzr-probe decides only whether a port may
//! be asked at all and what to ask it. Until a stack is built in, its kinds
//! answer [`Outcome::Unavailable`] and the radio is reported by what the table
//! says it is.

use crate::table::{ProbeKind, Protocol};
use std::path::Path;

/// Where a radio is reached.
#[derive(Debug, Clone, Copy)]
pub enum Port<'a> {
    /// A serial device node, `/dev/ttyUSB0`.
    Serial(&'a Path),
    /// A coordinator on the LAN serving its serial port over TCP.
    Tcp(&'a str, u16),
}

/// What a radio said about itself.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Answer {
    pub protocol: Option<Protocol>,
    pub ncp: Option<&'static str>,
    pub firmware: Option<String>,
    /// EZSP protocol version, for a Silicon Labs coordinator.
    pub ezsp: Option<u8>,
    pub chip: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Answered(Answer),
    /// Opened and asked, and nothing valid came back.
    NoAnswer,
    /// This build cannot ask this kind.
    Unavailable,
    /// The port could not be opened or read.
    Error(String),
}

pub trait Prober {
    /// One exchange of one kind, bounded in time; the port is closed again
    /// before it returns.
    fn probe(&self, kind: ProbeKind, port: Port) -> Outcome;
}

/// The build with no stack in it: every kind is unavailable, so nothing is
/// ever written to any port.
pub struct NoStacks;

impl Prober for NoStacks {
    fn probe(&self, _kind: ProbeKind, _port: Port) -> Outcome {
        Outcome::Unavailable
    }
}

/// What this build can ask: an OpenThread RCP, whose two Spinel reads are
/// pulzr-probe's own ([`crate::spinel`]); a Z-Wave controller through
/// zwave-bridge's `zwave_serial::probe` (feature `zwave`); and an EmberZNet
/// coordinator through zigbee-bridge's `zigbee_host::probe` (feature
/// `zigbee`). ZNP and deCONZ are unavailable until zigbee-host probes them.
pub struct Builtin;

impl Prober for Builtin {
    fn probe(&self, kind: ProbeKind, port: Port) -> Outcome {
        match (kind, port) {
            (ProbeKind::Spinel, Port::Serial(path)) => crate::spinel::probe(path),
            #[cfg(feature = "zwave")]
            (ProbeKind::ZwaveSerial, Port::Serial(path)) => match path.to_str() {
                Some(path) => zwave(path),
                None => Outcome::Error("device path is not UTF-8".into()),
            },
            #[cfg(feature = "zwave")]
            (ProbeKind::ZwaveSerial, Port::Tcp(host, port)) => {
                zwave(&format!("tcp://{host}:{port}"))
            }
            #[cfg(feature = "zigbee")]
            (ProbeKind::Ezsp | ProbeKind::Znp | ProbeKind::Deconz, Port::Serial(path)) => {
                match path.to_str() {
                    Some(path) => zigbee(kind, path),
                    None => Outcome::Error("device path is not UTF-8".into()),
                }
            }
            #[cfg(feature = "zigbee")]
            (ProbeKind::Ezsp | ProbeKind::Znp | ProbeKind::Deconz, Port::Tcp(host, port)) => {
                zigbee(kind, &format!("tcp://{host}:{port}"))
            }
            _ => Outcome::Unavailable,
        }
    }
}

/// NAK, then GET_SERIAL_API_CAPABILITIES, and the controller reads only if
/// that answered like a controller; nothing that writes NVM, resets or forms
/// a network. Bounded at 2.9 s by zwave-serial, port closed on return.
#[cfg(feature = "zwave")]
fn zwave(path: &str) -> Outcome {
    match zwave_serial::probe_blocking(path) {
        Ok(Some(id)) => Outcome::Answered(Answer {
            protocol: Some(Protocol::Zwave),
            ncp: Some("zwave-serial"),
            firmware: Some(id.firmware.clone()),
            ezsp: None,
            chip: id.series.map(|s| {
                match s {
                    zwave_serial::Series::S500 => "Z-Wave 500 series",
                    zwave_serial::Series::S700 => "Z-Wave 700 series",
                    zwave_serial::Series::S800 => "Z-Wave 800 series",
                }
                .to_string()
            }),
        }),
        Ok(None) => Outcome::NoAnswer,
        Err(e) => Outcome::Error(e.to_string()),
    }
}

/// Asks each kind in turn until one answers. `Answered` if any did; else
/// `NoAnswer` if any kind was actually tried, `Error` if every try failed to
/// open, `Unavailable` if none could be tried.
pub fn first_answer(prober: &dyn Prober, kinds: &[ProbeKind], port: Port) -> Outcome {
    let mut tried = false;
    let mut error = None;
    for &kind in kinds {
        match prober.probe(kind, port) {
            Outcome::Answered(mut a) => {
                a.protocol.get_or_insert(kind.protocol());
                a.ncp.get_or_insert(kind.ncp());
                return Outcome::Answered(a);
            }
            Outcome::NoAnswer => tried = true,
            Outcome::Error(e) => error = Some(e),
            Outcome::Unavailable => {}
        }
    }
    match (tried, error) {
        (true, _) => Outcome::NoAnswer,
        (false, Some(e)) => Outcome::Error(e),
        (false, None) => Outcome::Unavailable,
    }
}

/// A Zigbee coordinator, through zigbee-host's probe (zigbee-bridge v0.2.0),
/// reads only, nothing that writes NVM or touches a network, bounded at 3 s,
/// at 115200 baud, as every common stick ships:
///
/// - EZSP: ASH RST, `version`, `getValue(VERSION_INFO)`, `getEui64`, the
///   manufacturer and board tokens.
/// - ZNP: the CC2652 bootloader-skip byte, SYS_PING, SYS_VERSION,
///   UTIL_GET_DEVICE_INFO.
/// - deCONZ: VERSION, MAC_ADDRESS.
///
/// On a runtime that lives for this one probe.
#[cfg(feature = "zigbee")]
fn zigbee(kind: ProbeKind, path: &str) -> Outcome {
    use zigbee_host::probe::{probe, NcpKind};
    let ncp = match kind {
        ProbeKind::Ezsp => NcpKind::Ezsp,
        ProbeKind::Znp => NcpKind::Znp,
        ProbeKind::Deconz => NcpKind::Deconz,
        _ => return Outcome::Unavailable,
    };
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return Outcome::Error(e.to_string()),
    };
    let found = rt.block_on(probe(path, ncp, 115_200));
    drop(rt);
    match found {
        Ok(Some(id)) => Outcome::Answered(Answer {
            protocol: Some(Protocol::Zigbee),
            ncp: Some(kind.ncp()),
            firmware: Some(id.firmware),
            // The EZSP protocol version means something only for EZSP.
            ezsp: (kind == ProbeKind::Ezsp).then_some(id.protocol),
            // The hardware a ZNP or deCONZ stick names ("Z-Stack 3.x.0",
            // "ConBee III"); EZSP names none, and its board token is nearest.
            chip: id.product.or(id.board).filter(|b| !b.trim().is_empty()),
        }),
        Ok(None) => Outcome::NoAnswer,
        Err(e) => Outcome::Error(e.to_string()),
    }
}
