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
