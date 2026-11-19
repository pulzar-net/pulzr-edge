//! Asking an OpenThread radio co-processor what it is: Spinel over HDLC-lite,
//! `PROP_VALUE_GET NCP_VERSION` and `PROP_VALUE_GET CAPS`.
//!
//! In pulzr-probe itself rather than behind a stack's crate: Thread's stack is
//! OpenThread's border router (the pulzr-thread add-on), and these two frames
//! are all a probe needs. Both are reads; nothing here resets the radio or
//! touches its settings. An RCP answers at once, so one baud rate costs about
//! half a second when nothing is there.

// The wire constants are used by the Linux probe and the tests.
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use crate::probe::{Answer, Outcome};
use crate::table::Protocol;
use std::path::Path;
#[cfg(target_os = "linux")]
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::fd::AsRawFd,
    time::{Duration, Instant},
};

const FLAG: u8 = 0x7e;
const ESCAPE: u8 = 0x7d;
// Spinel header: flag bit, interface 0, transaction id 1.
const HEADER: u8 = 0x81;
const CMD_PROP_VALUE_GET: u8 = 0x02;
const CMD_PROP_VALUE_IS: u8 = 0x06;
const PROP_NCP_VERSION: u8 = 0x02;
const PROP_CAPS: u8 = 0x05;
const CAP_CONFIG_RADIO: u32 = 34;

/// OpenThread's RCP images run at 460800 (nRF52840, Silicon Labs MG21 and
/// MG24); older multiprotocol images at 115200. A CDC-ACM dongle ignores it.
pub const BAUDS: [u32; 2] = [460_800, 115_200];
#[cfg(target_os = "linux")]
const WAIT: Duration = Duration::from_millis(600);

/// PPP's FCS-16 (RFC 1662), which HDLC-lite uses.
pub fn fcs16(data: &[u8]) -> u16 {
    let mut fcs: u16 = 0xffff;
    for &b in data {
        fcs ^= u16::from(b);
        for _ in 0..8 {
            fcs = if fcs & 1 != 0 {
                (fcs >> 1) ^ 0x8408
            } else {
                fcs >> 1
            };
        }
    }
    fcs ^ 0xffff
}

fn needs_escape(b: u8) -> bool {
    matches!(b, FLAG | ESCAPE | 0x11 | 0x13 | 0xf8)
}

/// One frame on the wire: flag, payload and FCS escaped, flag.
pub fn encode(payload: &[u8]) -> Vec<u8> {
    let fcs = fcs16(payload);
    let mut out = vec![FLAG];
    for &b in payload.iter().chain(&fcs.to_le_bytes()) {
        if needs_escape(b) {
            out.extend([ESCAPE, b ^ 0x20]);
        } else {
            out.push(b);
        }
    }
    out.push(FLAG);
    out
}

/// The payloads of every complete frame with a good FCS in `bytes`.
pub fn decode(bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    let mut cur = Vec::new();
    let mut esc = false;
    for &b in bytes {
        match b {
            FLAG => {
                if cur.len() > 2 {
                    let (body, fcs) = cur.split_at(cur.len() - 2);
                    if fcs16(body).to_le_bytes() == [fcs[0], fcs[1]] {
                        frames.push(body.to_vec());
                    }
                }
                cur.clear();
                esc = false;
            }
            ESCAPE => esc = true,
            _ => {
                cur.push(if esc { b ^ 0x20 } else { b });
                esc = false;
            }
        }
    }
    frames
}

/// Spinel's packed unsigned integers, as CAPS lists them.
pub fn unpack_uints(mut data: &[u8]) -> Vec<u32> {
    let mut out = Vec::new();
    while !data.is_empty() {
        let (mut v, mut shift, mut used) = (0u32, 0u32, 0usize);
        for &b in data {
            used += 1;
            if shift < 32 {
                v |= u32::from(b & 0x7f) << shift;
            }
            shift += 7;
            if b & 0x80 == 0 {
                break;
            }
        }
        out.push(v);
        data = &data[used..];
    }
    out
}

/// The value of a `PROP_VALUE_IS` for `prop` among `frames`.
fn value_is(frames: &[Vec<u8>], prop: u8) -> Option<Vec<u8>> {
    frames.iter().find_map(|f| match f.as_slice() {
        [h, CMD_PROP_VALUE_IS, p, rest @ ..] if h & 0x80 != 0 && *p == prop => Some(rest.to_vec()),
        _ => None,
    })
}

/// What an RCP's two answers say: its version string, and whether its caps
/// name it a radio co-processor (an NCP runs the whole stack itself, which the
/// border router does not take).
pub fn answer(frames: &[Vec<u8>]) -> Option<Answer> {
    let version = value_is(frames, PROP_NCP_VERSION)?;
    let text =
        String::from_utf8_lossy(version.split(|&b| b == 0).next().unwrap_or(&[])).into_owned();
    if text.is_empty() {
        return None;
    }
    let rcp = value_is(frames, PROP_CAPS).map(|c| unpack_uints(&c).contains(&CAP_CONFIG_RADIO));
    // "OPENTHREAD/<version>; <platform>; <date>": the first two, short enough
    // for the report's firmware field.
    let firmware: String = text
        .split("; ")
        .take(2)
        .collect::<Vec<_>>()
        .join("; ")
        .chars()
        .take(64)
        .collect();
    Some(Answer {
        protocol: Some(Protocol::Thread),
        ncp: Some(if rcp == Some(false) {
            "spinel-ncp"
        } else {
            "spinel"
        }),
        firmware: Some(firmware),
        ezsp: None,
        chip: text.split("; ").nth(1).map(str::to_owned),
    })
}

#[cfg(target_os = "linux")]
fn set_raw(file: &File, baud: u32) -> std::io::Result<()> {
    let speed = match baud {
        460_800 => libc::B460800,
        115_200 => libc::B115200,
        _ => return Err(std::io::Error::other("unsupported baud")),
    };
    let fd = file.as_raw_fd();
    // SAFETY: termios is plain data, and the fd is open for the call's length.
    unsafe {
        let mut t: libc::termios = std::mem::zeroed();
        if libc::tcgetattr(fd, &mut t) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        libc::cfmakeraw(&mut t);
        libc::cfsetispeed(&mut t, speed);
        libc::cfsetospeed(&mut t, speed);
        t.c_cflag |= libc::CLOCAL | libc::CREAD;
        t.c_cflag &= !libc::CRTSCTS;
        t.c_cc[libc::VMIN] = 0;
        t.c_cc[libc::VTIME] = 1;
        if libc::tcsetattr(fd, libc::TCSANOW, &t) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        libc::tcflush(fd, libc::TCIOFLUSH);
    }
    Ok(())
}

/// Elsewhere (a developer's Mac) there is no gateway serial port to ask.
#[cfg(not(target_os = "linux"))]
pub fn probe(_path: &Path) -> Outcome {
    Outcome::Unavailable
}

/// Ask a serial port, at each baud rate in turn.
#[cfg(target_os = "linux")]
pub fn probe(path: &Path) -> Outcome {
    let mut tried = false;
    for baud in BAUDS {
        let mut file = match OpenOptions::new().read(true).write(true).open(path) {
            Ok(f) => f,
            Err(e) => return Outcome::Error(e.to_string()),
        };
        if let Err(e) = set_raw(&file, baud) {
            return Outcome::Error(e.to_string());
        }
        tried = true;
        let mut out = encode(&[HEADER, CMD_PROP_VALUE_GET, PROP_NCP_VERSION]);
        out.extend(encode(&[HEADER, CMD_PROP_VALUE_GET, PROP_CAPS]));
        if file.write_all(&out).is_err() {
            continue;
        }
        let start = Instant::now();
        let mut got = Vec::new();
        let mut buf = [0u8; 256];
        while start.elapsed() < WAIT && got.len() < 4096 {
            match file.read(&mut buf) {
                Ok(n) => got.extend_from_slice(&buf[..n]),
                Err(_) => break,
            }
            let frames = decode(&got);
            if value_is(&frames, PROP_NCP_VERSION).is_some()
                && value_is(&frames, PROP_CAPS).is_some()
            {
                break;
            }
        }
        if let Some(a) = answer(&decode(&got)) {
            return Outcome::Answered(a);
        }
    }
    if tried {
        Outcome::NoAnswer
    } else {
        Outcome::Unavailable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fcs_matches_rfc1662() {
        // RFC 1662's check value for "123456789".
        assert_eq!(fcs16(b"123456789"), 0x906e);
    }

    #[test]
    fn frames_round_trip_with_escapes() {
        let payload = [
            HEADER,
            CMD_PROP_VALUE_IS,
            PROP_NCP_VERSION,
            0x7e,
            0x7d,
            0x11,
            0x13,
            0xf8,
            b'x',
        ];
        let wire = encode(&payload);
        assert!(!wire[1..wire.len() - 1].contains(&FLAG));
        assert_eq!(decode(&wire), vec![payload.to_vec()]);
        // A damaged frame is dropped, and the good one after it kept.
        let mut bad = wire.clone();
        bad[3] ^= 1;
        bad.extend(&wire);
        assert_eq!(decode(&bad), vec![payload.to_vec()]);
    }

    #[test]
    fn the_request_is_what_openthread_expects() {
        // spinel_datatype_pack of (header 0x81, CMD 2, PROP 2) and its FCS.
        let f = encode(&[0x81, 0x02, 0x02]);
        assert_eq!(f[..4], [0x7e, 0x81, 0x02, 0x02]);
        assert_eq!(*f.last().unwrap(), 0x7e);
    }

    #[test]
    fn an_rcp_answer() {
        let mut version = vec![HEADER, CMD_PROP_VALUE_IS, PROP_NCP_VERSION];
        version.extend(b"SL-OPENTHREAD/2.4.4.0_GitHub-7074a43e4; EFR32; Oct 21 2024 14:40:57\0");
        // CAPS: 5 (CONFIG?), 34 CONFIG_RADIO, 512+ as a two-byte packed value.
        let caps = vec![HEADER, CMD_PROP_VALUE_IS, PROP_CAPS, 5, 34, 0x80, 0x04];
        let a = answer(&[version.clone(), caps]).expect("answered");
        assert_eq!(a.protocol, Some(Protocol::Thread));
        assert_eq!(a.ncp, Some("spinel"));
        assert_eq!(
            a.firmware.as_deref(),
            Some("SL-OPENTHREAD/2.4.4.0_GitHub-7074a43e4; EFR32")
        );
        assert_eq!(a.chip.as_deref(), Some("EFR32"));
        assert_eq!(unpack_uints(&[5, 34, 0x80, 0x04]), vec![5, 34, 512]);

        let ncp = vec![HEADER, CMD_PROP_VALUE_IS, PROP_CAPS, 32];
        assert_eq!(answer(&[version, ncp]).unwrap().ncp, Some("spinel-ncp"));
        assert!(answer(&[vec![HEADER, CMD_PROP_VALUE_IS, PROP_CAPS, 34]]).is_none());
    }
}
