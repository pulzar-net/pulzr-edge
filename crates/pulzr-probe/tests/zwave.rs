//! The Z-Wave probe end to end: `Builtin` through zwave-serial's open, frames
//! and close, against a fake controller. The fake serves its serial port over
//! TCP, as a networked stick does; a pty would do on Linux, but macOS refuses
//! a pty the baud-rate ioctl, and the frames are the same either way.
#![cfg(feature = "zwave")]

use pulzr_probe::probe::{Builtin, Outcome, Port, Prober};
use pulzr_probe::table::{ProbeKind, Protocol};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

fn frame(kind: u8, func: u8, payload: &[u8]) -> Vec<u8> {
    let len = 3 + payload.len() as u8;
    let mut f = vec![0x01, len, kind, func];
    f.extend_from_slice(payload);
    let sum = f[1..].iter().fold(0xff, |a, b| a ^ b);
    f.push(sum);
    f
}

/// A controller on a local port. With `answer`, it is an 800-series stick on
/// firmware 7.22 implementing GET_SERIAL_API_CAPABILITIES alone, so the probe
/// asks nothing after it; without, it ACKs and stays silent. Returns the
/// functions it was asked for once the probe hangs up.
fn controller(answer: bool) -> (u16, thread::JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let fake = thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let mut asked = Vec::new();
        let mut pending = Vec::new();
        let mut buf = [0u8; 64];
        while let Ok(n) = s.read(&mut buf) {
            if n == 0 {
                break;
            }
            pending.extend_from_slice(&buf[..n]);
            while let Some(start) = pending.iter().position(|&b| b == 0x01) {
                let Some(&len) = pending.get(start + 1) else {
                    break;
                };
                let end = start + 2 + len as usize;
                if pending.len() < end {
                    break;
                }
                let func = pending[start + 3];
                asked.push(func);
                pending.drain(..end);
                let _ = s.write_all(&[0x06]);
                if answer && func == 0x07 {
                    let mut caps = vec![7, 22, 0x00, 0x00, 0x00, 0x04, 0x00, 0x04];
                    let mut mask = [0u8; 32];
                    mask[0] = 1 << 6; // function 0x07
                    caps.extend_from_slice(&mask);
                    let _ = s.write_all(&frame(0x01, 0x07, &caps));
                }
            }
        }
        asked
    });
    (port, fake)
}

#[test]
fn a_controller_answers_with_its_firmware() {
    let (port, fake) = controller(true);
    let out = Builtin.probe(ProbeKind::ZwaveSerial, Port::Tcp("127.0.0.1", port));
    let Outcome::Answered(a) = out else {
        panic!("{out:?}")
    };
    assert_eq!(a.protocol, Some(Protocol::Zwave));
    assert_eq!(a.ncp, Some("zwave-serial"));
    assert_eq!(a.firmware.as_deref(), Some("7.22"));
    // Asked once, and the connection was closed before probe returned.
    assert_eq!(fake.join().unwrap(), vec![0x07]);
}

#[test]
fn silence_is_no_answer_not_an_error() {
    let (port, fake) = controller(false);
    let out = Builtin.probe(ProbeKind::ZwaveSerial, Port::Tcp("127.0.0.1", port));
    assert_eq!(out, Outcome::NoAnswer);
    assert_eq!(fake.join().unwrap(), vec![0x07]);
}

#[test]
fn a_port_that_cannot_open_is_an_error() {
    let out = Builtin.probe(
        ProbeKind::ZwaveSerial,
        Port::Serial(std::path::Path::new("/dev/pulzr-probe-no-such-port")),
    );
    assert!(matches!(out, Outcome::Error(_)), "{out:?}");
}
