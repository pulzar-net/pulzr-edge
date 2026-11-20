//! The Zigbee probes end to end: `Builtin` through zigbee-host's open,
//! exchange and close, against zigbee-bridge's simulated coordinators (an
//! EmberZNet, a Z-Stack and a deCONZ stick) served over TCP, as a networked
//! coordinator serves its serial port.
#![cfg(feature = "zigbee")]

use pulzr_probe::probe::{Builtin, Outcome, Port, Prober};
use pulzr_probe::table::{ProbeKind, Protocol};
use std::time::{Duration, Instant};
use zigbee_sim::{SimConfig, SimNcp, Simulator};

/// A simulated coordinator of `ncp` on a local port, on a runtime of its own;
/// it serves one connection and reports when that connection ended.
fn coordinator(ncp: SimNcp) -> (u16, std::sync::mpsc::Receiver<()>, tokio::runtime::Runtime) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let (ended, rx) = std::sync::mpsc::channel();
    rt.spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let sim = Simulator::new(SimConfig {
            ncp,
            ..SimConfig::default()
        });
        let _ = sim.serve(stream).await;
        let _ = ended.send(());
    });
    (port, rx, rt)
}

fn answered(kind: ProbeKind, ncp: SimNcp) -> pulzr_probe::probe::Answer {
    let (port, ended, _rt) = coordinator(ncp);
    let started = Instant::now();
    let out = Builtin.probe(kind, Port::Tcp("127.0.0.1", port));
    let Outcome::Answered(a) = out else {
        panic!("{kind:?}: {out:?}")
    };
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{kind:?} took too long"
    );
    ended
        .recv_timeout(Duration::from_secs(2))
        .expect("the probe left the port open");
    assert_eq!(a.protocol, Some(Protocol::Zigbee));
    a
}

#[test]
fn an_emberznet_coordinator_answers_with_its_firmware() {
    let a = answered(ProbeKind::Ezsp, SimNcp::Ezsp { protocol: 13 });
    assert_eq!(a.ncp, Some("ezsp"));
    assert_eq!(a.firmware.as_deref(), Some("7.4.4.0"));
    assert_eq!(a.ezsp, Some(13));
}

#[test]
fn a_z_stack_coordinator_answers_with_its_product() {
    let a = answered(ProbeKind::Znp, SimNcp::Znp { product: 1 });
    assert_eq!(a.ncp, Some("znp"));
    assert!(a.firmware.is_some());
    // The EZSP protocol version is EZSP's alone.
    assert_eq!(a.ezsp, None);
    assert_eq!(a.chip.as_deref(), Some("Z-Stack 3.x.0"));
}

#[test]
fn a_deconz_coordinator_answers() {
    let a = answered(ProbeKind::Deconz, SimNcp::Deconz);
    assert_eq!(a.ncp, Some("deconz"));
    assert!(a.firmware.is_some());
    assert!(a
        .chip
        .as_deref()
        .is_some_and(|c| c.contains("ConBee") || c.contains("RaspBee")));
}

#[test]
fn asked_as_the_wrong_family_a_stick_does_not_answer() {
    let (port, _ended, _rt) = coordinator(SimNcp::Znp { product: 1 });
    let out = Builtin.probe(ProbeKind::Ezsp, Port::Tcp("127.0.0.1", port));
    assert!(!matches!(out, Outcome::Answered(_)), "{out:?}");
}

#[test]
fn a_port_that_cannot_open_is_an_error() {
    let out = Builtin.probe(
        ProbeKind::Znp,
        Port::Serial(std::path::Path::new("/dev/pulzr-probe-no-such-port")),
    );
    assert!(matches!(out, Outcome::Error(_)), "{out:?}");
}
