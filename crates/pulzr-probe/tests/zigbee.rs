//! The EZSP probe end to end: `Builtin` through zigbee-host's open, ASH and
//! EZSP exchange and close, against zigbee-bridge's simulated EmberZNet NCP
//! served over TCP, as a networked coordinator serves its serial port.
#![cfg(feature = "zigbee")]

use pulzr_probe::probe::{Builtin, Outcome, Port, Prober};
use pulzr_probe::table::{ProbeKind, Protocol};
use std::time::{Duration, Instant};
use zigbee_sim::{SimConfig, Simulator};

/// The simulator on a local port, on a runtime of its own; it serves one
/// connection and reports whether that connection ended.
fn ncp() -> (u16, std::sync::mpsc::Receiver<()>, tokio::runtime::Runtime) {
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
        let sim = Simulator::new(SimConfig::default());
        let _ = sim.serve(stream).await;
        let _ = ended.send(());
    });
    (port, rx, rt)
}

#[test]
fn an_emberznet_coordinator_answers_with_its_firmware() {
    let (port, ended, _rt) = ncp();
    let started = Instant::now();
    let out = Builtin.probe(ProbeKind::Ezsp, Port::Tcp("127.0.0.1", port));
    let Outcome::Answered(a) = out else {
        panic!("{out:?}")
    };
    assert_eq!(a.protocol, Some(Protocol::Zigbee));
    assert_eq!(a.ncp, Some("ezsp"));
    assert_eq!(a.firmware.as_deref(), Some("7.4.4.0"));
    assert_eq!(a.ezsp, Some(SimConfig::default().protocol));
    assert!(started.elapsed() < Duration::from_secs(5));
    // The probe closed the port before it returned.
    ended
        .recv_timeout(Duration::from_secs(2))
        .expect("the probe left the port open");
}

#[test]
fn a_port_that_cannot_open_is_an_error() {
    let out = Builtin.probe(
        ProbeKind::Ezsp,
        Port::Serial(std::path::Path::new("/dev/pulzr-probe-no-such-port")),
    );
    assert!(matches!(out, Outcome::Error(_)), "{out:?}");
}
