//! The simulator in process, read and written through the daemon's own bus client.

use pulzr_edge::{
    config::BusKind,
    modbus::{bus, regmap::RegisterMap},
    outbox::Outbox,
    sim::{self, Sim},
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
};
use tokio_modbus::{client::tcp, prelude::*, ExceptionCode, Slave};

const GW: &str = "aabbccddeeff0011";
const METER: &str = include_str!("../../../examples/maps/generic-energy-meter.toml");

fn key(unit: u8) -> String {
    format!("modbus:{GW}.lan-0.{unit}")
}

async fn start() -> (Arc<Sim>, std::net::SocketAddr) {
    let map = RegisterMap::from_toml(METER).unwrap();
    let sim = Sim::new(vec![(17, map.clone()), (18, map)]);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(sim::serve(listener, sim.clone()));
    (sim, addr)
}

#[tokio::test]
async fn the_daemons_bus_client_reads_and_writes_the_sim() {
    let (sim, addr) = start().await;
    let dir = tempfile::tempdir().unwrap();
    let out = Outbox::open(dir.path(), &format!("edge-{GW}"), GW, 1 << 20).unwrap();
    let mut map = RegisterMap::from_toml(METER).unwrap();
    for f in &mut map.fields {
        f.poll_interval = Some(0.1);
    }
    let map = Arc::new(map);
    let (tx, rx) = mpsc::channel(8);
    let mut router = bus::Router::default();
    router.add(&key(17), map.clone(), tx);
    let units = vec![bus::UnitSpec {
        key: key(17),
        unit: 17,
        map,
        offline_after: 3,
    }];
    let (stop, stop_rx) = watch::channel(false);
    let settings = bus::BusSettings {
        id: "lan-0".into(),
        kind: BusKind::Tcp {
            address: addr.to_string(),
        },
        timeout: Duration::from_millis(500),
        retries: 0,
        backoff: Duration::from_millis(10),
        turnaround: Duration::ZERO,
    };
    tokio::spawn(bus::run(settings, units, out.clone(), rx, stop_rx));

    let mut seen: Vec<(String, Value)> = vec![];
    let drain = |seen: &mut Vec<(String, Value)>| {
        while let Some(e) = out.peek() {
            seen.push((e.topic.clone(), serde_json::from_slice(&e.payload).unwrap()));
            out.ack(&e);
        }
    };

    // every field is read; the energy meter's voltage is a plausible number
    let mut first = None;
    for _ in 0..100 {
        drain(&mut seen);
        first = seen
            .iter()
            .find(|(t, v)| t.ends_with("/up") && v["r"][0]["v"]["voltage"].is_number())
            .map(|(_, v)| v["r"][0]["v"].clone());
        if first.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let v = first.expect("no up with voltage");
    assert!(v["voltage"].as_f64().unwrap().is_finite());
    assert_eq!(v["report_period"], 1.0, "writable starts at its minimum");
    assert_eq!(v["relay"], false);

    // a write through settings.set round-trips
    let body = json!({"v":1,"id":"cmd-set-0001","name":"settings.set",
        "params":{"report_period": 42, "relay": true}});
    router.dispatch(
        &out,
        &out.topic(&key(17), "cmd"),
        body.to_string().as_bytes(),
    );
    let mut readback = false;
    let mut ok = false;
    for _ in 0..100 {
        drain(&mut seen);
        ok = seen
            .iter()
            .any(|(_, v)| v["id"] == "cmd-set-0001" && v["status"] == "ok");
        readback = seen.iter().any(|(t, v)| {
            t.ends_with("/up")
                && v["r"][0]["v"]["report_period"] == 42.0
                && v["r"][0]["v"]["relay"] == true
        });
        if ok && readback {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ok, "write not acknowledged: {seen:#?}");
    assert!(readback, "the sim did not keep the written value");

    // values move
    let before = sim_input(addr, 17).await;
    sim.tick(50.0);
    sim.tick(100.0);
    assert_ne!(before, sim_input(addr, 17).await);
    let _ = stop.send(true);
}

async fn sim_input(addr: std::net::SocketAddr, unit: u8) -> Vec<u16> {
    let mut c = tcp::connect_slave(addr, Slave(unit)).await.unwrap();
    c.read_input_registers(0, 8).await.unwrap().unwrap()
}

#[tokio::test]
async fn writes_elsewhere_are_refused_and_units_are_separate() {
    let (_sim, addr) = start().await;
    let mut c = tcp::connect_slave(addr, Slave(17)).await.unwrap();
    // input registers and unmapped holding registers are not writable
    assert_eq!(
        c.write_single_register(0, 1).await.unwrap(),
        Err(ExceptionCode::IllegalDataAddress)
    );
    assert_eq!(
        c.write_multiple_registers(99, &[1, 2]).await.unwrap(),
        Err(ExceptionCode::IllegalDataAddress)
    );
    assert_eq!(
        c.write_single_coil(5, true).await.unwrap(),
        Err(ExceptionCode::IllegalDataAddress)
    );
    // writable ones keep the value, on this unit only
    c.write_single_register(100, 7).await.unwrap().unwrap();
    c.write_multiple_coils(0, &[true]).await.unwrap().unwrap();
    assert_eq!(
        c.read_holding_registers(100, 1).await.unwrap().unwrap(),
        [7]
    );
    assert_eq!(c.read_coils(0, 1).await.unwrap().unwrap(), [true]);
    let mut other = tcp::connect_slave(addr, Slave(18)).await.unwrap();
    assert_eq!(
        other.read_holding_registers(100, 1).await.unwrap().unwrap(),
        [1]
    );
}
