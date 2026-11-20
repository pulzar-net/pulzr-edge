//! The `ble` module.
//!
//! `up_matches_the_protocol` checks the body against the protocol's own JSON
//! Schema. `bumble_*` run the whole module against Google's Bumble: a
//! virtual Bluetooth controller (over H4 on TCP, as a real one is reached)
//! and a scripted beacon on the same simulated radio link,
//! `tests/bumble/beacon.py`. They skip without `BUMBLE_PYTHON` unless
//! `BUMBLE_REQUIRED` is set (CI).

#![cfg(target_endian = "little")]

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use base64::Engine;
use pulzr_edge::ble::{self, commands::Router, window::Window, BleCfg};
use pulzr_edge::mqtt::Dispatch;
use pulzr_edge::outbox::Outbox;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const EUI: &str = "aabbccddeeff0011";
/// The KKM S7S parking sensor frame the scripted beacon sends (Eddystone
/// service data, KSensor type 0x21).
const KSENSOR: &str = "020106 1116aafe21001b0bb81980ffff0002000301";

fn schema_check(kind: &str, body: &Value) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../pulzr-protocol/schema/protocol.schema.json");
    let mut schema: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    schema["$ref"] = json!(format!("#/$defs/{kind}"));
    let v = jsonschema::validator_for(&schema).unwrap();
    let errs: Vec<String> = v.iter_errors(body).map(|e| e.to_string()).collect();
    assert!(errs.is_empty(), "{kind}: {errs:?}\n{body}");
}

fn drain(out: &Outbox) -> Vec<(String, Value)> {
    let mut v = Vec::new();
    while let Some(e) = out.peek() {
        out.ack(&e);
        v.push((e.topic, serde_json::from_slice(&e.payload).unwrap()));
    }
    v
}

#[test]
fn up_matches_the_protocol() {
    let dir = tempfile::tempdir().unwrap();
    let out = Outbox::open(dir.path(), &format!("edge-{EUI}"), EUI, 1 << 20).unwrap();
    let mut w = Window::new(vec![vec![0xbc, 0x57, 0x29]], -127);
    let data = hex::decode(KSENSOR.replace(' ', "")).unwrap();
    w.add(
        &bluetooth_host::Report {
            address: "BC:57:29:00:00:01".parse().unwrap(),
            identity: None,
            rssi: -61,
            tx_power: Some(-4),
            primary_phy: bluetooth_host::Phy::Le1m,
            secondary_phy: None,
            sid: None,
            periodic_interval: None,
            connectable: false,
            scannable: false,
            directed: false,
            legacy: true,
            direct_address: None,
            data: data.clone(),
            scan_response: None,
            truncated: false,
            time: SystemTime::now(),
        },
        1_791_784_502.0,
    );
    for parsed in [false, true] {
        let h = w.drain().pop().unwrap_or_else(|| panic!("heard"));
        out.up_body(
            &ble::key_for(&h.device()),
            ble::up_body(&h, &out.via(), parsed),
        );
        let (topic, body) = drain(&out).pop().unwrap();
        assert_eq!(
            topic,
            format!("pulzr/a/edge-{EUI}/d/ble:bc5729fffe000001/up")
        );
        schema_check("up", &body);
        assert_eq!(
            body["raw"]["b64"],
            base64::engine::general_purpose::STANDARD.encode(&data)
        );
        assert_eq!(body["rx"][0]["via"], format!("gw:{EUI}"));
        assert_eq!(body["rx"][0]["rssi"].as_f64(), Some(-61.0));
        assert_eq!(body["link"]["ble"]["adv"], "adv_nonconn_ind");
        assert!(
            body["link"]["ble"].get("adv_type").is_none(),
            "no class: fPort 0, as before"
        );
        assert_eq!(body["link"]["ble"]["tx_power"], -4);
        if parsed {
            assert_eq!(body["link"]["ble"]["parsed"]["beacons"][0]["format"], "kkm");
        } else {
            assert!(body["link"]["ble"].get("parsed").is_none());
        }
        // The next interval: the same device again.
        w.add(
            &bluetooth_host::Report {
                address: "BC:57:29:00:00:01".parse().unwrap(),
                identity: None,
                rssi: -61,
                tx_power: Some(-4),
                primary_phy: bluetooth_host::Phy::Le1m,
                secondary_phy: None,
                sid: None,
                periodic_interval: None,
                connectable: false,
                scannable: false,
                directed: false,
                legacy: true,
                direct_address: None,
                data: data.clone(),
                scan_response: None,
                truncated: false,
                time: SystemTime::now(),
            },
            1_791_784_512.0,
        );
    }
}

#[test]
fn other_modules_keys_are_not_answered() {
    let dir = tempfile::tempdir().unwrap();
    let out = Outbox::open(dir.path(), &format!("edge-{EUI}"), EUI, 1 << 20).unwrap();
    let router = Arc::new(Router::new(true));
    let cmd = br#"{"v":1,"id":"abcdefgh-1","name":"register.write","params":{}}"#;
    router.dispatch(
        &out,
        &format!("pulzr/a/edge-{EUI}/d/modbus:{EUI}.bus1.1/cmd"),
        cmd,
    );
    router.dispatch(&out, &format!("pulzr/a/edge-{EUI}/d/gw:{EUI}/cmd"), cmd);
    assert!(drain(&out).is_empty());
    // A device this gateway never heard: another gateway may reach it.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let read =
            br#"{"v":1,"id":"abcdefgh-2","name":"gatt.read","params":{"characteristic":"2a19"}}"#;
        router.dispatch(
            &out,
            &format!("pulzr/a/edge-{EUI}/d/ble:bc5729fffe000001/cmd"),
            read,
        );
    });
    let res = drain(&out);
    assert_eq!(res[0].1["status"], "failed");
    assert_eq!(res[0].1["reason"], "undeliverable");
    schema_check("cmdResult", &res[0].1);
}

// ------------------------------------------------------------------ Bumble

struct Peer {
    _child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    lines: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    port: u16,
}

impl Peer {
    async fn start() -> Option<Peer> {
        let py = match std::env::var("BUMBLE_PYTHON") {
            Ok(p) => p,
            Err(_) if std::env::var("BUMBLE_REQUIRED").is_ok() => {
                panic!("BUMBLE_PYTHON is not set")
            }
            Err(_) => {
                eprintln!("skipping: set BUMBLE_PYTHON to a Python with bumble installed");
                return None;
            }
        };
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let mut child = tokio::process::Command::new(py)
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/bumble/beacon.py"
            ))
            .arg(port.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let mut p = Peer {
            _child: child,
            stdin,
            lines,
            port,
        };
        p.expect("ready").await;
        Some(p)
    }

    async fn expect(&mut self, event: &str) -> Value {
        loop {
            let line = tokio::time::timeout(Duration::from_secs(30), self.lines.next_line())
                .await
                .unwrap_or_else(|_| panic!("timed out waiting for the beacon's {event:?}"))
                .unwrap()
                .unwrap_or_else(|| panic!("the beacon exited before {event:?}"));
            let v: Value = serde_json::from_str(&line).unwrap_or(Value::Null);
            assert_ne!(v["event"], "error", "{line}");
            if v["event"] == event {
                return v;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bumble_publishes_beacons_and_answers_gatt() {
    let Some(mut peer) = Peer::start().await else {
        return;
    };
    peer.expect("advertising").await;
    let dir = tempfile::tempdir().unwrap();
    let out = Outbox::open(
        &dir.path().join("outbox"),
        &format!("edge-{EUI}"),
        EUI,
        1 << 20,
    )
    .unwrap();
    let cfg = BleCfg {
        enabled: true,
        transport: format!("tcp:127.0.0.1:{}", peer.port),
        interval_secs: 1,
        admit: vec!["F1:22:33".into()],
        admit_all: false,
        min_rssi: -127,
        active: false,
        parsed: true,
        commands: true,
        // Bumble's simulated link carries LE data for random addresses only.
        random_address: true,
    };
    let router = Arc::new(Router::new(true));
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(ble::run(
        cfg,
        dir.path().join("keys"),
        out.clone(),
        router.clone(),
        stop_rx,
    ));

    // An up for the beacon, with its KSensor frame decoded in link.ble.parsed.
    let key = "ble:f12233fffe445566";
    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let up = loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no up for {key}: {seen:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        let msgs = drain(&out);
        if let Some((_, b)) = msgs
            .iter()
            .find(|(t, _)| t.ends_with(&format!("/d/{key}/up")))
        {
            break b.clone();
        }
        seen.extend(msgs.into_iter().map(|(t, _)| t));
    };
    schema_check("up", &up);
    let raw = base64::engine::general_purpose::STANDARD
        .decode(up["raw"]["b64"].as_str().unwrap())
        .unwrap();
    assert!(
        hex::encode(&raw).contains("aafe21"),
        "{}",
        hex::encode(&raw)
    );
    assert_eq!(up["link"]["ble"]["parsed"]["beacons"][0]["format"], "kkm");
    assert!(up["rx"][0]["rssi"].is_number());

    // A GATT read through the device's key: queued, delivered, ok with the value.
    let cmd = json!({"v": 1, "id": "read-0001", "name": "gatt.read",
        "params": {"characteristic": "2a19"}});
    router.dispatch(
        &out,
        &format!("pulzr/a/edge-{EUI}/d/{key}/cmd"),
        cmd.to_string().as_bytes(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut statuses = Vec::new();
    let ok = loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no answer: {statuses:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        let res: Vec<Value> = drain(&out)
            .into_iter()
            .filter(|(t, _)| t.ends_with(&format!("/d/{key}/cmd/res")))
            .map(|(_, b)| b)
            .collect();
        for r in &res {
            schema_check("cmdResult", r);
            statuses.push(r["status"].as_str().unwrap().to_string());
        }
        if let Some(r) = res
            .iter()
            .find(|r| r["status"] == "ok" || r["status"] == "failed")
        {
            break r.clone();
        }
    };
    assert_eq!(ok["status"], "ok", "{ok}");
    assert_eq!(
        ok["data"]["value"],
        base64::engine::general_purpose::STANDARD.encode([87u8])
    );
    assert_eq!(
        statuses[..2],
        ["queued".to_string(), "delivered".to_string()]
    );

    let _ = stop_tx.send(true);
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let _ = peer.stdin.write_all(b"stop\n").await;
}
