//! The radio module seam: a fake module behind a real control socket, as the
//! console API reaches it, and what a module's context writes.

use pulzr_edge::outbox::Outbox;
use pulzr_edge::radio::{control, BoxFuture, Paths, RadioCtx, RadioError, RadioModule};
use serde_json::{json, Value};
use std::os::unix::fs::PermissionsExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Answers `status`, echoes `form`'s network (never its key), refuses
/// `channel`, and holds `slow` for a while.
struct Fake {
    seen: Mutex<Vec<(String, Value)>>,
}

impl RadioModule for Fake {
    fn protocol(&self) -> &'static str {
        "zigbee"
    }
    fn call<'a>(&'a self, op: &'a str, params: Value) -> BoxFuture<'a, Result<Value, RadioError>> {
        Box::pin(async move {
            self.seen
                .lock()
                .unwrap()
                .push((op.to_string(), params.clone()));
            match op {
                "status" => Ok(json!({ "state": "unformed" })),
                "form" => Ok(json!({ "state": "up", "network": params["network"] })),
                "channel" => Err(RadioError::Refused("No network on this radio yet.".into())),
                "slow" => {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    Ok(json!({}))
                }
                _ => Err(RadioError::BadRequest(format!("No such op: {op}."))),
            }
        })
    }
}

struct Bench {
    _dir: tempfile::TempDir,
    paths: Paths,
    fake: Arc<Fake>,
    stop: tokio::sync::watch::Sender<bool>,
}

const RADIO: &str = "usb-10c4_ea60-9e2adbd1";

async fn bench() -> Bench {
    let dir = tempfile::tempdir().unwrap();
    let paths = Paths {
        radios: dir.path().join("radios"),
        run: dir.path().join("run"),
        probe_report: dir.path().join("probe.json"),
        backup_hook: dir.path().join("hook"),
    };
    let fake = Arc::new(Fake {
        seen: Mutex::new(Vec::new()),
    });
    let (stop, rx) = tokio::sync::watch::channel(false);
    tokio::spawn(control::listen(
        paths.socket(RADIO),
        RADIO.into(),
        fake.clone(),
        rx,
    ));
    for _ in 0..50 {
        if paths.socket(RADIO).exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Bench {
        _dir: dir,
        paths,
        fake,
        stop,
    }
}

#[tokio::test]
async fn an_op_goes_through_and_its_answer_comes_back() {
    let b = bench().await;
    let socket = b.paths.socket(RADIO);
    let a = control::ask(&socket, RADIO, "status", json!({})).await;
    assert_eq!(a, json!({ "ok": true, "result": { "state": "unformed" } }));
    let a = control::ask(
        &socket,
        RADIO,
        "form",
        json!({ "network": "n1", "network_key": "00".repeat(16) }),
    )
    .await;
    assert_eq!(a["result"], json!({ "state": "up", "network": "n1" }));
    // The module had the key; the answer did not carry it.
    assert_eq!(
        b.fake.seen.lock().unwrap()[1].1["network_key"],
        "00".repeat(16)
    );
    assert!(!a.to_string().contains(&"00".repeat(16)));
    let _ = b.stop.send(true);
}

#[tokio::test]
async fn the_socket_and_its_directory_are_roots_alone() {
    let b = bench().await;
    let socket = b.paths.socket(RADIO);
    let mode = |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&socket), 0o600);
    assert_eq!(mode(socket.parent().unwrap()), 0o700);
    let _ = b.stop.send(true);
}

#[tokio::test]
async fn refusals_carry_a_code_and_a_reason() {
    let b = bench().await;
    let socket = b.paths.socket(RADIO);
    let a = control::ask(&socket, RADIO, "channel", json!({ "channel": 20 })).await;
    assert_eq!(
        a,
        json!({ "ok": false, "code": "refused", "error": "No network on this radio yet." })
    );
    // use and release are the gateway's; the module never sees them.
    let a = control::ask(&socket, RADIO, "use", json!({})).await;
    assert_eq!(a["code"], "bad-request");
    let a = control::ask(&socket, RADIO, "Form; rm -rf", json!({})).await;
    assert_eq!(a["code"], "bad-request");
    let a = control::ask(&socket, "usb-other", "status", json!({})).await;
    assert_eq!(a["code"], "bad-request");
    assert!(b
        .fake
        .seen
        .lock()
        .unwrap()
        .iter()
        .all(|(op, _)| op == "channel"));
    let _ = b.stop.send(true);
}

#[tokio::test]
async fn one_change_at_a_time() {
    let b = bench().await;
    let socket = b.paths.socket(RADIO);
    let first = tokio::spawn({
        let socket = socket.clone();
        async move { control::ask(&socket, RADIO, "slow", json!({})).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let second = control::ask(&socket, RADIO, "status", json!({})).await;
    assert_eq!(second["code"], "busy");
    assert_eq!(first.await.unwrap()["ok"], true);
    let _ = b.stop.send(true);
}

#[tokio::test]
async fn nothing_listening_is_no_module() {
    let dir = tempfile::tempdir().unwrap();
    let a = control::ask(&dir.path().join("none.sock"), RADIO, "status", json!({})).await;
    assert_eq!(a["code"], "no-module");
}

#[test]
fn a_backup_is_written_roots_alone_and_handed_to_the_hook() {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("hook-ran");
    let hook = dir.path().join("hook");
    std::fs::write(
        &hook,
        format!("#!/bin/sh\necho \"$@\" > {}\n", marker.display()),
    )
    .unwrap();
    std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755)).unwrap();
    let paths = Paths {
        radios: dir.path().join("radios"),
        run: dir.path().join("run"),
        probe_report: dir.path().join("probe.json"),
        backup_hook: hook,
    };
    let out = Outbox::open(
        &dir.path().join("outbox"),
        "edge-test",
        "aabbccddeeff0011",
        1 << 20,
    )
    .unwrap();
    let ctx = RadioCtx::new(RADIO, "/dev/ttyUSB0", out.clone(), paths.clone());
    ctx.save(
        &json!({ "radio": RADIO, "protocol": "zigbee", "coordinator": "zigbee:00124b0012345678" }),
    )
    .unwrap();
    assert_eq!(
        std::fs::metadata(paths.state_file(RADIO))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    let backup = json!({ "v": 1, "protocol": "zigbee", "network": "n1", "zigbee": { "network_key": "secret" } });
    ctx.backup(&backup).unwrap();
    let written = paths.backup_file(RADIO);
    assert_eq!(
        std::fs::metadata(&written).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&std::fs::read(&written).unwrap()).unwrap(),
        backup
    );
    for _ in 0..100 {
        if marker.exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        std::fs::read_to_string(&marker).unwrap().trim(),
        format!("backup {RADIO}")
    );

    // The platform hears that a backup was taken, on the coordinator's key,
    // and nothing of what is in it.
    let e = out.peek().expect("an event queued");
    assert!(
        e.topic.ends_with("/d/zigbee:00124b0012345678/event"),
        "{}",
        e.topic
    );
    let body: Value = serde_json::from_slice(&e.payload).unwrap();
    assert_eq!(body["name"], "network.backup");
    assert!(!String::from_utf8_lossy(&e.payload).contains("secret"));
}

#[test]
fn radio_ids_are_pulzr_probes() {
    use pulzr_edge::radio::valid_radio_id;
    assert!(valid_radio_id("usb-10c4_ea60-9e2adbd1"));
    assert!(valid_radio_id("lan-slzb-06.local"));
    assert!(!valid_radio_id("usb-"));
    assert!(!valid_radio_id("usb-../../etc/shadow"));
    assert!(!valid_radio_id("ttyUSB0"));
}

#[test]
fn the_port_comes_from_pulzr_probes_report() {
    use pulzr_edge::radio::port_of;
    let dir = tempfile::tempdir().unwrap();
    let report = dir.path().join("radios.json");
    std::fs::write(
        &report,
        json!({ "detected": [ { "id": RADIO, "bus": "usb", "state": "in-use", "port": "/dev/ttyUSB1" } ] }).to_string(),
    )
    .unwrap();
    assert_eq!(port_of(&report, RADIO).unwrap(), "/dev/ttyUSB1");
    assert!(port_of(&report, "usb-gone").is_err());
}
