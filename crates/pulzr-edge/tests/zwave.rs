//! The Z-Wave radio module against zwave-bridge's Serial API simulator over
//! TCP: form (refused, then with erase), S2 inclusion with grant and PIN,
//! interview, readings, a switch command, settings, a sleeping node's queued
//! command, SmartStart, topology, backup, removal, leave and restore. Through
//! the module's ops and its outbox, as the gateway's console and MQTT
//! session would see it; and no network key in anything published.
#![cfg(feature = "zwave")]

use pulzr_edge::outbox::Outbox;
use pulzr_edge::radio::{Paths, RadioModule};
use serde_json::{json, Value};
use std::time::Duration;
use zwave_host::SecurityClass;
use zwave_sim::node::NodeSpec;
use zwave_sim::{SimConfig, SimHandle, Simulator};

const RADIO: &str = "usb-0658_0200-sim";
const NETWORK: &str = "7a1e0c2b-0000-4000-8000-000000000002";

struct Rig {
    sim: SimHandle,
    module: std::sync::Arc<dyn RadioModule>,
    out: Outbox,
    paths: Paths,
    seen: Vec<(String, Value)>,
    _dir: tempfile::TempDir,
}

/// A 700-series NVM3 image, so a backup restores onto the simulator.
fn nvm_700(home: u32) -> Vec<u8> {
    use zwave_host::nvm3::*;
    let mut ci = home.to_be_bytes().to_vec();
    ci.push(1);
    ci.resize(13, 0);
    let app = write_volume(
        APPLICATION_SIZE_700,
        2048,
        &[(APPLICATION_VERSION_FILE_700, vec![1, 18, 7, 0])],
    )
    .unwrap();
    let proto = write_volume(
        IMAGE_SIZE_700 - APPLICATION_SIZE_700,
        2048,
        &[
            (PROTOCOL_VERSION_FILE, vec![0, 17, 7, 3]),
            (CONTROLLER_INFO_FILE, ci),
        ],
    )
    .unwrap();
    [app, proto].concat()
}

async fn rig() -> Rig {
    let sim = Simulator::new(SimConfig {
        nvm: Some(nvm_700(0xc0ffee01)),
        ..SimConfig::default()
    })
    .with_joined_node(NodeSpec::switch("old"))
    .with_node(NodeSpec::switch("switch"))
    .with_node(NodeSpec::sleeping_sensor("sleeper"))
    .with_node(NodeSpec::s2_sensor(
        "s2",
        vec![
            SecurityClass::S2Authenticated,
            SecurityClass::S2Unauthenticated,
        ],
    ))
    .with_node(NodeSpec::smartstart_sensor("smartstart"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (htx, hrx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        // The module reconnects after a reset or a restore: same controller.
        let (first, _) = listener.accept().await.unwrap();
        let h = sim.spawn(first);
        let _ = htx.send(h.clone());
        while let Ok((s, _)) = listener.accept().await {
            h.attach(s).await;
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let paths = Paths {
        radios: dir.path().join("radios"),
        run: dir.path().join("run"),
        probe_report: dir.path().join("radios-report.json"),
        backup_hook: dir.path().join("no-hook"),
    };
    std::fs::create_dir_all(&paths.radios).unwrap();
    std::fs::write(
        paths.state_file(RADIO),
        json!({ "radio": RADIO, "protocol": "zwave" }).to_string(),
    )
    .unwrap();
    let out = Outbox::open(
        &dir.path().join("outbox"),
        "gw-sim",
        "b827ebfffe67f36b",
        8 << 20,
    )
    .unwrap();
    let ctx = pulzr_edge::radio::RadioCtx::new(
        RADIO,
        &format!("tcp://{addr}"),
        out.clone(),
        paths.clone(),
    );
    let factory = pulzr_edge::radio::factory("zwave").expect("a zwave module in this build");
    let module = factory(ctx).unwrap();
    let sim = hrx.await.unwrap();
    Rig {
        sim,
        module,
        out,
        paths,
        seen: Vec::new(),
        _dir: dir,
    }
}

impl Rig {
    async fn call(&self, op: &str, params: Value) -> Value {
        match self.module.call(op, params).await {
            Ok(v) => v,
            Err(e) => panic!("{op}: {}", e.message()),
        }
    }

    async fn refused(&self, op: &str, params: Value) -> String {
        match self.module.call(op, params).await {
            Ok(v) => panic!("{op} answered {v}"),
            Err(e) => e.message().to_string(),
        }
    }

    async fn state(&self, want: &str) -> Value {
        for _ in 0..200 {
            let s = self.call("status", json!({})).await;
            if s["state"] == want {
                return s;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("the radio never reached {want}");
    }

    fn drain(&mut self) {
        while let Some(e) = self.out.peek() {
            self.out.ack(&e);
            let v: Value = serde_json::from_slice(&e.payload).unwrap();
            self.seen.push((e.topic.clone(), v));
        }
    }

    /// The first message since `from` (an index into `seen`) on a topic
    /// ending with `suffix` whose body satisfies `pred`.
    async fn wait_from(
        &mut self,
        from: usize,
        suffix: &str,
        mut pred: impl FnMut(&str, &Value) -> bool,
    ) -> (String, Value) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(40);
        loop {
            self.drain();
            if let Some((t, v)) = self.seen[from..]
                .iter()
                .find(|(t, v)| t.ends_with(suffix) && pred(t, v))
            {
                return (t.clone(), v.clone());
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {suffix}; seen {:#?}",
                &self.seen[from..]
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn event(
        &mut self,
        from: usize,
        name: &str,
        mut pred: impl FnMut(&Value) -> bool,
    ) -> Value {
        self.wait_from(from, "/event", |_, v| v["name"] == name && pred(&v["data"]))
            .await
            .1["data"]
            .clone()
    }

    fn mark(&mut self) -> usize {
        self.drain();
        self.seen.len()
    }

    fn command(&self, device: &str, id: &str, name: &str, params: Value) {
        let topic = self.out.topic(device, "cmd");
        let body = json!({ "v": 1, "id": id, "name": name, "params": params });
        self.module
            .dispatch(&self.out, &topic, &serde_json::to_vec(&body).unwrap());
    }

    async fn cmd_res(&mut self, from: usize, id: &str, status: &str) -> Value {
        self.wait_from(from, "/cmd/res", |_, v| {
            v["id"] == id
                && (v["status"] == status || v["status"] == "failed" || v["status"] == "refused")
        })
        .await
        .1
    }

    /// Opens the network, presses `name`'s button and answers S2's grant
    /// and PIN; returns the new device's key once its interview is done.
    async fn include(&mut self, name: &str, pin: Option<u16>) -> String {
        let from = self.mark();
        let r = self.call("permit", json!({ "seconds": 60 })).await;
        assert!(r["until"].as_f64().unwrap() > 0.0);
        self.event(from, "network.permit", |d| d["until"].is_number())
            .await;
        self.sim.press(name).await.expect("node pressed");
        if let Some(pin) = pin {
            let grant = self
                .event(from, "network.inclusion", |d| d["stage"] == "grant")
                .await;
            assert_eq!(grant["request"], r["request"]);
            let dsk = self
                .refused(
                    "dsk",
                    json!({ "request": grant["request"], "pin": "00000" }),
                )
                .await;
            assert!(dsk.contains("not waiting"), "{dsk}");
            self.call(
                "grant",
                json!({ "request": grant["request"], "classes": grant["requested"] }),
            )
            .await;
            let ask = self
                .event(from, "network.inclusion", |d| d["stage"] == "dsk")
                .await;
            assert!(ask["dsk"].as_str().unwrap().starts_with("xxxxx-"), "{ask}");
            self.call(
                "dsk",
                json!({ "request": grant["request"], "pin": format!("{pin:05}") }),
            )
            .await;
            let done = self
                .event(from, "network.inclusion", |d| d["stage"] == "done")
                .await;
            assert_eq!(done["granted"], grant["requested"]);
        }
        let joined = self.event(from, "network.joined", |_| true).await;
        let device = joined["device"].as_str().unwrap().to_string();
        self.event(from, "network.interview", |d| {
            d["device"] == device.as_str() && d["complete"] == true
        })
        .await;
        self.event(from, "network.permit", |d| d["until"].is_null())
            .await;
        device
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zwave_network_end_to_end() {
    let mut rig = rig().await;
    let status = rig.state("unformed").await;
    assert_eq!(status["protocol"], "zwave");
    assert_eq!(status["devices"], 1, "the stick has a node from elsewhere");

    // Keys checked before anything reaches the radio; never echoed.
    let mut keys = json!({});
    for (i, k) in [
        "s0",
        "s2_unauthenticated",
        "s2_authenticated",
        "s2_access_control",
        "lr_s2_authenticated",
        "lr_s2_access_control",
    ]
    .iter()
    .enumerate()
    {
        keys[*k] = json!(format!("{:02x}", 0xa0 + i).repeat(16));
    }
    let secrets: Vec<String> = keys
        .as_object()
        .unwrap()
        .values()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    let mut short = keys.clone();
    short["s2_access_control"] = json!("abcd");
    let e = rig
        .refused("form", json!({ "network": NETWORK, "keys": short }))
        .await;
    assert!(
        e.contains("s2_access_control") && !e.contains("abcd"),
        "{e}"
    );
    let e = rig
        .refused(
            "form",
            json!({ "network": NETWORK, "keys": keys, "region": "US" }),
        )
        .await;
    assert!(e.contains("1 devices") && e.contains("erase"), "{e}");
    assert_eq!(
        rig.refused("scan", json!({})).await,
        "Z-Wave has no channels: the region sets its frequencies."
    );

    let from = rig.mark();
    let up = rig
        .call(
            "form",
            json!({ "network": NETWORK, "keys": keys, "region": "US", "erase": true }),
        )
        .await;
    assert_eq!(up["state"], "up");
    assert_eq!(up["network"], NETWORK);
    assert_eq!(up["region"], "US");
    assert_eq!(up["devices"], 0, "erased");
    let home = up["home_id"].as_str().unwrap().to_string();
    let coordinator = format!("zwave:{home}.1");
    assert_eq!(up["coordinator"], coordinator.as_str());
    let (t, net) = rig
        .wait_from(from, "/state", |_, v| v["network"]["state"] == "up")
        .await;
    assert!(t.ends_with(&format!("/d/{coordinator}/state")), "{t}");
    assert_eq!(net["network"]["home_id"], home.as_str());
    let file: Value =
        serde_json::from_slice(&std::fs::read(rig.paths.state_file(RADIO)).unwrap()).unwrap();
    assert_eq!(file["network"], NETWORK);
    assert_eq!(file["keys"]["s2_authenticated"], keys["s2_authenticated"]);

    // S2 inclusion with the grant and the PIN from the console.
    let pin = NodeSpec::s2_sensor("x", vec![]).dsk().pin();
    let s2 = rig.include("s2", Some(pin)).await;
    assert!(s2.starts_with(&format!("zwave:{home}.")));
    let (_, st) = rig
        .wait_from(0, "/state", |t, v| {
            t.ends_with(&format!("/d/{s2}/state")) && v["interview"]["zwave"]["complete"] == true
        })
        .await;
    let i = &st["interview"]["zwave"];
    assert_eq!(i["security"], "s2_authenticated");
    assert_eq!(i["product_id"], 3);
    assert!(i["ccs"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c["cc"] == 0x31 && c["secure"] == true));
    assert!(st["diag"]["dsk"].as_str().is_some());

    // A reading as the protocol's `up`.
    let from = rig.mark();
    assert!(rig.sim.set_sensor("s2", 1, 23.5).await);
    let (_, upm) = rig
        .wait_from(from, "/up", |t, v| {
            t.ends_with(&format!("/d/{s2}/up")) && v["r"][0]["v"]["air_temperature"] == 23.5
        })
        .await;
    let node: u64 = s2.rsplit('.').next().unwrap().parse().unwrap();
    assert_eq!(upm["link"]["zwave"]["node"], node);
    assert_eq!(upm["link"]["zwave"]["cc"], 0x31);
    assert_eq!(upm["rx"][0]["via"], "gw:b827ebfffe67f36b");

    // A switch: a value set and a configuration parameter, by command.
    let switch = rig.include("switch", None).await;
    rig.wait_from(0, "/up", |t, v| {
        t.ends_with(&format!("/d/{switch}/up")) && v["r"][0]["v"].get("current").is_some()
    })
    .await;
    let from = rig.mark();
    rig.command(
        &switch,
        "cmd-switch-0001",
        "zwave.set",
        json!({ "field": "current", "value": true }),
    );
    let r = rig.cmd_res(from, "cmd-switch-0001", "ok").await;
    assert_eq!(r["status"], "ok", "{r}");
    rig.command(
        &switch,
        "cmd-switch-0002",
        "settings.set",
        json!({ "3": 20 }),
    );
    let r = rig.cmd_res(from, "cmd-switch-0002", "ok").await;
    assert_eq!(r["status"], "ok", "{r}");
    let (_, s) = rig
        .wait_from(from, "/settings", |t, _| {
            t.ends_with(&format!("/d/{switch}/settings"))
        })
        .await;
    assert_eq!(s["values"]["3"].as_f64(), Some(20.0));
    rig.command(
        &switch,
        "cmd-switch-0003",
        "zwave.set",
        json!({ "field": "nope", "value": 1 }),
    );
    let r = rig.cmd_res(from, "cmd-switch-0003", "refused").await;
    assert_eq!(r["reason"], "invalid");
    rig.command(
        &coordinator,
        "cmd-coord-0001",
        "network.permit_join",
        json!({ "seconds": 60 }),
    );
    let r = rig.cmd_res(from, "cmd-coord-0001", "refused").await;
    assert_eq!(r["status"], "refused", "network ops never over MQTT");

    // A sleeping node: its command waits for the wake-up.
    let sleeper = rig.include("sleeper", None).await;
    tokio::time::sleep(Duration::from_secs(3)).await; // back asleep
    let from = rig.mark();
    rig.command(&sleeper, "cmd-sleep-0001", "zwave.ping", json!({}));
    let r = rig.cmd_res(from, "cmd-sleep-0001", "queued").await;
    assert_eq!(r["status"], "queued", "{r}");
    assert!(rig.sim.wake_up("sleeper").await);
    let r = rig
        .wait_from(from, "/cmd/res", |_, v| {
            v["id"] == "cmd-sleep-0001" && v["status"] != "queued"
        })
        .await
        .1;
    assert_eq!(r["status"], "ok", "{r}");

    // SmartStart: provisioned from the console, joins when powered on.
    let dsk = NodeSpec::smartstart_sensor("x").dsk().to_string();
    let r = rig
        .call(
            "provision",
            json!({ "entries": [ { "dsk": dsk, "classes": ["s2_authenticated", "s2_unauthenticated"], "protocol": "zwave", "name": "hall" } ] }),
        )
        .await;
    assert_eq!(r["count"], 1);
    let from = rig.mark();
    assert!(rig.sim.power_on("smartstart").await);
    let joined = rig.event(from, "network.joined", |_| true).await;
    let ss = joined["device"].as_str().unwrap().to_string();
    rig.event(from, "network.interview", |d| d["device"] == ss.as_str())
        .await;

    // Topology on the coordinator's key, from the routes in use.
    let (_, topo) = rig
        .wait_from(0, "/topology", |t, v| {
            t.ends_with(&format!("/d/{coordinator}/topology"))
                && v["edges"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|e| e["kind"] == "route" && e["from"] == switch.as_str())
        })
        .await;
    assert_eq!(topo["protocol"], "zwave");

    // A backup: the NVM, the keys, the node store; and the scheduled one.
    let b = rig.call("backup", json!({})).await["backup"].clone();
    assert_eq!(b["protocol"], "zwave");
    assert_eq!(b["network"], NETWORK);
    assert_eq!(b["zwave"]["home_id"], home.as_str());
    assert!(b["zwave"]["nvm"].as_str().unwrap().len() > 100);
    assert_eq!(b["zwave"]["keys"]["s0"], keys["s0"]);
    rig.event(0, "network.backup", |d| d["bytes"].as_u64() > Some(100))
        .await;
    assert!(
        rig.paths.backup_file(RADIO).exists(),
        "handed to the hook's file"
    );

    // A dead node removed by force.
    assert!(rig.sim.power_off("switch").await);
    let from = rig.mark();
    rig.call("remove", json!({ "device": switch, "force": true }))
        .await;
    let left = rig.event(from, "network.left", |_| true).await;
    assert_eq!(left["device"], switch.as_str());
    assert_eq!(left["reason"], "removed");

    // Leave, then restore the backup onto the same stick. (The simulator
    // keeps its NVM as bytes, so an erase would not come back with it.)
    let gone = rig.call("leave", json!({})).await;
    assert_eq!(gone["state"], "unformed");
    let file: Value =
        serde_json::from_slice(&std::fs::read(rig.paths.state_file(RADIO)).unwrap()).unwrap();
    assert!(file.get("keys").is_none() && file.get("network").is_none());
    let e = rig
        .refused(
            "restore",
            json!({ "network": "another-network", "backup": b }),
        )
        .await;
    assert!(e.contains("another network"), "{e}");
    let back = rig
        .call("restore", json!({ "network": NETWORK, "backup": b }))
        .await;
    assert_eq!(back["state"], "up");
    assert_eq!(back["home_id"], home.as_str());
    assert_eq!(back["network"], NETWORK);

    // No network key in anything the module published.
    rig.drain();
    for (t, v) in &rig.seen {
        let text = v.to_string();
        for k in &secrets {
            assert!(!text.contains(k.as_str()), "a key in {t}");
        }
    }
}
