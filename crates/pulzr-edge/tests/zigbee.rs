//! The Zigbee radio module against zigbee-bridge's NCP simulator, for every
//! coordinator family: form, join, interview, report, command, backup,
//! channel, leave. Through the module's ops and its outbox, as the gateway's
//! console and MQTT session would see it.
#![cfg(feature = "zigbee")]

use std::time::Duration;

use base64::Engine;
use pulzr_edge::outbox::Outbox;
use pulzr_edge::radio::{Paths, RadioCtx, RadioModule};
use serde_json::{json, Value};
use zigbee_primitives::Ieee;
use zigbee_sim::{SimConfig, SimDevice, SimNcp, Simulator};
use zigbee_zcl::{attr, cluster_id, Value as Zv};

const TH: Ieee = Ieee(0x00124B0000000001);
const PLUG: Ieee = Ieee(0x00124B0000000003);
const RADIO: &str = "usb-10c4_ea60-sim";
const NETWORK: &str = "5d4c0f2a-0000-4000-8000-000000000001";

struct Rig {
    sim: Simulator,
    module: std::sync::Arc<dyn RadioModule>,
    out: Outbox,
    seen: Vec<(String, Value)>,
    _dir: tempfile::TempDir,
}

async fn rig(ncp: SimNcp) -> Rig {
    let sim = Simulator::new(SimConfig {
        ncp,
        devices: vec![
            SimDevice::temperature_humidity_sensor(TH),
            SimDevice::plug(PLUG),
        ],
        ..SimConfig::default()
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let s = sim.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let _ = s.serve(stream).await;
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let paths = Paths {
        radios: dir.path().join("radios"),
        run: dir.path().join("run"),
        probe_report: dir.path().join("radios-report.json"),
        backup_hook: dir.path().join("no-hook"),
    };
    let port = format!("tcp://{addr}");
    std::fs::create_dir_all(&paths.radios).unwrap();
    std::fs::write(
        paths.state_file(RADIO),
        json!({ "radio": RADIO, "protocol": "zigbee" }).to_string(),
    )
    .unwrap();
    let out = Outbox::open(
        &dir.path().join("outbox"),
        "gw-sim",
        "b827ebfffe67f36b",
        4 << 20,
    )
    .unwrap();
    let ctx = RadioCtx::new(RADIO, &port, out.clone(), paths).with_firmware(
        pulzr_edge::config::FirmwareCfg {
            hosts: vec!["assets.example".into()],
        },
    );
    let factory = pulzr_edge::radio::factory("zigbee").expect("a zigbee module in this build");
    let module = factory(ctx).unwrap();
    Rig {
        sim,
        module,
        out,
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

    /// Waits for a queued message whose topic ends with `suffix` and whose
    /// body satisfies `pred`.
    async fn wait(&mut self, suffix: &str, mut pred: impl FnMut(&Value) -> bool) -> Value {
        if let Some((_, v)) = self
            .seen
            .iter()
            .find(|(t, v)| t.ends_with(suffix) && pred(v))
        {
            return v.clone();
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            while let Some(e) = self.out.peek() {
                self.out.ack(&e);
                let v: Value = serde_json::from_slice(&e.payload).unwrap();
                self.seen.push((e.topic.clone(), v.clone()));
                if e.topic.ends_with(suffix) && pred(&v) {
                    return v;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {suffix}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn command(&self, device: Ieee, id: &str, name: &str, params: Value) {
        let topic = self.out.topic(&format!("zigbee:{device}"), "cmd");
        let body = json!({ "v": 1, "id": id, "name": name, "params": params });
        self.module
            .dispatch(&self.out, &topic, &serde_json::to_vec(&body).unwrap());
    }
}

fn b64(b: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}

async fn end_to_end(ncp: SimNcp, family: &str) {
    let mut rig = rig(ncp).await;
    let status = rig.call("status", json!({})).await;
    assert_eq!(status["state"], "unformed");

    let scan = rig.call("scan", json!({ "seconds": 3 })).await;
    assert_eq!(scan["channels"].as_array().unwrap().len(), 16);

    // A bad request never reaches the radio.
    let err = rig
        .module
        .call("form", json!({ "network": NETWORK, "channel": 30 }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), "bad-request");

    let status = rig
        .call(
            "form",
            json!({ "network": NETWORK, "channel": 15, "pan_id": "1a62",
                    "ext_pan_id": "dddddddddddddddd", "network_key": "01030507090b0d0f00020406080a0c0d" }),
        )
        .await;
    assert_eq!(status["state"], "up");
    assert_eq!(status["ncp"], family);
    assert_eq!(status["channel"], 15);
    let coordinator = status["coordinator"].as_str().unwrap().to_string();
    // The key went to the radio's file, never to a message.
    rig.wait("/state", |v| v["network"]["state"] == "up").await;

    let permit = rig.call("permit", json!({ "seconds": 60 })).await;
    assert!(permit["until"].as_f64().unwrap() > 0.0);
    rig.wait("/event", |v| v["name"] == "network.permit").await;
    let joined = rig
        .wait("/event", |v| {
            v["name"] == "network.joined" && v["data"]["device"] == format!("zigbee:{TH}")
        })
        .await;
    assert_eq!(joined["data"]["via"], Value::Null);

    // The interview, in the device's state, for the device library.
    let state = rig
        .wait(&format!("zigbee:{TH}/state"), |v| {
            v["interview"]["zigbee"]["complete"] == true
        })
        .await;
    let interview = &state["interview"]["zigbee"];
    assert_eq!(interview["model_id"], "SIM-TH1");
    assert_eq!(interview["type"], "end_device");
    assert!(interview["endpoints"][0]["in"]
        .as_array()
        .unwrap()
        .contains(&json!(0x0402)));
    rig.wait(&format!("zigbee:{PLUG}/state"), |v| {
        v["interview"]["zigbee"]["complete"] == true
    })
    .await;

    // A report: the raw ZCL frame, its link, and the standard reading.
    rig.sim.set_attribute(
        TH,
        1,
        cluster_id::TEMPERATURE_MEASUREMENT,
        attr::temperature_measurement::MEASURED_VALUE,
        Zv::i16(2150),
    );
    let up = rig
        .wait(&format!("zigbee:{TH}/up"), |v| {
            v["link"]["zigbee"]["cluster"] == 0x0402 && v.get("r").is_some()
        })
        .await;
    assert_eq!(up["r"][0]["v"]["temperature"], 21.5);
    assert_eq!(up["link"]["zigbee"]["command"], 0x0A);
    assert!(up["raw"]["b64"].as_str().is_some());
    assert_eq!(up["rx"][0]["via"], "gw:b827ebfffe67f36b");

    // Commands, in order per device: an On command (delivered), a wait, and
    // a read of on/off with its reply.
    rig.command(PLUG, "c0000001-on", "zigbee.zcl", json!({ "endpoint": 1, "cluster": 6, "frame": b64(&[0x01, 0x00, 0x01]), "response": false }));
    rig.command(PLUG, "c0000002-wait", "zigbee.wait", json!({ "ms": 50 }));
    rig.command(PLUG, "c0000003-read", "zigbee.zcl", json!({ "endpoint": 1, "cluster": 6, "frame": b64(&[0x00, 0x00, 0x00, 0x00, 0x00]), "response": true }));
    let delivered = rig.wait("/cmd/res", |v| v["id"] == "c0000001-on").await;
    assert_eq!(delivered["status"], "delivered", "{delivered}");
    rig.wait("/cmd/res", |v| {
        v["id"] == "c0000002-wait" && v["status"] == "ok"
    })
    .await;
    let read = rig.wait("/cmd/res", |v| v["id"] == "c0000003-read").await;
    assert_eq!(read["status"], "ok", "{read}");
    let reply = base64::engine::general_purpose::STANDARD
        .decode(read["data"]["frame"].as_str().unwrap())
        .unwrap();
    // Read attributes response: attribute 0, success, bool, on.
    assert_eq!(
        reply.get(3..),
        Some(&[0x00, 0x00, 0x00, 0x10, 0x01][..]),
        "{reply:?}"
    );
    // ZDO: a node descriptor request with its reply.
    rig.command(
        PLUG,
        "c0000004-zdo",
        "zigbee.zdo",
        json!({ "cluster": 2, "payload": b64(&[0, 0x2B, 0x1A]), "response": true }),
    );
    rig.command(PLUG, "c0000005-bad", "zigbee.zcl", json!({ "endpoint": 1 }));
    rig.wait("/cmd/res", |v| {
        v["id"] == "c0000004-zdo" && v["status"] == "ok"
    })
    .await;
    let bad = rig.wait("/cmd/res", |v| v["id"] == "c0000005-bad").await;
    assert_eq!(
        (bad["status"].as_str(), bad["reason"].as_str()),
        (Some("refused"), Some("invalid"))
    );
    // The coordinator takes no network command over MQTT.
    let ieee: Ieee = coordinator.trim_start_matches("zigbee:").parse().unwrap();
    rig.command(
        ieee,
        "c0000006-net",
        "network.permit_join",
        json!({ "seconds": 60 }),
    );
    let net = rig.wait("/cmd/res", |v| v["id"] == "c0000006-net").await;
    assert_eq!(net["status"], "refused");

    // A map of the mesh: refused while joining is open, then a walk of
    // every router's tables, published as one complete topology.
    let busy = rig.module.call("map", json!({})).await.unwrap_err();
    assert_eq!(busy.code(), "busy");
    rig.call("permit-stop", json!({})).await;
    let map = rig.call("map", json!({})).await;
    assert_eq!(map["started"], true);
    assert!(map["requests"].as_u64().unwrap() >= 2);
    let topology = rig
        .wait(&format!("{coordinator}/topology"), |v| {
            v["source"] == "scan"
        })
        .await;
    assert_eq!(topology["complete"], true);
    let edges = topology["edges"].as_array().unwrap();
    assert!(
        edges.iter().any(|e| e["from"] == format!("zigbee:{PLUG}")
            && e["to"] == coordinator
            && e["kind"] == "route"),
        "{topology}"
    );
    assert!(
        edges
            .iter()
            .any(|e| e["kind"] == "neighbor" && e["quality"]["lqi"].is_u64()),
        "{topology}"
    );

    // A backup: the open coordinator backup in the radio's envelope.
    let backup = rig.call("backup", json!({})).await;
    assert_eq!(backup["backup"]["network"], NETWORK);
    assert_eq!(
        backup["backup"]["zigbee"]["metadata"]["format"],
        "zigpy/open-coordinator-backup"
    );

    // Another channel.
    let moved = rig.call("channel", json!({ "channel": 20 })).await;
    assert_eq!(moved["channel"], 20);
    rig.wait("/event", |v| v["name"] == "network.channel").await;
    assert_eq!(rig.sim.snapshot().network.unwrap().channel, 20);

    // Topology: the gateway's bus link, and the devices' last hops.
    rig.call("permit-stop", json!({})).await;
    let remove = rig
        .call(
            "remove",
            json!({ "device": format!("zigbee:{TH}"), "force": true }),
        )
        .await;
    assert_eq!(remove, json!({}));
    rig.wait("/event", |v| v["name"] == "network.left").await;

    // Leave with erase: the network is gone from the radio and the file.
    let status = rig.call("leave", json!({ "erase": true })).await;
    assert_eq!(status["state"], "unformed");
    assert!(rig.sim.snapshot().network.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zigbee_module_on_emberznet() {
    end_to_end(SimNcp::Ezsp { protocol: 13 }, "ezsp").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zigbee_module_on_zstack() {
    end_to_end(SimNcp::Znp { product: 1 }, "znp").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zigbee_module_on_deconz() {
    end_to_end(SimNcp::Deconz, "deconz").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backup_restores_onto_another_dongle() {
    let mut old = rig(SimNcp::Ezsp { protocol: 13 }).await;
    old.call(
        "form",
        json!({ "network": NETWORK, "channel": 25, "pan_id": "4242",
                "ext_pan_id": "0102030405060708", "network_key": "11111111111111111111111111111111" }),
    )
    .await;
    old.wait("/state", |v| v["network"]["state"] == "up").await;
    let backup = old.call("backup", json!({})).await["backup"].clone();
    let counter = backup["zigbee"]["network_key"]["frame_counter"]
        .as_u64()
        .unwrap();

    let new = rig(SimNcp::Znp { product: 1 }).await;
    let refused = new
        .module
        .call("restore", json!({ "network": "another", "backup": backup }))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), "refused");
    let status = new
        .call("restore", json!({ "network": NETWORK, "backup": backup }))
        .await;
    assert_eq!(status["state"], "up");
    assert_eq!(status["pan_id"], "4242");
    let net = new.sim.snapshot().network.unwrap();
    assert!(u64::from(net.frame_counter) >= counter + 1_000_000);
    assert_eq!(
        status["coordinator"],
        backup["zigbee"]["coordinator_ieee"]
            .as_str()
            .map(|i| format!("zigbee:{i}"))
            .unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_device_types_firmware_image_reaches_the_device() {
    use sha2::{Digest, Sha256};
    // file:// stands in for the device type's https URL.
    std::env::set_var("PULZR_EDGE_OTA_FILE_URLS", "1");
    let mut rig = rig(SimNcp::Ezsp { protocol: 13 }).await;
    rig.call(
        "form",
        json!({ "network": NETWORK, "channel": 15, "pan_id": "1a62",
                "ext_pan_id": "dddddddddddddddd", "network_key": "01030507090b0d0f00020406080a0c0d" }),
    )
    .await;
    rig.call("permit", json!({ "seconds": 60 })).await;
    rig.wait(&format!("zigbee:{PLUG}/state"), |v| {
        v["interview"]["zigbee"]["complete"] == true
    })
    .await;

    let image = zigbee_host::ota::build(0x1234, 0x0001, 0x0200_0000, &[0x5A; 900]);
    let file = rig._dir.path().join("plug-2.0.ota");
    std::fs::write(&file, &image).unwrap();
    let url = format!("file://{}", file.display());
    let sha = hex::encode(Sha256::digest(&image));

    // Only the gateway's firmware hosts: never one the command names alone.
    rig.command(
        PLUG,
        "o0000000-host",
        "zigbee.ota",
        json!({ "url": "https://elsewhere.example/plug.ota", "sha256": sha }),
    );
    let host = rig.wait("/cmd/res", |v| v["id"] == "o0000000-host").await;
    assert_eq!(
        (host["status"].as_str(), host["reason"].as_str()),
        (Some("refused"), Some("invalid"))
    );
    // An image for another maker is refused.
    let other = zigbee_host::ota::build(0x9999, 0x0001, 0x0200_0000, &[1; 64]);
    let other_file = rig._dir.path().join("other.ota");
    std::fs::write(&other_file, &other).unwrap();
    rig.command(PLUG, "o0000009-maker", "zigbee.ota", json!({ "url": format!("file://{}", other_file.display()), "sha256": hex::encode(Sha256::digest(&other)) }));
    let maker = rig.wait("/cmd/res", |v| v["id"] == "o0000009-maker").await;
    assert_eq!(
        (maker["status"].as_str(), maker["reason"].as_str()),
        (Some("refused"), Some("invalid")),
        "{maker}"
    );

    // A hash that does not match is never offered.
    rig.command(
        PLUG,
        "o0000001-bad",
        "zigbee.ota",
        json!({ "url": url, "sha256": "0".repeat(64) }),
    );
    let bad = rig.wait("/cmd/res", |v| v["id"] == "o0000001-bad").await;
    assert_eq!(bad["status"], "failed", "{bad}");
    // Plain http is refused outright.
    rig.command(
        PLUG,
        "o0000002-http",
        "zigbee.ota",
        json!({ "url": "http://example.com/x.ota", "sha256": sha }),
    );
    let http = rig.wait("/cmd/res", |v| v["id"] == "o0000002-http").await;
    assert_eq!(http["status"], "refused");

    rig.command(
        PLUG,
        "o0000003-good",
        "zigbee.ota",
        json!({ "url": url, "sha256": sha, "file_version": 0x0200_0000 }),
    );
    rig.wait("/cmd/res", |v| {
        v["id"] == "o0000003-good" && v["status"] == "delivered"
    })
    .await;
    rig.wait(&format!("zigbee:{PLUG}/event"), |v| {
        v["name"] == "ota.progress" && v["data"]["percent"] == 100
    })
    .await;
    let done = rig
        .wait("/cmd/res", |v| {
            v["id"] == "o0000003-good" && v["status"] != "delivered"
        })
        .await;
    assert_eq!(done["status"], "ok", "{done}");
    assert_eq!(done["data"]["from"], 0x0100_0000);
    assert_eq!(done["data"]["to"], 0x0200_0000);
    let mut firmware = rig.sim.snapshot().firmware[0].1.clone();
    for _ in 0..50 {
        if firmware.file_version == 0x0200_0000 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        firmware = rig.sim.snapshot().firmware[0].1.clone();
    }
    assert_eq!(
        firmware.image, image,
        "the device received the image byte for byte"
    );
}
