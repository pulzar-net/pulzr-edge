//! The Modbus module against an in-process Modbus TCP server, with the real outbox
//! and router. No broker and no hardware.

use pulzr_edge::{
    config::BusKind,
    modbus::{bus, regmap::RegisterMap},
    outbox::Outbox,
};
use serde_json::{json, Value};
use std::{
    future::{ready, Ready},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
};
use tokio_modbus::{
    server::{
        tcp::{accept_tcp_connection, Server},
        Service,
    },
    ExceptionCode, Request, Response, SlaveRequest,
};

const GW: &str = "b827ebfffe67f36b";
const MAP: &str = r#"
schema = 0
model = "test-meter"
poll_interval = 0.1
[[fields]]
id = "power"
function = 4
address = 0
type = "f32"
deadband = 5
[[fields]]
id = "voltage"
function = 4
address = 2
type = "u16"
scale = 0.1
[[fields]]
id = "sp"
function = 3
address = 10
type = "u16"
writable = true
min = 0
max = 100
[[fields]]
id = "relay"
function = 1
address = 0
type = "bool"
writable = true
"#;

#[derive(Default)]
struct Mem {
    input: Vec<u16>,
    hold: Vec<u16>,
    coils: Vec<bool>,
}

#[derive(Clone)]
struct Dev(Arc<Mutex<Mem>>);

impl Service for Dev {
    type Request = SlaveRequest<'static>;
    type Response = Option<Response>;
    type Exception = ExceptionCode;
    type Future = Ready<Result<Option<Response>, ExceptionCode>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        if req.slave != 17 {
            return ready(Ok(None)); // nobody home: no answer at all
        }
        let mut m = self.0.lock().unwrap();
        let bad = ExceptionCode::IllegalDataAddress;
        let r = match req.request {
            Request::ReadInputRegisters(a, n) => m
                .input
                .get(a as usize..(a + n) as usize)
                .map(|s| Response::ReadInputRegisters(s.to_vec()))
                .ok_or(bad),
            Request::ReadHoldingRegisters(a, n) => m
                .hold
                .get(a as usize..(a + n) as usize)
                .map(|s| Response::ReadHoldingRegisters(s.to_vec()))
                .ok_or(bad),
            Request::ReadCoils(a, n) => m
                .coils
                .get(a as usize..(a + n) as usize)
                .map(|s| Response::ReadCoils(s.to_vec()))
                .ok_or(bad),
            Request::WriteSingleRegister(a, v) => match m.hold.get_mut(a as usize) {
                Some(r) => {
                    *r = v;
                    Ok(Response::WriteSingleRegister(a, v))
                }
                None => Err(bad),
            },
            Request::WriteSingleCoil(a, v) => match m.coils.get_mut(a as usize) {
                Some(c) => {
                    *c = v;
                    Ok(Response::WriteSingleCoil(a, v))
                }
                None => Err(bad),
            },
            _ => Err(ExceptionCode::IllegalFunction),
        };
        ready(r.map(Some))
    }
}

async fn serve(mem: Arc<Mutex<Mem>>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let server = Server::new(listener);
        let on = move |stream, a| {
            let dev = Dev(mem.clone());
            async move { accept_tcp_connection(stream, a, |_| Ok(Some(dev.clone()))) }
        };
        let _ = server.serve(&on, |e| eprintln!("server: {e}")).await;
    });
    addr
}

fn key(unit: u8) -> String {
    format!("modbus:{GW}.lan-0.{unit}")
}

struct Rig {
    out: Outbox,
    router: bus::Router,
    stop: watch::Sender<bool>,
    seen: Vec<(String, Value)>,
}

impl Rig {
    /// Collect messages until `pred` holds (or fail after 10 s).
    async fn until(&mut self, what: &str, pred: impl Fn(&[(String, Value)]) -> bool) {
        for _ in 0..200 {
            while let Some(e) = self.out.peek() {
                self.seen
                    .push((e.topic.clone(), serde_json::from_slice(&e.payload).unwrap()));
                self.out.ack(&e);
            }
            if pred(&self.seen) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("timed out waiting for {what}; saw:\n{:#?}", self.seen);
    }

    fn of(&self, key: &str, kind: &str) -> Vec<&Value> {
        let t = format!("pulzr/a/edge-{GW}/d/{key}/{kind}");
        self.seen
            .iter()
            .filter(|(topic, _)| *topic == t)
            .map(|(_, v)| v)
            .collect()
    }

    fn cmd(&self, key: &str, id: &str, name: &str, params: Value) {
        let body = json!({"v": 1, "id": id, "name": name, "params": params});
        self.router.dispatch(
            &self.out,
            &self.out.topic(key, "cmd"),
            body.to_string().as_bytes(),
        );
    }

    fn statuses(&self, key: &str, id: &str) -> Vec<String> {
        self.of(key, "cmd/res")
            .iter()
            .filter(|v| v["id"] == id)
            .map(|v| v["status"].as_str().unwrap().to_string())
            .collect()
    }
}

async fn rig(dir: &std::path::Path) -> (Rig, Arc<Mutex<Mem>>) {
    let mem = Arc::new(Mutex::new(Mem {
        input: vec![0x41A8, 0x0000, 2300], // power 21.0 W, voltage 230.0
        hold: vec![0; 20],
        coils: vec![false; 4],
    }));
    let addr = serve(mem.clone()).await;
    let out = Outbox::open(dir, &format!("edge-{GW}"), GW, 1 << 20).unwrap();
    let map = Arc::new(RegisterMap::from_toml(MAP).unwrap());
    let (tx, rx) = mpsc::channel(8);
    let mut router = bus::Router::default();
    let mut units = vec![];
    for (unit, offline_after) in [(17u8, 3u32), (18, 2)] {
        router.add(&key(unit), map.clone(), tx.clone());
        units.push(bus::UnitSpec {
            key: key(unit),
            unit,
            map: map.clone(),
            offline_after,
        });
    }
    let (stop, stop_rx) = watch::channel(false);
    let settings = bus::BusSettings {
        id: "lan-0".into(),
        kind: BusKind::Tcp {
            address: addr.to_string(),
        },
        timeout: Duration::from_millis(150),
        retries: 0,
        backoff: Duration::from_millis(10),
        turnaround: Duration::ZERO,
    };
    tokio::spawn(bus::run(settings, units, out.clone(), rx, stop_rx));
    (
        Rig {
            out,
            router,
            stop,
            seen: vec![],
        },
        mem,
    )
}

#[tokio::test]
async fn polls_reports_on_change_writes_and_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let (mut r, mem) = rig(dir.path()).await;
    let k = key(17);

    // 1. first poll: online, then every field once, seq 0
    r.until("first up", |s| {
        !s.is_empty() && s.iter().any(|(t, _)| t.ends_with("/up"))
    })
    .await;
    let state = r.of(&k, "state")[0].clone();
    assert_eq!(state["online"], true);
    assert_eq!(state["package"], "test-meter");
    let up = r.of(&k, "up")[0].clone();
    assert_eq!(up["seq"], 0);
    let v = &up["r"][0]["v"];
    assert_eq!(v["power"], 21.0);
    assert!((v["voltage"].as_f64().unwrap() - 230.0).abs() < 1e-9);
    assert_eq!(v["sp"], 0.0);
    assert_eq!(v["relay"], false);
    assert_eq!(up["rx"][0]["via"], format!("gw:{GW}"));
    assert!(
        up["link"]["modbus"]["latency_ms"].is_number() && up["link"]["modbus"]["count"].is_number()
    );

    // 2. a change inside the deadband stays quiet; one outside is reported, alone
    mem.lock().unwrap().input[1] = 0x0000; // power stays 21.0
    mem.lock().unwrap().input[0] = 0x41A9; // 21.1 .. within the deadband of 5
    mem.lock().unwrap().input[2] = 2310; // voltage 231.0: any change counts
    r.until("voltage change", |s| {
        s.iter().filter(|(t, _)| t.ends_with("/up")).count() >= 2
    })
    .await;
    let ups = r.of(&k, "up");
    assert_eq!(ups[1]["seq"], 1, "seq continues");
    let v = &ups[1]["r"][0]["v"];
    assert!((v["voltage"].as_f64().unwrap() - 231.0).abs() < 1e-9);
    assert!(
        v.get("power").is_none(),
        "power moved less than its deadband"
    );
    assert!(
        v.get("sp").is_none(),
        "flat values are not repeated before the heartbeat"
    );

    // 3. a write: queued, sent, delivered, ok, the device holds it, settings published
    r.cmd(
        &k,
        "cmd-set-0001",
        "settings.set",
        json!({"sp": 42, "relay": true}),
    );
    r.until("write done", |s| {
        s.iter()
            .any(|(_, v)| v["id"] == "cmd-set-0001" && v["status"] == "ok")
    })
    .await;
    assert_eq!(
        r.statuses(&k, "cmd-set-0001"),
        ["queued", "sent", "delivered", "ok"]
    );
    assert_eq!(mem.lock().unwrap().hold[10], 42);
    assert!(mem.lock().unwrap().coils[0]);
    let ok = r
        .of(&k, "cmd/res")
        .into_iter()
        .find(|v| v["status"] == "ok")
        .unwrap()
        .clone();
    assert_eq!(ok["data"]["sp"], 42);
    let settings = r.of(&k, "settings")[0].clone();
    assert_eq!(settings["by"], "cmd-set-0001");
    assert_eq!(settings["values"]["sp"].as_f64(), Some(42.0));
    // and the next poll reports what the device now holds
    r.until("readback", |s| {
        s.iter()
            .any(|(t, v)| t.ends_with("/up") && v["r"][0]["v"]["sp"] == 42.0)
    })
    .await;

    // 4. refusals never reach the device
    r.cmd(
        &k,
        "cmd-ro-0001",
        "register.write",
        json!({"field": "power", "value": 3}),
    );
    r.cmd(
        &k,
        "cmd-ro-0002",
        "register.write",
        json!({"address": 2, "function": 4, "value": 3}),
    );
    r.cmd(&k, "cmd-range-01", "settings.set", json!({"sp": 101}));
    r.cmd(&k, "cmd-name-001", "reboot", json!({}));
    r.cmd(&key(99), "cmd-nodev-01", "settings.set", json!({"sp": 1}));
    r.until("refusals", |s| {
        s.iter().filter(|(_, v)| v["status"] == "refused").count() >= 5
    })
    .await;
    for id in ["cmd-ro-0001", "cmd-ro-0002", "cmd-range-01"] {
        assert_eq!(r.statuses(&k, id), ["refused"], "{id}");
        let res = r
            .of(&k, "cmd/res")
            .into_iter()
            .find(|v| v["id"] == id)
            .unwrap()
            .clone();
        assert_eq!(res["reason"], "invalid");
    }
    let by = |id: &str| r.seen.iter().find(|(_, v)| v["id"] == id).unwrap().1["reason"].clone();
    assert_eq!(by("cmd-name-001"), "unknown");
    assert_eq!(by("cmd-nodev-01"), "unknown");
    assert_eq!(mem.lock().unwrap().hold[10], 42, "unchanged");

    // 5. an expired command fails, untouched
    r.router.dispatch(
        &r.out,
        &r.out.topic(&k, "cmd"),
        json!({"v":1,"id":"cmd-old-0001","name":"settings.set","params":{"sp":7},"expires":1.0})
            .to_string()
            .as_bytes(),
    );
    r.until("expiry", |s| {
        s.iter().any(|(_, v)| v["id"] == "cmd-old-0001")
    })
    .await;
    assert_eq!(
        r.of(&k, "cmd/res")
            .iter()
            .find(|v| v["id"] == "cmd-old-0001")
            .unwrap()["reason"],
        "expired"
    );
    assert_eq!(mem.lock().unwrap().hold[10], 42);

    // 6. a unit that never answers goes offline after offline_after failed polls
    r.until("unit 18 offline", |s| {
        s.iter()
            .any(|(t, v)| t.contains(".18/state") && v["online"] == false)
    })
    .await;
    assert!(r.of(&key(18), "up").is_empty());

    let _ = r.stop.send(true);
}

#[tokio::test]
async fn a_bus_that_comes_back_is_published_online_again() {
    // server not yet listening: connect fails, unit goes offline, then it appears
    let dir = tempfile::tempdir().unwrap();
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);
    let out = Outbox::open(dir.path(), &format!("edge-{GW}"), GW, 1 << 20).unwrap();
    let map = Arc::new(RegisterMap::from_toml(MAP).unwrap());
    let (_tx, rx) = mpsc::channel(8);
    let (stop, stop_rx) = watch::channel(false);
    let settings = bus::BusSettings {
        id: "lan-0".into(),
        kind: BusKind::Tcp {
            address: addr.to_string(),
        },
        timeout: Duration::from_millis(100),
        retries: 0,
        backoff: Duration::from_millis(10),
        turnaround: Duration::ZERO,
    };
    let units = vec![bus::UnitSpec {
        key: key(17),
        unit: 17,
        map,
        offline_after: 1,
    }];
    tokio::spawn(bus::run(settings, units, out.clone(), rx, stop_rx));
    let mut r = Rig {
        out,
        router: bus::Router::default(),
        stop,
        seen: vec![],
    };
    r.until("offline", |s| s.iter().any(|(_, v)| v["online"] == false))
        .await;

    let mem = Arc::new(Mutex::new(Mem {
        input: vec![0x41A8, 0, 2300],
        hold: vec![0; 20],
        coils: vec![false; 4],
    }));
    let listener = TcpListener::bind(addr).await.unwrap();
    tokio::spawn(async move {
        let server = Server::new(listener);
        let on = move |stream, a| {
            let dev = Dev(mem.clone());
            async move { accept_tcp_connection(stream, a, |_| Ok(Some(dev.clone()))) }
        };
        let _ = server.serve(&on, |_| {}).await;
    });
    r.until("online again", |s| {
        s.iter().any(|(_, v)| v["online"] == true)
    })
    .await;
    r.until("readings", |s| s.iter().any(|(t, _)| t.ends_with("/up")))
        .await;
    let _ = r.stop.send(true);
}
