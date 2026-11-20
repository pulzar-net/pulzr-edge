//! GATT and advertising intents.
//!
//! | Key | Intent | Params | Answer |
//! |---|---|---|---|
//! | a device's `ble:` key | `gatt.discover` | `secure` | `delivered` when connected, `ok {services}` |
//! | a device's `ble:` key | `gatt.read` | `characteristic` (UUID), `service`, `secure` | `delivered`, `ok {value}` (base64) |
//! | a device's `ble:` key | `gatt.write` | `characteristic`, `value` (base64), `service`, `response` (default true), `secure` | `delivered`, `ok` |
//! | the controller's `ble:` key | `ble.advertise` | `data`, `scan_response` (base64 AD structures), `interval_ms`, `connectable`, `legacy` | `ok {set}` |
//! | the controller's `ble:` key | `ble.advertise.stop` | `set` | `ok` |
//!
//! A device this gateway has not heard (in the last few thousand) answers
//! `failed undeliverable`: another gateway may reach it. One GATT operation
//! runs at a time; scanning goes on meanwhile.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use bluetooth_adv::Uuid;
use bluetooth_host::{Address, AdvertisingParams, ConnectParams, Host, Report};
use pulzr_protocol as proto;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use super::key_for;
use crate::outbox::{now, Outbox};

/// Devices remembered for GATT intents.
const SEEN: usize = 4096;
/// The longest a GATT intent may take, connection included.
const OP_TIMEOUT: Duration = Duration::from_secs(60);

struct Attached {
    host: Host,
    controller: String,
}

/// Answers the `ble:` keys' intents.
pub struct Router {
    enabled: bool,
    attached: Mutex<Option<Attached>>,
    seen: Mutex<(HashMap<String, Address>, VecDeque<String>)>,
    gatt: tokio::sync::Mutex<()>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GattParams {
    characteristic: Option<String>,
    service: Option<String>,
    value: Option<String>,
    #[serde(default = "yes")]
    response: bool,
    #[serde(default)]
    secure: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AdvertiseParams {
    data: String,
    scan_response: Option<String>,
    interval_ms: Option<u32>,
    #[serde(default)]
    connectable: bool,
    legacy: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StopParams {
    set: u8,
}

fn yes() -> bool {
    true
}

enum Op {
    Discover,
    Read(Uuid, Option<Uuid>),
    Write(Uuid, Option<Uuid>, Vec<u8>, bool),
}

fn b64(s: &str) -> Result<Vec<u8>, String> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| format!("not base64: {e}"))
}

fn uuid(s: &Option<String>, what: &str) -> Result<Option<Uuid>, String> {
    s.as_deref()
        .map(|s| Uuid::parse(s).ok_or_else(|| format!("{what} {s:?} is not a UUID")))
        .transpose()
}

impl Router {
    pub fn new(enabled: bool) -> Self {
        Router {
            enabled,
            attached: Mutex::new(None),
            seen: Mutex::new((HashMap::new(), VecDeque::new())),
            gatt: tokio::sync::Mutex::new(()),
        }
    }

    /// The host is up.
    pub fn attach(&self, host: Host, controller: String) {
        *self.attached.lock().unwrap() = Some(Attached { host, controller });
    }

    /// The host went away.
    pub fn detach(&self) {
        self.attached.lock().unwrap().take();
    }

    /// Remembers where a device was heard, for its GATT intents.
    pub fn heard(&self, r: &Report) {
        if !r.connectable {
            return;
        }
        let key = key_for(&r.device());
        let mut s = self.seen.lock().unwrap();
        if s.0.insert(key.clone(), r.address).is_none() {
            s.1.push_back(key);
            if s.1.len() > SEEN {
                if let Some(old) = s.1.pop_front() {
                    s.0.remove(&old);
                }
            }
        }
    }

    fn host(&self) -> Option<(Host, String)> {
        self.attached
            .lock()
            .unwrap()
            .as_ref()
            .map(|a| (a.host.clone(), a.controller.clone()))
    }

    fn handle(self: &Arc<Self>, out: &Outbox, key: &str, cmd: proto::Cmd) {
        let id = String::from(cmd.id.clone());
        let name = String::from(cmd.name.clone());
        let refuse = |reason: &str, detail: &str| {
            out.cmd_res(key, &id, "refused", Some(reason), Some(detail), None)
        };
        if cmd.v != proto::v1() {
            return refuse("invalid", "unsupported protocol version");
        }
        if !self.enabled {
            return refuse("unknown", "Bluetooth intents are off on this gateway");
        }
        if cmd
            .expires
            .as_ref()
            .is_some_and(|t| f64::from(t.clone()) < now())
        {
            return out.cmd_res(key, &id, "failed", Some("expired"), None, None);
        }
        let Some((host, controller)) = self.host() else {
            return out.cmd_res(
                key,
                &id,
                "failed",
                Some("undeliverable"),
                Some("the Bluetooth controller is not up"),
                None,
            );
        };
        let params = serde_json::to_value(&cmd.params).unwrap_or(Value::Null);
        let params = if params.is_null() { json!({}) } else { params };
        let out = out.clone();
        let key = key.to_string();
        if key == controller {
            match name.as_str() {
                "ble.advertise" => {
                    let p: AdvertiseParams = match serde_json::from_value(params) {
                        Ok(p) => p,
                        Err(e) => return refuse("invalid", &e.to_string()),
                    };
                    let (data, sr) = match (
                        b64(&p.data),
                        p.scan_response.as_deref().map(b64).transpose(),
                    ) {
                        (Ok(d), Ok(s)) => (d, s.unwrap_or_default()),
                        (Err(e), _) | (_, Err(e)) => return refuse("invalid", &e),
                    };
                    let interval = p.interval_ms.unwrap_or(100).max(20);
                    let ap = AdvertisingParams {
                        connectable: p.connectable,
                        legacy: p.legacy,
                        interval_min_ms: interval,
                        interval_max_ms: interval + interval / 2,
                        ..AdvertisingParams::default()
                    };
                    tokio::spawn(async move {
                        match host.advertise(&ap, &data, &sr).await {
                            Ok(set) => {
                                let mut d = Map::new();
                                d.insert("set".into(), json!(set));
                                out.cmd_res(&key, &id, "ok", None, None, Some(d));
                            }
                            Err(e) => out.cmd_res(
                                &key,
                                &id,
                                "failed",
                                Some("error"),
                                Some(&e.to_string()),
                                None,
                            ),
                        }
                    });
                }
                "ble.advertise.stop" => {
                    let p: StopParams = match serde_json::from_value(params) {
                        Ok(p) => p,
                        Err(e) => return refuse("invalid", &e.to_string()),
                    };
                    tokio::spawn(async move {
                        match host.stop_advertising(p.set).await {
                            Ok(()) => out.cmd_res(&key, &id, "ok", None, None, None),
                            Err(e) => out.cmd_res(
                                &key,
                                &id,
                                "failed",
                                Some("error"),
                                Some(&e.to_string()),
                                None,
                            ),
                        }
                    });
                }
                _ => refuse(
                    "unknown",
                    "the controller takes ble.advertise and ble.advertise.stop",
                ),
            }
            return;
        }
        let p: GattParams = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return refuse("invalid", &e.to_string()),
        };
        let op = match name.as_str() {
            "gatt.discover" => Op::Discover,
            "gatt.read" | "gatt.write" => {
                let (c, s) = match (
                    uuid(&p.characteristic, "characteristic"),
                    uuid(&p.service, "service"),
                ) {
                    (Ok(Some(c)), Ok(s)) => (c, s),
                    (Ok(None), _) => return refuse("invalid", "characteristic is required"),
                    (Err(e), _) | (_, Err(e)) => return refuse("invalid", &e),
                };
                if name == "gatt.read" {
                    Op::Read(c, s)
                } else {
                    match p.value.as_deref().map(b64) {
                        Some(Ok(v)) => Op::Write(c, s, v, p.response),
                        Some(Err(e)) => return refuse("invalid", &e),
                        None => return refuse("invalid", "value is required"),
                    }
                }
            }
            _ => {
                return refuse(
                    "unknown",
                    "a device takes gatt.discover, gatt.read and gatt.write",
                )
            }
        };
        let Some(address) = self.seen.lock().unwrap().0.get(&key).copied() else {
            return out.cmd_res(
                &key,
                &id,
                "failed",
                Some("undeliverable"),
                Some("not heard connectable by this gateway"),
                None,
            );
        };
        out.cmd_res(&key, &id, "queued", None, None, None);
        let me = self.clone();
        let secure = p.secure;
        tokio::spawn(async move {
            let _one = me.gatt.lock().await;
            let r = tokio::time::timeout(
                OP_TIMEOUT,
                gatt(&host, &out, &key, &id, address, op, secure),
            )
            .await;
            match r {
                Ok(Ok(data)) => out.cmd_res(&key, &id, "ok", None, None, data),
                Ok(Err((reason, detail))) => {
                    out.cmd_res(&key, &id, "failed", Some(reason), Some(&detail), None)
                }
                Err(_) => out.cmd_res(&key, &id, "failed", Some("error"), Some("timed out"), None),
            }
        });
    }
}

type OpResult = Result<Option<Map<String, Value>>, (&'static str, String)>;

async fn gatt(
    host: &Host,
    out: &Outbox,
    key: &str,
    id: &str,
    address: Address,
    op: Op,
    secure: bool,
) -> OpResult {
    let conn = host
        .connect(address, &ConnectParams::default())
        .await
        .map_err(|e| ("undeliverable", format!("could not connect: {e}")))?;
    out.cmd_res(key, id, "delivered", None, None, None);
    let r = async {
        let err = |e: bluetooth_host::Error| ("error", e.to_string());
        if secure {
            conn.secure().await.map_err(err)?;
        }
        let g = conn.gatt();
        let _ = g.exchange_mtu(247).await;
        let find = |c: Uuid, s: Option<Uuid>| {
            let g = g.clone();
            async move {
                match s {
                    None => g.characteristic(c).await,
                    Some(s) => {
                        for svc in g.services_by_uuid(s).await? {
                            if let Some(ch) = g
                                .characteristics(&svc)
                                .await?
                                .into_iter()
                                .find(|x| x.uuid == c)
                            {
                                return Ok(ch);
                            }
                        }
                        Err(bluetooth_host::Error::InvalidParameter(format!(
                            "no characteristic {c} in {s}"
                        )))
                    }
                }
            }
        };
        match op {
            Op::Discover => {
                let services = g.discover().await.map_err(err)?;
                let mut d = Map::new();
                d.insert(
                    "services".into(),
                    serde_json::to_value(services).unwrap_or(Value::Null),
                );
                Ok(Some(d))
            }
            Op::Read(c, s) => {
                let ch = find(c, s).await.map_err(err)?;
                let v = g.read(ch.value_handle).await.map_err(err)?;
                let mut d = Map::new();
                d.insert(
                    "value".into(),
                    json!(base64::engine::general_purpose::STANDARD.encode(v)),
                );
                Ok(Some(d))
            }
            Op::Write(c, s, v, response) => {
                let ch = find(c, s).await.map_err(err)?;
                if response {
                    g.write(ch.value_handle, &v).await.map_err(err)?;
                } else {
                    g.write_without_response(ch.value_handle, &v)
                        .await
                        .map_err(err)?;
                }
                Ok(None)
            }
        }
    }
    .await;
    let _ = conn.disconnect().await;
    r
}

impl crate::mqtt::Dispatch for Arc<Router> {
    fn dispatch(&self, out: &Outbox, topic: &str, payload: &[u8]) {
        let prefix = format!("pulzr/a/{}/d/", out.adapter());
        let Some(key) = topic
            .strip_prefix(&prefix)
            .and_then(|t| t.strip_suffix("/cmd"))
        else {
            return;
        };
        if !key.starts_with("ble:") {
            return; // another module's device
        }
        let raw: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(topic, error = %e, "command is not JSON");
                return;
            }
        };
        match serde_json::from_value::<proto::Cmd>(raw.clone()) {
            Ok(cmd) => self.handle(out, key, cmd),
            Err(e) => match raw.get("id").and_then(Value::as_str) {
                Some(id) if proto::CmdResultId::try_from(id).is_ok() => out.cmd_res(
                    key,
                    id,
                    "refused",
                    Some("invalid"),
                    Some(&e.to_string()),
                    None,
                ),
                _ => tracing::warn!(topic, error = %e, "command without a usable id"),
            },
        }
    }
}
