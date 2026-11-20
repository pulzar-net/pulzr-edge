//! The platform's commands for Zigbee devices, run in order per device.
//!
//! | `name` | `params` | Answer |
//! |---|---|---|
//! | `zigbee.zcl` | `endpoint`, `cluster`, `frame` (base64), `response`, `profile`, `src_endpoint`, `timeout_ms` | `ok {frame}` with `response`, else `delivered` |
//! | `zigbee.zdo` | `cluster`, `payload` (base64, sequence number first), `response` | `ok {payload}` with `response`, else `delivered` |
//! | `zigbee.wait` | `ms` | `ok` once that long has passed since the device's previous command finished |
//!
//! A sleeping end device's commands are answered `queued` first: the
//! coordinator holds each frame until the device next polls. The network's
//! own lifecycle never comes this way: it is the console's.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use base64::Engine;
use pulzr_protocol as proto;
use serde_json::{json, Map, Value};
use tokio::sync::mpsc;
use zigbee_host::{AnyAdapter, Host, HostError, RawZcl};
use zigbee_types::Ieee;

use crate::outbox::{now, Outbox};

/// Commands waiting per device before more are refused as busy.
const QUEUE: usize = 32;
/// The longest `zigbee.wait`.
const MAX_WAIT_MS: u64 = 60_000;
/// Devices with a queue at once.
const MAX_DEVICES: usize = 4096;

struct Job {
    id: String,
    name: String,
    params: Value,
    expires: Option<f64>,
}

/// One queue per device.
#[derive(Default)]
pub struct Queues {
    queues: Mutex<HashMap<String, mpsc::Sender<Job>>>,
}

fn b64(s: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::STANDARD.decode(s).ok()
}

fn enc(b: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}

impl Queues {
    /// A device left: drop its queue.
    pub fn forget(&self, key: &str) {
        self.queues.lock().unwrap().remove(key);
    }

    /// A message on `pulzr/a/<adapter>/d/<key>/cmd`.
    pub fn dispatch(
        &self,
        host: Option<Host<AnyAdapter>>,
        out: &Outbox,
        topic: &str,
        payload: &[u8],
    ) {
        let prefix = format!("pulzr/a/{}/d/", out.adapter());
        let Some(key) = topic
            .strip_prefix(&prefix)
            .and_then(|t| t.strip_suffix("/cmd"))
        else {
            return;
        };
        let Some(ieee) = super::ieee_of(key) else {
            return; // another module's device
        };
        let raw: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(topic, error = %e, "command is not JSON");
                return;
            }
        };
        let cmd = match serde_json::from_value::<proto::Cmd>(raw.clone()) {
            Ok(c) => c,
            Err(e) => {
                if let Some(id) = raw.get("id").and_then(Value::as_str) {
                    if proto::CmdResultId::try_from(id).is_ok() {
                        out.cmd_res(
                            key,
                            id,
                            "refused",
                            Some("invalid"),
                            Some(&e.to_string()),
                            None,
                        );
                    }
                }
                return;
            }
        };
        let id = String::from(cmd.id.clone());
        let name = String::from(cmd.name.clone());
        let Some(host) = host else {
            return out.cmd_res(
                key,
                &id,
                "failed",
                Some("undeliverable"),
                Some("no network runs on this radio"),
                None,
            );
        };
        // A coordinator's key belongs to this module too, but its network
        // commands are the console's, never MQTT's.
        if ieee == host.network().ieee {
            return out.cmd_res(
                key,
                &id,
                "refused",
                Some("unknown"),
                Some("network operations come from Pulzar Net's console"),
                None,
            );
        }
        if !matches!(name.as_str(), "zigbee.zcl" | "zigbee.zdo" | "zigbee.wait") {
            return out.cmd_res(
                key,
                &id,
                "refused",
                Some("unknown"),
                Some("a Zigbee device takes zigbee.zcl, zigbee.zdo and zigbee.wait"),
                None,
            );
        }
        if host.device(ieee).is_none() {
            return out.cmd_res(
                key,
                &id,
                "failed",
                Some("undeliverable"),
                Some("the device is not on this network"),
                None,
            );
        }
        let params = serde_json::to_value(&cmd.params).unwrap_or(Value::Null);
        let expires = cmd.expires.as_ref().map(|t| f64::from(t.clone()));
        let job = Job {
            id: id.clone(),
            name,
            params,
            expires,
        };
        let mut queues = self.queues.lock().unwrap();
        if !queues.contains_key(key) && queues.len() >= MAX_DEVICES {
            queues.retain(|_, tx| !tx.is_closed() && tx.capacity() < QUEUE);
        }
        let tx = queues.entry(key.to_string()).or_insert_with(|| {
            let (tx, rx) = mpsc::channel(QUEUE);
            tokio::spawn(worker(host.clone(), out.clone(), key.to_string(), ieee, rx));
            tx
        });
        if tx.try_send(job).is_err() {
            out.cmd_res(
                key,
                &id,
                "failed",
                Some("busy"),
                Some("too many commands waiting for this device"),
                None,
            );
        }
    }
}

async fn worker(
    host: Host<AnyAdapter>,
    out: Outbox,
    key: String,
    ieee: Ieee,
    mut rx: mpsc::Receiver<Job>,
) {
    let mut finished = tokio::time::Instant::now();
    while let Some(job) = rx.recv().await {
        if job.expires.is_some_and(|t| t < now()) {
            out.cmd_res(&key, &job.id, "failed", Some("expired"), None, None);
            continue;
        }
        let sleepy = host.device(ieee).is_some_and(|d| d.is_sleepy());
        if sleepy && job.name != "zigbee.wait" {
            out.cmd_res(&key, &job.id, "queued", None, None, None);
        }
        let result = run(&host, ieee, &job, finished).await;
        match result {
            Ok((status, data)) => out.cmd_res(&key, &job.id, status, None, None, data),
            Err((reason, detail)) => {
                let status = if reason == "invalid" {
                    "refused"
                } else {
                    "failed"
                };
                out.cmd_res(&key, &job.id, status, Some(reason), Some(&detail), None)
            }
        }
        finished = tokio::time::Instant::now();
    }
}

type Outcome = Result<(&'static str, Option<Map<String, Value>>), (&'static str, String)>;

fn failure(e: HostError) -> (&'static str, String) {
    match e {
        HostError::Timeout(_) | HostError::DeliveryFailed(_) => ("undeliverable", e.to_string()),
        HostError::Invalid(m) | HostError::BadFrame(m) => ("invalid", m),
        other => ("error", other.to_string()),
    }
}

async fn run(
    host: &Host<AnyAdapter>,
    ieee: Ieee,
    job: &Job,
    finished: tokio::time::Instant,
) -> Outcome {
    let p = &job.params;
    let int = |k: &str| p.get(k).and_then(Value::as_u64);
    let invalid = |m: &str| ("invalid", m.to_string());
    match job.name.as_str() {
        "zigbee.wait" => {
            let ms = int("ms")
                .ok_or_else(|| invalid("ms is required"))?
                .min(MAX_WAIT_MS);
            tokio::time::sleep_until(finished + Duration::from_millis(ms)).await;
            Ok(("ok", None))
        }
        "zigbee.zcl" => {
            let endpoint = int("endpoint")
                .and_then(|v| u8::try_from(v).ok())
                .ok_or_else(|| invalid("endpoint is 0 to 255"))?;
            let cluster = int("cluster")
                .and_then(|v| u16::try_from(v).ok())
                .ok_or_else(|| invalid("cluster is 0 to 65535"))?;
            let frame = p
                .get("frame")
                .and_then(Value::as_str)
                .and_then(b64)
                .ok_or_else(|| invalid("frame is base64"))?;
            let request = RawZcl {
                endpoint,
                cluster,
                frame,
                profile: int("profile").and_then(|v| u16::try_from(v).ok()),
                source_endpoint: int("src_endpoint")
                    .and_then(|v| u8::try_from(v).ok())
                    .unwrap_or(1),
                response: p.get("response").and_then(Value::as_bool).unwrap_or(false),
                timeout: int("timeout_ms").map(|ms| Duration::from_millis(ms.clamp(100, 120_000))),
            };
            match host.zcl_raw(ieee, request).await.map_err(failure)? {
                Some(reply) => {
                    let mut d = Map::new();
                    d.insert("frame".into(), json!(enc(&reply)));
                    Ok(("ok", Some(d)))
                }
                None => Ok(("delivered", None)),
            }
        }
        "zigbee.zdo" => {
            let cluster = int("cluster")
                .and_then(|v| u16::try_from(v).ok())
                .ok_or_else(|| invalid("cluster is 0 to 65535"))?;
            let payload = p
                .get("payload")
                .and_then(Value::as_str)
                .and_then(b64)
                .ok_or_else(|| invalid("payload is base64"))?;
            let response = p.get("response").and_then(Value::as_bool).unwrap_or(false);
            match host
                .zdo_raw(ieee, cluster, payload, response, None)
                .await
                .map_err(failure)?
            {
                Some(reply) => {
                    let mut d = Map::new();
                    d.insert("payload".into(), json!(enc(&reply)));
                    Ok(("ok", Some(d)))
                }
                None => Ok(("delivered", None)),
            }
        }
        _ => Err(invalid("unknown command")),
    }
}
