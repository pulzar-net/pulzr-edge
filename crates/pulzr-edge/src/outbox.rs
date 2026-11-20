//! Everything the daemon says to the platform goes through the outbox: it assigns
//! `seq`, builds the topic, checks the body against the protocol types, and queues
//! the message on disk for the MQTT session to send.

use crate::buffer::{Buffer, Entry, SeqStore};
use anyhow::Result;
use pulzr_protocol as proto;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Notify;

pub fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

struct Inner {
    adapter: String,
    gateway_eui: String,
    buffer: Mutex<Buffer>,
    seq: Mutex<SeqStore>,
    wake: Notify,
}

#[derive(Clone)]
pub struct Outbox(Arc<Inner>);

impl Outbox {
    pub fn open(dir: &Path, adapter: &str, gateway_eui: &str, max_bytes: u64) -> Result<Self> {
        Ok(Self(Arc::new(Inner {
            adapter: adapter.into(),
            gateway_eui: gateway_eui.into(),
            buffer: Mutex::new(Buffer::open(dir, max_bytes)?),
            seq: Mutex::new(SeqStore::open(dir)?),
            wake: Notify::new(),
        })))
    }

    pub fn adapter(&self) -> &str {
        &self.0.adapter
    }

    /// `pulzr/a/<adapter>/d/<key>/<suffix>`
    pub fn topic(&self, key: &str, suffix: &str) -> String {
        format!("pulzr/a/{}/d/{key}/{suffix}", self.0.adapter)
    }

    fn queue<T: DeserializeOwned + Serialize>(&self, key: &str, suffix: &str, body: Value) {
        // Parsing into the generated type enforces the schema's string patterns.
        let typed: T = match serde_json::from_value(body) {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(%key, suffix, error = %e, "dropping a message that is not valid protocol");
                return;
            }
        };
        let payload = serde_json::to_vec(&typed).expect("serialisable");
        let mut buf = self.0.buffer.lock().unwrap();
        match buf.push(&self.topic(key, suffix), &payload) {
            Ok(0) => {}
            Ok(n) => tracing::warn!(dropped = n, "buffer full, oldest messages dropped"),
            Err(e) => tracing::error!(error = %e, "buffer write failed"),
        }
        drop(buf);
        self.0.wake.notify_one();
    }

    /// Readings for one device, with the next `seq`.
    pub fn up(
        &self,
        key: &str,
        ts: f64,
        values: serde_json::Map<String, Value>,
        link: Option<Value>,
    ) {
        let seq = match self.0.seq.lock().unwrap().next(key) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "cannot persist seq");
                return;
            }
        };
        let mut body = json!({
            "v": 1, "seq": seq,
            "r": [ { "ts": ts, "v": values } ],
            "rx": [ { "via": format!("gw:{}", self.0.gateway_eui), "ts": ts, "time": "received" } ],
        });
        if let Some(l) = link {
            body["link"] = json!({ "modbus": l });
        }
        self.queue::<proto::Up>(key, "up", body);
    }

    /// An `up` whose body (everything but `v` and `seq`) a module built
    /// itself: the BLE module's `raw`, `rx` and `link.ble`.
    pub fn up_body(&self, key: &str, mut body: Value) {
        let seq = match self.0.seq.lock().unwrap().next(key) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "cannot persist seq");
                return;
            }
        };
        body["v"] = json!(1);
        body["seq"] = json!(seq);
        self.queue::<proto::Up>(key, "up", body);
    }

    /// `gw:<gateway eui>` in `rx`, the gateway that heard it.
    pub fn via(&self) -> String {
        format!("gw:{}", self.0.gateway_eui)
    }

    pub fn state(&self, key: &str, online: bool, last_heard: Option<f64>, package: Option<&str>) {
        let mut body = json!({ "v": 1, "online": online });
        if let Some(t) = last_heard {
            body["last_heard"] = json!(t);
        }
        if let Some(p) = package {
            body["package"] = json!(p);
        }
        self.queue::<proto::State>(key, "state", body);
    }

    /// A `state` whose body (everything but `v`) a module built itself: a
    /// coordinator's `network`, a joined device's `interview`.
    pub fn state_body(&self, key: &str, mut body: Value) {
        body["v"] = json!(1);
        self.queue::<proto::State>(key, "state", body);
    }

    /// An `event`: `name` as the protocol's pattern allows, `data` free-form.
    /// Never a key in `data`; events are logged and kept by the platform.
    pub fn event(&self, key: &str, name: &str, data: Value, sev: Option<&str>) {
        let seq = match self.0.seq.lock().unwrap().next(key) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "cannot persist seq");
                return;
            }
        };
        let mut body = json!({ "v": 1, "seq": seq, "ts": now(), "name": name, "data": data });
        if let Some(s) = sev {
            body["sev"] = json!(s);
        }
        self.queue::<proto::Event>(key, "event", body);
    }

    /// A network's links, on its coordinator's key: everything but `v`.
    pub fn topology(&self, key: &str, mut body: Value) {
        body["v"] = json!(1);
        self.queue::<proto::Topology>(key, "topology", body);
    }

    pub fn settings(&self, key: &str, values: serde_json::Map<String, Value>, by: &str) {
        self.queue::<proto::Settings>(
            key,
            "settings",
            json!({ "v": 1, "values": values, "by": by }),
        );
    }

    /// An answer to a command; `reason` for refused and failed.
    pub fn cmd_res(
        &self,
        key: &str,
        id: &str,
        status: &str,
        reason: Option<&str>,
        detail: Option<&str>,
        data: Option<serde_json::Map<String, Value>>,
    ) {
        let mut body = json!({ "v": 1, "id": id, "status": status });
        if let Some(r) = reason {
            body["reason"] = json!(r);
        }
        if let Some(d) = detail {
            body["detail"] = json!(d.chars().take(500).collect::<String>());
        }
        if let Some(d) = data {
            body["data"] = Value::Object(d);
        }
        self.queue::<proto::CmdResult>(key, "cmd/res", body);
    }

    /// Oldest queued message, for the MQTT session.
    pub fn peek(&self) -> Option<Entry> {
        match self.0.buffer.lock().unwrap().peek() {
            Ok(e) => e,
            Err(e) => {
                tracing::error!(error = %e, "buffer read failed");
                None
            }
        }
    }

    pub fn ack(&self, e: &Entry) {
        self.0.buffer.lock().unwrap().ack(e);
    }

    pub fn pending(&self) -> u64 {
        self.0.buffer.lock().unwrap().len()
    }

    /// Resolves when something is queued (or was, since the last wait).
    pub async fn wait(&self) {
        self.0.wake.notified().await;
    }

    pub fn flush(&self) {
        if let Err(e) = self.0.seq.lock().unwrap().flush() {
            tracing::error!(error = %e, "cannot save seq on shutdown");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn up(o: &Outbox, key: &str, v: f64) {
        o.up(
            key,
            1.0,
            serde_json::Map::from_iter([("x".to_string(), json!(v))]),
            None,
        );
    }

    fn seqs(o: &Outbox) -> Vec<u64> {
        let mut out = vec![];
        while let Some(e) = o.peek() {
            out.push(
                serde_json::from_slice::<Value>(&e.payload).unwrap()["seq"]
                    .as_u64()
                    .unwrap(),
            );
            o.ack(&e);
        }
        out
    }

    #[test]
    fn queued_messages_and_seq_survive_a_restart() {
        let d = tempfile::tempdir().unwrap();
        let k = "modbus:aabbccddeeff0011.rs485-0.17";
        let o = Outbox::open(
            d.path(),
            "edge-aabbccddeeff0011",
            "aabbccddeeff0011",
            1 << 20,
        )
        .unwrap();
        for i in 0..3 {
            up(&o, k, f64::from(i));
        }
        o.flush();
        drop(o);
        let o = Outbox::open(
            d.path(),
            "edge-aabbccddeeff0011",
            "aabbccddeeff0011",
            1 << 20,
        )
        .unwrap();
        up(&o, k, 9.0);
        up(&o, "modbus:aabbccddeeff0011.rs485-0.18", 1.0);
        assert_eq!(
            seqs(&o),
            [0, 1, 2, 3, 0],
            "replayed in order, then continued per device"
        );
    }

    #[test]
    fn topics_follow_the_protocol() {
        let d = tempfile::tempdir().unwrap();
        let o = Outbox::open(
            d.path(),
            "edge-aabbccddeeff0011",
            "aabbccddeeff0011",
            1 << 20,
        )
        .unwrap();
        o.state("modbus:aabbccddeeff0011.rs485-0.17", true, Some(5.0), None);
        let e = o.peek().unwrap();
        assert_eq!(
            e.topic,
            "pulzr/a/edge-aabbccddeeff0011/d/modbus:aabbccddeeff0011.rs485-0.17/state"
        );
    }

    #[test]
    fn a_body_that_breaks_the_protocol_is_not_queued() {
        let d = tempfile::tempdir().unwrap();
        let o = Outbox::open(d.path(), "a", "aabbccddeeff0011", 1 << 20).unwrap();
        o.cmd_res("k", "short", "ok", None, None, None); // id needs 8-64 characters
        assert_eq!(o.pending(), 0);
    }
}
