//! IP requests for devices on the gateway's LAN (pulzr-protocol README, "IP
//! requests"): `ip.http`, `ip.tcp` and `ip.udp` for an `ext:` key, made
//! against the address the platform names and run in order per device.
//!
//! Nothing here knows a device. A device type's codec, in the platform, turns
//! a command into these requests and reads their answers, as a Zigbee codec
//! does with frames; this only makes them, inside fences:
//!
//! - only to an address on one of the LAN's subnets (`[ip] lan`, the bridge's
//!   own addresses with their prefixes), never the gateway's own address, the
//!   subnet's network or broadcast address, or anything beyond the LAN;
//! - no redirects followed and no proxy, so an answer cannot send a request
//!   somewhere else;
//! - at most 30 s and 48 KiB a request, so an answer fits a 64 KiB message;
//! - nothing of a request or its answer in the log: a device's login rides in
//!   them.
//!
//! Runs as its own process, `pulzr-edge ip <config.toml>`, with its own buffer
//! and MQTT session (client id `<adapter>-ip`) under the same adapter.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Result};
use base64::Engine;
use pulzr_protocol as proto;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::outbox::{now, Outbox};

const MAX_TIMEOUT_MS: u64 = 30_000;
const MAX_BYTES: usize = 49_152;
/// Commands waiting per device before more are refused as busy.
const QUEUE: usize = 32;
/// Devices with a queue at once.
const MAX_DEVICES: usize = 256;

/// `[ip]` in the configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IpCfg {
    /// The LAN's subnets as the gateway holds them: its own address on each,
    /// with the prefix (`192.168.8.1/24`, `10.0.0.2/24`).
    pub lan: Vec<String>,
}

/// One of the LAN's subnets: the gateway's address on it and the mask.
#[derive(Debug, Clone, Copy)]
struct Subnet {
    own: u32,
    mask: u32,
}

fn parse_subnet(s: &str) -> Result<Subnet> {
    let Some((addr, prefix)) = s.split_once('/') else {
        bail!("[ip] lan {s:?}: an address with its prefix, e.g. 192.168.8.1/24");
    };
    let own: Ipv4Addr = addr.parse()?;
    let prefix: u32 = prefix.parse()?;
    if !(8..=30).contains(&prefix) {
        bail!("[ip] lan {s:?}: a prefix from /8 to /30");
    }
    Ok(Subnet {
        own: own.into(),
        mask: u32::MAX << (32 - prefix),
    })
}

impl IpCfg {
    pub fn validate(&self) -> Result<()> {
        for s in &self.lan {
            parse_subnet(s)?;
        }
        Ok(())
    }

    fn subnets(&self) -> Vec<Subnet> {
        self.lan
            .iter()
            .filter_map(|s| parse_subnet(s).ok())
            .collect()
    }
}

type Refusal = (&'static str, &'static str, String);

fn invalid(detail: impl Into<String>) -> Refusal {
    ("refused", "invalid", detail.into())
}

fn unreachable(detail: impl Into<String>) -> Refusal {
    ("failed", "undeliverable", detail.into())
}

/// The address, if it is a device on the LAN: on one of its subnets, and not
/// the gateway, the network or the broadcast address.
fn allowed(subnets: &[Subnet], address: &str) -> Result<Ipv4Addr, Refusal> {
    let ip: Ipv4Addr = address
        .parse()
        .map_err(|_| invalid("address is an IPv4 address"))?;
    let n = u32::from(ip);
    let ok = subnets.iter().any(|s| {
        let net = s.own & s.mask;
        n & s.mask == net && n != s.own && n != net && n != net | !s.mask
    });
    if !ok {
        return Err(invalid(format!(
            "{ip} is not a device on this gateway's LAN"
        )));
    }
    Ok(ip)
}

fn b64(s: &str) -> Result<Vec<u8>, Refusal> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|_| invalid("not base64"))
}

fn enc(b: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}

fn timeout(ms: Option<u64>, default: u64) -> Duration {
    Duration::from_millis(ms.unwrap_or(default).clamp(100, MAX_TIMEOUT_MS))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Http {
    address: String,
    port: Option<u16>,
    method: String,
    path: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    body: Option<String>,
    body_b64: Option<String>,
    timeout_ms: Option<u64>,
    max_bytes: Option<usize>,
    #[allow(dead_code)]
    label: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Tcp {
    address: String,
    port: u16,
    send_b64: Option<String>,
    until_b64: Option<String>,
    max_bytes: Option<usize>,
    timeout_ms: Option<u64>,
    #[allow(dead_code)]
    label: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Udp {
    address: String,
    port: u16,
    send_b64: String,
    #[serde(default)]
    response: bool,
    max_bytes: Option<usize>,
    timeout_ms: Option<u64>,
    #[allow(dead_code)]
    label: Option<String>,
}

fn params<T: serde::de::DeserializeOwned>(p: &Value) -> Result<T, Refusal> {
    serde_json::from_value(p.clone()).map_err(|e| invalid(e.to_string()))
}

/// What an answer is: `Ok(None)` for `delivered`, `Ok(Some(data))` for `ok`.
type Answer = Result<Option<Map<String, Value>>, Refusal>;

async fn http(subnets: &[Subnet], p: Http) -> Answer {
    let ip = allowed(subnets, &p.address)?;
    let method: reqwest::Method = match p.method.as_str() {
        "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" => p.method.parse().unwrap(),
        _ => return Err(invalid("method: GET, POST, PUT, PATCH, DELETE or HEAD")),
    };
    if !p.path.starts_with('/') || p.path.chars().any(char::is_whitespace) {
        return Err(invalid("path starts with / and has no spaces"));
    }
    let body = match (p.body, p.body_b64) {
        (Some(_), Some(_)) => return Err(invalid("body or body_b64, not both")),
        (Some(t), None) => t.into_bytes(),
        (None, Some(b)) => b64(&b)?,
        (None, None) => Vec::new(),
    };
    if body.len() > 65_536 {
        return Err(invalid("a body is at most 64 KiB"));
    }
    let max = p.max_bytes.unwrap_or(MAX_BYTES).min(MAX_BYTES);
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .timeout(timeout(p.timeout_ms, 10_000))
        .build()
        .map_err(|e| ("failed", "error", e.to_string()))?;
    let url = format!("http://{ip}:{}{}", p.port.unwrap_or(80), p.path);
    let mut req = client.request(method, url);
    for (k, v) in &p.headers {
        req = req.header(k.as_str(), v.as_str());
    }
    if !body.is_empty() {
        req = req.body(body);
    }
    // Without the URL: it is the device's, but a path may carry a login.
    let gone = |e: reqwest::Error| unreachable(format!("{ip} did not answer: {}", e.without_url()));
    let mut resp = req.send().await.map_err(gone)?;
    let mut headers = Map::new();
    for (k, v) in resp.headers() {
        let v = String::from_utf8_lossy(v.as_bytes()).into_owned();
        match headers.get_mut(k.as_str()) {
            Some(Value::String(s)) => {
                s.push_str(", ");
                s.push_str(&v);
            }
            _ => {
                headers.insert(k.as_str().to_string(), Value::String(v));
            }
        }
    }
    let status = resp.status().as_u16();
    let mut read = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(gone)? {
        if read.len() + chunk.len() > max {
            return Err(("failed", "error", format!("the answer is over {max} bytes")));
        }
        read.extend_from_slice(&chunk);
    }
    let mut d = Map::new();
    d.insert("status".into(), json!(status));
    d.insert("headers".into(), Value::Object(headers));
    match String::from_utf8(read) {
        Ok(text) => d.insert("body".into(), Value::String(text)),
        Err(e) => d.insert("body_b64".into(), Value::String(enc(e.as_bytes()))),
    };
    Ok(Some(d))
}

async fn tcp(subnets: &[Subnet], p: Tcp) -> Answer {
    let ip = allowed(subnets, &p.address)?;
    let deadline = tokio::time::Instant::now() + timeout(p.timeout_ms, 10_000);
    let max = p.max_bytes.unwrap_or(4096).min(MAX_BYTES);
    let until = p.until_b64.as_deref().map(b64).transpose()?;
    let send = p
        .send_b64
        .as_deref()
        .map(b64)
        .transpose()?
        .unwrap_or_default();
    let addr = SocketAddr::from((ip, p.port));
    let mut s = tokio::time::timeout_at(deadline, tokio::net::TcpStream::connect(addr))
        .await
        .map_err(|_| unreachable(format!("{addr} did not answer")))?
        .map_err(|e| unreachable(format!("{addr}: {e}")))?;
    if !send.is_empty() {
        tokio::time::timeout_at(deadline, s.write_all(&send))
            .await
            .map_err(|_| unreachable(format!("{addr} took nothing")))?
            .map_err(|e| unreachable(format!("{addr}: {e}")))?;
    }
    let mut read = Vec::new();
    let mut buf = [0u8; 2048];
    while read.len() < max {
        let n = match tokio::time::timeout_at(deadline, s.read(&mut buf)).await {
            Err(_) => break, // what arrived in time is the answer
            Ok(Err(e)) => return Err(unreachable(format!("{addr}: {e}"))),
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => n,
        };
        read.extend_from_slice(&buf[..n.min(max - read.len())]);
        if let Some(u) = &until {
            if !u.is_empty() && read.windows(u.len()).any(|w| w == u.as_slice()) {
                break;
            }
        }
    }
    let mut d = Map::new();
    d.insert("b64".into(), Value::String(enc(&read)));
    Ok(Some(d))
}

async fn udp(subnets: &[Subnet], p: Udp) -> Answer {
    let ip = allowed(subnets, &p.address)?;
    let send = b64(&p.send_b64)?;
    let addr = SocketAddr::from((ip, p.port));
    let sock = tokio::net::UdpSocket::bind("0.0.0.0:0")
        .await
        .map_err(|e| ("failed", "error", e.to_string()))?;
    sock.connect(addr)
        .await
        .map_err(|e| unreachable(format!("{addr}: {e}")))?;
    sock.send(&send)
        .await
        .map_err(|e| unreachable(format!("{addr}: {e}")))?;
    if !p.response {
        return Ok(None);
    }
    let mut buf = vec![0u8; p.max_bytes.unwrap_or(8192).min(MAX_BYTES)];
    let n = tokio::time::timeout(timeout(p.timeout_ms, 2000), sock.recv(&mut buf))
        .await
        .map_err(|_| unreachable(format!("{addr} did not answer")))?
        .map_err(|e| unreachable(format!("{addr}: {e}")))?;
    let mut d = Map::new();
    d.insert("b64".into(), Value::String(enc(&buf[..n])));
    Ok(Some(d))
}

/// One request, checked and made.
pub async fn request(cfg: &IpCfg, name: &str, p: &Value) -> Answer {
    let subnets = cfg.subnets();
    match name {
        "ip.http" => http(&subnets, params(p)?).await,
        "ip.tcp" => tcp(&subnets, params(p)?).await,
        "ip.udp" => udp(&subnets, params(p)?).await,
        _ => Err(("refused", "unknown", "ip.http, ip.tcp or ip.udp".into())),
    }
}

struct Job {
    id: String,
    name: String,
    params: Value,
    expires: Option<f64>,
}

/// One queue per device: a device's requests in the order they came.
pub struct Router {
    cfg: Arc<IpCfg>,
    queues: Mutex<HashMap<String, mpsc::Sender<Job>>>,
}

impl Router {
    pub fn new(cfg: IpCfg) -> Self {
        Router {
            cfg: Arc::new(cfg),
            queues: Mutex::new(HashMap::new()),
        }
    }

    fn queue(&self, out: &Outbox, key: &str) -> Option<mpsc::Sender<Job>> {
        let mut queues = self.queues.lock().unwrap();
        queues.retain(|_, tx| !tx.is_closed());
        if let Some(tx) = queues.get(key) {
            return Some(tx.clone());
        }
        if queues.len() >= MAX_DEVICES {
            return None;
        }
        let (tx, mut rx) = mpsc::channel::<Job>(QUEUE);
        let (cfg, out, k) = (self.cfg.clone(), out.clone(), key.to_string());
        tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                if job.expires.is_some_and(|t| t < now()) {
                    out.cmd_res(&k, &job.id, "failed", Some("expired"), None, None);
                    continue;
                }
                match request(&cfg, &job.name, &job.params).await {
                    Ok(Some(d)) => out.cmd_res(&k, &job.id, "ok", None, None, Some(d)),
                    Ok(None) => out.cmd_res(&k, &job.id, "delivered", None, None, None),
                    Err((status, reason, detail)) => {
                        out.cmd_res(&k, &job.id, status, Some(reason), Some(&detail), None)
                    }
                }
                // The name and the outcome, never the params or the answer.
                tracing::debug!(key = %k, name = %job.name, "ip request done");
            }
        });
        queues.insert(key.to_string(), tx.clone());
        Some(tx)
    }

    fn handle(&self, out: &Outbox, key: &str, cmd: proto::Cmd) {
        let id = String::from(cmd.id.clone());
        let name = String::from(cmd.name.clone());
        if !name.starts_with("ip.") {
            return; // a command for whatever else speaks for this key
        }
        if cmd.v != proto::v1() {
            return out.cmd_res(
                key,
                &id,
                "refused",
                Some("invalid"),
                Some("unsupported protocol version"),
                None,
            );
        }
        let params = serde_json::to_value(&cmd.params).unwrap_or(Value::Null);
        let job = Job {
            id: id.clone(),
            name,
            params: if params.is_null() { json!({}) } else { params },
            expires: cmd.expires.as_ref().map(|t| f64::from(t.clone())),
        };
        match self.queue(out, key).map(|tx| tx.try_send(job).is_ok()) {
            Some(true) => out.cmd_res(key, &id, "queued", None, None, None),
            _ => out.cmd_res(
                key,
                &id,
                "failed",
                Some("busy"),
                Some("too many requests waiting"),
                None,
            ),
        }
    }
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
        if !key.starts_with("ext:") {
            return; // another module's device
        }
        let raw: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(_) => return tracing::warn!(topic, "command is not JSON"),
        };
        if !raw
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(|n| n.starts_with("ip."))
        {
            return;
        }
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
                _ => tracing::warn!(topic, "command without a usable id"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    fn cfg() -> IpCfg {
        IpCfg {
            lan: vec!["127.0.0.2/8".into()],
        }
    }

    #[test]
    fn only_devices_on_the_lan() {
        let s = IpCfg {
            lan: vec!["192.168.8.1/24".into(), "10.0.0.2/24".into()],
        }
        .subnets();
        assert!(allowed(&s, "10.0.0.112").is_ok());
        assert!(allowed(&s, "192.168.8.20").is_ok());
        for no in [
            "10.0.0.2",
            "192.168.8.1",
            "10.0.0.0",
            "10.0.0.255",
            "10.0.1.5",
            "8.8.8.8",
            "127.0.0.1",
            "cam.local",
        ] {
            let e = allowed(&s, no).unwrap_err();
            assert_eq!((e.0, e.1), ("refused", "invalid"), "{no}");
        }
        assert!(IpCfg {
            lan: vec!["10.0.0.2".into()]
        }
        .validate()
        .is_err());
        assert!(IpCfg {
            lan: vec!["0.0.0.0/0".into()]
        }
        .validate()
        .is_err());
    }

    /// A one-shot server on loopback: what it read, answered with `reply`.
    fn serve(reply: Vec<u8>) -> (u16, std::thread::JoinHandle<Vec<u8>>) {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let t = std::thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            s.set_read_timeout(Some(Duration::from_millis(300)))
                .unwrap();
            let mut got = Vec::new();
            let mut buf = [0u8; 4096];
            while let Ok(n) = s.read(&mut buf) {
                if n == 0 {
                    break;
                }
                got.extend_from_slice(&buf[..n]);
                if got.windows(4).any(|w| w == b"\r\n\r\n") && !got.starts_with(b"POST") {
                    break;
                }
                if got.starts_with(b"POST") && got.ends_with(b"</soap:Envelope>") {
                    break;
                }
            }
            s.write_all(&reply).unwrap();
            got
        });
        (port, t)
    }

    #[tokio::test]
    async fn http_as_the_codec_asked() {
        let (port, t) = serve(b"HTTP/1.1 200 OK\r\nContent-Type: text/xml\r\nContent-Length: 12\r\nConnection: close\r\n\r\n<UserConfig>".to_vec());
        let d = request(
            &cfg(),
            "ip.http",
            &json!({
            "address": "127.0.0.1", "port": port, "method": "POST", "path": "/getUserConfig",
            "headers": {"Content-Type": "application/x-www-form-urlencoded"},
            "body": "<soap:Envelope></soap:Envelope>"}),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(d["status"], 200);
        assert_eq!(d["headers"]["content-type"], "text/xml");
        assert_eq!(d["body"], "<UserConfig>");
        let sent = String::from_utf8(t.join().unwrap()).unwrap();
        assert!(sent.starts_with("POST /getUserConfig HTTP/1.1"), "{sent}");
        assert!(
            sent.to_lowercase()
                .contains("content-type: application/x-www-form-urlencoded\r\n"),
            "{sent}"
        );
    }

    #[tokio::test]
    async fn a_redirect_is_not_followed_and_bytes_come_back_as_base64() {
        let (port, _t) = serve(b"HTTP/1.1 302 Found\r\nLocation: http://8.8.8.8/\r\nContent-Length: 2\r\nConnection: close\r\n\r\n\xff\x00".to_vec());
        let d = request(
            &cfg(),
            "ip.http",
            &json!({"address": "127.0.0.1", "port": port, "method": "GET", "path": "/"}),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            (d["status"].as_u64(), d["body_b64"].as_str()),
            (Some(302), Some("/wA="))
        );
    }

    #[tokio::test]
    async fn an_answer_over_its_limit_fails() {
        let mut reply =
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n".to_vec();
        reply.extend([b'x'; 100]);
        let (port, _t) = serve(reply);
        let e = request(
            &cfg(),
            "ip.http",
            &json!({"address": "127.0.0.1", "port": port, "method": "GET",
                                                    "path": "/", "max_bytes": 10}),
        )
        .await
        .unwrap_err();
        assert_eq!((e.0, e.1), ("failed", "error"));
    }

    #[tokio::test]
    async fn refused_before_anything_is_sent() {
        for (name, p) in [
            (
                "ip.http",
                json!({"address": "8.8.8.8", "method": "GET", "path": "/"}),
            ),
            (
                "ip.http",
                json!({"address": "127.0.0.1", "method": "CONNECT", "path": "/"}),
            ),
            (
                "ip.http",
                json!({"address": "127.0.0.1", "method": "GET", "path": "x"}),
            ),
            (
                "ip.http",
                json!({"address": "127.0.0.1", "method": "GET", "path": "/", "url": "http://x"}),
            ),
            ("ip.tcp", json!({"address": "127.0.0.2", "port": 22})),
            (
                "ip.udp",
                json!({"address": "127.0.0.1", "port": 9, "send_b64": "!!"}),
            ),
        ] {
            let e = request(&cfg(), name, &p).await.unwrap_err();
            assert_eq!((e.0, e.1), ("refused", "invalid"), "{p}");
        }
        let e = request(
            &cfg(),
            "ip.http",
            &json!({"address": "127.0.0.3", "port": 9, "method": "GET",
                                                    "path": "/", "timeout_ms": 500}),
        )
        .await
        .unwrap_err();
        assert_eq!(e.1, "undeliverable");
    }

    #[tokio::test]
    async fn tcp_reads_until_its_marker() {
        let (port, t) = serve(b"RTSP/1.0 200 OK\r\nCSeq: 1\r\n\r\nleft over".to_vec());
        let d = request(&cfg(), "ip.tcp", &json!({"address": "127.0.0.1", "port": port,
            "send_b64": enc(b"OPTIONS * RTSP/1.0\r\nCSeq: 1\r\n\r\n"), "until_b64": enc(b"\r\n\r\n"),
            "timeout_ms": 2000}))
            .await
            .unwrap()
            .unwrap();
        let got = b64(d["b64"].as_str().unwrap()).unwrap();
        assert!(got.starts_with(b"RTSP/1.0 200 OK"));
        assert!(String::from_utf8(t.join().unwrap())
            .unwrap()
            .starts_with("OPTIONS"));
    }

    #[tokio::test]
    async fn udp_answers_with_the_first_datagram() {
        let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = s.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let mut buf = [0u8; 64];
            let (n, from) = s.recv_from(&mut buf).unwrap();
            s.send_to(&buf[..n].iter().rev().copied().collect::<Vec<_>>(), from)
                .unwrap();
        });
        let d = request(
            &cfg(),
            "ip.udp",
            &json!({"address": "127.0.0.1", "port": port, "send_b64": enc(b"abc"),
                                                   "response": true}),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(d["b64"], enc(b"cba"));
        assert_eq!(
            request(
                &cfg(),
                "ip.udp",
                &json!({"address": "127.0.0.1", "port": port, "send_b64": "AA=="})
            )
            .await
            .unwrap(),
            None
        );
    }
}
