//! The platform's commands for Zigbee devices, run in order per device.
//!
//! | `name` | `params` | Answer |
//! |---|---|---|
//! | `zigbee.zcl` | `endpoint`, `cluster`, `frame` (base64), `response`, `profile`, `src_endpoint`, `timeout_ms` | `ok {frame}` with `response`, else `delivered` |
//! | `zigbee.zdo` | `cluster`, `payload` (base64, sequence number first), `response` | `ok {payload}` with `response`, else `delivered` |
//! | `zigbee.wait` | `ms` | `ok` once that long has passed since the device's previous command finished |
//! | `zigbee.ota` | `url` (https), `sha256`, `file_version`, `size` | `delivered` once the device takes the image, `ok {from, to}` once installed |
//!
//! `zigbee.ota` fetches the device type's image into the run directory
//! (tmpfs), checks its SHA-256 and OTA header, and offers it to that device
//! alone; the image is deleted when the upgrade ends. Progress goes out as
//! the device's `ota.progress` events.
//!
//! A sleeping end device's commands are answered `queued` first: the
//! coordinator holds each frame until the device next polls. The network's
//! own lifecycle never comes this way: it is the console's.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use base64::Engine;
use pulzr_protocol as proto;
use serde_json::{json, Map, Value};
use tokio::sync::mpsc;
use zigbee_host::{AnyAdapter, Host, HostError, RawZcl};
use zigbee_primitives::Ieee;

use crate::outbox::{now, Outbox};

/// Commands waiting per device before more are refused as busy.
const QUEUE: usize = 32;
/// The longest `zigbee.wait` (the protocol's limit).
const MAX_WAIT_MS: u64 = 600_000;
/// The largest OTA image fetched.
const MAX_IMAGE: u64 = 16 * 1024 * 1024;
/// The longest an image download may take.
const FETCH_TIMEOUT: Duration = Duration::from_secs(300);
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
    /// Upgrades running: the command that started each, and its image.
    ota: std::sync::Arc<Mutex<HashMap<Ieee, (String, PathBuf)>>>,
    /// Where images are fetched to (the host's OTA directory).
    ota_dir: Mutex<Option<PathBuf>>,
    /// The hosts images may come from.
    firmware: Mutex<crate::config::FirmwareCfg>,
}

fn b64(s: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::STANDARD.decode(s).ok()
}

fn enc(b: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}

impl Queues {
    /// Whether a firmware upgrade is running.
    pub fn upgrading(&self) -> bool {
        !self.ota.lock().unwrap().is_empty()
    }

    /// The hosts firmware images may be fetched from.
    pub fn set_firmware(&self, firmware: crate::config::FirmwareCfg) {
        *self.firmware.lock().unwrap() = firmware;
    }

    /// The host's OTA directory, once a network runs.
    pub fn set_ota_dir(&self, dir: PathBuf) {
        *self.ota_dir.lock().unwrap() = Some(dir);
    }

    /// An upgrade ended: answer the command that started it and delete the
    /// image.
    pub fn ota_finished(
        &self,
        out: &Outbox,
        ieee: Ieee,
        from: u32,
        to: u32,
        success: bool,
        status: u8,
    ) {
        let Some((id, image)) = self.ota.lock().unwrap().remove(&ieee) else {
            return;
        };
        let _ = std::fs::remove_file(&image);
        let key = super::key(ieee);
        if success {
            let mut d = Map::new();
            d.insert("from".into(), json!(from));
            d.insert("to".into(), json!(to));
            out.cmd_res(&key, &id, "ok", None, None, Some(d));
        } else {
            out.cmd_res(
                &key,
                &id,
                "failed",
                Some("error"),
                Some(&format!(
                    "the device ended the upgrade with status {status:#04x}"
                )),
                None,
            );
        }
    }

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
                Some("network operations come from Pulzr Net's console"),
                None,
            );
        }
        if !matches!(
            name.as_str(),
            "zigbee.zcl" | "zigbee.zdo" | "zigbee.wait" | "zigbee.ota"
        ) {
            return out.cmd_res(
                key,
                &id,
                "refused",
                Some("unknown"),
                Some("a Zigbee device takes zigbee.zcl, zigbee.zdo, zigbee.wait and zigbee.ota"),
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
        let ota = Ota {
            running: self.ota.clone(),
            dir: self.ota_dir.lock().unwrap().clone(),
            firmware: self.firmware.lock().unwrap().clone(),
        };
        let tx = queues.entry(key.to_string()).or_insert_with(|| {
            let (tx, rx) = mpsc::channel(QUEUE);
            tokio::spawn(worker(
                host.clone(),
                out.clone(),
                key.to_string(),
                ieee,
                ota,
                rx,
            ));
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

#[derive(Clone)]
struct Ota {
    running: std::sync::Arc<Mutex<HashMap<Ieee, (String, PathBuf)>>>,
    dir: Option<PathBuf>,
    firmware: crate::config::FirmwareCfg,
}

async fn worker(
    host: Host<AnyAdapter>,
    out: Outbox,
    key: String,
    ieee: Ieee,
    ota: Ota,
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
        let result = if job.name == "zigbee.ota" {
            upgrade(&host, ieee, &job, &ota).await
        } else {
            run(&host, ieee, &job, finished).await
        };
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

/// Fetch, check and offer a firmware image. Answers `delivered` once the
/// device took it; the final answer comes with the upgrade's end.
async fn upgrade(host: &Host<AnyAdapter>, ieee: Ieee, job: &Job, ota: &Ota) -> Outcome {
    let p = &job.params;
    let invalid = |m: &str| ("invalid", m.to_string());
    let url = p
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("url is required"))?;
    // Only the platform's own asset hosts ([firmware] hosts), never one a
    // command names alone. file:// only where a test asks for it.
    let file = url.starts_with("file://") && std::env::var_os("PULZR_EDGE_OTA_FILE_URLS").is_some();
    if url.len() > 2048 || !(file || ota.firmware.allows(url)) {
        return Err(invalid(
            "url must be https on one of this gateway's firmware hosts",
        ));
    }
    let sha256 = p
        .get("sha256")
        .and_then(Value::as_str)
        .filter(|s| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
        .ok_or_else(|| invalid("sha256 is 64 lower-case hex digits"))?;
    let want_version = p.get("file_version").and_then(Value::as_u64);
    let dir = ota
        .dir
        .clone()
        .ok_or_else(|| ("error", "no network runs on this radio".to_string()))?;
    if ota.running.lock().unwrap().contains_key(&ieee) {
        return Err((
            "busy",
            "an upgrade is already running for this device".into(),
        ));
    }
    let image = dir.join(format!("{sha256}.ota"));
    let part = dir.join(format!("{sha256}.part"));
    fetch(url, &part).await.map_err(|e| ("error", e))?;
    let header = match check(&part, sha256, want_version) {
        Ok(h) => h,
        Err(e) => {
            let _ = std::fs::remove_file(&part);
            return Err(("error", e));
        }
    };
    // The image must be for this device's maker (its image type is checked
    // against what the device reports when it asks).
    let maker = host
        .device(ieee)
        .and_then(|d| d.node_descriptor.map(|n| n.manufacturer_code));
    if let Some(m) = maker.filter(|m| *m != header.manufacturer) {
        let _ = std::fs::remove_file(&part);
        return Err(invalid(&format!(
            "the image is for manufacturer {:#06x}, not this device's {m:#06x}",
            header.manufacturer
        )));
    }
    std::fs::rename(&part, &image).map_err(|e| ("error", e.to_string()))?;
    ota.running
        .lock()
        .unwrap()
        .insert(ieee, (job.id.clone(), image.clone()));
    match host.ota_update(ieee).await {
        Ok(_) => Ok(("delivered", None)),
        Err(e) => {
            ota.running.lock().unwrap().remove(&ieee);
            let _ = std::fs::remove_file(&image);
            Err(failure(e))
        }
    }
}

/// Download with curl (in every PulzrOS image), bounded in size and time.
async fn fetch(url: &str, to: &Path) -> Result<(), String> {
    let mut cmd = tokio::process::Command::new("curl");
    cmd.args(["-fsS", "--proto", "=https,file", "--max-filesize"])
        .arg(MAX_IMAGE.to_string())
        .arg("-o")
        .arg(to)
        .arg(url)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let run = tokio::time::timeout(FETCH_TIMEOUT, cmd.output())
        .await
        .map_err(|_| "the image download timed out".to_string())?
        .map_err(|e| format!("curl: {e}"))?;
    if !run.status.success() {
        let _ = std::fs::remove_file(to);
        // curl's message names the URL, which may be a signed link: keep it
        // out of the answer and the logs.
        return Err(match run.status.code() {
            Some(22) => "the image server refused the download".into(),
            Some(63) => "the image is over 16 MB".into(),
            Some(c) => format!("the image download failed (curl {c})"),
            None => "the image download was stopped".into(),
        });
    }
    Ok(())
}

/// The image's hash and OTA header.
fn check(
    path: &Path,
    sha256: &str,
    version: Option<u64>,
) -> Result<zigbee_host::ota::OtaHeader, String> {
    use sha2::{Digest, Sha256};
    let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
    if bytes.len() as u64 > MAX_IMAGE {
        return Err("the image is over 16 MB".into());
    }
    let got = hex::encode(Sha256::digest(&bytes));
    if got != sha256 {
        return Err(format!(
            "the image's SHA-256 is {got}, not the one asked for"
        ));
    }
    let (_, header) =
        zigbee_host::ota::OtaHeader::find(&bytes).map_err(|e| format!("not an OTA image: {e}"))?;
    if let Some(v) = version {
        if u64::from(header.file_version) != v {
            return Err(format!(
                "the image is version {:#010x}, not {v:#010x}",
                header.file_version
            ));
        }
    }
    Ok(header)
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
