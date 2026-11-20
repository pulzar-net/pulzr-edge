//! IP cameras on the gateway's LAN, managed through their own settings API so
//! nobody has to open a camera's web page.
//!
//! Each camera is `ext:camera.<its MAC, 12 hex digits>`. Its device package
//! declares the commands below; the platform seals a package's secret params
//! (the passwords) and opens them only in the command it sends here.
//!
//! | Command | Params | Answer |
//! |---|---|---|
//! | `user.list` | | `ok {users: [{username, group, enabled}]}` |
//! | `user.add` | `username`, `password`, `group` (`viewer`, `operator`, `admin`) | `ok` |
//! | `user.password` | `username`, `password` | `ok` |
//! | `user.remove` | `username` | `ok` |
//!
//! A user's password never leaves the camera or this process: `user.list`
//! names users, their group and whether they are enabled. The gateway's own
//! login (`user` in the config) cannot be removed, demoted or re-passworded
//! here, which would lock the gateway out.
//!
//! Runs as its own process, `pulzr-edge camera <config.toml>`, with its own
//! buffer and MQTT session (client id `<adapter>-camera`) under the same
//! adapter and login.

pub mod ipc;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use pulzr_protocol as proto;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::outbox::{now, Outbox};

/// One `[[camera]]` in the configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CameraCfg {
    /// The camera's MAC, 12 hex digits (separators allowed); its key is
    /// `ext:camera.<mac>`.
    pub mac: String,
    /// Host or `host:port` of its web server on the LAN.
    pub address: String,
    /// The settings API it speaks. `ipc` is the only one so far.
    #[serde(default = "default_family")]
    pub family: String,
    /// The administrator login the gateway uses.
    #[serde(default = "default_user")]
    pub user: String,
    /// A file holding that login's password alone, read at every command so a
    /// new one is picked up without a restart.
    pub password_file: PathBuf,
}

fn default_family() -> String {
    "ipc".into()
}
fn default_user() -> String {
    "admin".into()
}

impl CameraCfg {
    pub fn key(&self) -> String {
        let mac: String = self.mac.chars().filter(|c| c.is_ascii_hexdigit()).collect();
        format!("ext:camera.{}", mac.to_ascii_lowercase())
    }

    pub fn validate(&self) -> Result<()> {
        let hex = self.mac.chars().filter(|c| c.is_ascii_hexdigit()).count();
        if hex != 12
            || self
                .mac
                .chars()
                .any(|c| !c.is_ascii_hexdigit() && c != ':' && c != '-')
        {
            bail!("camera mac {:?} must be 12 hex digits", self.mac);
        }
        if self.family != "ipc" {
            bail!(
                "camera {}: unknown family {:?} (ipc is the only one)",
                self.mac,
                self.family
            );
        }
        if self.address.is_empty() || self.address.contains(['/', ' ', '@']) {
            bail!("camera {}: address must be a host or host:port", self.mac);
        }
        Ok(())
    }
}

/// Answers the cameras' commands, one at a time per camera.
pub struct Router {
    cameras: HashMap<String, (CameraCfg, tokio::sync::Mutex<()>)>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UserParams {
    username: Option<String>,
    password: Option<String>,
    group: Option<String>,
}

type Answer = std::result::Result<Option<Map<String, Value>>, (&'static str, &'static str, String)>;

fn invalid(detail: impl Into<String>) -> (&'static str, &'static str, String) {
    ("refused", "invalid", detail.into())
}

fn username(p: &UserParams) -> std::result::Result<&str, (&'static str, &'static str, String)> {
    let u = p
        .username
        .as_deref()
        .ok_or_else(|| invalid("username is required"))?;
    if u.is_empty()
        || u.len() > 31
        || !u
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.')
    {
        return Err(invalid(
            "username: 1 to 31 letters, digits, '.', '_' or '-'",
        ));
    }
    Ok(u)
}

fn password(p: &UserParams) -> std::result::Result<&str, (&'static str, &'static str, String)> {
    let pw = p
        .password
        .as_deref()
        .ok_or_else(|| invalid("password is required"))?;
    // The camera's page allows at most 20; printable ASCII, no spaces.
    if !(8..=20).contains(&pw.len()) || !pw.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        return Err(invalid("password: 8 to 20 printable characters, no spaces"));
    }
    Ok(pw)
}

fn group(
    p: &UserParams,
) -> std::result::Result<&'static str, (&'static str, &'static str, String)> {
    let g = p.group.as_deref().unwrap_or("viewer");
    ipc::GROUPS
        .iter()
        .find(|(ours, _)| *ours == g)
        .map(|(_, theirs)| *theirs)
        .ok_or_else(|| invalid("group: viewer, operator or admin"))
}

fn failed(e: ipc::Error) -> (&'static str, &'static str, String) {
    match e {
        ipc::Error::Unreachable(_) => ("failed", "undeliverable", e.to_string()),
        _ => ("failed", "error", e.to_string()),
    }
}

impl Router {
    pub fn new(cameras: &[CameraCfg]) -> Self {
        Router {
            cameras: cameras
                .iter()
                .map(|c| (c.key(), (c.clone(), tokio::sync::Mutex::new(()))))
                .collect(),
        }
    }

    fn handle(self: &Arc<Self>, out: &Outbox, key: &str, cmd: proto::Cmd) {
        let id = String::from(cmd.id.clone());
        let name = String::from(cmd.name.clone());
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
        if !self.cameras.contains_key(key) {
            return out.cmd_res(
                key,
                &id,
                "failed",
                Some("undeliverable"),
                Some("not a camera of this gateway"),
                None,
            );
        }
        if cmd
            .expires
            .as_ref()
            .is_some_and(|t| f64::from(t.clone()) < now())
        {
            return out.cmd_res(key, &id, "failed", Some("expired"), None, None);
        }
        let params = serde_json::to_value(&cmd.params).unwrap_or(Value::Null);
        let params = if params.is_null() { json!({}) } else { params };
        let p: UserParams = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => {
                return out.cmd_res(
                    key,
                    &id,
                    "refused",
                    Some("invalid"),
                    Some(&e.to_string()),
                    None,
                )
            }
        };
        if !matches!(
            name.as_str(),
            "user.list" | "user.add" | "user.password" | "user.remove"
        ) {
            return out.cmd_res(
                key,
                &id,
                "refused",
                Some("unknown"),
                Some("a camera takes user.list, user.add, user.password and user.remove"),
                None,
            );
        }
        out.cmd_res(key, &id, "queued", None, None, None);
        let (me, out, key) = (self.clone(), out.clone(), key.to_string());
        tokio::spawn(async move {
            let (cfg, lock) = &me.cameras[&key];
            let _one = lock.lock().await;
            match run(cfg, &name, &p).await {
                Ok(data) => out.cmd_res(&key, &id, "ok", None, None, data),
                Err((status, reason, detail)) => {
                    out.cmd_res(&key, &id, status, Some(reason), Some(&detail), None)
                }
            }
        });
    }
}

async fn run(cfg: &CameraCfg, name: &str, p: &UserParams) -> Answer {
    // Check the request before touching the camera.
    let target = match name {
        "user.list" => None,
        _ => Some(username(p)?),
    };
    let new_password = match name {
        "user.add" | "user.password" => Some(password(p)?),
        _ => None,
    };
    let new_group = match name {
        "user.add" => Some(group(p)?),
        _ => None,
    };
    if let Some(u) = target {
        if name != "user.add" && u.eq_ignore_ascii_case(&cfg.user) {
            return Err(invalid(
                "that is the gateway's own login; change it in the gateway's camera settings",
            ));
        }
    }

    let login = std::fs::read_to_string(&cfg.password_file)
        .with_context(|| format!("reading {}", cfg.password_file.display()))
        .map_err(|e| ("failed", "error", e.to_string()))?;
    let cam = ipc::Camera::new(
        &cfg.address,
        &cfg.user,
        login.trim_end_matches(['\r', '\n']),
    );
    let users = cam.users().await.map_err(failed)?;
    let find = |u: &str| {
        users
            .iter()
            .find(|a| a.username.eq_ignore_ascii_case(u))
            .cloned()
    };

    match name {
        "user.list" => {
            let list: Vec<Value> = users
                .iter()
                .map(|a| {
                    let group = ipc::GROUPS
                        .iter()
                        .find(|(_, theirs)| *theirs == a.group)
                        .map_or(a.group.as_str(), |(ours, _)| ours);
                    json!({ "username": a.username, "group": group, "enabled": a.status == "Enable" })
                })
                .collect();
            let mut d = Map::new();
            d.insert("users".into(), Value::Array(list));
            Ok(Some(d))
        }
        "user.add" => {
            let (u, pw, g) = (target.unwrap(), new_password.unwrap(), new_group.unwrap());
            if find(u).is_some() {
                return Err(invalid(format!("the camera already has a user {u}")));
            }
            cam.add_user(u, pw, g).await.map_err(failed)?;
            Ok(None)
        }
        "user.password" => {
            let u = target.unwrap();
            let a = find(u).ok_or_else(|| invalid(format!("the camera has no user {u}")))?;
            cam.edit_user(&a.with_password(new_password.unwrap()))
                .await
                .map_err(failed)?;
            Ok(None)
        }
        _ => {
            let u = target.unwrap();
            let a = find(u).ok_or_else(|| invalid(format!("the camera has no user {u}")))?;
            cam.remove_user(&a).await.map_err(failed)?;
            Ok(None)
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
        if !key.starts_with("ext:camera.") {
            return; // another module's device
        }
        let raw: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(e) => return tracing::warn!(topic, error = %e, "command is not JSON"),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(mac: &str) -> CameraCfg {
        CameraCfg {
            mac: mac.into(),
            address: "192.168.8.20".into(),
            family: "ipc".into(),
            user: "admin".into(),
            password_file: "/nonexistent".into(),
        }
    }

    #[test]
    fn keys_and_config_checks() {
        assert_eq!(cfg("AA:BB:CC:00:11:22").key(), "ext:camera.aabbcc001122");
        pulzr_protocol::DeviceKey::try_from(cfg("aabbcc001122").key().as_str()).unwrap();
        assert!(cfg("aabbcc0011").validate().is_err());
        assert!(cfg("aabbcc001122").validate().is_ok());
    }

    fn params(v: Value) -> UserParams {
        serde_json::from_value(v).unwrap()
    }

    #[tokio::test]
    async fn requests_are_checked_before_the_camera_is_asked() {
        let c = cfg("aabbcc001122");
        // Each of these is refused without reading the password file or calling the camera.
        for (name, p, why) in [
            (
                "user.add",
                json!({"username": "site", "password": "short", "group": "viewer"}),
                "password",
            ),
            (
                "user.add",
                json!({"username": "a b", "password": "longenough1", "group": "viewer"}),
                "username",
            ),
            (
                "user.add",
                json!({"username": "site", "password": "longenough1", "group": "root"}),
                "group",
            ),
            ("user.remove", json!({"username": "ADMIN"}), "own login"),
            (
                "user.password",
                json!({"username": "admin", "password": "longenough1"}),
                "own login",
            ),
        ] {
            let e = run(&c, name, &params(p)).await.unwrap_err();
            assert_eq!((e.0, e.1), ("refused", "invalid"), "{name}");
            assert!(e.2.contains(why), "{name}: {}", e.2);
        }
        // A good request goes on to the camera: here, the missing password file.
        let e = run(
            &c,
            "user.add",
            &params(json!({"username": "site", "password": "longenough1"})),
        )
        .await
        .unwrap_err();
        assert_eq!(e.0, "failed");
    }

    /// Against a real camera, which it changes and then puts back:
    /// `CAMERA_ADDRESS=10.0.0.112 CAMERA_PASSWORD_FILE=pw cargo test -p pulzr-edge -- --ignored a_viewer_made_here`.
    /// With ffprobe on the PATH it also plays the viewer's RTSP stream.
    #[tokio::test]
    #[ignore]
    async fn a_viewer_made_here_can_watch_rtsp() {
        let (Ok(address), Ok(file)) = (
            std::env::var("CAMERA_ADDRESS"),
            std::env::var("CAMERA_PASSWORD_FILE"),
        ) else {
            panic!("set CAMERA_ADDRESS and CAMERA_PASSWORD_FILE");
        };
        let c = CameraCfg {
            address: address.clone(),
            password_file: file.into(),
            ..cfg("aabbcc001122")
        };
        let (user, first, second) = ("pulzrtest", "Viewer-test-1", "Viewer-test-2");
        let names = |d: Option<Map<String, Value>>| d.unwrap()["users"].clone();
        let listed = names(run(&c, "user.list", &params(json!({}))).await.unwrap());
        assert!(
            !listed.to_string().contains(user),
            "left over from an earlier run: {listed}"
        );

        run(
            &c,
            "user.add",
            &params(json!({"username": user, "password": first, "group": "viewer"})),
        )
        .await
        .unwrap();
        let listed = names(run(&c, "user.list", &params(json!({}))).await.unwrap());
        assert!(
            listed
                .as_array()
                .unwrap()
                .contains(&json!({"username": user, "group": "viewer", "enabled": true})),
            "{listed}"
        );
        let again = run(
            &c,
            "user.add",
            &params(json!({"username": user, "password": first})),
        )
        .await
        .unwrap_err();
        assert!(again.2.contains("already"), "{}", again.2);

        run(
            &c,
            "user.password",
            &params(json!({"username": user, "password": second})),
        )
        .await
        .unwrap();
        let plays = |pw: &str| {
            std::process::Command::new("ffprobe")
                .args([
                    "-v",
                    "error",
                    "-rtsp_transport",
                    "tcp",
                    "-show_entries",
                    "stream=codec_name",
                    "-of",
                    "csv=p=0",
                ])
                .arg(format!("rtsp://{user}:{pw}@{address}:554/stream1"))
                .output()
                .map(|o| o.status.success())
        };
        let watched = plays(second);
        let refused_old = plays(first);
        run(&c, "user.remove", &params(json!({"username": user})))
            .await
            .unwrap();
        let listed = names(run(&c, "user.list", &params(json!({}))).await.unwrap());
        assert!(!listed.to_string().contains(user), "{listed}");
        if let (Ok(w), Ok(o)) = (watched, refused_old) {
            assert!(w, "the viewer could not play RTSP");
            assert!(!o, "RTSP still took the old password");
        }
    }
}
