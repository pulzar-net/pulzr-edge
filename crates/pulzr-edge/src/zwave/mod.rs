//! Z-Wave: one Serial API controller as a radio module (PulzrOS
//! `docs/radio-modules.md`), on zwave-bridge's host stack.
//!
//! The network's lifecycle (form, restore, backup, leave, inclusion with S2
//! and SmartStart, exclusion, replace, region) arrives only through
//! [`RadioModule::call`], from the gateway's console API. What happens on the
//! network goes out as protocol v1 on keys `zwave:<home id>.<node id>`:
//! readings as `up` with `link.zwave`, nodes' `state` with their interview,
//! the coordinator's `state.network`, `event`s and `topology`. Device intents
//! (`settings.set`, `zwave.set`, `zwave.device_file`, `zwave.ping`,
//! `zwave.firmware`, an image fetched from a signed link) arrive
//! as `cmd` and answer `queued` while a sleeping node is asleep.
//!
//! The radio's file holds the platform's network id, the region and the
//! keys (`keys`, zwave-bridge's `NetworkKeys`, the controller's S2 key pair
//! included). The node store lives in the module's data directory. No key
//! is ever logged, published or put in an op's answer, except in `backup`,
//! which the console sends on to the platform over HTTPS.

use crate::outbox::{now, Outbox};
use crate::radio::{BoxFuture, RadioCtx, RadioError, RadioModule};
use base64::Engine as _;
use pulzr_protocol as proto;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use zwave_host::device_config::{device_id, Library, MapSource};
use zwave_host::node::InterviewStage;
use zwave_host::{
    Controller, Event, GrantPolicy, HostConfig, InclusionOptions, NetworkKeys, NodeId, NodeRecord,
    NodeStatus, SecurityClass, ValueId, ValueKind, ValueUpdate,
};
use zwave_security::dsk::{parse_pin, Dsk};
use zwave_security::provisioning::{Entry, EntryStatus};
use zwave_serial::message::RfRegion;
use zwave_serial::{ControllerIdentity, SerialApi};

/// Backups after a change at most this often.
const BACKUP_SPACING: Duration = Duration::from_secs(60);
/// And at least this often.
const BACKUP_EVERY: Duration = Duration::from_secs(24 * 3600);
/// How long a node has to press its button for exclusion or replace.
const EXCLUSION_WAIT: Duration = Duration::from_secs(60);
const REPLACE_WAIT: Duration = Duration::from_secs(180);
/// The largest device file set `zwave.device_file` takes.
const DEVICE_FILES_MAX: usize = 512 * 1024;

/// The network key names, as `form` and the radio's file spell them.
const KEY_NAMES: [&str; 6] = [
    "s0",
    "s2_unauthenticated",
    "s2_authenticated",
    "s2_access_control",
    "lr_s2_authenticated",
    "lr_s2_access_control",
];

/// RF regions by the short names the console uses.
const REGIONS: [(&str, RfRegion); 12] = [
    ("EU", RfRegion::Europe),
    ("US", RfRegion::Usa),
    ("ANZ", RfRegion::AustraliaNewZealand),
    ("HK", RfRegion::HongKong),
    ("IN", RfRegion::India),
    ("IL", RfRegion::Israel),
    ("RU", RfRegion::Russia),
    ("CN", RfRegion::China),
    ("US_LR", RfRegion::UsaLongRange),
    ("EU_LR", RfRegion::EuropeLongRange),
    ("JP", RfRegion::Japan),
    ("KR", RfRegion::Korea),
];

fn region_by_name(s: &str) -> Option<RfRegion> {
    REGIONS
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(s.trim()))
        .map(|(_, r)| *r)
}

fn region_name(r: RfRegion) -> Option<&'static str> {
    REGIONS.iter().find(|(_, x)| *x == r).map(|(n, _)| *n)
}

/// zwave-host's `ControllerInfo.region` is the region's variant name.
fn region_from_info(s: &str) -> Option<&'static str> {
    REGIONS
        .iter()
        .find(|(_, r)| format!("{r:?}") == s)
        .map(|(n, _)| *n)
}

pub fn key(home: u32, node: NodeId) -> String {
    format!("zwave:{home:08x}.{node}")
}

fn parse_key(k: &str) -> Option<(u32, NodeId)> {
    let (home, node) = k.strip_prefix("zwave:")?.split_once('.')?;
    if home.len() != 8 {
        return None;
    }
    Some((u32::from_str_radix(home, 16).ok()?, node.parse().ok()?))
}

fn class_name(c: SecurityClass) -> &'static str {
    match c {
        SecurityClass::S0Legacy => "s0",
        other => other.name(),
    }
}

fn class_by_name(s: &str) -> Option<SecurityClass> {
    match s {
        "s0" | "s0_legacy" => Some(SecurityClass::S0Legacy),
        "s2_unauthenticated" => Some(SecurityClass::S2Unauthenticated),
        "s2_authenticated" => Some(SecurityClass::S2Authenticated),
        "s2_access_control" => Some(SecurityClass::S2AccessControl),
        _ => None,
    }
}

fn classes(v: &Value) -> Result<Vec<SecurityClass>, RadioError> {
    let list = v
        .as_array()
        .ok_or_else(|| bad("classes must be a list of security classes"))?;
    let mut out = Vec::new();
    for c in list {
        let c = c
            .as_str()
            .and_then(class_by_name)
            .ok_or_else(|| bad(&format!("{c} is not a security class")))?;
        if !out.contains(&c) {
            out.push(c);
        }
    }
    Ok(out)
}

fn bad(m: &str) -> RadioError {
    RadioError::BadRequest(m.to_string())
}
fn refused(m: &str) -> RadioError {
    RadioError::Refused(m.to_string())
}

fn host_error(e: zwave_host::Error) -> RadioError {
    match e {
        zwave_host::Error::Busy => {
            RadioError::Busy("Another inclusion, exclusion or replace is running.".into())
        }
        other => RadioError::Refused(other.to_string()),
    }
}

/// The 16-byte keys `form` takes, 32 hex each; never echoed.
fn parse_keys(v: &Value) -> Result<NetworkKeys, RadioError> {
    let mut k = [[0u8; 16]; 6];
    for (i, name) in KEY_NAMES.iter().enumerate() {
        let s = v
            .get(*name)
            .and_then(Value::as_str)
            .ok_or_else(|| bad(&format!("keys.{name} is missing")))?;
        let bytes = hex::decode(s.trim())
            .ok()
            .filter(|b| b.len() == 16)
            .ok_or_else(|| bad(&format!("keys.{name} is not 32 hex digits")))?;
        k[i].copy_from_slice(&bytes);
    }
    if (0..6).any(|i| (i + 1..6).any(|j| k[i] == k[j])) {
        return Err(bad("each network key must differ from the others"));
    }
    let ecdh_private = zwave_security::random_bytes::<32>()
        .map_err(|_| refused("The gateway has no random source for the controller's key pair."))?;
    Ok(NetworkKeys {
        s0: k[0],
        s2_unauthenticated: k[1],
        s2_authenticated: k[2],
        s2_access_control: k[3],
        ecdh_private,
        lr_s2_authenticated: Some(k[4]),
        lr_s2_access_control: Some(k[5]),
    })
}

fn valid_network_id(s: &str) -> bool {
    (1..=64).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

fn random_id() -> String {
    zwave_security::random_bytes::<8>()
        .map(hex::encode)
        .unwrap_or_else(|_| format!("{:016x}", now().to_bits()))
}

enum Link {
    /// Not open (starting, or the port failed).
    None,
    /// No network yet: the port held open, identified read-only.
    Idle(SerialApi),
    /// A network on the controller.
    Up(Controller),
}

struct St {
    link: Link,
    /// Bumped per controller started, so a stopped one's events are ignored.
    generation: u64,
    /// `unformed`, `forming`, `up`, `down` or `restoring`.
    state: &'static str,
    ident: Option<ControllerIdentity>,
    home_id: Option<u32>,
    controller_node: NodeId,
    network: Option<String>,
    region: Option<String>,
    joining_until: Option<f64>,
    inclusion: Option<Value>,
    /// The request id of the running inclusion or replace.
    request: Option<String>,
    busy: bool,
    leave_reason: HashMap<NodeId, &'static str>,
    backup_at: Option<f64>,
    backup_due: bool,
    last_backup: Instant,
    topology_due: bool,
    /// Settable values by node and field id, for `zwave.set`.
    writable: HashMap<(NodeId, String), (ValueId, ValueKind)>,
    /// A `map` walk is running.
    mapping: bool,
    /// Firmware updates running: the version and the last tenth reported.
    firmware: HashMap<NodeId, (Option<String>, u16)>,
    error: Option<String>,
}

pub struct ZWave {
    ctx: RadioCtx,
    me: Weak<ZWave>,
    st: Mutex<St>,
}

/// `radio::factory`'s entry for `zwave`.
pub fn start(ctx: RadioCtx) -> anyhow::Result<Arc<dyn RadioModule>> {
    let m = Arc::new_cyclic(|me| ZWave {
        ctx,
        me: me.clone(),
        st: Mutex::new(St {
            link: Link::None,
            generation: 0,
            state: "down",
            ident: None,
            home_id: None,
            controller_node: 1,
            network: None,
            region: None,
            joining_until: None,
            inclusion: None,
            request: None,
            busy: false,
            leave_reason: HashMap::new(),
            backup_at: None,
            backup_due: false,
            last_backup: Instant::now(),
            topology_due: false,
            writable: HashMap::new(),
            firmware: HashMap::new(),
            mapping: false,
            error: None,
        }),
    });
    tokio::spawn(m.clone().boot());
    tokio::spawn(m.clone().housekeeping());
    Ok(m)
}

impl ZWave {
    fn me(&self) -> Arc<ZWave> {
        self.me.upgrade().expect("module alive")
    }

    fn store_path(&self) -> PathBuf {
        self.ctx.file("zwave").join("nodes.json")
    }

    fn devices_dir(&self) -> PathBuf {
        self.ctx.file("zwave").join("devices")
    }

    fn file(&self) -> Value {
        self.ctx
            .load()
            .unwrap_or_else(|_| json!({ "radio": self.ctx.radio, "protocol": "zwave" }))
    }

    fn file_keys(&self) -> Option<NetworkKeys> {
        serde_json::from_value(self.file().get("keys")?.clone()).ok()
    }

    fn controller(&self) -> Result<Controller, RadioError> {
        match &self.st.lock().unwrap().link {
            Link::Up(c) => Ok(c.clone()),
            _ => Err(refused("No network on this radio yet.")),
        }
    }

    fn coordinator(&self) -> Option<String> {
        let st = self.st.lock().unwrap();
        st.home_id.map(|h| key(h, st.controller_node))
    }

    fn event(&self, name: &str, data: Value) {
        if let Some(k) = self.coordinator() {
            self.ctx.event(&k, name, data);
        }
    }

    /// Opens the radio as its file says, retrying while the port is missing.
    async fn boot(self: Arc<Self>) {
        loop {
            let r = match self.file_keys() {
                Some(keys) => self.start_controller(keys, true).await.map(|_| ()),
                None => self.open_idle().await,
            };
            match r {
                Ok(()) => return,
                Err(e) => {
                    tracing::warn!(radio = %self.ctx.radio, error = %e, "Z-Wave controller not reachable; retrying");
                    let mut st = self.st.lock().unwrap();
                    st.state = "down";
                    st.error = Some(e);
                }
            }
            tokio::time::sleep(Duration::from_secs(15)).await;
        }
    }

    /// Holds the port with no network: identifies the controller read-only.
    async fn open_idle(&self) -> Result<(), String> {
        let port = zwave_serial::port::open(&self.ctx.port)
            .await
            .map_err(|e| format!("{}: {e}", self.ctx.port))?;
        let (api, mut unsolicited) = SerialApi::start(port, zwave_serial::Config::default());
        tokio::spawn(async move { while unsolicited.recv().await.is_some() {} });
        let ident = zwave_serial::probe::identify(&api).await;
        if ident.is_none() {
            api.close();
            return Err("the radio does not answer as a Z-Wave controller".into());
        }
        let mut st = self.st.lock().unwrap();
        st.home_id = None;
        st.network = None;
        st.region = ident
            .as_ref()
            .and_then(|i| i.region)
            .and_then(region_name)
            .map(String::from);
        st.ident = ident;
        st.state = "unformed";
        st.error = None;
        st.link = Link::Idle(api);
        Ok(())
    }

    /// Starts the host stack on the radio with `keys` and the node store.
    /// `announce` publishes the network and its nodes; a start that only
    /// prepares the stick (before an erase or a restore) stays quiet.
    async fn start_controller(
        &self,
        keys: NetworkKeys,
        announce: bool,
    ) -> Result<Controller, String> {
        let dir = self.ctx.file("zwave");
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
        }
        let port = zwave_serial::port::open(&self.ctx.port)
            .await
            .map_err(|e| format!("{}: {e}", self.ctx.port))?;
        let cfg = HostConfig::new(keys, Some(self.store_path()));
        let (c, events) = Controller::start(port, cfg)
            .await
            .map_err(|e| e.to_string())?;
        let info = c.info();
        let file = self.file();
        let generation = {
            let mut st = self.st.lock().unwrap();
            st.generation += 1;
            st.home_id = Some(info.home_id);
            st.controller_node = info.node_id;
            st.network = file["network"].as_str().map(String::from);
            st.region = file["region"].as_str().map(String::from).or_else(|| {
                info.region
                    .as_deref()
                    .and_then(region_from_info)
                    .map(String::from)
            });
            if announce {
                st.state = "up";
                st.topology_due = true;
            }
            st.error = None;
            st.link = Link::Up(c.clone());
            st.generation
        };
        tokio::spawn(self.me().pump(generation, info.home_id, events));
        if !announce {
            return Ok(c);
        }
        self.publish_network();
        for rec in c.nodes() {
            if rec.id != info.node_id {
                self.publish_node(&c, info.home_id, rec.id);
            }
        }
        tracing::info!(radio = %self.ctx.radio, home_id = %format!("{:08x}", info.home_id), nodes = info.nodes.len(), "Z-Wave network up");
        Ok(c)
    }

    /// Stops whatever holds the port.
    fn close_link(&self) {
        let link = std::mem::replace(&mut self.st.lock().unwrap().link, Link::None);
        match link {
            Link::Up(c) => {
                if let Err(e) = c.close() {
                    tracing::warn!(error = %e, "node store not written on close");
                }
            }
            Link::Idle(api) => api.close(),
            Link::None => {}
        }
    }

    /// Restarts the stack after the controller reset itself (region, restore).
    async fn restart(&self, keys: NetworkKeys) -> Result<Controller, String> {
        self.close_link();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let mut last = String::new();
        for _ in 0..5 {
            match self.start_controller(keys.clone(), true).await {
                Ok(c) => return Ok(c),
                Err(e) => last = e,
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Err(last)
    }

    // ------------------------------------------------------------ status

    fn status(&self) -> Value {
        let st = self.st.lock().unwrap();
        let (firmware, devices, series) = match &st.link {
            Link::Up(c) => {
                let i = c.info();
                (
                    Some(i.firmware.clone()),
                    Some(i.nodes.len()),
                    i.chip_type.and_then(series_of),
                )
            }
            _ => (
                st.ident.as_ref().map(|i| i.firmware.clone()),
                st.ident.as_ref().and_then(|i| i.node_count),
                st.ident
                    .as_ref()
                    .and_then(|i| i.chip.map(|c| c.0))
                    .and_then(series_of),
            ),
        };
        let up = matches!(st.link, Link::Up(_));
        let mut s = json!({
            "radio": self.ctx.radio,
            "protocol": "zwave",
            "network": st.network,
            "state": st.state,
            "firmware": firmware,
            "series": series,
            "coordinator": if up { st.home_id.map(|h| key(h, st.controller_node)) } else { None },
            "channel": null, "pan_id": null, "ext_pan_id": null,
            "region": st.region,
            "home_id": if up { st.home_id.map(|h| format!("{h:08x}")) } else { None },
            "devices": devices,
            "joining": st.joining_until.map(|u| json!({ "until": u, "via": null })),
            "inclusion": st.inclusion,
            "backup_at": st.backup_at,
        });
        if let Some(i) = &st.ident {
            s["controller"] = json!({
                "manufacturer": i.manufacturer_id, "product_type": i.product_type,
                "product_id": i.product_id, "name": i.known_name(),
            });
        }
        if let Some(e) = &st.error {
            s["error"] = json!(e);
        }
        s
    }

    /// The coordinator's `state.network`, whenever it changes.
    fn publish_network(&self) {
        let st = self.st.lock().unwrap();
        let (Some(home), Link::Up(c)) = (st.home_id, &st.link) else {
            return;
        };
        let state = match st.state {
            "unformed" => return,
            s => s,
        };
        let mut n = json!({
            "protocol": "zwave", "state": state, "radio": self.ctx.radio,
            "home_id": format!("{home:08x}"), "devices": c.info().nodes.len(),
        });
        if let Some(r) = &st.region {
            n["region"] = json!(r);
        }
        if let Some(t) = st.joining_until {
            n["joining_until"] = json!(t);
        }
        if let Some(t) = st.backup_at {
            n["backup_at"] = json!(t);
        }
        let k = key(home, st.controller_node);
        drop(st);
        self.ctx.network_state(&k, n);
    }

    /// A node's `state`: online, firmware, its interview and diagnostics.
    fn publish_node(&self, c: &Controller, home: u32, node: NodeId) {
        let Some(rec) = c.node(node) else { return };
        let status = c.status(node);
        let mut body = json!({
            "online": status != NodeStatus::Dead,
            "diag": diag(c, &rec, status),
        });
        if let Some(fw) = rec.firmware.as_deref().filter(|f| firmware_ok(f)) {
            body["fw"] = json!(fw);
        }
        if let Some(i) = interview(&rec) {
            body["interview"] = json!({ "zwave": i });
        }
        self.ctx.out.state_body(&key(home, node), body);
    }

    // ------------------------------------------------------------ events

    async fn pump(self: Arc<Self>, generation: u64, home: u32, mut events: zwave_host::Events) {
        while let Some(e) = events.recv().await {
            let c = {
                let st = self.st.lock().unwrap();
                if st.generation != generation {
                    return;
                }
                if st.state != "up" {
                    continue; // a stick being prepared for form or restore
                }
                match &st.link {
                    Link::Up(c) => c.clone(),
                    _ => return,
                }
            };
            self.on_event(&c, home, e);
        }
    }

    fn on_event(&self, c: &Controller, home: u32, e: Event) {
        let out = &self.ctx.out;
        match e {
            Event::InclusionStarted => {
                let until = self.st.lock().unwrap().joining_until;
                if let Some(until) = until {
                    self.event("network.permit", json!({ "until": until, "via": null }));
                    self.publish_network();
                }
            }
            Event::Ready(_) | Event::InclusionStopped => {}
            Event::ExclusionStarted | Event::ExclusionStopped => {}
            Event::NodeAdded { node } => {
                self.event(
                    "network.joined",
                    json!({ "device": key(home, node), "via": null, "rssi": null }),
                );
                self.publish_node(c, home, node);
                self.publish_network();
                self.changed();
            }
            Event::NodeRemoved { node } => {
                let reason = self
                    .st
                    .lock()
                    .unwrap()
                    .leave_reason
                    .remove(&node)
                    .unwrap_or("removed");
                {
                    let mut st = self.st.lock().unwrap();
                    st.writable.retain(|(n, _), _| *n != node);
                }
                self.event(
                    "network.left",
                    json!({ "device": key(home, node), "reason": reason }),
                );
                out.state_body(&key(home, node), json!({ "online": false }));
                self.publish_network();
                self.changed();
            }
            Event::GrantSecurityClasses { node, requested } => {
                let i = {
                    let mut st = self.st.lock().unwrap();
                    let i = json!({
                        "request": st.request, "stage": "grant", "device": key(home, node),
                        "requested": requested.iter().map(|c| class_name(*c)).collect::<Vec<_>>(),
                        "client_auth": false,
                    });
                    st.inclusion = Some(i.clone());
                    i
                };
                self.event("network.inclusion", i);
            }
            Event::ValidateDsk { node, dsk } => {
                // zwave-host masks the PIN digits already (`xxxxx-...`).
                let i = {
                    let mut st = self.st.lock().unwrap();
                    let mut i = st.inclusion.clone().unwrap_or_else(
                        || json!({ "request": st.request, "device": key(home, node) }),
                    );
                    i["stage"] = json!("dsk");
                    i["dsk"] = json!(dsk);
                    st.inclusion = Some(i.clone());
                    i
                };
                self.event("network.inclusion", i);
            }
            Event::SecurityBootstrapped { node, classes } => {
                let i = {
                    let mut st = self.st.lock().unwrap();
                    let mut i = st.inclusion.take().unwrap_or_else(
                        || json!({ "request": st.request, "device": key(home, node) }),
                    );
                    i["stage"] = json!("done");
                    i["granted"] =
                        json!(classes.iter().map(|c| class_name(*c)).collect::<Vec<_>>());
                    i
                };
                self.event("network.inclusion", i);
                self.changed();
            }
            Event::SecurityFailed { node, reason } => {
                let i = {
                    let mut st = self.st.lock().unwrap();
                    let mut i = st.inclusion.take().unwrap_or_else(
                        || json!({ "request": st.request, "device": key(home, node) }),
                    );
                    i["stage"] = json!("failed");
                    i["reason"] = json!(reason);
                    i
                };
                self.event("network.inclusion", i);
            }
            Event::InterviewStage { node, stage } => {
                if stage >= InterviewStage::Identity {
                    self.publish_node(c, home, node);
                }
            }
            Event::InterviewCompleted { node } => {
                self.publish_node(c, home, node);
                let device = key(home, node);
                self.event(
                    "network.interview",
                    json!({ "device": device, "complete": true }),
                );
                self.event("network.configured", json!({ "device": device }));
                self.st.lock().unwrap().topology_due = true;
            }
            Event::InterviewFailed { node, reason } => {
                self.event(
                    "network.interview",
                    json!({ "device": key(home, node), "complete": false, "reason": reason }),
                );
            }
            Event::NodeUpdated { node } => self.publish_node(c, home, node),
            Event::NodeStatus { node, status } => {
                if let Some(rec) = c.node(node) {
                    let body = json!({
                        "online": status != NodeStatus::Dead,
                        "diag": diag(c, &rec, status),
                    });
                    out.state_body(&key(home, node), body);
                }
            }
            Event::Value(v) => self.reading(c, home, v),
            Event::Route { .. } => self.st.lock().unwrap().topology_due = true,
            Event::Command { node, command, .. } => {
                tracing::debug!(node, cc = command.cc(), "command not turned into a value");
            }
            Event::FirmwareUpdateProgress { node, sent, total } if total > 0 => {
                // `state.ota` every tenth of the way, not every fragment.
                let tenth = sent.saturating_mul(10) / total;
                let version = {
                    let mut st = self.st.lock().unwrap();
                    match st.firmware.get_mut(&node) {
                        Some((v, last)) if tenth > *last => {
                            *last = tenth;
                            Some(v.clone())
                        }
                        _ => None,
                    }
                };
                if let Some(v) = version {
                    let mut o = json!({ "phase": "installing", "progress": f64::from(sent) / f64::from(total) });
                    if let Some(v) = v {
                        o["version"] = json!(v);
                    }
                    out.state_body(&key(home, node), json!({ "ota": o }));
                }
            }
            Event::FirmwareUpdateProgress { .. } | Event::FirmwareUpdateFinished { .. } => {}
            Event::ProvisioningChanged { .. } => {}
        }
    }

    /// A reported value as an `up`: `r` with one field, `rx`, `link.zwave`.
    fn reading(&self, c: &Controller, home: u32, v: ValueUpdate) {
        let field = field_id(&v.id);
        let value = match &v.value {
            ValueKind::Bool(b) => json!(b),
            ValueKind::Number(n) if n.is_finite() => json!(n),
            ValueKind::Number(_) => return,
            ValueKind::Level(l) => json!(l),
            ValueKind::State { label, .. } => json!(label),
        };
        if v.writable {
            self.st
                .lock()
                .unwrap()
                .writable
                .insert((v.id.node, field.clone()), (v.id.clone(), v.value.clone()));
        }
        let ts = now();
        let mut rx = json!({ "via": self.ctx.out.via(), "ts": ts, "time": "received" });
        let mut link = json!({ "node": v.id.node, "endpoint": v.id.endpoint, "cc": v.id.cc });
        if let Some(r) = v.rssi {
            rx["rssi"] = json!(r);
            link["rssi"] = json!(r);
        }
        if let Some((reps, _)) = c.last_route(v.id.node) {
            // zwave-host lists repeaters from the controller outwards; the
            // protocol has the one nearest the controller last.
            let reps: Vec<NodeId> = reps.into_iter().rev().take(4).collect();
            link["repeaters"] = json!(reps);
        }
        let mut r = Map::new();
        r.insert(field, value);
        self.ctx.out.up_body(
            &key(home, v.id.node),
            json!({ "r": [ { "ts": ts, "v": r } ], "rx": [rx], "link": { "zwave": link } }),
        );
    }

    /// The network's links on the coordinator's key: neighbors from the
    /// controller's routing table, routes from the last transmissions.
    fn publish_topology(&self, c: &Controller, home: u32, source: &str) {
        let t = c.topology();
        let ts = now();
        let mut edges = Vec::new();
        let mut pairs = BTreeSet::new();
        for n in &t.nodes {
            for &nb in &n.neighbors {
                if nb != n.id && pairs.insert((n.id.min(nb), n.id.max(nb))) {
                    edges.push(json!({ "from": key(home, n.id), "to": key(home, nb), "kind": "neighbor", "ts": ts }));
                }
            }
        }
        for n in &t.nodes {
            if n.id == t.controller {
                continue;
            }
            // Long Range is a star: each node talks to the controller directly.
            let direct = Vec::new();
            let reps = match &n.last_route {
                Some(r) if !zwave_host::is_long_range(n.id) => r,
                _ if zwave_host::is_long_range(n.id) => &direct,
                _ => continue,
            };
            let mut path = vec![n.id];
            path.extend(reps.iter().rev());
            path.push(t.controller);
            for (i, w) in path.windows(2).enumerate() {
                let mut e = json!({ "from": key(home, w[0]), "to": key(home, w[1]), "kind": "route", "ts": ts });
                if i == path.len() - 2 {
                    if let Some(r) = n.last_rssi {
                        e["quality"] = json!({ "rssi": r });
                    }
                }
                edges.push(e);
            }
        }
        edges.truncate(2000);
        self.ctx.out.topology(
            &key(home, t.controller),
            json!({ "ts": ts, "protocol": "zwave", "source": source, "complete": true, "edges": edges }),
        );
    }

    fn changed(&self) {
        let mut st = self.st.lock().unwrap();
        st.backup_due = true;
        st.topology_due = true;
    }

    /// Backups after changes and daily; topology when links changed.
    async fn housekeeping(self: Arc<Self>) {
        let mut tick = tokio::time::interval(Duration::from_secs(10));
        loop {
            tick.tick().await;
            let (c, home, topology, backup) = {
                let mut st = self.st.lock().unwrap();
                let (Link::Up(c), Some(home)) = (&st.link, st.home_id) else {
                    continue;
                };
                let c = c.clone();
                let topology = std::mem::take(&mut st.topology_due);
                let since = st.last_backup.elapsed();
                let backup = !st.busy
                    && st.state == "up"
                    && ((st.backup_due && since >= BACKUP_SPACING) || since >= BACKUP_EVERY);
                (c, home, topology, backup)
            };
            if topology {
                self.publish_topology(&c, home, "passive");
            }
            if backup {
                match self.take_backup(&c).await {
                    Ok(b) => {
                        if let Err(e) = self.ctx.backup(&b) {
                            tracing::warn!(error = %e, "backup not handed on");
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "backup failed");
                        self.event(
                            "network.error",
                            json!({ "message": format!("backup failed: {e}") }),
                        );
                    }
                }
            }
        }
    }

    /// The backup object: the controller's NVM, the keys and the node store.
    async fn take_backup(&self, c: &Controller) -> Result<Value, String> {
        let image = c.backup_nvm().await.map_err(|e| e.to_string())?;
        let _ = c.flush();
        let info = c.info();
        let file = self.file();
        let store: Value = std::fs::read(self.store_path())
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or(Value::Null);
        let at = now();
        {
            let mut st = self.st.lock().unwrap();
            st.backup_at = Some(at);
            st.backup_due = false;
            st.last_backup = Instant::now();
        }
        self.publish_network();
        Ok(json!({
            "v": 1, "protocol": "zwave", "network": file["network"], "radio": self.ctx.radio,
            "taken_at": at, "firmware": info.firmware,
            "zwave": {
                "series": info.chip_type.and_then(series_of),
                "home_id": format!("{:08x}", info.home_id),
                "nvm": base64::engine::general_purpose::STANDARD.encode(&image),
                "keys": file["keys"],
                "region": file["region"],
                "store": store,
            }
        }))
    }

    // ------------------------------------------------------------ ops

    async fn op(&self, op: &str, p: Value) -> Result<Value, RadioError> {
        match op {
            "status" => Ok(self.status()),
            "form" => self.form(p).await,
            "restore" => self.restore(p).await,
            "backup" => {
                let c = self.controller()?;
                let b = self.take_backup(&c).await.map_err(|e| refused(&e))?;
                Ok(json!({ "backup": b }))
            }
            "leave" => self.leave(p).await,
            "permit" => self.permit(p),
            "permit-stop" | "abort" => {
                let c = self.controller()?;
                c.stop_inclusion();
                Ok(json!({ "until": null }))
            }
            "provision" => self.provision(p).await,
            "grant" => {
                let c = self.controller()?;
                self.check_request(&p)?;
                let granted = classes(&p["classes"])?;
                if !c.grant_security_classes(granted) {
                    return Err(refused("The inclusion is not waiting for keys."));
                }
                Ok(json!({}))
            }
            "dsk" => {
                let c = self.controller()?;
                self.check_request(&p)?;
                let pin = match &p["pin"] {
                    Value::String(s) => parse_pin(s).ok(),
                    Value::Number(n) => n.as_u64().filter(|n| *n <= 65535).map(|n| n as u16),
                    _ => None,
                }
                .ok_or_else(|| bad("pin must be the five digits under the device's QR code"))?;
                if !c.validate_dsk(Some(pin)) {
                    return Err(refused("The inclusion is not waiting for a PIN."));
                }
                Ok(json!({}))
            }
            "remove" => self.remove(p).await,
            "replace" => self.replace(p),
            "region" => self.set_region(p).await,
            "map" => self.map(),
            "scan" | "channel" => Err(refused(
                "Z-Wave has no channels: the region sets its frequencies.",
            )),
            _ => Err(bad(&format!("No such op: {op}."))),
        }
    }

    fn check_request(&self, p: &Value) -> Result<(), RadioError> {
        let st = self.st.lock().unwrap();
        match (&st.request, p.get("request").and_then(Value::as_str)) {
            (Some(r), Some(q)) if r == q => Ok(()),
            (None, _) => Err(refused("No inclusion is waiting.")),
            _ => Err(refused("That inclusion is over; another one is running.")),
        }
    }

    async fn form(&self, p: Value) -> Result<Value, RadioError> {
        let network = p["network"]
            .as_str()
            .filter(|s| valid_network_id(s))
            .ok_or_else(|| bad("network must be the platform's network id"))?
            .to_string();
        let keys = parse_keys(&p["keys"])?;
        let region = match p.get("region").filter(|v| !v.is_null()) {
            Some(v) => Some(v.as_str().and_then(region_by_name).ok_or_else(|| {
                bad("region is not one of EU, US, ANZ, HK, IN, IL, RU, CN, US_LR, EU_LR, JP, KR")
            })?),
            None => None,
        };
        let erase = p["erase"].as_bool().unwrap_or(false);
        let api = {
            let st = self.st.lock().unwrap();
            match (&st.link, st.state) {
                (Link::Idle(api), "unformed") => api.clone(),
                (Link::Up(_), _) => {
                    return Err(refused(
                        "A network is already on this radio; leave it first.",
                    ))
                }
                _ => return Err(refused("The radio is not ready; try again in a moment.")),
            }
        };
        let ident = zwave_serial::probe::identify(&api)
            .await
            .ok_or_else(|| refused("The radio does not answer as a Z-Wave controller."))?;
        let nodes = ident.node_count.unwrap_or(0);
        if nodes > 0 && !erase {
            return Err(refused(&format!(
                "The controller already has {nodes} devices from another network. Form with erase to wipe them; this cannot be undone."
            )));
        }
        if ident.primary == Some(false) && !erase {
            return Err(refused(
                "The controller is a secondary controller of another network. Form with erase to reset it.",
            ));
        }
        self.st.lock().unwrap().state = "forming";
        self.close_link();
        let _ = std::fs::remove_dir_all(self.ctx.file("zwave"));
        let r = self.form_on_radio(&network, keys, region, erase).await;
        if let Err(e) = &r {
            tracing::warn!(error = %e, "form failed");
            self.close_link();
            let _ = self.open_idle().await;
        }
        r.map_err(|e| refused(&format!("The network was not formed: {e}")))?;
        Ok(self.status())
    }

    async fn form_on_radio(
        &self,
        network: &str,
        keys: NetworkKeys,
        region: Option<RfRegion>,
        erase: bool,
    ) -> Result<(), String> {
        let mut c = self.start_controller(keys.clone(), !erase).await?;
        if erase {
            c.factory_reset().await.map_err(|e| e.to_string())?;
            let _ = std::fs::remove_file(self.store_path());
            c = self.restart(keys.clone()).await?;
        }
        if let Some(r) = region {
            if c.info().region.as_deref().and_then(region_from_info) != region_name(r) {
                c.set_region(r).await.map_err(|e| e.to_string())?;
                c = self.restart(keys.clone()).await?;
            }
        }
        let info = c.info();
        let region = region
            .and_then(region_name)
            .or_else(|| info.region.as_deref().and_then(region_from_info));
        self.save_network(network, info.home_id, info.node_id, region, &keys)?;
        {
            let mut st = self.st.lock().unwrap();
            st.network = Some(network.to_string());
            st.region = region.map(String::from);
            st.backup_due = true;
            st.last_backup = Instant::now() - BACKUP_SPACING;
        }
        self.publish_network();
        Ok(())
    }

    fn save_network(
        &self,
        network: &str,
        home: u32,
        node: NodeId,
        region: Option<&str>,
        keys: &NetworkKeys,
    ) -> Result<(), String> {
        let mut file = self.file();
        file["network"] = json!(network);
        file["coordinator"] = json!(key(home, node));
        file["region"] = json!(region);
        file["keys"] = serde_json::to_value(keys).map_err(|e| e.to_string())?;
        self.ctx.save(&file).map_err(|e| e.to_string())
    }

    async fn restore(&self, p: Value) -> Result<Value, RadioError> {
        let network = p["network"]
            .as_str()
            .filter(|s| valid_network_id(s))
            .ok_or_else(|| bad("network must be the platform's network id"))?
            .to_string();
        let b = &p["backup"];
        if b["v"] != 1 {
            return Err(bad("backup is not a version 1 radio backup"));
        }
        if b["protocol"] != "zwave" {
            return Err(refused("That backup is of another protocol's network."));
        }
        if b["network"].as_str() != Some(network.as_str()) {
            return Err(refused("That backup is of another network."));
        }
        let z = &b["zwave"];
        let image = z["nvm"]
            .as_str()
            .and_then(|s| base64::engine::general_purpose::STANDARD.decode(s).ok())
            .filter(|i| !i.is_empty())
            .ok_or_else(|| bad("backup.zwave.nvm is not a base64 NVM image"))?;
        let keys: NetworkKeys = serde_json::from_value(z["keys"].clone())
            .map_err(|_| bad("backup.zwave.keys is missing or incomplete"))?;
        {
            let st = self.st.lock().unwrap();
            if matches!(st.link, Link::Up(_)) && st.network.as_deref() != Some(network.as_str()) {
                return Err(refused("Another network is on this radio; leave it first."));
            }
            if st.busy {
                return Err(RadioError::Busy("An inclusion is running.".into()));
            }
        }
        let previous = self.file_keys();
        self.st.lock().unwrap().state = "restoring";
        self.publish_network();
        self.close_link();
        let _ = std::fs::remove_dir_all(self.ctx.file("zwave"));
        if let Some(store) = z.get("store").filter(|s| s.is_object()) {
            let _ = std::fs::create_dir_all(self.ctx.file("zwave"));
            let _ = crate::radio::write_private(&self.store_path(), store);
        }
        let r = async {
            let c = self.start_controller(keys.clone(), false).await?;
            c.restore_nvm(&image).await.map_err(|e| e.to_string())?;
            let c = self.restart(keys.clone()).await?;
            let info = c.info();
            let region = z["region"]
                .as_str()
                .and_then(region_by_name)
                .and_then(region_name)
                .or_else(|| info.region.as_deref().and_then(region_from_info));
            self.save_network(&network, info.home_id, info.node_id, region, &keys)?;
            let mut st = self.st.lock().unwrap();
            st.network = Some(network.clone());
            st.region = region.map(String::from);
            st.backup_due = true;
            Ok::<_, String>(())
        }
        .await;
        if let Err(e) = r {
            tracing::warn!(error = %e, "restore failed");
            self.close_link();
            let _ = std::fs::remove_file(self.store_path());
            // Back to what was there: the old network, or none.
            let back = match previous {
                Some(k) => self.start_controller(k, true).await.map(|_| ()),
                None => self.open_idle().await,
            };
            if let Err(e2) = back {
                let mut st = self.st.lock().unwrap();
                st.state = "down";
                st.error = Some(e2);
            }
            return Err(refused(&format!("The backup was not restored: {e}")));
        }
        self.publish_network();
        Ok(self.status())
    }

    async fn leave(&self, p: Value) -> Result<Value, RadioError> {
        let c = self.controller()?;
        if self.st.lock().unwrap().busy {
            return Err(RadioError::Busy(
                "An inclusion is running; abort it first.".into(),
            ));
        }
        let erase = p["erase"].as_bool().unwrap_or(false);
        if erase {
            c.factory_reset().await.map_err(host_error)?;
        }
        let coordinator = self.coordinator();
        self.close_link();
        drop(c);
        let mut file = self.file();
        if let Some(o) = file.as_object_mut() {
            for k in ["network", "coordinator", "region", "keys"] {
                o.remove(k);
            }
        }
        self.ctx.save(&file).map_err(|e| refused(&e.to_string()))?;
        let _ = std::fs::remove_dir_all(self.ctx.file("zwave"));
        if let Some(k) = coordinator {
            self.ctx.out.state_body(
                &k,
                json!({ "online": false, "network": { "protocol": "zwave", "state": "down", "radio": self.ctx.radio } }),
            );
        }
        {
            let mut st = self.st.lock().unwrap();
            st.network = None;
            st.home_id = None;
            st.backup_at = None;
            st.writable.clear();
        }
        if erase {
            tokio::time::sleep(Duration::from_millis(1500)).await;
        }
        if let Err(e) = self.open_idle().await {
            let mut st = self.st.lock().unwrap();
            st.state = "down";
            st.error = Some(e);
        }
        Ok(self.status())
    }

    /// Opens the network for one classic (button) inclusion.
    fn permit(&self, p: Value) -> Result<Value, RadioError> {
        let c = self.controller()?;
        if !p["via"].is_null() {
            return Err(refused(
                "Z-Wave devices join through the controller; open the network without via.",
            ));
        }
        let seconds = match &p["seconds"] {
            Value::Null => 180,
            v => v
                .as_u64()
                .filter(|s| (30..=600).contains(s))
                .ok_or_else(|| bad("seconds must be 30 to 600"))?,
        };
        let request = random_id();
        let until = now() + seconds as f64;
        {
            let mut st = self.st.lock().unwrap();
            if st.busy {
                return Err(RadioError::Busy(
                    "Another inclusion, exclusion or replace is running.".into(),
                ));
            }
            st.busy = true;
            st.request = Some(request.clone());
            st.joining_until = Some(until);
            st.inclusion = None;
        }
        // `network.permit` goes out when the controller is in inclusion mode
        // (Event::InclusionStarted), so a button pressed after it is heard.
        let me = self.me();
        tokio::spawn(async move {
            let r = c
                .include(InclusionOptions {
                    grant: GrantPolicy::Ask,
                    timeout: Some(Duration::from_secs(seconds)),
                    ..Default::default()
                })
                .await;
            if let Err(e) = &r {
                let quiet = matches!(e, zwave_host::Error::Inclusion(m) if m == "no node joined" || m == "inclusion stopped");
                if !quiet {
                    let i =
                        json!({ "request": request, "stage": "failed", "reason": e.to_string() });
                    me.event("network.inclusion", i);
                }
            }
            me.end_inclusion();
            me.event("network.permit", json!({ "until": null, "via": null }));
            me.publish_network();
        });
        Ok(json!({ "until": until, "request": self.st.lock().unwrap().request }))
    }

    /// `map`: reads every listening node's neighbor table from the
    /// controller, one request at a time, then publishes the whole topology
    /// as a scan. Long Range nodes have none; they are a star to the controller.
    fn map(&self) -> Result<Value, RadioError> {
        let c = self.controller()?;
        let home = c.info().home_id;
        let nodes: Vec<NodeId> = c
            .nodes()
            .into_iter()
            .filter(|r| r.id != c.info().node_id)
            .filter(|r| !r.protocol.sleeping() && !zwave_host::is_long_range(r.id))
            .map(|r| r.id)
            .collect();
        {
            let mut st = self.st.lock().unwrap();
            if st.busy || !st.firmware.is_empty() {
                return Err(RadioError::Busy(
                    "An inclusion or a firmware update is running.".into(),
                ));
            }
            if st.mapping {
                return Err(RadioError::Busy("A map is already running.".into()));
            }
            st.mapping = true;
        }
        let me = self.me();
        let n = nodes.len();
        tokio::spawn(async move {
            for node in nodes {
                if let Err(e) = c.refresh_neighbors(node).await {
                    tracing::debug!(node, error = %e, "routing info not read");
                }
            }
            me.st.lock().unwrap().mapping = false;
            me.publish_topology(&c, home, "scan");
        });
        Ok(json!({ "started": true, "requests": n }))
    }

    fn end_inclusion(&self) {
        let mut st = self.st.lock().unwrap();
        st.busy = false;
        st.request = None;
        st.joining_until = None;
        st.inclusion = None;
    }

    async fn provision(&self, p: Value) -> Result<Value, RadioError> {
        let c = self.controller()?;
        let list = p["entries"]
            .as_array()
            .ok_or_else(|| bad("entries must be a list"))?;
        if list.len() > 1000 {
            return Err(bad("at most 1000 entries"));
        }
        let mut want: Vec<Entry> = Vec::new();
        for (i, e) in list.iter().enumerate() {
            let dsk: Dsk = e["dsk"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| bad(&format!("entries[{i}].dsk is not a DSK")))?;
            match e.get("protocol").and_then(Value::as_str) {
                None | Some("zwave" | "lr") => {}
                Some(_) => return Err(bad(&format!("entries[{i}].protocol is zwave or lr"))),
            }
            let granted = match e.get("classes") {
                Some(v) if !v.is_null() => classes(v)?,
                _ => vec![],
            };
            want.push(Entry {
                dsk,
                granted,
                status: EntryStatus::Active,
                node_id: None,
                name: e["name"].as_str().map(|s| s.chars().take(64).collect()),
            });
        }
        let have = c.provisioning_list();
        for old in &have {
            if !want.iter().any(|w| w.dsk == old.dsk) {
                c.unprovision(&old.dsk).await;
            }
        }
        for w in want.iter() {
            if let Some(old) = have.iter().find(|o| o.dsk == w.dsk) {
                if old.granted == w.granted && old.name == w.name {
                    continue;
                }
                let mut e = w.clone();
                e.node_id = old.node_id;
                c.provision(e).await.map_err(host_error)?;
            } else {
                c.provision(w.clone()).await.map_err(host_error)?;
            }
        }
        Ok(json!({ "count": c.provisioning_list().len() }))
    }

    fn device_node(&self, p: &Value) -> Result<(Controller, u32, NodeId), RadioError> {
        let c = self.controller()?;
        let home = c.info().home_id;
        let (h, node) = p["device"]
            .as_str()
            .and_then(parse_key)
            .ok_or_else(|| bad("device must be a zwave: device key"))?;
        if h != home || c.node(node).is_none() {
            return Err(refused("That device is not on this network."));
        }
        if node == c.info().node_id {
            return Err(refused("That is the controller itself."));
        }
        Ok((c, home, node))
    }

    async fn remove(&self, p: Value) -> Result<Value, RadioError> {
        let (c, home, node) = self.device_node(&p)?;
        if p["force"].as_bool().unwrap_or(false) {
            {
                let mut st = self.st.lock().unwrap();
                if st.busy {
                    return Err(RadioError::Busy("An inclusion is running.".into()));
                }
                st.leave_reason.insert(node, "removed");
            }
            c.remove_failed_node(node).await.map_err(host_error)?;
            return Ok(json!({ "device": key(home, node), "stage": "done" }));
        }
        {
            let mut st = self.st.lock().unwrap();
            if st.busy {
                return Err(RadioError::Busy(
                    "Another inclusion, exclusion or replace is running.".into(),
                ));
            }
            st.busy = true;
        }
        self.event(
            "network.exclusion",
            json!({ "stage": "waiting", "device": key(home, node) }),
        );
        let me = self.me();
        tokio::spawn(async move {
            let r = c.exclude(Some(EXCLUSION_WAIT)).await;
            me.st.lock().unwrap().busy = false;
            match r {
                Ok(Some(n)) => me.event(
                    "network.exclusion",
                    json!({ "stage": "done", "device": key(home, n) }),
                ),
                Ok(None) => me.event(
                    "network.exclusion",
                    json!({ "stage": "done", "device": null, "reason": "the device was not in this network" }),
                ),
                Err(e) => me.event(
                    "network.exclusion",
                    json!({ "stage": "failed", "device": key(home, node), "reason": e.to_string() }),
                ),
            }
        });
        Ok(json!({ "device": key(home, node), "stage": "waiting" }))
    }

    fn replace(&self, p: Value) -> Result<Value, RadioError> {
        let (c, home, node) = self.device_node(&p)?;
        let request = random_id();
        {
            let mut st = self.st.lock().unwrap();
            if st.busy {
                return Err(RadioError::Busy(
                    "Another inclusion, exclusion or replace is running.".into(),
                ));
            }
            st.busy = true;
            st.request = Some(request.clone());
            st.inclusion = None;
            st.leave_reason.insert(node, "replaced");
        }
        let me = self.me();
        let req = request.clone();
        tokio::spawn(async move {
            let r = c
                .replace_failed_node(
                    node,
                    InclusionOptions {
                        grant: GrantPolicy::Ask,
                        timeout: Some(REPLACE_WAIT),
                        ..Default::default()
                    },
                )
                .await;
            match r {
                Ok(_) => {
                    me.event(
                        "network.left",
                        json!({ "device": key(home, node), "reason": "replaced" }),
                    );
                    me.changed();
                }
                Err(e) => {
                    me.st.lock().unwrap().leave_reason.remove(&node);
                    me.event(
                        "network.inclusion",
                        json!({ "request": req, "stage": "failed", "device": key(home, node), "reason": e.to_string() }),
                    );
                }
            }
            me.end_inclusion();
        });
        Ok(json!({ "device": key(home, node), "request": request }))
    }

    async fn set_region(&self, p: Value) -> Result<Value, RadioError> {
        let c = self.controller()?;
        let r = p["region"]
            .as_str()
            .and_then(region_by_name)
            .ok_or_else(|| {
                bad("region is not one of EU, US, ANZ, HK, IN, IL, RU, CN, US_LR, EU_LR, JP, KR")
            })?;
        if self.st.lock().unwrap().busy {
            return Err(RadioError::Busy("An inclusion is running.".into()));
        }
        let keys = self
            .file_keys()
            .ok_or_else(|| refused("No network on this radio yet."))?;
        c.set_region(r).await.map_err(host_error)?;
        drop(c);
        let c = self.restart(keys).await.map_err(|e| refused(&e))?;
        let mut file = self.file();
        file["region"] = json!(region_name(r));
        self.ctx.save(&file).map_err(|e| refused(&e.to_string()))?;
        self.st.lock().unwrap().region = region_name(r).map(String::from);
        drop(c);
        self.publish_network();
        Ok(self.status())
    }

    // ------------------------------------------------------------ commands

    fn command(&self, out: &Outbox, dev: &str, cmd: proto::Cmd) {
        let id = String::from(cmd.id.clone());
        let name = String::from(cmd.name.clone());
        let res = |status: &str, reason: Option<&str>, detail: Option<&str>| {
            out.cmd_res(dev, &id, status, reason, detail, None)
        };
        if cmd.v != proto::v1() {
            return res(
                "refused",
                Some("invalid"),
                Some("unsupported protocol version"),
            );
        }
        if cmd
            .expires
            .as_ref()
            .is_some_and(|t| f64::from(t.clone()) < now())
        {
            return res("failed", Some("expired"), None);
        }
        let Ok(c) = self.controller() else {
            return res(
                "failed",
                Some("undeliverable"),
                Some("the Z-Wave network is not up"),
            );
        };
        let home = c.info().home_id;
        let node = match parse_key(dev) {
            Some((h, n)) if h == home && n == c.info().node_id => {
                return res(
                    "refused",
                    Some("unknown"),
                    Some("network operations come only from Pulzar Net's console"),
                );
            }
            Some((h, n)) if h == home && c.node(n).is_some() => n,
            _ => return, // another network's device, or one no longer here
        };
        let params = serde_json::to_value(&cmd.params).unwrap_or(Value::Null);
        let params = if params.is_null() { json!({}) } else { params };
        let job = match name.as_str() {
            "settings.set" => Job::Settings(match settings(&params) {
                Ok(s) => s,
                Err(e) => return res("refused", Some("invalid"), Some(&e)),
            }),
            "zwave.set" => {
                let field = params["field"].as_str().unwrap_or_default().to_string();
                let found = self
                    .st
                    .lock()
                    .unwrap()
                    .writable
                    .get(&(node, field.clone()))
                    .cloned();
                let Some((vid, last)) = found else {
                    return res(
                        "refused",
                        Some("invalid"),
                        Some(&format!("{field:?} is not a settable value of this device")),
                    );
                };
                let value = match (&last, &params["value"]) {
                    (_, Value::Bool(b)) => match last {
                        ValueKind::Level(_) => ValueKind::Level(if *b { 0xff } else { 0 }),
                        _ => ValueKind::Bool(*b),
                    },
                    (ValueKind::Level(_), v) if v.as_u64().is_some_and(|n| n <= 99 || n == 255) => {
                        ValueKind::Level(v.as_u64().unwrap() as u8)
                    }
                    (ValueKind::Number(_), v) if v.as_f64().is_some() => {
                        ValueKind::Number(v.as_f64().unwrap())
                    }
                    _ => {
                        return res(
                            "refused",
                            Some("invalid"),
                            Some("value does not fit the field"),
                        )
                    }
                };
                Job::Set(vid, value)
            }
            "zwave.device_file" => {
                return match self.device_file(&c, node, &params) {
                    Ok(()) => res("ok", None, None),
                    Err(e) => res("refused", Some("invalid"), Some(&e)),
                }
            }
            "zwave.ping" => Job::Ping,
            "zwave.firmware" => match serde_json::from_value::<Firmware>(params.clone()) {
                Ok(f) => match f.check() {
                    Ok(()) => Job::Firmware(f),
                    Err(e) => return res("refused", Some("invalid"), Some(&e)),
                },
                Err(e) => return res("refused", Some("invalid"), Some(&e.to_string())),
            },
            _ => {
                return res(
                    "refused",
                    Some("unknown"),
                    Some(&format!("no command {name}")),
                )
            }
        };
        let asleep = c.node(node).is_some_and(|r| r.protocol.sleeping())
            && c.status(node) != NodeStatus::Awake;
        if asleep {
            res(
                "queued",
                None,
                Some("the device is asleep; sent when it wakes up"),
            );
        }
        let me = self.me();
        let out = out.clone();
        let dev = dev.to_string();
        tokio::spawn(async move {
            let r = match &job {
                Job::Settings(s) => me.apply_settings(&c, node, s).await,
                Job::Set(vid, v) => c
                    .set_value(vid, v)
                    .await
                    .map_err(|e| e.to_string())
                    .map(|_| None),
                Job::Ping => Ok(Some(json!({ "alive": c.ping(node).await }))),
                Job::Firmware(f) => me.firmware(&c, home, node, f).await,
            };
            match r {
                Ok(data) => {
                    out.cmd_res(
                        &dev,
                        &id,
                        "ok",
                        None,
                        None,
                        data.and_then(|d| d.as_object().cloned()),
                    );
                    if let Job::Settings(s) = &job {
                        let values: Map<String, Value> =
                            s.iter().map(|(k, v)| (k.clone(), json!(v))).collect();
                        out.settings(&dev, values, "gateway");
                    }
                }
                Err(e) => {
                    let reason =
                        if e.contains("did not acknowledge") || e.contains("did not answer") {
                            "undeliverable"
                        } else {
                            "error"
                        };
                    out.cmd_res(&dev, &id, "failed", Some(reason), Some(&e), None);
                }
            }
        });
    }

    /// Fetches, checks and sends a firmware image, with `state.ota` as it goes.
    async fn firmware(
        &self,
        c: &Controller,
        home: u32,
        node: NodeId,
        f: &Firmware,
    ) -> Result<Option<Value>, String> {
        let dev = key(home, node);
        let ota = |phase: &str, progress: Option<f64>, error: Option<&str>| {
            let mut o = json!({ "phase": phase });
            if let Some(v) = &f.version {
                o["version"] = json!(v);
            }
            if let Some(p) = progress {
                o["progress"] = json!(p);
            }
            if let Some(e) = error {
                o["error"] = json!(e);
            }
            self.ctx.out.state_body(&dev, json!({ "ota": o }));
        };
        ota("downloading", None, None);
        let r = async {
            let bytes = self.fetch(&f.url).await?;
            let sum = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&bytes));
            if !sum.eq_ignore_ascii_case(&f.sha256) {
                return Err("the file's SHA-256 does not match".to_string());
            }
            let image = zwave_host::firmware::read_image(&f.name, &bytes)?;
            ota("staged", None, None);
            self.st
                .lock()
                .unwrap()
                .firmware
                .insert(node, (f.version.clone(), 0));
            let r = c
                .update_firmware(node, &image, f.target)
                .await
                .map_err(|e| e.to_string())?;
            if !r.success() {
                return Err(format!(
                    "the device refused the image (status {:#04x})",
                    r.status
                ));
            }
            Ok(json!({ "status": r.status, "wait_time": r.wait_time }))
        }
        .await;
        self.st.lock().unwrap().firmware.remove(&node);
        match r {
            Ok(data) => {
                ota("confirmed", Some(1.0), None);
                Ok(Some(data))
            }
            Err(e) => {
                ota("failed", None, Some(&e));
                Err(e)
            }
        }
    }

    /// Downloads a firmware file with the system's curl, bounded in size and time.
    async fn fetch(&self, url: &str) -> Result<Vec<u8>, String> {
        let dir = self.ctx.file("zwave");
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let path = dir.join(format!("firmware-{}.tmp", random_id()));
        let out = tokio::process::Command::new("curl")
            .args(["-fsSL", "--proto", "=https,http", "--max-time", "600"])
            .args(["--max-filesize", &FIRMWARE_FILE_MAX.to_string(), "-o"])
            .arg(&path)
            .arg(url)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .output()
            .await;
        let read = std::fs::read(&path);
        let _ = std::fs::remove_file(&path);
        match out {
            Ok(o) if o.status.success() => read.map_err(|e| e.to_string()),
            // curl's message names the URL, which is a signed link: keep it out.
            Ok(o) => Err(format!(
                "the download failed (curl exit {})",
                o.status.code().unwrap_or(-1)
            )),
            Err(e) => Err(format!("curl did not run: {e}")),
        }
    }

    /// Writes settings with the device's Z-Wave JS file when the platform
    /// sent one, by raw parameter otherwise.
    async fn apply_settings(
        &self,
        c: &Controller,
        node: NodeId,
        s: &[(String, i64)],
    ) -> Result<Option<Value>, String> {
        let config = c.node(node).as_ref().and_then(device_id).and_then(|id| {
            let files = self.load_device_files(&id)?;
            Library::new(MapSource::new(files))
                .lookup(&id)
                .ok()
                .flatten()
        });
        match config {
            Some(cfg) => c.configure(node, &cfg, s).await,
            None => c.configure_raw(node, s).await,
        }
        .map_err(|e| e.to_string())
        .map(|_| None)
    }

    fn device_file_path(&self, id: &zwave_host::device_config::DeviceId) -> PathBuf {
        self.devices_dir().join(format!(
            "{:04x}-{:04x}-{:04x}.json",
            id.manufacturer_id, id.product_type, id.product_id
        ))
    }

    fn load_device_files(
        &self,
        id: &zwave_host::device_config::DeviceId,
    ) -> Option<BTreeMap<String, String>> {
        let b = std::fs::read(self.device_file_path(id)).ok()?;
        serde_json::from_slice(&b).ok()
    }

    /// `zwave.device_file`: the device's Z-Wave JS file and its templates as
    /// device-library serves them, `{"files": {path: text}}`.
    fn device_file(&self, c: &Controller, node: NodeId, p: &Value) -> Result<(), String> {
        let rec = c.node(node).ok_or("no such node")?;
        let id = device_id(&rec).ok_or("the device's identity is not known yet")?;
        let files: BTreeMap<String, String> = serde_json::from_value(p["files"].clone())
            .map_err(|_| "files must be an object of path to text")?;
        let size: usize = files.iter().map(|(k, v)| k.len() + v.len()).sum();
        if files.is_empty() || size > DEVICE_FILES_MAX {
            return Err(format!(
                "files must hold 1 byte to {DEVICE_FILES_MAX} bytes"
            ));
        }
        match Library::new(MapSource::new(files.clone())).lookup(&id) {
            Ok(Some(_)) => {}
            Ok(None) => return Err("no file in files matches this device".into()),
            Err(e) => return Err(format!("the device file does not load: {e}")),
        }
        std::fs::create_dir_all(self.devices_dir()).map_err(|e| e.to_string())?;
        let path = self.device_file_path(&id);
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_vec(&files).map_err(|e| e.to_string())?)
            .and_then(|_| std::fs::rename(&tmp, &path))
            .map_err(|e| e.to_string())
    }
}

enum Job {
    Settings(Vec<(String, i64)>),
    Set(ValueId, ValueKind),
    Ping,
    Firmware(Firmware),
}

/// `zwave.firmware`: an image the platform stored, fetched by the gateway.
#[derive(serde::Deserialize)]
struct Firmware {
    /// `https://` (a signed link; never logged). Plain `http://` only to
    /// the gateway itself, for tests.
    url: String,
    /// Hex SHA-256 of the file.
    sha256: String,
    /// The file's name: `.hex` is converted, others sent as they are.
    name: String,
    #[serde(default)]
    target: u8,
    /// The version the image carries, for `state.ota`.
    #[serde(default)]
    version: Option<String>,
}

impl Firmware {
    fn check(&self) -> Result<(), String> {
        let loopback = ["http://127.0.0.1:", "http://localhost:", "http://[::1]:"];
        if !self.url.starts_with("https://") && !loopback.iter().any(|p| self.url.starts_with(p)) {
            return Err("url must be https".into());
        }
        if self.sha256.len() != 64 || !self.sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("sha256 must be 64 hex digits".into());
        }
        if self.name.is_empty() || self.name.len() > 128 || self.name.contains('/') {
            return Err("name must be the file's name".into());
        }
        Ok(())
    }
}

/// Largest firmware file fetched (an Intel HEX of the largest image).
const FIRMWARE_FILE_MAX: usize = 3 * 8 * 1024 * 1024;

/// `settings.set` params: parameter key (`"3"`, `"41[0xf00]"`) to an integer.
fn settings(p: &Value) -> Result<Vec<(String, i64)>, String> {
    let o = p.as_object().ok_or("params must be an object")?;
    if o.is_empty() {
        return Err("no settings given".into());
    }
    o.iter()
        .map(|(k, v)| {
            if zwave_host::device_config::parse_param_key(k).is_none() {
                return Err(format!("{k} is not a parameter key"));
            }
            let n = match v {
                Value::Bool(b) => i64::from(*b),
                v => v.as_i64().ok_or(format!("{k} must be an integer"))?,
            };
            Ok((k.clone(), n))
        })
        .collect()
}

impl RadioModule for ZWave {
    fn protocol(&self) -> &'static str {
        "zwave"
    }

    fn call<'a>(&'a self, op: &'a str, params: Value) -> BoxFuture<'a, Result<Value, RadioError>> {
        Box::pin(self.op(op, params))
    }

    fn dispatch(&self, out: &Outbox, topic: &str, payload: &[u8]) {
        let prefix = format!("pulzr/a/{}/d/", out.adapter());
        let Some(dev) = topic
            .strip_prefix(&prefix)
            .and_then(|t| t.strip_suffix("/cmd"))
        else {
            return;
        };
        if !dev.starts_with("zwave:") {
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
            Ok(cmd) => self.command(out, dev, cmd),
            Err(e) => match raw.get("id").and_then(Value::as_str) {
                Some(id) if proto::CmdResultId::try_from(id).is_ok() => out.cmd_res(
                    dev,
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

fn series_of(chip: u8) -> Option<u16> {
    match chip {
        0x05 => Some(500),
        0x07 => Some(700),
        0x08 => Some(800),
        _ => None,
    }
}

fn firmware_ok(f: &str) -> bool {
    let parts: Vec<&str> = f.split('.').collect();
    (2..=3).contains(&parts.len())
        && parts
            .iter()
            .all(|p| (1..=3).contains(&p.len()) && p.bytes().all(|b| b.is_ascii_digit()))
}

/// The field id of a value in `up`: its property, `_<endpoint>` for an endpoint.
fn field_id(id: &ValueId) -> String {
    let mut f: String = id
        .property
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if f.is_empty() {
        f = format!("cc_{}", id.cc);
    }
    if id.endpoint != 0 {
        f = format!("{f}_{}", id.endpoint);
    }
    f.chars().take(64).collect()
}

fn diag(c: &Controller, rec: &NodeRecord, status: NodeStatus) -> Value {
    let mut d = json!({
        "status": serde_json::to_value(status).unwrap_or(Value::Null),
        "sleeping": rec.protocol.sleeping(),
        "queued": c.queued(rec.id),
        "stage": format!("{:?}", rec.stage).to_lowercase(),
    });
    if let Some(dsk) = &rec.dsk {
        d["dsk"] = json!(dsk);
    }
    if let Some(n) = &rec.name {
        d["name"] = json!(n);
    }
    if rec.id >= 256 {
        d["long_range"] = json!(true);
    }
    d
}

/// `state.interview.zwave`, once the node's identity is known.
fn interview(rec: &NodeRecord) -> Option<Value> {
    let m = rec.manufacturer?;
    let mut i = json!({
        "manufacturer": m.manufacturer_id, "product_type": m.product_type, "product_id": m.product_id,
        "security": rec.security.first().map_or("none", |c| class_name(*c)),
        "listening": rec.protocol.listening,
        "endpoints": rec.endpoints.keys().filter(|e| **e != 0).count().min(127),
        "ccs": rec.endpoints.get(&0).map(|e| e.ccs.iter().take(256).map(|(cc, s)| json!({ "cc": cc, "version": s.version, "secure": s.secure })).collect::<Vec<_>>()).unwrap_or_default(),
        "at": now(),
        "complete": rec.stage == InterviewStage::Complete,
    });
    if let Some(fw) = rec.firmware.as_deref().filter(|f| firmware_ok(f)) {
        i["firmware"] = json!(fw);
    }
    Some(i)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_and_fields() {
        assert_eq!(key(0xc0ffee01, 5), "zwave:c0ffee01.5");
        assert_eq!(parse_key("zwave:c0ffee01.260"), Some((0xc0ffee01, 260)));
        assert_eq!(parse_key("zwave:c0ffee1.5"), None);
        let id = ValueId {
            node: 5,
            endpoint: 2,
            cc: 0x31,
            property: "air_temperature".into(),
        };
        assert_eq!(field_id(&id), "air_temperature_2");
        assert!(
            firmware_ok("7.18")
                && firmware_ok("1.2.3")
                && !firmware_ok("7")
                && !firmware_ok("v7.1")
        );
        assert_eq!(region_by_name("us_lr"), Some(RfRegion::UsaLongRange));
        assert_eq!(region_from_info("UsaLongRange"), Some("US_LR"));
    }

    #[test]
    fn form_keys_are_checked_and_never_echoed() {
        let k = |n: u8| hex::encode([n; 16]);
        let mut v = json!({});
        for (i, name) in KEY_NAMES.iter().enumerate() {
            v[*name] = json!(k(i as u8 + 1));
        }
        let keys = parse_keys(&v).unwrap();
        assert_eq!(keys.s2_access_control, [4; 16]);
        assert_eq!(keys.lr_s2_access_control, Some([6; 16]));
        v["s0"] = json!("zz");
        let RadioError::BadRequest(m) = parse_keys(&v).unwrap_err() else {
            panic!()
        };
        assert!(!m.contains("zz"));
        v["s0"] = json!(k(2));
        assert!(parse_keys(&v).is_err(), "a key reused for two classes");
    }

    #[test]
    fn settings_params() {
        let s = settings(&json!({ "3": 20, "41[0xf00]": 2, "7": true })).unwrap();
        assert_eq!(s.len(), 3);
        assert!(settings(&json!({ "x": 1 })).is_err());
        assert!(settings(&json!({ "3": 1.5 })).is_err());
    }
}
