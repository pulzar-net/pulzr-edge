//! Zigbee: a coordinator on an EmberZNet, Z-Stack or deCONZ dongle, through
//! the `zigbee-host` stack (pulzar-net/zigbee-bridge), as a radio module
//! (PulzrOS `docs/radio-modules.md`).
//!
//! - Each device is `zigbee:<IEEE address>`; the coordinator is
//!   `zigbee:<its IEEE address>`. A device's `up` carries the ZCL frame as
//!   received in `raw` with `link.zigbee`, and `r` with the standard
//!   clusters' readings; its `state` carries the interview.
//! - The network's lifecycle (form, restore, permit, channel, leave) comes
//!   only through [`RadioModule::call`], from a person on the console. The
//!   network key lives in the radio's file, nowhere else.
//! - The platform's `zigbee.zcl`, `zigbee.zdo` and `zigbee.wait` commands
//!   run in order per device ([`commands`]).
//! - Topology: last hops and route records as they come, and a walk of
//!   every router's neighbor and routing tables on a schedule
//!   ([`TOPOLOGY_SCAN`]) or when a person asks (`map`).
//! - Backups after joins and leaves (at most one a minute) and hourly, in the
//!   open coordinator backup format, through [`RadioCtx::backup`].

pub mod commands;

use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use anyhow::Result;
use base64::Engine;
use serde_json::{json, Map, Value};
use tokio::sync::mpsc;
use zigbee_host::backup::RESTORE_FRAME_COUNTER_MARGIN;
use zigbee_host::probe::NcpKind;
use zigbee_host::{
    AdapterEvent, AnyAdapter, Backup, Device, Host, HostConfig, HostError, HostEvent, JoinPolicy,
    NetworkSettings, StartMode,
};
use zigbee_primitives::{ExtPanId, Ieee, Key};

use crate::outbox::{now, Outbox};
use crate::radio::{BoxFuture, RadioCtx, RadioError, RadioModule};

/// How far a restore advances the network frame counter in all.
pub const RESTORE_MARGIN: u32 = 1_000_000;
/// The shortest time between backups after joins and leaves.
const BACKUP_MIN_GAP: f64 = 60.0;
/// Scheduled backups.
const BACKUP_EVERY: Duration = Duration::from_secs(3600);
/// Every router's neighbor table, this often.
pub const TOPOLOGY_SCAN: Duration = Duration::from_secs(6 * 3600);
/// The passive topology, this often.
const TOPOLOGY_PUBLISH: Duration = Duration::from_secs(300);

/// `zigbee:<ieee>`.
pub fn key(ieee: Ieee) -> String {
    format!("zigbee:{ieee}")
}

/// The IEEE address in a `zigbee:` key.
pub fn ieee_of(key: &str) -> Option<Ieee> {
    key.strip_prefix("zigbee:")?.parse().ok()
}

fn hex16(v: u64) -> String {
    format!("{v:016x}")
}

fn bad(m: impl Into<String>) -> RadioError {
    RadioError::BadRequest(m.into())
}

fn refused(m: impl Into<String>) -> RadioError {
    RadioError::Refused(m.into())
}

/// An 802.15.4 energy reading (0–255) for a dBm figure, on Z-Stack's scale.
fn dbm_to_energy(dbm: i8) -> u8 {
    ((i16::from(dbm) + 100).clamp(0, 40) * 255 / 40) as u8
}

fn family(kind: NcpKind) -> &'static str {
    kind.name()
}

/// What the radio's file holds for a formed network. The file is the
/// radio's (`radio`, `protocol` from `pulzr-radios use`); this module adds
/// `network`, `coordinator`, `ncp` and `zigbee`.
#[derive(Debug, Clone)]
struct Stored {
    network: String,
    settings: NetworkSettings,
    join: JoinPolicy,
}

fn parse_hex_key(s: &str) -> Option<Key> {
    let bytes = hex::decode(s).ok()?;
    <[u8; 16]>::try_from(bytes.as_slice()).ok().map(Key)
}

fn stored(file: &Value) -> Option<Stored> {
    let z = file.get("zigbee")?;
    Some(Stored {
        network: file.get("network")?.as_str()?.to_string(),
        settings: NetworkSettings {
            channel: u8::try_from(z.get("channel")?.as_u64()?).ok()?,
            pan_id: u16::from_str_radix(z.get("pan_id")?.as_str()?, 16).ok()?,
            extended_pan_id: ExtPanId(
                u64::from_str_radix(z.get("ext_pan_id")?.as_str()?, 16).ok()?,
            ),
            network_key: parse_hex_key(z.get("network_key")?.as_str()?)?,
            tx_power: None,
        },
        join: JoinPolicy {
            install_code_only: z
                .get("install_codes_only")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            allow_legacy: z.get("legacy").and_then(Value::as_bool).unwrap_or(true),
        },
    })
}

struct Running {
    host: Host<AnyAdapter>,
    network: String,
    pump: tokio::task::JoinHandle<()>,
}

/// The coordinator before a network is formed: the port held open, the
/// adapter connected once something needs it.
enum Stage {
    Unformed(Option<(AnyAdapter, mpsc::UnboundedReceiver<AdapterEvent>)>),
    Running(Running),
}

struct Joining {
    until: f64,
    via: Option<String>,
    task: Option<tokio::task::JoinHandle<()>>,
}

pub struct Zigbee {
    /// Itself, for ops that spawn work outliving the call.
    me: Weak<Zigbee>,
    ctx: RadioCtx,
    stage: tokio::sync::Mutex<Stage>,
    /// The running host, for commands and status without the stage lock.
    host: Mutex<Option<Host<AnyAdapter>>>,
    kind: Mutex<Option<NcpKind>>,
    firmware: Mutex<Option<String>>,
    joining: Mutex<Option<Joining>>,
    backup_at: Mutex<Option<f64>>,
    /// A mesh walk is running.
    mapping: std::sync::atomic::AtomicBool,
    commands: commands::Queues,
}

/// Starts the module on its radio; resumes a network the radio's file
/// names. Nothing is written to the radio until then.
pub fn start(ctx: RadioCtx) -> Result<Arc<dyn RadioModule>> {
    let file = ctx.load()?;
    let kind = file
        .get("ncp")
        .and_then(Value::as_str)
        .and_then(|s| s.parse().ok());
    let module = Arc::new_cyclic(|me| Zigbee {
        me: me.clone(),
        commands: commands::Queues::default(),
        mapping: std::sync::atomic::AtomicBool::new(false),
        ctx,
        stage: tokio::sync::Mutex::new(Stage::Unformed(None)),
        host: Mutex::new(None),
        kind: Mutex::new(kind),
        firmware: Mutex::new(None),
        joining: Mutex::new(None),
        backup_at: Mutex::new(file.get("backup_at").and_then(Value::as_f64)),
    });
    if let Some(s) = stored(&file) {
        let m = module.clone();
        tokio::spawn(async move {
            let mode = StartMode::Normal {
                settings: s.settings.clone(),
                replace_other: false,
            };
            if let Err(e) = m.run(mode, s.network.clone(), s.join.clone()).await {
                tracing::error!(error = e.message(), "resuming the Zigbee network");
                m.network_error(e.message());
            }
        });
    }
    module.commands.set_firmware(module.ctx.firmware.clone());
    let m = module.clone();
    tokio::spawn(async move { m.schedules().await });
    Ok(module)
}

impl Zigbee {
    fn store_path(&self) -> PathBuf {
        self.ctx.file("devices.json")
    }

    fn host(&self) -> Option<Host<AnyAdapter>> {
        self.host.lock().unwrap().clone()
    }

    fn coordinator(&self) -> Option<String> {
        self.host().map(|h| key(h.network().ieee))
    }

    fn network_error(&self, message: &str) {
        let key = self.coordinator().unwrap_or_else(|| self.ctx.out.via());
        self.ctx.out.event(
            &key,
            "network.error",
            json!({ "message": message }),
            Some("error"),
        );
    }

    /// The coordinator family on the port: the radio's file, else a probe.
    async fn kind(&self) -> Result<NcpKind, RadioError> {
        if let Some(k) = *self.kind.lock().unwrap() {
            return Ok(k);
        }
        let port = self.ctx.port.clone();
        let found = zigbee_host::probe::probe_any(&port, 115_200)
            .await
            .map_err(|e| refused(format!("the radio's port: {e}")))?;
        let id =
            found.ok_or_else(|| refused("no Zigbee coordinator answered on the radio's port"))?;
        if !id.supported {
            return Err(refused(format!(
                "the dongle's {} firmware {} is too old; update it first",
                id.kind.name(),
                id.firmware
            )));
        }
        *self.kind.lock().unwrap() = Some(id.kind);
        *self.firmware.lock().unwrap() = Some(id.firmware);
        let mut file = self.ctx.load().map_err(|e| refused(e.to_string()))?;
        file["ncp"] = json!(id.kind.name());
        self.ctx.save(&file).map_err(|e| refused(e.to_string()))?;
        Ok(id.kind)
    }

    async fn connect(
        &self,
    ) -> Result<(AnyAdapter, mpsc::UnboundedReceiver<AdapterEvent>), RadioError> {
        let kind = self.kind().await?;
        let io = zigbee_host::port::open(
            &self.ctx.port,
            115_200,
            zigbee_host::port::FlowControl::None,
        )
        .await
        .map_err(|e| refused(format!("opening the radio's port: {e}")))?;
        AnyAdapter::connect(io, kind)
            .await
            .map_err(|e| refused(format!("the coordinator did not start: {e}")))
    }

    /// Starts the host on the radio and the event pump.
    async fn run(
        self: &Arc<Self>,
        mode: StartMode,
        network: String,
        join: JoinPolicy,
    ) -> Result<(), RadioError> {
        let mut stage = self.stage.lock().await;
        let held = match &mut *stage {
            Stage::Running(_) => return Err(refused("a network is already running on this radio")),
            Stage::Unformed(held) => held.take(),
        };
        let (adapter, events) = match held {
            Some(a) => a,
            None => self.connect().await?,
        };
        let ota_dir = self.ctx.scratch("ota");
        let _ = std::fs::create_dir_all(&ota_dir);
        self.commands.set_ota_dir(ota_dir.clone());
        let config = HostConfig {
            store_path: Some(self.store_path()),
            join,
            ota_dir: Some(ota_dir),
            ..HostConfig::default()
        };
        let restoring = matches!(mode, StartMode::Restore(_));
        let started = Host::start(adapter, events, mode.clone(), config.clone()).await;
        let started = match (started, &mode) {
            // EmberZNet took the backup's IEEE address: it applies after a
            // reset, which a new connection does.
            (Err(HostError::RestartRequired), StartMode::Restore(_)) => {
                let (adapter, events) = self.connect().await?;
                Host::start(adapter, events, mode, config).await
            }
            (r, _) => r,
        };
        let (host, rx) = started.map_err(|e| match e {
            HostError::NetworkMismatch { found } => refused(format!(
                "the dongle already runs another network ({found}); leave it with erase first"
            )),
            HostError::RestoreRefused(why) => refused(why),
            other => refused(other.to_string()),
        })?;
        *self.firmware.lock().unwrap() = Some(host.adapter_info().firmware);
        *self.host.lock().unwrap() = Some(host.clone());
        let pump = tokio::spawn(self.clone().pump(host.clone(), rx));
        *stage = Stage::Running(Running {
            host: host.clone(),
            network,
            pump,
        });
        drop(stage);
        let coordinator = key(host.network().ieee);
        let mut file = self.ctx.load().map_err(|e| refused(e.to_string()))?;
        file["coordinator"] = json!(coordinator);
        let _ = self.ctx.save(&file);
        self.publish_network("up");
        if restoring {
            self.backup_now().await;
        }
        Ok(())
    }

    fn network_value(&self, state: &str) -> Value {
        let Some(host) = self.host() else {
            return json!({ "protocol": "zigbee", "state": state, "radio": self.ctx.radio });
        };
        let n = host.network();
        let mut v = json!({
            "protocol": "zigbee", "state": state, "radio": self.ctx.radio,
            "channel": n.channel, "pan_id": format!("{:04x}", n.pan_id),
            "ext_pan_id": hex16(n.extended_pan_id.0), "devices": host.devices().len(),
        });
        if let Some(j) = self
            .joining
            .lock()
            .unwrap()
            .as_ref()
            .filter(|j| j.until > now())
        {
            v["joining_until"] = json!(j.until);
        }
        if let Some(at) = *self.backup_at.lock().unwrap() {
            v["backup_at"] = json!(at);
        }
        v
    }

    fn publish_network(&self, state: &str) {
        if let Some(c) = self.coordinator() {
            let fw = self.firmware.lock().unwrap().clone();
            let mut body = json!({ "online": true, "network": self.network_value(state) });
            if let Some(fw) = fw {
                body["fw"] = json!(fw);
            }
            self.ctx.out.state_body(&c, body);
        }
    }

    fn status(&self) -> Value {
        let file = self.ctx.load().unwrap_or_default();
        let host = self.host();
        let joining = self
            .joining
            .lock()
            .unwrap()
            .as_ref()
            .filter(|j| j.until > now())
            .map(|j| json!({ "until": j.until, "via": j.via }));
        let mut s = json!({
            "radio": self.ctx.radio, "protocol": "zigbee",
            "network": file.get("network").cloned().unwrap_or(Value::Null),
            "state": if host.is_some() { "up" } else { "unformed" },
            "firmware": self.firmware.lock().unwrap().clone(),
            "ncp": self.kind.lock().unwrap().map(family),
            "coordinator": self.coordinator(),
            "channel": null, "pan_id": null, "ext_pan_id": null,
            "region": null, "home_id": null, "devices": 0,
            "joining": joining.unwrap_or(json!({ "until": null, "via": null })),
            "inclusion": null,
            "backup_at": *self.backup_at.lock().unwrap(),
        });
        if let Some(h) = host {
            let n = h.network();
            s["channel"] = json!(n.channel);
            s["pan_id"] = json!(format!("{:04x}", n.pan_id));
            s["ext_pan_id"] = json!(hex16(n.extended_pan_id.0));
            s["devices"] = json!(h.devices().len());
        }
        s
    }

    /// The backup object the platform keeps: the open coordinator backup
    /// inside the radio's envelope.
    fn envelope(&self, backup: &Backup, network: &str) -> Value {
        json!({
            "v": 1, "protocol": "zigbee", "network": network, "radio": self.ctx.radio,
            "taken_at": now(), "firmware": self.firmware.lock().unwrap().clone(),
            "zigbee": serde_json::from_str::<Value>(&backup.to_json()).unwrap_or(Value::Null),
        })
    }

    async fn take_backup(&self) -> Result<Value, RadioError> {
        let (host, network) = {
            let stage = self.stage.lock().await;
            match &*stage {
                Stage::Running(r) => (r.host.clone(), r.network.clone()),
                Stage::Unformed(_) => return Err(refused("no network on this radio")),
            }
        };
        let b = host.backup().await.map_err(|e| refused(e.to_string()))?;
        Ok(self.envelope(&b, &network))
    }

    /// Back up and hand it to the platform.
    async fn backup_now(&self) {
        match self.take_backup().await {
            Ok(b) => {
                if let Err(e) = self.ctx.backup(&b) {
                    tracing::warn!(error = %e, "handing over a backup");
                    return;
                }
                let at = now();
                *self.backup_at.lock().unwrap() = Some(at);
                if let Ok(mut file) = self.ctx.load() {
                    file["backup_at"] = json!(at);
                    let _ = self.ctx.save(&file);
                }
                self.publish_network("up");
            }
            Err(e) => tracing::debug!(error = e.message(), "no backup"),
        }
    }

    /// A backup after a join or leave, at most one a minute.
    fn backup_soon(self: &Arc<Self>) {
        let m = self.clone();
        tokio::spawn(async move {
            let last = (*m.backup_at.lock().unwrap()).unwrap_or(0.0);
            let wait = (last + BACKUP_MIN_GAP - now()).max(5.0);
            tokio::time::sleep(Duration::from_secs_f64(wait)).await;
            let last = (*m.backup_at.lock().unwrap()).unwrap_or(0.0);
            if now() - last >= BACKUP_MIN_GAP - 1.0 {
                m.backup_now().await;
            }
        });
    }

    /// Hourly backups, the passive topology, and neighbor-table scans.
    async fn schedules(self: Arc<Self>) {
        let mut backups = tokio::time::interval(BACKUP_EVERY);
        let mut passive = tokio::time::interval(TOPOLOGY_PUBLISH);
        let mut scans = tokio::time::interval(TOPOLOGY_SCAN);
        backups.tick().await;
        passive.tick().await;
        scans.tick().await;
        loop {
            tokio::select! {
                _ = backups.tick() => self.backup_now().await,
                _ = passive.tick() => self.publish_topology("passive"),
                _ = scans.tick() => {
                    if !self.mapping.swap(true, std::sync::atomic::Ordering::SeqCst) {
                        self.walk().await;
                        self.mapping.store(false, std::sync::atomic::Ordering::SeqCst);
                    }
                }
            }
        }
    }

    fn publish_topology(&self, source: &str) {
        let Some(host) = self.host() else { return };
        let coordinator = key(host.network().ieee);
        let mut edges =
            vec![json!({ "from": coordinator, "to": self.ctx.out.via(), "kind": "bus" })];
        for (from, to, link) in host.topology() {
            let (Some(from), Some(to)) = (from, to) else {
                continue;
            };
            let mut quality = Map::new();
            if let Some(l) = link.lqi {
                quality.insert("lqi".into(), json!(l));
            }
            if let Some(r) = link.rssi {
                quality.insert("rssi".into(), json!(r));
            }
            let kind = match link.source {
                zigbee_host::topology::LinkSource::NeighborTable => "neighbor",
                _ => "route",
            };
            let mut e =
                json!({ "from": key(from), "to": key(to), "kind": kind, "ts": link.seen_unix });
            if !quality.is_empty() {
                e["quality"] = Value::Object(quality);
            }
            edges.push(e);
            if edges.len() >= 2000 {
                break;
            }
        }
        self.ctx.out.topology(
            &coordinator,
            json!({ "ts": now(), "protocol": "zigbee", "source": source, "complete": true, "edges": edges }),
        );
    }

    /// Ask every router (and the coordinator) for its neighbor table, then
    /// publish the whole topology.
    /// The routers to ask: the coordinator and every router that is not
    /// asleep.
    fn routers(host: &Host<AnyAdapter>) -> Vec<(Ieee, zigbee_primitives::NodeId)> {
        let n = host.network();
        let mut routers = vec![(n.ieee, n.nwk)];
        routers.extend(
            host.devices()
                .iter()
                .filter(|d| {
                    !d.is_sleepy()
                        && d.node_descriptor
                            .as_ref()
                            .is_some_and(|n| n.logical_type() == zigbee_zdo::LogicalType::Router)
                })
                .map(|d| (d.ieee, d.nwk)),
        );
        routers
    }

    /// Walk the mesh: each router's neighbor table (Mgmt_Lqi) and routing
    /// table (Mgmt_Rtg), page by page, one request in flight; then publish
    /// the whole topology. Routes in use are `route` edges, the rest of the
    /// neighbors `neighbor` edges.
    async fn walk(&self) {
        use zigbee_zdo::Body;
        let Some(host) = self.host() else { return };
        let coordinator = host.network();
        let name = |nwk: zigbee_primitives::NodeId| -> Option<String> {
            if nwk == coordinator.nwk {
                Some(key(coordinator.ieee))
            } else {
                host.device_by_nwk(nwk).map(|d| key(d.ieee))
            }
        };
        // (from, to) -> (kind, lqi)
        let mut edges: std::collections::BTreeMap<(String, String), (&str, Option<u8>)> =
            std::collections::BTreeMap::new();
        for (ieee, nwk) in Self::routers(&host) {
            let router = key(ieee);
            let mut start = 0u8;
            for _ in 0..16 {
                let Ok(m) = host
                    .zdo_request(nwk, Body::MgmtLqiReq { start_index: start })
                    .await
                else {
                    break;
                };
                let Body::MgmtLqiRsp {
                    status: 0,
                    total,
                    neighbors,
                    ..
                } = m.body
                else {
                    break;
                };
                if neighbors.is_empty() {
                    break;
                }
                for n in &neighbors {
                    if n.ieee == Ieee::ZERO || n.ieee == Ieee::UNSET {
                        continue;
                    }
                    edges
                        .entry((key(n.ieee), router.clone()))
                        .or_insert(("neighbor", Some(n.lqi)));
                }
                start = start.saturating_add(neighbors.len() as u8);
                if start >= total {
                    break;
                }
            }
            let mut start = 0u8;
            for _ in 0..16 {
                let Ok(m) = host
                    .zdo_request(nwk, Body::MgmtRtgReq { start_index: start })
                    .await
                else {
                    break;
                };
                let Body::MgmtRtgRsp {
                    status: 0,
                    total,
                    routes,
                    ..
                } = m.body
                else {
                    break;
                };
                if routes.is_empty() {
                    break;
                }
                for r in &routes {
                    // Status 0: active.
                    if r.flags & 0x07 != 0 {
                        continue;
                    }
                    if let Some(next) = name(r.next_hop).filter(|n| *n != router) {
                        let e = edges
                            .entry((router.clone(), next))
                            .or_insert(("route", None));
                        e.0 = "route";
                    }
                }
                start = start.saturating_add(routes.len() as u8);
                if start >= total {
                    break;
                }
            }
        }
        let coordinator_key = key(coordinator.ieee);
        let mut list =
            vec![json!({ "from": coordinator_key, "to": self.ctx.out.via(), "kind": "bus" })];
        for ((from, to), (kind, lqi)) in edges.into_iter().take(1999) {
            let mut e = json!({ "from": from, "to": to, "kind": kind });
            if let Some(l) = lqi {
                e["quality"] = json!({ "lqi": l });
            }
            list.push(e);
        }
        self.ctx.out.topology(
            &coordinator_key,
            json!({ "ts": now(), "protocol": "zigbee", "source": "scan", "complete": true, "edges": list }),
        );
    }

    /// The `map` op: start a walk unless joining is open, an upgrade runs,
    /// or a walk is already going.
    fn map(self: &Arc<Self>) -> Result<Value, RadioError> {
        let host = self
            .host()
            .ok_or_else(|| refused("no network on this radio"))?;
        if self
            .joining
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|j| j.until > now())
        {
            return Err(RadioError::Busy("the network is open for joining".into()));
        }
        if self.commands.upgrading() {
            return Err(RadioError::Busy("a firmware upgrade is running".into()));
        }
        if self.mapping.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Err(RadioError::Busy("a map is already being made".into()));
        }
        let requests = Self::routers(&host).len() * 2;
        let m = self.clone();
        tokio::spawn(async move {
            m.walk().await;
            m.mapping.store(false, std::sync::atomic::Ordering::SeqCst);
        });
        Ok(json!({ "started": true, "requests": requests }))
    }

    /// Host events to protocol messages.
    async fn pump(
        self: Arc<Self>,
        host: Host<AnyAdapter>,
        mut rx: mpsc::UnboundedReceiver<HostEvent>,
    ) {
        let via = self.ctx.out.via();
        while let Some(event) = rx.recv().await {
            let coordinator = key(host.network().ieee);
            match event {
                HostEvent::Started { .. } => self.publish_network("up"),
                HostEvent::PermitJoin { seconds, via: v } => {
                    let until = (seconds > 0).then(|| now() + f64::from(seconds));
                    let via_key = v.map(key);
                    // A longer window than one NCP permit is kept open by
                    // the module; it reports its own end.
                    let keep = self
                        .joining
                        .lock()
                        .unwrap()
                        .as_ref()
                        .is_some_and(|j| j.until > now() + 1.0);
                    if !keep {
                        self.ctx.out.event(
                            &coordinator,
                            "network.permit",
                            json!({ "until": until, "via": via_key }),
                            None,
                        );
                    }
                    self.publish_network("up");
                }
                HostEvent::DeviceJoined { ieee, rejoin, .. } => {
                    if !rejoin {
                        let rssi = host.device(ieee).and_then(|d| d.last_rssi);
                        self.ctx.out.event(
                            &coordinator,
                            "network.joined",
                            json!({ "device": key(ieee), "via": null, "rssi": rssi }),
                            None,
                        );
                        self.ctx.out.state(&key(ieee), true, Some(now()), None);
                        self.backup_soon();
                    }
                }
                HostEvent::DeviceLeft { ieee } => {
                    self.commands.forget(&key(ieee));
                    self.ctx.out.event(
                        &coordinator,
                        "network.left",
                        json!({ "device": key(ieee), "reason": "left" }),
                        None,
                    );
                    self.ctx.out.state(&key(ieee), false, None, None);
                    self.backup_soon();
                }
                HostEvent::DeviceRemoved { ieee, .. } => {
                    self.commands.forget(&key(ieee));
                    self.ctx.out.event(
                        &coordinator,
                        "network.left",
                        json!({ "device": key(ieee), "reason": "removed" }),
                        None,
                    );
                    self.ctx.out.state(&key(ieee), false, None, None);
                    self.backup_soon();
                }
                HostEvent::InterviewDone { device } => {
                    let k = key(device.ieee);
                    self.ctx.out.state_body(&k, json!({ "online": true, "last_heard": now(), "interview": { "zigbee": interview(&device, true) } }));
                    self.ctx.out.event(
                        &coordinator,
                        "network.interview",
                        json!({ "device": k, "complete": true }),
                        None,
                    );
                    if !device.reporting.is_empty() {
                        self.ctx.out.event(
                            &coordinator,
                            "network.configured",
                            json!({ "device": k }),
                            None,
                        );
                    }
                    self.publish_network("up");
                    self.backup_soon();
                }
                HostEvent::InterviewFailed { ieee, error, .. } => {
                    let k = key(ieee);
                    if let Some(d) = host.device(ieee) {
                        self.ctx.out.state_body(&k, json!({ "online": true, "interview": { "zigbee": interview(&d, false) } }));
                    }
                    self.ctx.out.event(
                        &coordinator,
                        "network.interview",
                        json!({ "device": k, "complete": false, "error": error }),
                        None,
                    );
                }
                HostEvent::Frame { ieee, link, frame } => {
                    let body = up_body(&host, ieee, &link, &frame, &via);
                    self.ctx.out.up_body(&key(ieee), body);
                }
                HostEvent::OtaProgress { ieee, percent } => {
                    self.ctx.out.event(
                        &key(ieee),
                        "ota.progress",
                        json!({ "percent": percent }),
                        None,
                    );
                }
                HostEvent::OtaFinished {
                    ieee,
                    from,
                    to,
                    success,
                    status,
                } => {
                    self.commands
                        .ota_finished(&self.ctx.out, ieee, from, to, success, status);
                }
                HostEvent::ChannelChanged { network } => {
                    self.ctx.out.event(
                        &coordinator,
                        "network.channel",
                        json!({ "channel": network.channel }),
                        None,
                    );
                    self.publish_network("up");
                    self.backup_soon();
                }
                HostEvent::AdapterFailed { reason } => {
                    tracing::error!(%reason, "the coordinator failed");
                    self.ctx.out.state_body(
                        &coordinator,
                        json!({ "online": false, "network": self.network_value("down") }),
                    );
                    self.network_error(&format!("the coordinator stopped: {reason}"));
                    // procd restarts the radio's process, which resumes.
                    std::process::exit(1);
                }
                _ => {}
            }
        }
    }

    async fn op(self: &Arc<Self>, op: &str, p: Value) -> Result<Value, RadioError> {
        match op {
            "status" => Ok(self.status()),
            "scan" => self.scan(&p).await,
            "form" => self.form(&p).await,
            "restore" => self.restore(&p).await,
            "backup" => Ok(json!({ "backup": self.take_backup().await? })),
            "leave" => self.leave(&p).await,
            "permit" => self.permit(&p).await,
            "permit-stop" => self.permit_stop().await,
            "provision" => self.provision(&p).await,
            "remove" => {
                let host = self
                    .host()
                    .ok_or_else(|| refused("no network on this radio"))?;
                let device = p
                    .get("device")
                    .and_then(Value::as_str)
                    .ok_or_else(|| bad("device is required"))?;
                let ieee =
                    ieee_of(device).ok_or_else(|| bad(format!("{device} is not a zigbee: key")))?;
                let force = p.get("force").and_then(Value::as_bool).unwrap_or(false);
                host.remove(ieee, force)
                    .await
                    .map_err(|e| refused(e.to_string()))?;
                Ok(json!({}))
            }
            "channel" => {
                let host = self
                    .host()
                    .ok_or_else(|| refused("no network on this radio"))?;
                let channel = p
                    .get("channel")
                    .and_then(Value::as_u64)
                    .filter(|c| (11..=26).contains(c))
                    .ok_or_else(|| bad("channel is 11 to 26"))?;
                let from = host.network().channel;
                host.change_channel(channel as u8)
                    .await
                    .map_err(|e| refused(e.to_string()))?;
                if let Some(c) = self.coordinator() {
                    self.ctx.out.event(
                        &c,
                        "network.channel",
                        json!({ "channel": channel, "from": from }),
                        None,
                    );
                }
                let mut file = self.ctx.load().map_err(|e| refused(e.to_string()))?;
                file["zigbee"]["channel"] = json!(channel);
                let _ = self.ctx.save(&file);
                Ok(self.status())
            }
            "map" => self.map(),
            "replace" => Err(refused(
                "Zigbee has no replace; remove the device and pair the new one",
            )),
            "region" => Err(refused("Zigbee has no regions")),
            "grant" | "dsk" | "abort" => Err(refused("that is a Z-Wave inclusion step")),
            other => Err(bad(format!("{other} is not a Zigbee op"))),
        }
    }

    async fn scan(&self, p: &Value) -> Result<Value, RadioError> {
        if p.get("seconds")
            .and_then(Value::as_u64)
            .is_some_and(|s| s > 10)
        {
            return Err(bad("seconds is at most 10"));
        }
        const ALL: u32 = 0x07FF_F800;
        let (energy, beacons) = match self.host() {
            Some(h) => (h.energy_scan().await, h.active_scan().await),
            None => {
                use zigbee_host::Adapter;
                let mut stage = self.stage.lock().await;
                let Stage::Unformed(held) = &mut *stage else {
                    return Err(RadioError::Busy("the network is starting".into()));
                };
                if held.is_none() {
                    *held = Some(self.connect().await?);
                }
                let Some((a, _)) = held.as_ref() else {
                    return Err(refused("the coordinator did not start"));
                };
                (a.energy_scan(ALL).await, a.active_scan(ALL).await)
            }
        };
        let energy = energy.unwrap_or_default();
        let beacons = beacons.map_err(|e| refused(e.to_string()))?;
        let channels: Vec<Value> = (11..=26u8)
            .map(|c| {
                json!({
                    "channel": c,
                    "energy": energy.iter().find(|(ch, _)| *ch == c).map(|(_, d)| dbm_to_energy(*d)),
                    "networks": beacons.iter().filter(|b| b.channel == c).map(|b| json!({
                        "pan_id": format!("{:04x}", b.pan_id), "ext_pan_id": hex16(b.extended_pan_id.0),
                        "permit_joining": b.permit_joining, "lqi": b.lqi,
                    })).collect::<Vec<_>>(),
                })
            })
            .collect();
        Ok(json!({ "channels": channels }))
    }

    async fn form(self: &Arc<Self>, p: &Value) -> Result<Value, RadioError> {
        let network = p
            .get("network")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && s.len() <= 64)
            .ok_or_else(|| bad("network is required"))?;
        let channel = p
            .get("channel")
            .and_then(Value::as_u64)
            .filter(|c| (11..=26).contains(c))
            .ok_or_else(|| bad("channel is 11 to 26"))? as u8;
        let pan_id = p
            .get("pan_id")
            .and_then(Value::as_str)
            .filter(|s| s.len() == 4)
            .and_then(|s| u16::from_str_radix(s, 16).ok())
            .filter(|p| *p != 0 && *p < 0xFFF8)
            .ok_or_else(|| bad("pan_id is 4 hex digits, not 0000 or fff8 to ffff"))?;
        let ext = p
            .get("ext_pan_id")
            .and_then(Value::as_str)
            .filter(|s| s.len() == 16)
            .and_then(|s| u64::from_str_radix(s, 16).ok())
            .filter(|e| *e != 0 && *e != u64::MAX)
            .ok_or_else(|| bad("ext_pan_id is 16 hex digits"))?;
        let key_hex = p
            .get("network_key")
            .and_then(Value::as_str)
            .ok_or_else(|| bad("network_key is required"))?;
        let network_key = parse_hex_key(key_hex)
            .filter(|k| !k.is_zero())
            .ok_or_else(|| bad("network_key is 32 hex digits"))?;
        let join = JoinPolicy {
            allow_legacy: p.get("legacy").and_then(Value::as_bool).unwrap_or(true),
            install_code_only: p
                .get("install_codes_only")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        };
        if self.host().is_some() {
            return Err(refused(
                "a network is already running on this radio; leave it first",
            ));
        }
        if join.install_code_only && self.kind().await? == NcpKind::Deconz {
            return Err(bad(
                "this ConBee/RaspBee firmware cannot require install codes",
            ));
        }
        let settings = NetworkSettings {
            channel,
            pan_id,
            extended_pan_id: ExtPanId(ext),
            network_key,
            tx_power: None,
        };
        // The keys go to the radio's file before the radio, so a crash
        // between the two resumes rather than forgets.
        let mut file = self.ctx.load().map_err(|e| refused(e.to_string()))?;
        file["network"] = json!(network);
        file["zigbee"] = json!({
            "channel": channel, "pan_id": format!("{pan_id:04x}"), "ext_pan_id": hex16(ext),
            "network_key": hex::encode(network_key.0), "legacy": join.allow_legacy,
            "install_codes_only": join.install_code_only,
        });
        self.ctx.save(&file).map_err(|e| refused(e.to_string()))?;
        let _ = std::fs::remove_file(self.store_path());
        if let Err(e) = self
            .run(
                StartMode::Normal {
                    settings,
                    replace_other: false,
                },
                network.to_string(),
                join,
            )
            .await
        {
            // Nothing formed: forget the keys again.
            if let Ok(mut file) = self.ctx.load() {
                if let Some(o) = file.as_object_mut() {
                    o.remove("zigbee");
                    o.remove("network");
                }
                let _ = self.ctx.save(&file);
            }
            return Err(e);
        }
        self.backup_soon();
        Ok(self.status())
    }

    async fn restore(self: &Arc<Self>, p: &Value) -> Result<Value, RadioError> {
        let network = p
            .get("network")
            .and_then(Value::as_str)
            .ok_or_else(|| bad("network is required"))?;
        let env = p.get("backup").ok_or_else(|| bad("backup is required"))?;
        let force = p.get("force").and_then(Value::as_bool).unwrap_or(false);
        if env.get("protocol").and_then(Value::as_str) != Some("zigbee") {
            return Err(refused("the backup is not of a Zigbee network"));
        }
        if env.get("network").and_then(Value::as_str) != Some(network) {
            return Err(refused("the backup is of another network"));
        }
        let inner = env
            .get("zigbee")
            .ok_or_else(|| bad("the backup has no zigbee section"))?;
        let mut backup = Backup::from_json(&inner.to_string()).map_err(|e| bad(e.to_string()))?;
        if self.host().is_some() {
            return Err(refused(
                "a network is already running on this radio; leave it first",
            ));
        }
        // zigbee-host advances the counter by its own margin; the rest here.
        backup.network_key.frame_counter = backup
            .network_key
            .frame_counter
            .saturating_add(RESTORE_MARGIN - RESTORE_FRAME_COUNTER_MARGIN);
        let settings = json!({
            "channel": backup.channel, "pan_id": format!("{:04x}", backup.pan_id),
            "ext_pan_id": hex16(backup.extended_pan_id.0), "network_key": hex::encode(backup.network_key.key.0),
            "legacy": true, "install_codes_only": false,
        });
        let mut file = self.ctx.load().map_err(|e| refused(e.to_string()))?;
        file["network"] = json!(network);
        file["zigbee"] = settings;
        self.ctx.save(&file).map_err(|e| refused(e.to_string()))?;
        if let Some(c) = self
            .coordinator()
            .or_else(|| Some(key(backup.coordinator_ieee)))
        {
            self.ctx.out.state_body(&c, json!({ "online": true, "network": { "protocol": "zigbee", "state": "restoring", "radio": self.ctx.radio } }));
        }
        let join = JoinPolicy::default();
        match self
            .run(
                StartMode::Restore(Box::new(backup.clone())),
                network.to_string(),
                join.clone(),
            )
            .await
        {
            Ok(()) => Ok(self.status()),
            Err(RadioError::Refused(why)) if force && why.contains("IEEE") => {
                // Under the dongle's own address: devices must rejoin.
                let own = self.dongle_ieee().await?;
                backup.coordinator_ieee = own;
                self.run(
                    StartMode::Restore(Box::new(backup)),
                    network.to_string(),
                    join,
                )
                .await?;
                Ok(self.status())
            }
            Err(e) => Err(e),
        }
    }

    async fn dongle_ieee(&self) -> Result<Ieee, RadioError> {
        let kind = self.kind().await?;
        let io = zigbee_host::port::open(
            &self.ctx.port,
            115_200,
            zigbee_host::port::FlowControl::None,
        )
        .await
        .map_err(|e| refused(e.to_string()))?;
        zigbee_host::probe::probe_stream(io, kind)
            .await
            .ok()
            .flatten()
            .and_then(|id| id.ieee)
            .ok_or_else(|| refused("the dongle did not say its IEEE address"))
    }

    async fn leave(self: &Arc<Self>, p: &Value) -> Result<Value, RadioError> {
        let erase = p.get("erase").and_then(Value::as_bool).unwrap_or(false);
        let mut stage = self.stage.lock().await;
        let coordinator = self.coordinator();
        match std::mem::replace(&mut *stage, Stage::Unformed(None)) {
            Stage::Running(r) => {
                if erase {
                    if let Err(e) = r.host.leave().await {
                        *stage = Stage::Running(r);
                        return Err(refused(format!("erasing the network: {e}")));
                    }
                } else {
                    let _ = r.host.shutdown().await;
                }
                r.pump.abort();
            }
            Stage::Unformed(held) => {
                if erase {
                    use zigbee_host::Adapter;
                    let (a, rx) = match held {
                        Some(h) => h,
                        None => self.connect().await?,
                    };
                    a.leave()
                        .await
                        .map_err(|e| refused(format!("erasing the radio: {e}")))?;
                    *stage = Stage::Unformed(Some((a, rx)));
                }
            }
        }
        *self.host.lock().unwrap() = None;
        self.joining.lock().unwrap().take();
        drop(stage);
        let _ = std::fs::remove_file(self.store_path());
        let mut file = self.ctx.load().map_err(|e| refused(e.to_string()))?;
        if let Some(o) = file.as_object_mut() {
            for k in ["zigbee", "network", "coordinator", "backup_at"] {
                o.remove(k);
            }
        }
        self.ctx.save(&file).map_err(|e| refused(e.to_string()))?;
        if let Some(c) = coordinator {
            self.ctx.out.state(&c, false, None, None);
        }
        Ok(self.status())
    }

    async fn permit(self: &Arc<Self>, p: &Value) -> Result<Value, RadioError> {
        let host = self
            .host()
            .ok_or_else(|| refused("no network on this radio"))?;
        let seconds = p.get("seconds").and_then(Value::as_u64).unwrap_or(180);
        if !(30..=600).contains(&seconds) {
            return Err(bad("seconds is 30 to 600"));
        }
        let via = match p.get("via").and_then(Value::as_str) {
            None => None,
            Some(k) => {
                let ieee = ieee_of(k).ok_or_else(|| bad(format!("{k} is not a zigbee: key")))?;
                host.device(ieee)
                    .ok_or_else(|| refused(format!("{k} is not on this network")))?;
                Some(ieee)
            }
        };
        let until = now() + seconds as f64;
        // An NCP permit lasts at most 254 s: keep re-opening until `until`.
        let window = |left: f64| left.clamp(1.0, 254.0) as u8;
        host.permit_join(window(seconds as f64), via)
            .await
            .map_err(|e| refused(e.to_string()))?;
        let m = self.clone();
        let task = tokio::spawn(async move {
            loop {
                let left = until - now();
                if left <= 254.0 {
                    tokio::time::sleep(Duration::from_secs_f64(left.max(0.0))).await;
                    break;
                }
                tokio::time::sleep(Duration::from_secs(240)).await;
                let left = until - now();
                if let Some(h) = m.host() {
                    let _ = h.permit_join(window(left), via).await;
                }
            }
            m.joining.lock().unwrap().take();
            if let Some(c) = m.coordinator() {
                m.ctx.out.event(
                    &c,
                    "network.permit",
                    json!({ "until": null, "via": null }),
                    None,
                );
                m.publish_network("up");
            }
        });
        if let Some(old) = self.joining.lock().unwrap().replace(Joining {
            until,
            via: via.map(key),
            task: Some(task),
        }) {
            if let Some(t) = old.task {
                t.abort();
            }
        }
        if let Some(c) = self.coordinator() {
            self.ctx.out.event(
                &c,
                "network.permit",
                json!({ "until": until, "via": via.map(key) }),
                None,
            );
        }
        self.publish_network("up");
        Ok(json!({ "until": until }))
    }

    async fn permit_stop(&self) -> Result<Value, RadioError> {
        let host = self
            .host()
            .ok_or_else(|| refused("no network on this radio"))?;
        if let Some(j) = self.joining.lock().unwrap().take() {
            if let Some(t) = j.task {
                t.abort();
            }
        }
        host.permit_join(0, None)
            .await
            .map_err(|e| refused(e.to_string()))?;
        if let Some(c) = self.coordinator() {
            self.ctx.out.event(
                &c,
                "network.permit",
                json!({ "until": null, "via": null }),
                None,
            );
        }
        self.publish_network("up");
        Ok(json!({ "until": null }))
    }

    async fn provision(&self, p: &Value) -> Result<Value, RadioError> {
        let host = self
            .host()
            .ok_or_else(|| refused("form the network first"))?;
        let entries = p
            .get("entries")
            .and_then(Value::as_array)
            .ok_or_else(|| bad("entries is required"))?;
        if entries.len() > 500 {
            return Err(bad("at most 500 entries"));
        }
        let mut list = Vec::with_capacity(entries.len());
        for e in entries {
            let ieee = e
                .get("ieee")
                .and_then(Value::as_str)
                .and_then(|s| s.trim_start_matches("zigbee:").parse::<Ieee>().ok())
                .ok_or_else(|| bad("each entry needs an ieee address (16 hex digits)"))?;
            let code = e
                .get("install_code")
                .and_then(Value::as_str)
                .and_then(|s| zigbee_host::install_code::parse_hex(s).ok())
                .ok_or_else(|| bad(format!("{ieee}: the install code is not hex")))?;
            list.push((ieee, code));
        }
        let count = host
            .set_install_codes(&list)
            .await
            .map_err(|e| bad(e.to_string()))?;
        Ok(json!({ "count": count }))
    }
}

/// The protocol's interview of a device.
pub fn interview(d: &Device, complete: bool) -> Value {
    let mut v = json!({
        "ieee": d.ieee.to_string(),
        "nwk": format!("{:04x}", d.nwk),
        "endpoints": d.endpoints.iter().filter(|e| (1..=254).contains(&e.endpoint)).take(240).map(|e| json!({
            "id": e.endpoint, "profile": e.profile_id, "device_id": e.device_id,
            "in": e.input_clusters.iter().take(128).collect::<Vec<_>>(),
            "out": e.output_clusters.iter().take(128).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "at": d.joined_unix,
        "complete": complete,
    });
    let o = v.as_object_mut().expect("an object");
    if let Some(n) = d.node_descriptor.as_ref() {
        o.insert("manufacturer_code".into(), json!(n.manufacturer_code));
        o.insert(
            "type".into(),
            json!(match n.logical_type() {
                zigbee_zdo::LogicalType::EndDevice => "end_device",
                _ => "router",
            }),
        );
    }
    let text = |s: &Option<String>, max: usize| {
        s.as_ref().map(|s| s.chars().take(max).collect::<String>())
    };
    for (k, val) in [
        (
            "manufacturer_name",
            text(&d.manufacturer, 64).map(Value::from),
        ),
        ("model_id", text(&d.model, 64).map(Value::from)),
        ("build_id", text(&d.sw_build_id, 32).map(Value::from)),
        ("date_code", text(&d.date_code, 32).map(Value::from)),
        ("power_source", d.power_source.map(Value::from)),
        ("app_version", d.app_version.map(Value::from)),
        ("zcl_version", d.zcl_version.map(Value::from)),
        ("stack_version", d.stack_version.map(Value::from)),
        ("hw_version", d.hw_version.map(Value::from)),
    ] {
        if let Some(val) = val {
            o.insert(k.into(), val);
        }
    }
    v
}

/// The `up` for a ZCL frame: `raw`, `rx`, `link.zigbee`, and `r` for the
/// standard readings it carries.
pub fn up_body(
    host: &Host<AnyAdapter>,
    ieee: Ieee,
    link: &zigbee_host::FrameLink,
    frame: &[u8],
    via: &str,
) -> Value {
    let ts = now();
    let mut z = json!({
        "endpoint": link.source_endpoint, "dst_endpoint": link.destination_endpoint,
        "cluster": link.cluster, "profile": link.profile, "lqi": link.lqi,
        "nwk": format!("{:04x}", link.nwk), "last_hop": format!("{:04x}", link.last_hop),
    });
    let mut rx = json!({ "via": via, "lqi": link.lqi, "ts": ts, "time": "received" });
    if let Some(r) = link.rssi {
        z["rssi"] = json!(r);
        rx["rssi"] = json!(r);
    }
    if let Some(g) = link.group {
        z["group"] = json!(g);
    }
    let parsed = zigbee_zcl::Frame::parse(frame).ok();
    if let Some(f) = &parsed {
        z["command"] = json!(f.header.command);
        if let Some(m) = f.header.manufacturer {
            z["manufacturer"] = json!(m);
        }
    }
    let mut body = json!({
        "raw": { "ts": ts, "b64": base64::engine::general_purpose::STANDARD.encode(frame) },
        "rx": [rx],
        "link": { "zigbee": z },
    });
    if let Some(r) = parsed.and_then(|f| readings(host, ieee, link, &f)) {
        body["r"] = json!([{ "ts": ts, "v": r }]);
    }
    body
}

/// The standard readings in an attribute report or read response.
fn readings(
    host: &Host<AnyAdapter>,
    ieee: Ieee,
    link: &zigbee_host::FrameLink,
    f: &zigbee_zcl::Frame,
) -> Option<Map<String, Value>> {
    use zigbee_zcl::{FrameType, Global};
    if f.header.frame_type != FrameType::Global || f.header.manufacturer.is_some() {
        return None;
    }
    let values: Vec<(u16, zigbee_zcl::Value)> =
        match Global::decode(f.header.command, &f.payload).ok()? {
            Global::ReportAttributes(a) => a,
            Global::ReadAttributesResponse(r) => r
                .into_iter()
                .filter_map(|r| r.value.map(|v| (r.attribute, v)))
                .collect(),
            _ => return None,
        };
    let device = host.device(ieee)?;
    let mut out = Map::new();
    for (attribute, value) in values {
        let Some(e) = zigbee_bridge::exposes::find(link.cluster, attribute) else {
            continue;
        };
        if let Some(v) = zigbee_bridge::exposes::convert(e, &value) {
            out.insert(
                zigbee_bridge::exposes::field_name(&device, link.source_endpoint, e),
                v,
            );
        }
    }
    (!out.is_empty()).then_some(out)
}

impl RadioModule for Zigbee {
    fn protocol(&self) -> &'static str {
        "zigbee"
    }

    fn call<'a>(&'a self, op: &'a str, params: Value) -> BoxFuture<'a, Result<Value, RadioError>> {
        Box::pin(async move {
            let me = self
                .me
                .upgrade()
                .ok_or_else(|| refused("the module is stopping"))?;
            me.op(op, params).await
        })
    }

    fn dispatch(&self, out: &Outbox, topic: &str, payload: &[u8]) {
        self.commands.dispatch(self.host(), out, topic, payload);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_and_energy() {
        let i = Ieee(0x00124B0012345678);
        assert_eq!(key(i), "zigbee:00124b0012345678");
        assert_eq!(ieee_of("zigbee:00124b0012345678"), Some(i));
        assert_eq!(ieee_of("ble:00124b0012345678"), None);
        assert_eq!(dbm_to_energy(-100), 0);
        assert_eq!(dbm_to_energy(-60), 255);
    }
}
