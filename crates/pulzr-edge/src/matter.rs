//! Matter: the gateway's controller in its organization's Matter fabric, on
//! matter-bridge's `matter-controller` (pulzar-net/matter-bridge).
//!
//! It runs as the radio `matter` (`pulzr-edge radio serve matter`), on no
//! port of its own: Matter rides on IP, and on Thread through the border
//! router. Like every radio module, everything that changes the fabric or
//! carries a secret arrives as an op from the gateway's console API, as a
//! person's session (PulzrOS `docs/radio-modules.md`):
//!
//! - `csr`: the gateway makes its intermediate CA key (it never leaves the
//!   gateway) and returns the key's CSR; the platform's device CA issues an
//!   intermediate certificate for it under the organization's root.
//! - `join`: the issued certificates, the fabric's IPK and a range of node
//!   ids; the controller starts on the fabric. A further `join` brings a
//!   new intermediate or a further range.
//! - `commission`, `share` (a commissioning window for Apple Home, Google
//!   Home and the like), `decommission`, `abort`, `status`.
//!
//! What happens afterwards goes out as protocol v1 on MQTT, with no secret
//! in it: the controller's `state.network` and `network.commission` events
//! on `matter:<compressed fabric id>.<controller node id>`, and each device
//! as `matter:<compressed fabric id>.<node id>` with its readings (scalar
//! attributes, `<endpoint>.<cluster>.<attribute>`), events, availability
//! and `state.interview.matter`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use matter_controller as mc;
use pulzr_protocol as proto;
use serde_json::{json, Map, Value};

use crate::outbox::{now, Outbox};
use crate::radio::{BoxFuture, RadioCtx, RadioError, RadioModule};

/// The UDP port of the gateway's controller.
const PORT: u16 = 5541;
/// Where the Thread border router (the pulzr-thread add-on's otbr-agent,
/// REST on loopback only) gives its active dataset.
const OTBR_DATASET: &str = "http://127.0.0.1:8081/node/dataset/active";
/// How often attribute reports are gathered into one `up` per device.
const UP_EVERY: Duration = Duration::from_secs(1);

type Result<T> = std::result::Result<T, RadioError>;

fn bad(m: impl Into<String>) -> RadioError {
    RadioError::BadRequest(m.into())
}

fn refused(m: impl std::fmt::Display) -> RadioError {
    RadioError::Refused(m.to_string())
}

pub fn start(ctx: RadioCtx) -> anyhow::Result<Arc<dyn RadioModule>> {
    let m = Arc::new_cyclic(|me| MatterModule {
        ctx,
        me: me.clone(),
        st: Mutex::new(St::default()),
    });
    tokio::spawn(m.clone().boot());
    Ok(m)
}

#[derive(Default)]
struct St {
    controller: Option<mc::Controller>,
    /// The platform's id of this fabric (a UUID), from `join`.
    network: Option<String>,
    /// The commissioning running, as `(device key, stage)`.
    commissioning: Option<(String, String)>,
    devices: usize,
}

struct MatterModule {
    ctx: RadioCtx,
    me: std::sync::Weak<MatterModule>,
    st: Mutex<St>,
}

fn key(cfid: u64, node: u64) -> String {
    format!("matter:{cfid:016x}.{node:016x}")
}

fn parse_key(key: &str) -> Option<(u64, u64)> {
    let (cfid, node) = key.strip_prefix("matter:")?.split_once('.')?;
    if cfid.len() != 16 || node.len() != 16 {
        return None;
    }
    Some((
        u64::from_str_radix(cfid, 16).ok()?,
        u64::from_str_radix(node, 16).ok()?,
    ))
}

/// A certificate as the platform sends it, in hex: X.509 DER in the Matter
/// profile (converted here, and refused if it would not verify as Matter
/// TLV), or Matter TLV.
fn cert(params: &Value, field: &str) -> Result<Vec<u8>> {
    let hex_text = params[field]
        .as_str()
        .ok_or_else(|| bad(format!("`{field}` is required")))?;
    let bytes = hex::decode(hex_text).map_err(|_| bad(format!("`{field}` is not hex")))?;
    match bytes.first() {
        // A Matter TLV certificate is an anonymous structure.
        Some(0x15) => Ok(bytes),
        // An X.509 DER certificate is a SEQUENCE.
        Some(0x30) => {
            mc::x509::to_matter_tlv(&bytes).map_err(|e| refused(format!("`{field}`: {e}")))
        }
        _ => Err(bad(format!("`{field}` is not a certificate"))),
    }
}

fn hex_u64(params: &Value, field: &str) -> Result<u64> {
    params[field]
        .as_str()
        .and_then(|s| u64::from_str_radix(s, 16).ok())
        .ok_or_else(|| bad(format!("`{field}` must be hex")))
}

impl MatterModule {
    fn state_dir(&self) -> PathBuf {
        self.ctx.file("controller")
    }

    fn controller(&self) -> Result<mc::Controller> {
        self.st
            .lock()
            .unwrap()
            .controller
            .clone()
            .ok_or_else(|| refused("this gateway has not joined a Matter fabric yet"))
    }

    fn controller_key(&self, c: &mc::Controller) -> String {
        key(c.info().compressed_fabric_id, c.info().node_id)
    }

    /// At start: run the controller if the gateway has joined a fabric.
    async fn boot(self: Arc<Self>) {
        let dir = self.state_dir();
        if !mc::intermediate_path(&dir).exists() {
            tracing::info!("Matter: no fabric joined yet");
            return;
        }
        if let Ok(state) = self.ctx.load() {
            self.st.lock().unwrap().network = state["network"].as_str().map(String::from);
        }
        if let Err(e) = self.clone().run_controller().await {
            tracing::error!(error = e.message(), "Matter controller did not start");
        }
    }

    /// (Re)start the controller on the installed fabric, and its event pump.
    async fn run_controller(self: Arc<Self>) -> Result<mc::Controller> {
        let mut config = mc::Config::new(self.state_dir());
        config.ca = mc::CaChoice::Intermediate;
        config.port = PORT;
        let controller = tokio::task::spawn_blocking(move || mc::Controller::start(config))
            .await
            .map_err(refused)?
            .map_err(refused)?;
        let previous = self
            .st
            .lock()
            .unwrap()
            .controller
            .replace(controller.clone());
        drop(previous);
        tokio::spawn(self.clone().pump(controller.clone()));
        self.publish_network(&controller).await;
        Ok(controller)
    }

    async fn publish_network(&self, c: &mc::Controller) {
        let devices = c.nodes().await.map(|n| n.len()).unwrap_or(0);
        let left = mc::IntermediateCa::open(&mc::intermediate_path(&self.state_dir()))
            .map(|ca| ca.node_ids_left())
            .unwrap_or(0);
        self.st.lock().unwrap().devices = devices;
        let info = c.info();
        self.ctx.network_state(
            &self.controller_key(c),
            json!({
                "protocol": "matter",
                "state": "up",
                "fabric_id": format!("{:016x}", info.fabric_id),
                "compressed_fabric_id": format!("{:016x}", info.compressed_fabric_id),
                "devices": devices,
                "node_ids_left": left,
            }),
        );
    }

    /// The controller's events, as protocol v1.
    async fn pump(self: Arc<Self>, c: mc::Controller) {
        let cfid = c.info().compressed_fabric_id;
        let me = self.controller_key(&c);
        let out = self.ctx.out.clone();
        let mut events = c.events();
        let mut pending: HashMap<u64, Map<String, Value>> = HashMap::new();
        let mut tick = tokio::time::interval(UP_EVERY);
        loop {
            tokio::select! {
                event = events.recv() => {
                    let Some(event) = event else { return };
                    match event {
                        mc::Event::Attribute(a) => {
                            if let Some((name, value)) = reading(&a) {
                                pending.entry(a.node).or_default().insert(name, value);
                            }
                        }
                        mc::Event::Event(e) => out.event(
                            &key(cfid, e.node),
                            &format!(
                                "matter.{}.{}",
                                cluster_name(e.cluster),
                                event_name(e.cluster, e.event)
                            ),
                            json!({ "number": e.number, "data": e.data }),
                            None,
                        ),
                        mc::Event::Availability { node, online } => {
                            out.state(&key(cfid, node), online, Some(now()), None)
                        }
                        mc::Event::NodeUpdated(node) => {
                            out.state_body(&key(cfid, node.node_id), json!({
                                "online": true,
                                "interview": { "matter": interview(&node) },
                            }));
                            self.publish_network(&c).await;
                        }
                        mc::Event::NodeRemoved { node } => {
                            self.ctx.event(&me, "network.left", json!({ "device": key(cfid, node) }));
                            self.publish_network(&c).await;
                        }
                        mc::Event::Commissioning { node, step } => {
                            let stage = serde_json::to_value(step)
                                .ok()
                                .and_then(|v| v.as_str().map(String::from))
                                .unwrap_or_default();
                            self.st.lock().unwrap().commissioning =
                                Some((key(cfid, node), stage.clone()));
                            self.ctx.event(&me, "network.commission", json!({
                                "stage": stage,
                                "device": key(cfid, node),
                            }));
                        }
                    }
                }
                _ = tick.tick() => {
                    for (node, values) in pending.drain() {
                        out.up(&key(cfid, node), now(), values, None);
                    }
                }
            }
        }
    }

    async fn op(self: Arc<Self>, op: &str, params: Value) -> Result<Value> {
        match op {
            "status" => Ok(self.status().await),
            "csr" => {
                let dir = self.state_dir();
                let csr = tokio::task::spawn_blocking(move || mc::intermediate_csr(&dir))
                    .await
                    .map_err(refused)?
                    .map_err(refused)?;
                Ok(json!({ "csr": hex::encode(csr) }))
            }
            "join" => self.join(params).await,
            "commission" => self.commission(params).await,
            "share" => {
                let c = self.controller()?;
                let node = self.device(&c, &params)?;
                let seconds = params["seconds"].as_u64().unwrap_or(300);
                let seconds = u16::try_from(seconds)
                    .ok()
                    .filter(|s| (180..=900).contains(s))
                    .ok_or_else(|| bad("`seconds` is 180 to 900"))?;
                let window = c
                    .open_commissioning_window(node, seconds)
                    .await
                    .map_err(refused)?;
                // The codes go back to the person who asked, and nowhere
                // else: not MQTT, not a log.
                Ok(json!({
                    "manual_code": window.manual_code,
                    "qr_code": window.qr_code,
                    "seconds": seconds,
                }))
            }
            "decommission" => {
                let c = self.controller()?;
                let node = self.device(&c, &params)?;
                match c.remove(node, false).await {
                    Ok(()) => Ok(json!({ "removed": true, "reached": true })),
                    Err(e) => {
                        tracing::warn!(error = %e, "the device could not be told to leave; forgetting it");
                        c.remove(node, true).await.map_err(refused)?;
                        Ok(json!({ "removed": true, "reached": false }))
                    }
                }
            }
            "abort" => {
                let c = self.controller()?;
                let running = c.abort_commissioning().await.map_err(refused)?;
                Ok(json!({ "aborted": running }))
            }
            other => Err(bad(format!("no op `{other}` for Matter"))),
        }
    }

    fn device(&self, c: &mc::Controller, params: &Value) -> Result<u64> {
        let device = params["device"]
            .as_str()
            .ok_or_else(|| bad("`device` is required"))?;
        match parse_key(device) {
            Some((cfid, node)) if cfid == c.info().compressed_fabric_id => Ok(node),
            _ => Err(bad(format!("{device} is not a device of this fabric"))),
        }
    }

    async fn status(&self) -> Value {
        let (controller, commissioning, network, devices) = {
            let st = self.st.lock().unwrap();
            (
                st.controller.clone(),
                st.commissioning.clone(),
                st.network.clone(),
                st.devices,
            )
        };
        let Some(c) = controller else {
            return json!({ "state": "unjoined", "network": network });
        };
        let info = c.info();
        let left = mc::IntermediateCa::open(&mc::intermediate_path(&self.state_dir()))
            .map(|ca| ca.node_ids_left())
            .unwrap_or(0);
        json!({
            "state": "up",
            "network": network,
            "fabric_id": format!("{:016x}", info.fabric_id),
            "compressed_fabric_id": format!("{:016x}", info.compressed_fabric_id),
            "node_id": format!("{:016x}", info.node_id),
            "devices": devices,
            "node_ids_left": left,
            "commissioning": commissioning.map(|(device, stage)| json!({ "device": device, "stage": stage })),
        })
    }

    async fn join(self: Arc<Self>, params: Value) -> Result<Value> {
        let network = params["network"]
            .as_str()
            .ok_or_else(|| bad("`network` is required"))?
            .to_string();
        let ipk = params["ipk"]
            .as_str()
            .and_then(|s| hex::decode(s).ok())
            .filter(|k| k.len() == 16)
            .ok_or_else(|| bad("`ipk` must be 32 hex"))?;
        let first = hex_u64(&params["node_ids"], "first")?;
        let last = hex_u64(&params["node_ids"], "last")?;
        let admin_cat = match params["admin_cat"].as_str() {
            Some(s) => {
                Some(u32::from_str_radix(s, 16).map_err(|_| bad("`admin_cat` must be hex"))?)
            }
            None => None,
        };
        let issued = mc::Issued {
            fabric_id: hex_u64(&params, "fabric_id")?,
            root_cert: cert(&params, "rcac")?,
            intermediate_cert: cert(&params, "icac")?,
            ipk,
            node_ids: mc::NodeIdRange { next: first, last },
            admin_cat,
        };
        let dir = self.state_dir();
        let only_range =
            mc::intermediate_path(&dir).exists() && !dir.join("intermediate.key.json").exists();
        tokio::task::spawn_blocking(move || mc::install_intermediate(&dir, issued))
            .await
            .map_err(refused)?
            .map_err(refused)?;

        let mut file = self.ctx.load().unwrap_or_else(|_| json!({}));
        file["radio"] = json!(crate::radio::MATTER);
        file["protocol"] = json!("matter");
        file["network"] = json!(network);
        self.ctx.save(&file).map_err(refused)?;
        self.st.lock().unwrap().network = Some(network);

        let running = self.st.lock().unwrap().controller.clone();
        match running {
            // A further range: the running controller reads it as it
            // allocates.
            Some(c) if only_range => self.publish_network(&c).await,
            _ => {
                self.clone().run_controller().await?;
            }
        }
        Ok(self.status().await)
    }

    async fn commission(self: Arc<Self>, params: Value) -> Result<Value> {
        let c = self.controller()?;
        let code = params["code"]
            .as_str()
            .ok_or_else(|| bad("`code` is required"))?
            .to_string();
        mc::Onboarding::parse(&code).map_err(|e| bad(e.to_string()))?;
        let thread_dataset = if params["thread_network"].is_string() {
            let text = reqwest::Client::new()
                .get(OTBR_DATASET)
                .header("Accept", "text/plain")
                .send()
                .await
                .and_then(|r| r.error_for_status())
                .map_err(|e| refused(format!("the Thread border router's dataset: {e}")))?
                .text()
                .await
                .map_err(refused)?;
            Some(
                mc::network::parse_thread_dataset_hex(&text)
                    .map_err(|e| refused(format!("the Thread border router's dataset: {e}")))?,
            )
        } else {
            None
        };
        if self.st.lock().unwrap().commissioning.is_some() {
            return Err(RadioError::Busy("a device is being commissioned".into()));
        }
        let me = self.controller_key(&c);
        self.st.lock().unwrap().commissioning = Some((String::new(), "discovery".into()));
        let module = self.clone();
        tokio::spawn(async move {
            let result = c
                .commission(mc::CommissionRequest {
                    code,
                    address: None,
                    name: None,
                    discovery_timeout: 60,
                    networks: mc::Networks {
                        thread_dataset,
                        wifi: None,
                    },
                })
                .await;
            let device = module
                .st
                .lock()
                .unwrap()
                .commissioning
                .take()
                .map(|(d, _)| d)
                .unwrap_or_default();
            let cfid = c.info().compressed_fabric_id;
            match result {
                Ok(node) => module.ctx.event(
                    &me,
                    "network.commission",
                    json!({ "stage": "done", "device": key(cfid, node.node_id) }),
                ),
                Err(e) => module.ctx.event(
                    &me,
                    "network.commission",
                    json!({ "stage": "failed", "device": device, "reason": e.to_string() }),
                ),
            }
        });
        Ok(json!({ "started": true }))
    }

    /// A protocol `cmd` for one of the fabric's devices: `matter.read`,
    /// `matter.write`, `matter.invoke`.
    fn command(&self, out: &Outbox, dev: &str, cmd: proto::Cmd) {
        let id = String::from(cmd.id.clone());
        let name = String::from(cmd.name.clone());
        let res = |status: &str,
                   reason: Option<&str>,
                   detail: Option<&str>,
                   result: Option<Map<String, Value>>| {
            out.cmd_res(dev, &id, status, reason, detail, result)
        };
        if cmd.v != proto::v1() {
            return res(
                "refused",
                Some("invalid"),
                Some("unsupported protocol version"),
                None,
            );
        }
        if cmd
            .expires
            .as_ref()
            .is_some_and(|t| f64::from(t.clone()) < now())
        {
            return res("failed", Some("expired"), None, None);
        }
        let Ok(c) = self.controller() else {
            return res(
                "failed",
                Some("undeliverable"),
                Some("the Matter controller is not up"),
                None,
            );
        };
        let node = match parse_key(dev) {
            Some((cfid, node)) if cfid == c.info().compressed_fabric_id => node,
            _ => return, // another fabric's device
        };
        if node == c.info().node_id {
            return res(
                "refused",
                Some("unknown"),
                Some("fabric operations come only from Pulzr Net's console"),
                None,
            );
        }
        let params = serde_json::to_value(&cmd.params).unwrap_or(Value::Null);
        let out = out.clone();
        let dev = dev.to_string();
        tokio::spawn(async move {
            let result = run_command(&c, node, &name, &params).await;
            match result {
                Ok(Value::Object(m)) => out.cmd_res(&dev, &id, "ok", None, None, Some(m)),
                Ok(v) => {
                    let mut m = Map::new();
                    m.insert("value".into(), v);
                    out.cmd_res(&dev, &id, "ok", None, None, Some(m))
                }
                Err((reason, detail)) => {
                    out.cmd_res(&dev, &id, "failed", Some(reason), Some(&detail), None)
                }
            }
        });
    }
}

async fn run_command(
    c: &mc::Controller,
    node: u64,
    name: &str,
    params: &Value,
) -> std::result::Result<Value, (&'static str, String)> {
    let invalid = |m: &str| ("invalid", m.to_string());
    let id = |field: &str,
              parse: &dyn Fn(&str) -> Option<u32>|
     -> std::result::Result<u32, (&'static str, String)> {
        match &params[field] {
            Value::Number(n) => n.as_u64().and_then(|n| u32::try_from(n).ok()),
            Value::String(s) => parse(s),
            _ => None,
        }
        .ok_or_else(|| invalid(&format!("`{field}` is not a known name or number")))
    };
    let endpoint = || {
        params["endpoint"]
            .as_u64()
            .and_then(|e| u16::try_from(e).ok())
            .ok_or_else(|| invalid("`endpoint` is required"))
    };
    let cluster_of = |s: &str| matter_model::cluster_by_name(s).map(|c| c.id);
    let failed = |e: mc::Error| ("undeliverable", e.to_string());
    match name {
        "matter.read" => {
            let cluster = id("cluster", &cluster_of)?;
            let attribute = id("attribute", &|s| {
                matter_model::cluster(cluster)?
                    .attribute_by_name(s)
                    .map(|a| a.id)
            })?;
            let values = c
                .read(
                    node,
                    mc::AttributePath {
                        endpoint: Some(endpoint()?),
                        cluster: Some(cluster),
                        attribute: Some(attribute),
                    },
                )
                .await
                .map_err(failed)?;
            Ok(values
                .first()
                .map(|a| a.value.clone())
                .unwrap_or(Value::Null))
        }
        "matter.write" => {
            let cluster = id("cluster", &cluster_of)?;
            let attribute = id("attribute", &|s| {
                matter_model::cluster(cluster)?
                    .attribute_by_name(s)
                    .map(|a| a.id)
            })?;
            c.write(
                node,
                endpoint()?,
                cluster,
                attribute,
                params["value"].clone(),
                None,
            )
            .await
            .map_err(failed)?;
            Ok(Value::Null)
        }
        "matter.invoke" => {
            let cluster = id("cluster", &cluster_of)?;
            let command = id("command", &|s| {
                matter_model::cluster(cluster)?
                    .command_by_name(s)
                    .map(|c| c.id)
            })?;
            c.invoke(
                node,
                endpoint()?,
                cluster,
                command,
                params["fields"].clone(),
                None,
            )
            .await
            .map_err(failed)
        }
        other => Err(("unknown", format!("{other} is not a Matter command"))),
    }
}

fn cluster_name(id: u32) -> String {
    matter_model::cluster(id).map_or_else(
        || format!("0x{id:04x}"),
        |c| matter_model::snake_case(c.name),
    )
}

fn attribute_name(cluster: u32, id: u32) -> String {
    matter_model::cluster(cluster)
        .and_then(|c| c.attribute(id))
        .map_or_else(
            || format!("0x{id:04x}"),
            |a| matter_model::snake_case(a.name),
        )
}

fn event_name(cluster: u32, id: u32) -> String {
    matter_model::cluster(cluster)
        .and_then(|c| c.event(id))
        .map_or_else(
            || format!("0x{id:04x}"),
            |e| matter_model::snake_case(e.name),
        )
}

/// A reading for an attribute report: scalar values only (protocol v1's
/// readings are numbers, booleans and strings), the root endpoint's power
/// source only, and no global attributes.
fn reading(a: &mc::AttributeValue) -> Option<(String, Value)> {
    const POWER_SOURCE: u32 = 0x002F;
    if a.status.is_some() || a.attribute >= 0xFFF8 || (a.endpoint == 0 && a.cluster != POWER_SOURCE)
    {
        return None;
    }
    if !matches!(
        a.value,
        Value::Number(_) | Value::Bool(_) | Value::String(_)
    ) {
        return None;
    }
    let name = format!(
        "{}.{}.{}",
        a.endpoint,
        cluster_name(a.cluster),
        attribute_name(a.cluster, a.attribute)
    );
    (name.len() <= 64).then_some((name, a.value.clone()))
}

/// `state.interview.matter`.
fn interview(node: &mc::Node) -> Value {
    let b = &node.basic;
    let mut v = json!({
        "endpoints": node.endpoints.iter().map(|(id, ep)| json!({
            "id": id,
            "device_types": ep.device_types.iter().map(|d| json!({ "id": d.id, "revision": d.revision })).collect::<Vec<_>>(),
            "server": ep.server_clusters,
            "client": ep.client_clusters,
            "parts": ep.parts,
        })).collect::<Vec<_>>(),
        "at": node.commissioned_at,
        "complete": node.interviewed,
    });
    let fields = [
        ("vendor_id", b.vendor_id.map(Value::from)),
        ("product_id", b.product_id.map(Value::from)),
        ("vendor_name", b.vendor_name.clone().map(Value::from)),
        ("product_name", b.product_name.clone().map(Value::from)),
        ("node_label", b.node_label.clone().map(Value::from)),
        (
            "hardware_version",
            b.hardware_version.clone().map(Value::from),
        ),
        (
            "software_version",
            b.software_version.clone().map(Value::from),
        ),
        ("serial_number", b.serial_number.clone().map(Value::from)),
        ("unique_id", b.unique_id.clone().map(Value::from)),
    ];
    for (k, value) in fields {
        if let Some(value) = value {
            v[k] = value;
        }
    }
    v
}

impl RadioModule for MatterModule {
    fn protocol(&self) -> &'static str {
        "matter"
    }

    fn call<'a>(&'a self, op: &'a str, params: Value) -> BoxFuture<'a, Result<Value>> {
        let me = self.me.upgrade().expect("module alive");
        Box::pin(me.op(op, params))
    }

    fn dispatch(&self, out: &Outbox, topic: &str, payload: &[u8]) {
        let prefix = format!("pulzr/a/{}/d/", out.adapter());
        let Some(dev) = topic
            .strip_prefix(&prefix)
            .and_then(|t| t.strip_suffix("/cmd"))
        else {
            return;
        };
        if !dev.starts_with("matter:") {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys() {
        let k = key(0x87e1_b004_e235_a130, 0x1_0004);
        assert_eq!(k, "matter:87e1b004e235a130.0000000000010004");
        assert_eq!(parse_key(&k), Some((0x87e1_b004_e235_a130, 0x1_0004)));
        assert_eq!(parse_key("matter:87e1b004e235a130.10004"), None);
        assert_eq!(parse_key("zwave:e1f2a3b4.14"), None);
    }

    #[test]
    fn readings_are_scalars_named_by_the_data_model() {
        let a = |endpoint, cluster, attribute, value: Value| mc::AttributeValue {
            node: 1,
            endpoint,
            cluster,
            attribute,
            value,
            status: None,
        };
        assert_eq!(
            reading(&a(1, 6, 0, json!(true))),
            Some(("1.on_off.on_off".into(), json!(true)))
        );
        assert_eq!(
            reading(&a(1, 0x0402, 0, json!(2150))).unwrap().0,
            "1.temperature_measurement.measured_value"
        );
        assert_eq!(
            reading(&a(1, 6, 0xFFFD, json!(5))),
            None,
            "global attribute"
        );
        assert_eq!(reading(&a(0, 0x28, 1, json!("x"))), None, "root endpoint");
        assert!(reading(&a(0, 0x2F, 0x0C, json!(200))).is_some(), "battery");
        assert_eq!(reading(&a(1, 0x1D, 0, json!([1]))), None, "a list");
    }

    #[test]
    fn certificates_are_matter_tlv_or_x509() {
        assert!(cert(&json!({"rcac": "1530"}), "rcac").is_ok());
        assert!(
            matches!(
                cert(&json!({"rcac": "3082"}), "rcac"),
                Err(RadioError::Refused(_))
            ),
            "malformed X.509"
        );
        assert!(matches!(
            cert(&json!({"rcac": "zz"}), "rcac"),
            Err(RadioError::BadRequest(_))
        ));
    }
}
