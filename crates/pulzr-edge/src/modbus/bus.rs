//! One bus, one task: every request to the bus goes through this loop, one at a
//! time, so polls and writes never interleave. Polling, retries, per-unit online
//! state and command execution live here.

use super::{
    codec::{decode, Encoded},
    plan::{plan_groups, plan_write, Block, Group, WriteOp},
    regmap::RegisterMap,
    track::Tracker,
};
use crate::{
    config::BusKind,
    outbox::{now, Outbox},
};
use pulzr_protocol as proto;
use serde_json::{json, Map, Value};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    time::{sleep, sleep_until, timeout, Instant},
};
use tokio_modbus::{
    client::{rtu, tcp, Context, Reader, Writer},
    slave::SlaveContext,
    ExceptionCode, Slave,
};

pub struct UnitSpec {
    pub key: String,
    pub unit: u8,
    pub map: Arc<RegisterMap>,
    pub offline_after: u32,
}

pub struct BusSettings {
    pub id: String,
    pub kind: BusKind,
    pub timeout: Duration,
    pub retries: u32,
    pub backoff: Duration,
    pub turnaround: Duration,
}

/// A command for a unit on this bus, already checked by the [`Router`].
pub struct CmdMsg {
    pub key: String,
    pub cmd: proto::Cmd,
}

const COMMANDS: [&str; 2] = ["settings.set", "register.write"];

/// Finds the bus for a command, refuses what can be refused without touching the
/// bus, and answers `queued` for the rest.
#[derive(Default)]
pub struct Router {
    units: HashMap<String, (Arc<RegisterMap>, mpsc::Sender<CmdMsg>)>,
}

impl crate::mqtt::Dispatch for Router {
    fn dispatch(&self, out: &Outbox, topic: &str, payload: &[u8]) {
        Router::dispatch(self, out, topic, payload)
    }
}

impl Router {
    pub fn add(&mut self, key: &str, map: Arc<RegisterMap>, tx: mpsc::Sender<CmdMsg>) {
        self.units.insert(key.to_string(), (map, tx));
    }

    /// Handle a message received on `pulzr/a/<adapter>/d/<key>/cmd`.
    pub fn dispatch(&self, out: &Outbox, topic: &str, payload: &[u8]) {
        let prefix = format!("pulzr/a/{}/d/", out.adapter());
        let Some(key) = topic
            .strip_prefix(&prefix)
            .and_then(|t| t.strip_suffix("/cmd"))
        else {
            tracing::warn!(topic, "message on a topic that is not a command");
            return;
        };
        // Other modules' devices (`ble:`, `gw:`) are theirs to answer.
        if !key.starts_with("modbus:") {
            return;
        }
        let raw: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(topic, error = %e, "command is not JSON");
                return;
            }
        };
        let cmd: proto::Cmd = match serde_json::from_value(raw.clone()) {
            Ok(c) => c,
            Err(e) => {
                // answer when there is an id to answer to
                match raw.get("id").and_then(Value::as_str) {
                    Some(id) if proto::CmdResultId::try_from(id).is_ok() => out.cmd_res(
                        key,
                        id,
                        "refused",
                        Some("invalid"),
                        Some(&e.to_string()),
                        None,
                    ),
                    _ => tracing::warn!(topic, error = %e, "command without a usable id"),
                }
                return;
            }
        };
        let id = String::from(cmd.id.clone());
        let name = String::from(cmd.name.clone());
        let refuse = |reason: &str, detail: &str| {
            out.cmd_res(key, &id, "refused", Some(reason), Some(detail), None)
        };

        let Some((map, tx)) = self.units.get(key) else {
            return refuse("unknown", "no such device on this gateway");
        };
        if cmd.v != proto::v1() {
            return refuse("invalid", "unsupported protocol version");
        }
        if !COMMANDS.contains(&name.as_str()) {
            return refuse(
                "unknown",
                "this adapter supports settings.set and register.write",
            );
        }
        if cmd
            .expires
            .as_ref()
            .is_some_and(|t| f64::from(t.clone()) < now())
        {
            return out.cmd_res(key, &id, "failed", Some("expired"), None, None);
        }
        if let Err(r) = plan_write(map, &name, &cmd.params) {
            return refuse("invalid", &r.detail);
        }
        out.cmd_res(key, &id, "queued", None, None, None);
        if tx
            .try_send(CmdMsg {
                key: key.to_string(),
                cmd,
            })
            .is_err()
        {
            out.cmd_res(
                key,
                &id,
                "failed",
                Some("busy"),
                Some("the bus has too many commands waiting"),
                None,
            );
        }
    }
}

// ---------------------------------------------------------------- requests

enum Op<'a> {
    Read {
        function: u8,
        address: u16,
        count: u16,
    },
    Write(&'a WriteOp),
}

enum Reply {
    Bits(Vec<bool>),
    Words(Vec<u16>),
    Done,
}

#[derive(Debug)]
enum ReqErr {
    /// No answer, or the link broke: the device may be gone.
    Io(String),
    /// The device answered with an exception: it is alive but said no.
    Exception(ExceptionCode),
}

impl std::fmt::Display for ReqErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReqErr::Io(e) => write!(f, "{e}"),
            ReqErr::Exception(e) => write!(f, "device exception: {e}"),
        }
    }
}

fn io<E: std::fmt::Display>(e: E) -> ReqErr {
    ReqErr::Io(e.to_string())
}

fn ex<T>(r: tokio_modbus::Result<T>) -> Result<T, ReqErr> {
    match r {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(ReqErr::Exception(e)),
        Err(e) => Err(io(e)),
    }
}

async fn call(ctx: &mut Context, op: &Op<'_>) -> Result<Reply, ReqErr> {
    match op {
        Op::Read {
            function,
            address,
            count,
        } => match function {
            1 => Ok(Reply::Bits(ex(ctx.read_coils(*address, *count).await)?)),
            2 => Ok(Reply::Bits(ex(ctx
                .read_discrete_inputs(*address, *count)
                .await)?)),
            3 => Ok(Reply::Words(ex(ctx
                .read_holding_registers(*address, *count)
                .await)?)),
            _ => Ok(Reply::Words(ex(ctx
                .read_input_registers(*address, *count)
                .await)?)),
        },
        Op::Write(w) => {
            match (w.function, &w.data) {
                (5, Encoded::Coil(b)) => ex(ctx.write_single_coil(w.address, *b).await)?,
                (15, Encoded::Coil(b)) => ex(ctx.write_multiple_coils(w.address, &[*b]).await)?,
                (6, Encoded::Regs(r)) => ex(ctx.write_single_register(w.address, r[0]).await)?,
                (16, Encoded::Regs(r)) => ex(ctx.write_multiple_registers(w.address, r).await)?,
                _ => return Err(io("write function does not match the data")),
            }
            Ok(Reply::Done)
        }
    }
}

/// Seconds to wait before polling a group again after `fails` failed polls in a row:
/// the interval doubled each time, up to a minute (never below the interval itself).
pub fn backoff_interval(interval: f64, fails: u32) -> f64 {
    (interval * f64::from(1u32 << fails.min(6)))
        .min(60.0)
        .max(interval)
}

// ---------------------------------------------------------------- the bus

struct GroupState {
    plan: Group,
    due: Instant,
    fails: u32,
}

struct Unit {
    spec: UnitSpec,
    groups: Vec<GroupState>,
    trackers: Vec<Tracker>,
    online: Option<bool>,
    failures: u32,
}

struct Bus {
    s: BusSettings,
    ctx: Option<Context>,
    units: Vec<Unit>,
    out: Outbox,
    epoch: Instant,
}

pub async fn run(
    s: BusSettings,
    units: Vec<UnitSpec>,
    out: Outbox,
    mut cmds: mpsc::Receiver<CmdMsg>,
    mut shutdown: watch::Receiver<bool>,
) {
    let start = Instant::now();
    let units = units
        .into_iter()
        .map(|spec| Unit {
            groups: plan_groups(&spec.map)
                .into_iter()
                .map(|plan| GroupState {
                    plan,
                    due: start,
                    fails: 0,
                })
                .collect(),
            trackers: spec.map.fields.iter().map(|_| Tracker::default()).collect(),
            spec,
            online: None,
            failures: 0,
        })
        .collect();
    let mut bus = Bus {
        s,
        ctx: None,
        units,
        out,
        epoch: start,
    };
    tracing::info!(bus = %bus.s.id, units = bus.units.len(), "bus started");
    loop {
        let next = bus.next_due();
        let wait = async {
            match next {
                Some((_, _, at)) => sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            msg = cmds.recv() => match msg {
                Some(m) => bus.command(m).await,
                None => break,
            },
            () = wait => {
                let (ui, gi, _) = next.unwrap();
                bus.poll(ui, gi).await;
            }
        }
    }
    tracing::info!(bus = %bus.s.id, "bus stopped");
}

impl Bus {
    fn next_due(&self) -> Option<(usize, usize, Instant)> {
        let mut best: Option<(usize, usize, Instant)> = None;
        for (ui, u) in self.units.iter().enumerate() {
            for (gi, g) in u.groups.iter().enumerate() {
                if best.is_none_or(|b| g.due < b.2) {
                    best = Some((ui, gi, g.due));
                }
            }
        }
        best
    }

    async fn connect(&mut self) -> Result<(), ReqErr> {
        if self.ctx.is_some() {
            return Ok(());
        }
        let t = self.s.timeout.max(Duration::from_secs(2));
        let ctx = match &self.s.kind {
            BusKind::Tcp { address } => {
                let addr = timeout(t, tokio::net::lookup_host(address.as_str()))
                    .await
                    .map_err(|_| io("resolving timed out"))?
                    .map_err(io)?
                    .next()
                    .ok_or_else(|| io("no address"))?;
                timeout(t, tcp::connect(addr))
                    .await
                    .map_err(|_| io("connect timed out"))?
                    .map_err(io)?
            }
            BusKind::Rtu {
                serial,
                baud,
                parity,
                data_bits,
                stop_bits,
            } => {
                use tokio_serial::{DataBits, Parity, SerialPortBuilderExt, StopBits};
                let port = tokio_serial::new(serial, *baud)
                    .parity(match parity.as_str() {
                        "even" => Parity::Even,
                        "odd" => Parity::Odd,
                        _ => Parity::None,
                    })
                    .data_bits(match data_bits {
                        5 => DataBits::Five,
                        6 => DataBits::Six,
                        7 => DataBits::Seven,
                        _ => DataBits::Eight,
                    })
                    .stop_bits(if *stop_bits == 2 {
                        StopBits::Two
                    } else {
                        StopBits::One
                    })
                    .open_native_async()
                    .map_err(io)?;
                rtu::attach(port)
            }
        };
        self.ctx = Some(ctx);
        Ok(())
    }

    /// One request with timeout, retries and reconnects. An exception is an answer
    /// and is not retried.
    async fn request(&mut self, slave: u8, op: &Op<'_>) -> Result<Reply, ReqErr> {
        let mut delay = self.s.backoff;
        let mut last = ReqErr::Io("not attempted".into());
        for attempt in 0..=self.s.retries {
            if attempt > 0 {
                sleep(delay).await;
                delay *= 2;
            }
            let res = match self.connect().await {
                Ok(()) => {
                    let ctx = self.ctx.as_mut().unwrap();
                    ctx.set_slave(Slave(slave));
                    match timeout(self.s.timeout, call(ctx, op)).await {
                        Ok(r) => r,
                        Err(_) => Err(ReqErr::Io("timeout".into())),
                    }
                }
                Err(e) => Err(e),
            };
            if !self.s.turnaround.is_zero() {
                sleep(self.s.turnaround).await;
            }
            match res {
                Ok(r) => return Ok(r),
                Err(e @ ReqErr::Exception(_)) => return Err(e),
                Err(e) => {
                    tracing::debug!(bus = %self.s.id, slave, attempt, error = %e, "request failed");
                    // the connection may be dead or out of step: start a new one
                    self.ctx = None;
                    last = e;
                }
            }
        }
        Err(last)
    }

    async fn poll(&mut self, ui: usize, gi: usize) {
        let (slave, key, map, blocks, interval) = {
            let u = &self.units[ui];
            (
                u.spec.unit,
                u.spec.key.clone(),
                u.spec.map.clone(),
                u.groups[gi].plan.blocks.clone(),
                u.groups[gi].plan.interval,
            )
        };
        let mut values = Map::new();
        let mut link: Option<Value> = None;
        let (mut heard, mut failed) = (false, false);
        for Block {
            function,
            address,
            count,
            members,
        } in blocks
        {
            let t0 = Instant::now();
            match self
                .request(
                    slave,
                    &Op::Read {
                        function,
                        address,
                        count,
                    },
                )
                .await
            {
                Ok(reply) => {
                    heard = true;
                    let latency = t0.elapsed().as_secs_f64() * 1000.0;
                    let words: Vec<u16> = match reply {
                        Reply::Bits(b) => b.into_iter().map(u16::from).collect(),
                        Reply::Words(w) => w,
                        Reply::Done => vec![],
                    };
                    let now_m = self.epoch.elapsed().as_secs_f64();
                    for (fi, off) in members {
                        let f = &map.fields[fi];
                        let slice = words.get(usize::from(off)..).unwrap_or(&[]);
                        let sample = match decode(f, slice) {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::warn!(%key, error = %e, "cannot decode");
                                continue;
                            }
                        };
                        let tr = &mut self.units[ui].trackers[fi];
                        if tr.due(sample, now_m, f.deadband, f.heartbeat_secs(&map)) {
                            tr.reported(sample, now_m);
                            values.insert(f.id.clone(), sample.to_json());
                            link.get_or_insert_with(|| json!({ "function": function, "address": address, "count": count, "latency_ms": (latency * 10.0).round() / 10.0 }));
                        }
                    }
                }
                Err(ReqErr::Exception(e)) => {
                    heard = true;
                    tracing::warn!(%key, function, address, count, exception = %e, "device refused a read");
                }
                Err(e) => {
                    tracing::warn!(%key, function, address, count, error = %e, "read failed");
                    failed = true;
                    break;
                }
            }
        }

        let ts = now();
        let u = &mut self.units[ui];
        if failed || !heard {
            u.failures += 1;
            u.groups[gi].fails += 1;
            if u.failures >= u.spec.offline_after && u.online != Some(false) {
                u.online = Some(false);
                self.out.state(&key, false, None, Some(&map.model));
            }
        } else {
            u.failures = 0;
            u.groups[gi].fails = 0;
            if u.online != Some(true) {
                u.online = Some(true);
                self.out.state(&key, true, Some(ts), Some(&map.model));
            }
        }
        if !values.is_empty() {
            self.out.up(&key, ts, values, link);
        }
        let g = &mut self.units[ui].groups[gi];
        let step = Duration::from_secs_f64(backoff_interval(interval, g.fails));
        g.due = (g.due + step).max(Instant::now());
    }

    async fn command(&mut self, m: CmdMsg) {
        let CmdMsg { key, cmd } = m;
        let id = String::from(cmd.id.clone());
        let name = String::from(cmd.name.clone());
        let Some(ui) = self.units.iter().position(|u| u.spec.key == key) else {
            return;
        };
        let (slave, map) = (self.units[ui].spec.unit, self.units[ui].spec.map.clone());
        let out = self.out.clone();
        let fail = |reason: &str, detail: &str| {
            out.cmd_res(&key, &id, "failed", Some(reason), Some(detail), None)
        };
        if cmd
            .expires
            .as_ref()
            .is_some_and(|t| f64::from(t.clone()) < now())
        {
            return fail("expired", "the command expired while waiting for the bus");
        }
        let ops = match plan_write(&map, &name, &cmd.params) {
            Ok(o) => o,
            Err(r) => {
                return self.out.cmd_res(
                    &key,
                    &id,
                    "refused",
                    Some("invalid"),
                    Some(&r.detail),
                    None,
                )
            }
        };
        self.out.cmd_res(&key, &id, "sent", None, None, None);
        let mut written = Map::new();
        for op in &ops {
            match self.request(slave, &Op::Write(op)).await {
                Ok(_) => {
                    written.insert(map.fields[op.field].id.clone(), op.value.clone());
                }
                Err(e) => {
                    let (reason, how) = match e {
                        ReqErr::Io(_) => ("undeliverable", "no answer from the device"),
                        ReqErr::Exception(_) => ("error", "the device refused the write"),
                    };
                    let done = if written.is_empty() {
                        String::new()
                    } else {
                        format!(
                            "; already written: {}",
                            written.keys().cloned().collect::<Vec<_>>().join(", ")
                        )
                    };
                    if !written.is_empty() {
                        self.out.settings(&key, written.clone(), &id);
                    }
                    return fail(reason, &format!("{how} ({e}){done}"));
                }
            }
        }
        self.out.cmd_res(&key, &id, "delivered", None, None, None);
        self.out.settings(&key, written.clone(), &id);
        self.out.cmd_res(&key, &id, "ok", None, None, Some(written));
        // read the new values back at once
        let now_i = Instant::now();
        for g in &mut self.units[ui].groups {
            g.due = now_i;
            g.fails = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_caps() {
        assert_eq!(backoff_interval(10.0, 0), 10.0);
        assert_eq!(backoff_interval(10.0, 1), 20.0);
        assert_eq!(backoff_interval(10.0, 2), 40.0);
        assert_eq!(backoff_interval(10.0, 3), 60.0);
        assert_eq!(backoff_interval(10.0, 30), 60.0);
        assert_eq!(
            backoff_interval(120.0, 5),
            120.0,
            "never below the interval"
        );
    }
}
