//! A Modbus TCP device simulator: serves register maps (the same files the daemon
//! reads) with values that move, so the whole Modbus path can run without hardware.
//!
//! Encodings come from [`crate::modbus::codec`], so word order, byte order, scale and
//! types match what the daemon decodes. Readable fields follow a slow sine or a random
//! walk inside a range taken from the map; writable fields (functions 1 and 3) hold
//! what was last written to them; writes anywhere else answer illegal data address.

use crate::modbus::{
    codec::{self, Encoded},
    regmap::{DataType, Field, RegisterMap},
};
use std::{
    collections::BTreeMap,
    future::{ready, Ready},
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::net::TcpListener;
use tokio_modbus::{
    server::{
        tcp::{accept_tcp_connection, Server},
        Service,
    },
    ExceptionCode, Request, Response, SlaveRequest,
};

const SPACE: usize = 0x10000;

struct Unit {
    map: RegisterMap,
    input: Vec<u16>,
    hold: Vec<u16>,
    coils: Vec<bool>,
    discrete: Vec<bool>,
    writable_hold: Vec<bool>,
    writable_coil: Vec<bool>,
    /// Random walk position (0..1) per field.
    walk: Vec<f64>,
}

/// The simulated devices, by unit id.
pub struct Sim {
    units: Mutex<BTreeMap<u8, Unit>>,
    rng: Mutex<u64>,
}

fn range(f: &Field) -> (f64, f64) {
    let (rlo, rhi) = match f.data_type {
        DataType::U16 => (0.0, 65535.0),
        DataType::I16 => (-32768.0, 32767.0),
        DataType::U32 => (0.0, 4294967295.0),
        DataType::I32 => (-2147483648.0, 2147483647.0),
        DataType::F32 => (-1e6, 1e6),
        DataType::Bool => (0.0, 1.0),
    };
    let (a, b) = (rlo * f.scale + f.offset, rhi * f.scale + f.offset);
    let (rep_lo, rep_hi) = (a.min(b), a.max(b));
    let (mut lo, mut hi) = match (f.min, f.max) {
        (Some(lo), Some(hi)) => (lo, hi),
        (Some(lo), None) => (lo, lo + 100.0),
        (None, Some(hi)) => (hi - 100.0, hi),
        (None, None) => (0.0, 100.0),
    };
    // wide enough that the deadband is crossed now and then
    if f.min.is_none() || f.max.is_none() {
        hi = hi.max(lo + 20.0 * f.deadband);
    }
    lo = lo.max(rep_lo);
    hi = hi.min(rep_hi);
    if lo >= hi {
        (rep_lo.max(-100.0), rep_hi.min(100.0).max(rep_lo + 1.0))
    } else {
        (lo, hi)
    }
}

fn set_bit(w: u16, bit: u8, on: bool) -> u16 {
    if on {
        w | 1 << bit
    } else {
        w & !(1 << bit)
    }
}

impl Unit {
    fn new(map: RegisterMap) -> Self {
        let mut u = Unit {
            input: vec![0; SPACE],
            hold: vec![0; SPACE],
            coils: vec![false; SPACE],
            discrete: vec![false; SPACE],
            writable_hold: vec![false; SPACE],
            writable_coil: vec![false; SPACE],
            walk: vec![0.5; map.fields.len()],
            map,
        };
        for f in &u.map.fields.clone() {
            if !f.writable {
                continue;
            }
            let at = usize::from(f.address);
            let n = usize::from(f.count());
            if f.function == 1 {
                u.writable_coil[at] = true;
            } else {
                u.writable_hold[at..at + n].fill(true);
                // start inside the accepted range
                let start = f.min.unwrap_or(0.0).max(0.0).min(f.max.unwrap_or(f64::MAX));
                if let Ok(Encoded::Regs(w)) = codec::encode(f, &serde_json::json!(start)) {
                    u.hold[at..at + n].copy_from_slice(&w);
                }
            }
        }
        u
    }

    fn tick(&mut self, t: f64, rng: &mut impl FnMut() -> f64) {
        for (i, f) in self.map.fields.iter().enumerate() {
            if f.writable {
                continue;
            }
            let at = usize::from(f.address);
            if f.data_type == DataType::Bool {
                let cur = match (f.function, f.bit) {
                    (1, _) => self.coils[at],
                    (2, _) => self.discrete[at],
                    (3, Some(b)) => self.hold[at] >> b & 1 == 1,
                    (4, Some(b)) => self.input[at] >> b & 1 == 1,
                    (3, None) => self.hold[at] != 0,
                    _ => self.input[at] != 0,
                };
                let on = if t == 0.0 {
                    false
                } else if rng() < 0.05 {
                    !cur
                } else {
                    cur
                };
                match (f.function, f.bit) {
                    (1, _) => self.coils[at] = on,
                    (2, _) => self.discrete[at] = on,
                    (3, Some(b)) => self.hold[at] = set_bit(self.hold[at], b, on),
                    (4, Some(b)) => self.input[at] = set_bit(self.input[at], b, on),
                    (3, None) => self.hold[at] = u16::from(on),
                    _ => self.input[at] = u16::from(on),
                }
                continue;
            }
            let (lo, hi) = range(f);
            let x = if i % 2 == 0 {
                let period = 120.0 + 37.0 * (i % 5) as f64;
                0.5 + 0.4 * (std::f64::consts::TAU * t / period + i as f64).sin()
            } else {
                let w = &mut self.walk[i];
                *w = (*w + (rng() - 0.5) * 0.1).clamp(0.0, 1.0);
                *w
            };
            let Ok(Encoded::Regs(w)) = codec::encode(f, &serde_json::json!(lo + x * (hi - lo)))
            else {
                continue;
            };
            let space = if f.function == 3 {
                &mut self.hold
            } else {
                &mut self.input
            };
            space[at..at + w.len()].copy_from_slice(&w);
        }
    }

    fn names(&self, function: u8, at: usize, n: usize) -> String {
        let hit: Vec<&str> = self
            .map
            .fields
            .iter()
            .filter(|f| {
                f.function == function
                    && usize::from(f.address) < at + n
                    && at < usize::from(f.address) + usize::from(f.count())
            })
            .map(|f| f.id.as_str())
            .collect();
        hit.join(",")
    }
}

impl Sim {
    /// `(unit id, register map)` pairs. Starts with every field at its first value.
    pub fn new(units: Vec<(u8, RegisterMap)>) -> Arc<Self> {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(1, |d| d.as_nanos() as u64)
            | 1;
        let sim = Arc::new(Sim {
            units: Mutex::new(units.into_iter().map(|(u, m)| (u, Unit::new(m))).collect()),
            rng: Mutex::new(seed),
        });
        sim.tick(0.0);
        sim
    }

    /// Move every readable value to where it is at `t` seconds.
    pub fn tick(&self, t: f64) {
        let mut state = self.rng.lock().unwrap();
        let mut rng = || {
            // xorshift64*
            *state ^= *state >> 12;
            *state ^= *state << 25;
            *state ^= *state >> 27;
            (state.wrapping_mul(0x2545F4914F6CDD1D) >> 11) as f64 / (1u64 << 53) as f64
        };
        for u in self.units.lock().unwrap().values_mut() {
            u.tick(t, &mut rng);
        }
    }

    fn handle(&self, unit: u8, req: Request<'_>) -> Result<Option<Response>, ExceptionCode> {
        let mut units = self.units.lock().unwrap();
        let Some(u) = units.get_mut(&unit) else {
            return Ok(None); // nobody home: no answer at all
        };
        let bad = ExceptionCode::IllegalDataAddress;
        let span = |a: u16, n: usize| {
            let (a, e) = (usize::from(a), usize::from(a) + n);
            (n >= 1 && e <= SPACE).then_some(a..e).ok_or(bad)
        };
        let r = match req {
            Request::ReadInputRegisters(a, n) => {
                Response::ReadInputRegisters(u.input[span(a, n.into())?].to_vec())
            }
            Request::ReadHoldingRegisters(a, n) => {
                Response::ReadHoldingRegisters(u.hold[span(a, n.into())?].to_vec())
            }
            Request::ReadCoils(a, n) => Response::ReadCoils(u.coils[span(a, n.into())?].to_vec()),
            Request::ReadDiscreteInputs(a, n) => {
                Response::ReadDiscreteInputs(u.discrete[span(a, n.into())?].to_vec())
            }
            Request::WriteSingleRegister(a, v) => {
                u.write_regs(unit, a, &[v])?;
                Response::WriteSingleRegister(a, v)
            }
            Request::WriteMultipleRegisters(a, v) => {
                u.write_regs(unit, a, &v)?;
                Response::WriteMultipleRegisters(a, v.len() as u16)
            }
            Request::WriteSingleCoil(a, v) => {
                u.write_coils(unit, a, &[v])?;
                Response::WriteSingleCoil(a, v)
            }
            Request::WriteMultipleCoils(a, v) => {
                u.write_coils(unit, a, &v)?;
                Response::WriteMultipleCoils(a, v.len() as u16)
            }
            _ => return Err(ExceptionCode::IllegalFunction),
        };
        Ok(Some(r))
    }
}

impl Unit {
    fn write_regs(&mut self, unit: u8, a: u16, v: &[u16]) -> Result<(), ExceptionCode> {
        let at = usize::from(a);
        let ok = !v.is_empty()
            && at + v.len() <= SPACE
            && self.writable_hold[at..at + v.len()].iter().all(|w| *w);
        if !ok {
            tracing::warn!(
                unit,
                function = if v.len() == 1 { 6 } else { 16 },
                address = a,
                ?v,
                "write refused: not a writable register"
            );
            return Err(ExceptionCode::IllegalDataAddress);
        }
        self.hold[at..at + v.len()].copy_from_slice(v);
        tracing::info!(unit, function = if v.len() == 1 { 6 } else { 16 }, address = a, values = ?v, fields = %self.names(3, at, v.len()), "write");
        Ok(())
    }

    fn write_coils(&mut self, unit: u8, a: u16, v: &[bool]) -> Result<(), ExceptionCode> {
        let at = usize::from(a);
        let ok = !v.is_empty()
            && at + v.len() <= SPACE
            && self.writable_coil[at..at + v.len()].iter().all(|w| *w);
        if !ok {
            tracing::warn!(
                unit,
                function = if v.len() == 1 { 5 } else { 15 },
                address = a,
                ?v,
                "write refused: not a writable coil"
            );
            return Err(ExceptionCode::IllegalDataAddress);
        }
        self.coils[at..at + v.len()].copy_from_slice(v);
        tracing::info!(unit, function = if v.len() == 1 { 5 } else { 15 }, address = a, values = ?v, fields = %self.names(1, at, v.len()), "write");
        Ok(())
    }
}

#[derive(Clone)]
struct Dev(Arc<Sim>);

impl Service for Dev {
    type Request = SlaveRequest<'static>;
    type Response = Option<Response>;
    type Exception = ExceptionCode;
    type Future = Ready<Result<Option<Response>, ExceptionCode>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        ready(self.0.handle(req.slave, req.request))
    }
}

/// Serve Modbus TCP on `listener` until the task is dropped.
pub async fn serve(listener: TcpListener, sim: Arc<Sim>) -> io::Result<()> {
    let server = Server::new(listener);
    let on = move |stream, addr| {
        let dev = Dev(sim.clone());
        async move { accept_tcp_connection(stream, addr, |_| Ok(Some(dev.clone()))) }
    };
    server
        .serve(&on, |e| tracing::warn!("connection: {e}"))
        .await
        .map(|_| ())
}

/// Move the values every `period`, for as long as the task runs.
pub async fn run_ticker(sim: Arc<Sim>, period: Duration) {
    let start = tokio::time::Instant::now();
    let mut every = tokio::time::interval(period);
    loop {
        every.tick().await;
        sim.tick(start.elapsed().as_secs_f64());
    }
}
