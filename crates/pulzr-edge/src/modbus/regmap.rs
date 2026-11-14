//! The register map: which registers a Modbus device has and what they mean.
//!
//! TOML or JSON on disk (chosen by extension), documented by
//! `schema/register-map.schema.json`, which is generated from these types
//! (`cargo test -p pulzr-edge --test schema` fails when the checked-in file is stale;
//! `UPDATE_SCHEMA=1` rewrites it).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, path::Path};

/// Data type of a field, as stored in the device.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum DataType {
    /// Unsigned 16 bit, one register.
    U16,
    /// Signed 16 bit, one register.
    I16,
    /// Unsigned 32 bit, two registers.
    U32,
    /// Signed 32 bit, two registers.
    I32,
    /// IEEE 754 single precision, two registers.
    F32,
    /// One coil or discrete input (function 1, 2), or a register that is non-zero
    /// (function 3, 4), or one bit of a register when `bit` is set.
    Bool,
}

impl DataType {
    /// Registers (or bits, for coils) the type occupies.
    pub fn width(self) -> u16 {
        match self {
            DataType::U32 | DataType::I32 | DataType::F32 => 2,
            _ => 1,
        }
    }
}

/// Order of the 16 bit words of a 32 bit value, as they appear on the wire.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum WordOrder {
    /// High word first (AB CD).
    #[default]
    Big,
    /// Low word first (CD AB).
    Little,
}

/// Order of the two bytes inside each 16 bit register.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ByteOrder {
    /// High byte first, as the Modbus standard says.
    #[default]
    Big,
    /// Low byte first (some devices swap bytes inside the register).
    Little,
}

fn one() -> f64 {
    1.0
}
fn zero() -> f64 {
    0.0
}
fn default_poll() -> f64 {
    10.0
}
fn default_heartbeat() -> f64 {
    300.0
}
fn default_gap() -> u16 {
    4
}

/// A register map (the Modbus part of a device package).
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RegisterMap {
    /// Format version, 0 for now.
    pub schema: u32,
    /// Device model, reported as the unit's `package` in `state`.
    pub model: String,
    /// Default poll interval in seconds for fields that do not set one.
    #[serde(default = "default_poll")]
    pub poll_interval: f64,
    /// Default heartbeat in seconds: a value is re-reported at least this often
    /// even when it has not moved past its deadband.
    #[serde(default = "default_heartbeat")]
    pub heartbeat: f64,
    /// Fields closer together than this many registers are read in one request.
    #[serde(default = "default_gap")]
    pub max_gap: u16,
    pub fields: Vec<Field>,
}

/// One value of the device.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Field {
    /// Field id as in the device package (`^[A-Za-z0-9_.-]{1,64}$`), the key in `up` readings.
    pub id: String,
    /// Read function code: 1 coils, 2 discrete inputs, 3 holding registers, 4 input registers.
    pub function: u8,
    /// Zero-based address on the wire (register 40001 is address 0).
    pub address: u16,
    #[serde(rename = "type")]
    pub data_type: DataType,
    #[serde(default)]
    pub word_order: WordOrder,
    #[serde(default)]
    pub byte_order: ByteOrder,
    /// Bit 0-15 of a register for `bool` fields on function 3 or 4. Read only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bit: Option<u8>,
    /// Engineering value = raw * scale + offset.
    #[serde(default = "one")]
    pub scale: f64,
    #[serde(default = "zero")]
    pub offset: f64,
    /// UCUM unit, informational.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    /// Seconds between polls; the map's `poll_interval` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub poll_interval: Option<f64>,
    /// Report on change: a new value is sent when it differs from the last reported
    /// one by more than this (0 = any change). Booleans report any change.
    #[serde(default = "zero")]
    pub deadband: f64,
    /// Seconds; the map's `heartbeat` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat: Option<f64>,
    /// May be set by `settings.set` and `register.write`. Only functions 1 and 3.
    #[serde(default)]
    pub writable: bool,
    /// Write function: 5 or 15 for coils, 6 or 16 for registers. Default 5 for
    /// coils, 6 for one register, 16 for two.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_function: Option<u8>,
    /// Smallest engineering value a write accepts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    /// Largest engineering value a write accepts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
}

impl Field {
    pub fn is_bit_space(&self) -> bool {
        matches!(self.function, 1 | 2)
    }
    /// Registers (or coils) a read of this field covers.
    pub fn count(&self) -> u16 {
        self.data_type.width()
    }
    pub fn interval(&self, map: &RegisterMap) -> f64 {
        self.poll_interval.unwrap_or(map.poll_interval)
    }
    pub fn heartbeat_secs(&self, map: &RegisterMap) -> f64 {
        self.heartbeat.unwrap_or(map.heartbeat)
    }
}

impl RegisterMap {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        let map: RegisterMap = if path.extension().is_some_and(|e| e == "json") {
            serde_json::from_str(&text)?
        } else {
            toml::from_str(&text)?
        };
        map.validate()
            .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        Ok(map)
    }

    pub fn from_toml(text: &str) -> anyhow::Result<Self> {
        let map: RegisterMap = toml::from_str(text)?;
        map.validate().map_err(|e| anyhow::anyhow!(e))?;
        Ok(map)
    }

    pub fn field(&self, id: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.id == id)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema != 0 {
            return Err(format!("unsupported register map schema {}", self.schema));
        }
        if self.model.is_empty() || self.model.len() > 72 {
            return Err("model must be 1-72 characters".into());
        }
        if self.fields.is_empty() {
            return Err("no fields".into());
        }
        for (what, v) in [
            ("poll_interval", self.poll_interval),
            ("heartbeat", self.heartbeat),
        ] {
            if !(v.is_finite() && v > 0.0) {
                return Err(format!("{what} must be positive"));
            }
        }
        let mut seen = HashSet::new();
        for f in &self.fields {
            let id = &f.id;
            if pulzr_protocol::ReadingVKey::try_from(id.as_str()).is_err() {
                return Err(format!("field id {id:?} is not a valid reading key"));
            }
            if !seen.insert(id.as_str()) {
                return Err(format!("duplicate field id {id}"));
            }
            let bad = |m: &str| Err(format!("field {id}: {m}"));
            if !(1..=4).contains(&f.function) {
                return bad("function must be 1, 2, 3 or 4");
            }
            if f.is_bit_space() && f.data_type != DataType::Bool {
                return bad("coils and discrete inputs are bool");
            }
            if let Some(bit) = f.bit {
                if f.data_type != DataType::Bool || f.is_bit_space() || bit > 15 {
                    return bad("bit applies to bool on function 3 or 4, 0-15");
                }
            }
            if u32::from(f.address) + u32::from(f.count()) > 0x10000 {
                return bad("runs past address 65535");
            }
            if !(f.scale.is_finite() && f.scale != 0.0 && f.offset.is_finite()) {
                return bad("scale must be non-zero and finite");
            }
            if !(f.deadband.is_finite() && f.deadband >= 0.0) {
                return bad("deadband must be >= 0");
            }
            for v in [f.poll_interval, f.heartbeat].into_iter().flatten() {
                if !(v.is_finite() && v > 0.0) {
                    return bad("poll_interval and heartbeat must be positive");
                }
            }
            if f.writable {
                if !matches!(f.function, 1 | 3) {
                    return bad("only coils (1) and holding registers (3) are writable");
                }
                if f.bit.is_some() {
                    return bad("a single bit of a register is not writable");
                }
                let ok = match (f.function, f.write_function) {
                    (_, None) => true,
                    (1, Some(5 | 15)) => true,
                    (3, Some(6)) => f.count() == 1,
                    (3, Some(16)) => true,
                    _ => false,
                };
                if !ok {
                    return bad("write_function does not fit the field");
                }
            } else if f.write_function.is_some() {
                return bad("write_function on a field that is not writable");
            }
            if let (Some(lo), Some(hi)) = (f.min, f.max) {
                if lo > hi {
                    return bad("min above max");
                }
            }
        }
        Ok(())
    }
}
