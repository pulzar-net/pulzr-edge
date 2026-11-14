//! Which requests read a map's fields, and which writes a command asks for.

use super::{
    codec::{encode, Encoded},
    regmap::{DataType, Field, RegisterMap},
};
use serde_json::{Map, Value};

/// One read request covering one or more fields.
#[derive(Clone, Debug, PartialEq)]
pub struct Block {
    pub function: u8,
    pub address: u16,
    pub count: u16,
    /// (field index, offset in the block) of each field it feeds.
    pub members: Vec<(usize, u16)>,
}

/// Fields sharing a poll interval, read together.
#[derive(Clone, Debug)]
pub struct Group {
    pub interval: f64,
    pub blocks: Vec<Block>,
}

/// Largest read a device is asked for: 125 registers or 2000 coils.
fn limit(function: u8) -> u16 {
    if function <= 2 {
        2000
    } else {
        125
    }
}

/// Merge `fields` (indexes into `map.fields`) into as few reads as `max_gap` allows.
pub fn plan_blocks(map: &RegisterMap, fields: &[usize]) -> Vec<Block> {
    let mut sorted: Vec<usize> = fields.to_vec();
    sorted.sort_by_key(|&i| (map.fields[i].function, map.fields[i].address));
    let mut out: Vec<Block> = Vec::new();
    for i in sorted {
        let f = &map.fields[i];
        let end = u32::from(f.address) + u32::from(f.count());
        if let Some(b) = out.last_mut() {
            let b_end = u32::from(b.address) + u32::from(b.count);
            let new_count = end.max(b_end) - u32::from(b.address);
            if b.function == f.function
                && u32::from(f.address) <= b_end + u32::from(map.max_gap)
                && new_count <= u32::from(limit(f.function))
            {
                b.members.push((i, f.address - b.address));
                b.count = new_count as u16;
                continue;
            }
        }
        out.push(Block {
            function: f.function,
            address: f.address,
            count: f.count(),
            members: vec![(i, 0)],
        });
    }
    out
}

/// One group per distinct poll interval.
pub fn plan_groups(map: &RegisterMap) -> Vec<Group> {
    let mut intervals: Vec<f64> = Vec::new();
    for f in &map.fields {
        let iv = f.interval(map);
        if !intervals.contains(&iv) {
            intervals.push(iv);
        }
    }
    intervals
        .into_iter()
        .map(|iv| {
            let idx: Vec<usize> = (0..map.fields.len())
                .filter(|&i| map.fields[i].interval(map) == iv)
                .collect();
            Group {
                interval: iv,
                blocks: plan_blocks(map, &idx),
            }
        })
        .collect()
}

/// A validated write.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteOp {
    pub field: usize,
    pub function: u8,
    pub address: u16,
    pub data: Encoded,
    /// The value as the platform gave it (engineering units).
    pub value: Value,
}

/// Why a command is refused before it touches the bus.
#[derive(Debug, PartialEq)]
pub struct Refusal {
    pub detail: String,
}

fn refuse<T>(m: impl Into<String>) -> Result<T, Refusal> {
    Err(Refusal { detail: m.into() })
}

fn write_op(map: &RegisterMap, idx: usize, value: &Value) -> Result<WriteOp, Refusal> {
    let f: &Field = &map.fields[idx];
    if !f.writable {
        return refuse(format!("{} is read only", f.id));
    }
    let data = encode(f, value).map_err(|d| Refusal { detail: d })?;
    let function = f.write_function.unwrap_or(match (f.function, f.data_type) {
        (1, _) => 5,
        (_, DataType::U16 | DataType::I16 | DataType::Bool) => 6,
        _ => 16,
    });
    Ok(WriteOp {
        field: idx,
        function,
        address: f.address,
        data,
        value: value.clone(),
    })
}

/// Turn a `settings.set` or `register.write` command into writes, all or nothing.
///
/// `settings.set` params map writable field ids to values. `register.write` takes
/// `{"field": id | "address": n, "value": v}`; either way only fields the map marks
/// writable are accepted.
pub fn plan_write(
    map: &RegisterMap,
    name: &str,
    params: &Map<String, Value>,
) -> Result<Vec<WriteOp>, Refusal> {
    match name {
        "settings.set" => {
            if params.is_empty() {
                return refuse("no settings given");
            }
            let mut ops = Vec::new();
            for (id, v) in params {
                let Some(idx) = map.fields.iter().position(|f| &f.id == id) else {
                    return refuse(format!("no field {id}"));
                };
                ops.push(write_op(map, idx, v)?);
            }
            ops.sort_by_key(|o| o.field);
            Ok(ops)
        }
        "register.write" => {
            let Some(value) = params.get("value") else {
                return refuse("value missing");
            };
            if params
                .keys()
                .any(|k| !matches!(k.as_str(), "field" | "address" | "function" | "value"))
            {
                return refuse("unknown parameter");
            }
            let idx = match (params.get("field"), params.get("address")) {
                (Some(Value::String(id)), None) => map.fields.iter().position(|f| &f.id == id),
                (None, Some(a)) => {
                    let Some(a) = a.as_u64() else {
                        return refuse("address must be a number");
                    };
                    let func = params.get("function").and_then(Value::as_u64);
                    map.fields.iter().position(|f| {
                        u64::from(f.address) == a && func.is_none_or(|x| x == u64::from(f.function))
                    })
                }
                _ => return refuse("give field or address"),
            };
            match idx {
                Some(i) => Ok(vec![write_op(map, i, value)?]),
                None => refuse("no such register in the map"),
            }
        }
        _ => unreachable!("caller checks the command name"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    pub fn sample_map() -> RegisterMap {
        toml::from_str(
            r#"
schema = 0
model = "t"
max_gap = 2
[[fields]]
id = "a"
function = 3
address = 0
type = "f32"
[[fields]]
id = "b"
function = 3
address = 2
type = "u16"
[[fields]]
id = "far"
function = 3
address = 100
type = "u16"
poll_interval = 60
[[fields]]
id = "in"
function = 4
address = 0
type = "u16"
[[fields]]
id = "sp"
function = 3
address = 50
type = "i16"
writable = true
min = 0
max = 100
[[fields]]
id = "relay"
function = 1
address = 7
type = "bool"
writable = true
"#,
        )
        .unwrap()
    }

    #[test]
    fn adjacent_fields_share_a_request_and_functions_do_not_mix() {
        let m = sample_map();
        let b = plan_blocks(&m, &[0, 1, 3]);
        assert_eq!(b.len(), 2);
        assert_eq!((b[0].function, b[0].address, b[0].count), (3, 0, 3));
        assert_eq!(b[0].members, vec![(0, 0), (1, 2)]);
        assert_eq!((b[1].function, b[1].address, b[1].count), (4, 0, 1));
    }

    #[test]
    fn gap_and_size_limits_split_blocks() {
        let m = sample_map();
        assert_eq!(plan_blocks(&m, &[0, 2]).len(), 2, "gap of 97 registers");
        let mut wide = sample_map();
        wide.max_gap = 1000;
        let b = plan_blocks(&wide, &[0, 2]);
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].count, 101);
        // beyond 125 registers a block is split
        let mut m2 = sample_map();
        m2.max_gap = 1000;
        m2.fields[2].address = 200;
        assert_eq!(plan_blocks(&m2, &[0, 2]).len(), 2);
    }

    #[test]
    fn groups_follow_poll_intervals() {
        let g = plan_groups(&sample_map());
        assert_eq!(g.len(), 2);
        assert_eq!(g[0].interval, 10.0);
        assert_eq!(g[1].interval, 60.0);
    }

    #[test]
    fn settings_set_accepts_writable_fields_only() {
        let m = sample_map();
        let ok = plan_write(
            &m,
            "settings.set",
            json!({"sp": 42, "relay": true}).as_object().unwrap(),
        )
        .unwrap();
        assert_eq!(ok.len(), 2);
        assert_eq!(
            (ok[0].function, ok[0].address, ok[0].data.clone()),
            (6, 50, Encoded::Regs(vec![42]))
        );
        assert_eq!(
            (ok[1].function, ok[1].data.clone()),
            (5, Encoded::Coil(true))
        );
        let e = plan_write(&m, "settings.set", json!({"a": 1.0}).as_object().unwrap()).unwrap_err();
        assert!(e.detail.contains("read only"));
        assert!(plan_write(&m, "settings.set", json!({"nope": 1}).as_object().unwrap()).is_err());
        assert!(plan_write(&m, "settings.set", &Map::new()).is_err());
    }

    #[test]
    fn settings_set_is_all_or_nothing() {
        let m = sample_map();
        // sp is fine, "a" is read only: nothing is planned
        assert!(plan_write(
            &m,
            "settings.set",
            json!({"sp": 1, "a": 2.0}).as_object().unwrap()
        )
        .is_err());
        // out of the field's limits
        assert!(plan_write(&m, "settings.set", json!({"sp": 101}).as_object().unwrap()).is_err());
    }

    #[test]
    fn register_write_by_field_or_address() {
        let m = sample_map();
        let by_field = plan_write(
            &m,
            "register.write",
            json!({"field": "sp", "value": 5}).as_object().unwrap(),
        )
        .unwrap();
        let by_addr = plan_write(
            &m,
            "register.write",
            json!({"address": 50, "function": 3, "value": 5})
                .as_object()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(by_field, by_addr);
        // a read-only register is refused even when addressed directly
        assert!(plan_write(
            &m,
            "register.write",
            json!({"address": 0, "function": 4, "value": 5})
                .as_object()
                .unwrap()
        )
        .is_err());
        assert!(plan_write(
            &m,
            "register.write",
            json!({"address": 9999, "value": 5}).as_object().unwrap()
        )
        .is_err());
        assert!(plan_write(
            &m,
            "register.write",
            json!({"field": "sp"}).as_object().unwrap()
        )
        .is_err());
        assert!(plan_write(
            &m,
            "register.write",
            json!({"field": "sp", "address": 50, "value": 1})
                .as_object()
                .unwrap()
        )
        .is_err());
    }
}
