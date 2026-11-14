//! Register words to values and back.

use super::regmap::{ByteOrder, DataType, Field, WordOrder};

/// A decoded value, before it becomes a protocol `Value`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Sample {
    Num(f64),
    Bool(bool),
}

impl Sample {
    pub fn to_json(self) -> serde_json::Value {
        match self {
            Sample::Num(n) => serde_json::json!(n),
            Sample::Bool(b) => serde_json::json!(b),
        }
    }
}

/// What a write puts on the wire.
#[derive(Clone, Debug, PartialEq)]
pub enum Encoded {
    Coil(bool),
    Regs(Vec<u16>),
}

fn fix_byte(w: u16, bo: ByteOrder) -> u16 {
    match bo {
        ByteOrder::Big => w,
        ByteOrder::Little => w.swap_bytes(),
    }
}

fn to_u32(regs: &[u16], wo: WordOrder, bo: ByteOrder) -> u32 {
    let (a, b) = (fix_byte(regs[0], bo), fix_byte(regs[1], bo));
    match wo {
        WordOrder::Big => (u32::from(a) << 16) | u32::from(b),
        WordOrder::Little => (u32::from(b) << 16) | u32::from(a),
    }
}

fn from_u32(v: u32, wo: WordOrder, bo: ByteOrder) -> Vec<u16> {
    let (hi, lo) = ((v >> 16) as u16, v as u16);
    let (a, b) = match wo {
        WordOrder::Big => (hi, lo),
        WordOrder::Little => (lo, hi),
    };
    vec![fix_byte(a, bo), fix_byte(b, bo)]
}

/// Decode a field from the registers it covers (or one coil as a one-element slice,
/// non zero = on).
pub fn decode(f: &Field, words: &[u16]) -> Result<Sample, String> {
    if words.len() < usize::from(f.count()) {
        return Err(format!("{}: short read", f.id));
    }
    let raw = match f.data_type {
        DataType::Bool => {
            let w = fix_byte(words[0], f.byte_order);
            let on = match f.bit {
                Some(b) => w >> b & 1 == 1,
                None => w != 0,
            };
            return Ok(Sample::Bool(on));
        }
        DataType::U16 => f64::from(fix_byte(words[0], f.byte_order)),
        DataType::I16 => f64::from(fix_byte(words[0], f.byte_order) as i16),
        DataType::U32 => f64::from(to_u32(words, f.word_order, f.byte_order)),
        DataType::I32 => f64::from(to_u32(words, f.word_order, f.byte_order) as i32),
        DataType::F32 => {
            let v = f32::from_bits(to_u32(words, f.word_order, f.byte_order));
            if !v.is_finite() {
                return Err(format!("{}: not a finite number", f.id));
            }
            f64::from(v)
        }
    };
    Ok(Sample::Num(raw * f.scale + f.offset))
}

/// Encode a value for writing: the inverse of [`decode`], checking limits and range.
pub fn encode(f: &Field, value: &serde_json::Value) -> Result<Encoded, String> {
    if f.data_type == DataType::Bool {
        let on = match value {
            serde_json::Value::Bool(b) => *b,
            serde_json::Value::Number(n) if n.as_f64() == Some(0.0) => false,
            serde_json::Value::Number(n) if n.as_f64() == Some(1.0) => true,
            _ => return Err(format!("{}: expected true or false", f.id)),
        };
        return Ok(if f.is_bit_space() {
            Encoded::Coil(on)
        } else {
            Encoded::Regs(vec![fix_byte(u16::from(on), f.byte_order)])
        });
    }
    let v = value
        .as_f64()
        .filter(|v| v.is_finite())
        .ok_or_else(|| format!("{}: expected a number", f.id))?;
    if f.min.is_some_and(|m| v < m) || f.max.is_some_and(|m| v > m) {
        return Err(format!("{}: {v} is outside the allowed range", f.id));
    }
    let raw = (v - f.offset) / f.scale;
    let out_of_range = || format!("{}: {v} does not fit {:?}", f.id, f.data_type);
    let words = match f.data_type {
        DataType::U16 => vec![fix_byte(
            int_in(raw, 0.0, 65535.0).ok_or_else(out_of_range)? as u16,
            f.byte_order,
        )],
        DataType::I16 => vec![fix_byte(
            int_in(raw, -32768.0, 32767.0).ok_or_else(out_of_range)? as i16 as u16,
            f.byte_order,
        )],
        DataType::U32 => from_u32(
            int_in(raw, 0.0, 4294967295.0).ok_or_else(out_of_range)? as u32,
            f.word_order,
            f.byte_order,
        ),
        DataType::I32 => from_u32(
            int_in(raw, -2147483648.0, 2147483647.0).ok_or_else(out_of_range)? as i32 as u32,
            f.word_order,
            f.byte_order,
        ),
        DataType::F32 => {
            let s = raw as f32;
            if !s.is_finite() {
                return Err(out_of_range());
            }
            from_u32(s.to_bits(), f.word_order, f.byte_order)
        }
        DataType::Bool => unreachable!(),
    };
    Ok(Encoded::Regs(words))
}

fn int_in(raw: f64, lo: f64, hi: f64) -> Option<f64> {
    let r = raw.round();
    (r >= lo && r <= hi).then_some(r)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modbus::regmap::*;

    fn field(t: DataType, wo: WordOrder, bo: ByteOrder) -> Field {
        Field {
            id: "x".into(),
            function: 3,
            address: 0,
            data_type: t,
            word_order: wo,
            byte_order: bo,
            bit: None,
            scale: 1.0,
            offset: 0.0,
            unit: None,
            poll_interval: None,
            deadband: 0.0,
            heartbeat: None,
            writable: true,
            write_function: None,
            min: None,
            max: None,
        }
    }

    #[test]
    fn sixteen_bit_types() {
        let f = field(DataType::U16, WordOrder::Big, ByteOrder::Big);
        assert_eq!(decode(&f, &[0xFFFF]).unwrap(), Sample::Num(65535.0));
        let f = field(DataType::I16, WordOrder::Big, ByteOrder::Big);
        assert_eq!(decode(&f, &[0xFFFE]).unwrap(), Sample::Num(-2.0));
        let f = field(DataType::U16, WordOrder::Big, ByteOrder::Little);
        assert_eq!(
            decode(&f, &[0x3412]).unwrap(),
            Sample::Num(f64::from(0x1234))
        );
    }

    #[test]
    fn thirty_two_bit_in_all_four_orders() {
        // 0x12345678 as ABCD, CDAB, BADC, DCBA
        let cases = [
            (WordOrder::Big, ByteOrder::Big, [0x1234, 0x5678]),
            (WordOrder::Little, ByteOrder::Big, [0x5678, 0x1234]),
            (WordOrder::Big, ByteOrder::Little, [0x3412, 0x7856]),
            (WordOrder::Little, ByteOrder::Little, [0x7856, 0x3412]),
        ];
        for (wo, bo, regs) in cases {
            let u = field(DataType::U32, wo, bo);
            assert_eq!(
                decode(&u, &regs).unwrap(),
                Sample::Num(f64::from(0x12345678u32)),
                "{wo:?}/{bo:?}"
            );
            assert_eq!(
                encode(&u, &serde_json::json!(f64::from(0x12345678u32))).unwrap(),
                Encoded::Regs(regs.to_vec())
            );
            let i = field(DataType::I32, wo, bo);
            let neg = from_u32((-123456i32) as u32, wo, bo);
            assert_eq!(decode(&i, &neg).unwrap(), Sample::Num(-123456.0));
            assert_eq!(
                encode(&i, &serde_json::json!(-123456)).unwrap(),
                Encoded::Regs(neg)
            );
            let fl = field(DataType::F32, wo, bo);
            let w = from_u32(21.5f32.to_bits(), wo, bo);
            assert_eq!(decode(&fl, &w).unwrap(), Sample::Num(21.5));
            assert_eq!(
                encode(&fl, &serde_json::json!(21.5)).unwrap(),
                Encoded::Regs(w)
            );
        }
    }

    #[test]
    fn f32_known_pattern_and_rejects_nan() {
        let f = field(DataType::F32, WordOrder::Big, ByteOrder::Big);
        assert_eq!(decode(&f, &[0x41A8, 0x0000]).unwrap(), Sample::Num(21.0));
        assert!(decode(&f, &[0x7FC0, 0x0000]).is_err());
    }

    #[test]
    fn bool_forms() {
        let mut f = field(DataType::Bool, WordOrder::Big, ByteOrder::Big);
        assert_eq!(decode(&f, &[0]).unwrap(), Sample::Bool(false));
        assert_eq!(decode(&f, &[7]).unwrap(), Sample::Bool(true));
        f.bit = Some(3);
        assert_eq!(decode(&f, &[0b1000]).unwrap(), Sample::Bool(true));
        assert_eq!(decode(&f, &[0b0100]).unwrap(), Sample::Bool(false));
        let mut c = field(DataType::Bool, WordOrder::Big, ByteOrder::Big);
        c.function = 1;
        assert_eq!(
            encode(&c, &serde_json::json!(true)).unwrap(),
            Encoded::Coil(true)
        );
        assert!(encode(&c, &serde_json::json!("on")).is_err());
    }

    #[test]
    fn scale_and_offset_both_ways() {
        let mut f = field(DataType::I16, WordOrder::Big, ByteOrder::Big);
        f.scale = 0.1;
        f.offset = -40.0;
        assert_eq!(decode(&f, &[600]).unwrap(), Sample::Num(20.0));
        assert_eq!(
            encode(&f, &serde_json::json!(20.0)).unwrap(),
            Encoded::Regs(vec![600])
        );
    }

    #[test]
    fn write_limits_and_range() {
        let mut f = field(DataType::U16, WordOrder::Big, ByteOrder::Big);
        assert!(encode(&f, &serde_json::json!(70000)).is_err());
        assert!(encode(&f, &serde_json::json!(-1)).is_err());
        f.min = Some(10.0);
        f.max = Some(20.0);
        assert!(encode(&f, &serde_json::json!(9.9)).is_err());
        assert!(encode(&f, &serde_json::json!(15)).is_ok());
        assert!(encode(&f, &serde_json::json!(true)).is_err());
    }
}
