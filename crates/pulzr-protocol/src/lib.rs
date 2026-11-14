//! Pulzar protocol v1 message bodies, generated at build time from
//! `schema/protocol.schema.json` (a copy of the protocol repository's schema).
#![allow(clippy::all, unused_imports, dead_code)]
include!(concat!(env!("OUT_DIR"), "/protocol.rs"));

/// The `"v": 1` every message body carries.
pub fn v1() -> ::serde_json::Value {
    ::serde_json::Value::from(1)
}
