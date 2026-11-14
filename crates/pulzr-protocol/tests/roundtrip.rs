//! Serde round trips and schema validation of the protocol repository's examples
//! (copied to `tests/fixtures/`), plus the cross-field rules the Rust types cannot carry.
use pulzr_protocol::*;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value as J};

const SCHEMA: &str = include_str!("../schema/protocol.schema.json");

fn validator(def: &str) -> jsonschema::Validator {
    let mut doc: J = serde_json::from_str(SCHEMA).unwrap();
    doc["$ref"] = json!(format!("#/$defs/{def}"));
    jsonschema::validator_for(&doc).unwrap()
}

/// Numbers compare by value (1793051880 and 1793051880.0 are the same time).
fn same(a: &J, b: &J) -> bool {
    match (a, b) {
        (J::Number(x), J::Number(y)) => x.as_f64() == y.as_f64(),
        (J::Array(x), J::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| same(p, q))
        }
        (J::Object(x), J::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| same(v, w)))
        }
        _ => a == b,
    }
}

fn fixture(name: &str) -> J {
    let path = format!("{}/tests/fixtures/{name}.json", env!("CARGO_MANIFEST_DIR"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn check<T: DeserializeOwned + Serialize>(name: &str, def: &str) {
    let original = fixture(name);
    assert!(
        validator(def).is_valid(&original),
        "{name} is not valid {def}"
    );
    let typed: T = serde_json::from_value(original.clone()).unwrap();
    let back = serde_json::to_value(&typed).unwrap();
    assert!(
        same(&original, &back),
        "{name} did not round-trip:\n{original}\n{back}"
    );
    assert!(
        validator(def).is_valid(&back),
        "{name} re-serialised is not valid {def}"
    );
}

#[test]
fn up_modbus() {
    check::<Up>("up-modbus", "up");
}
#[test]
fn up_lorawan() {
    check::<Up>("up-lorawan", "up");
}
#[test]
fn cmd() {
    check::<Cmd>("cmd", "cmd");
}
#[test]
fn cmd_res_queued() {
    check::<CmdResult>("cmd-res-queued", "cmdResult");
}
#[test]
fn cmd_res_failed() {
    check::<CmdResult>("cmd-res-failed", "cmdResult");
}

#[test]
fn built_messages_validate() {
    let up = Up {
        dedup: None,
        link: Some(Link {
            modbus: Some(LinkModbus {
                function: 3.try_into().ok(),
                address: Some(0),
                count: 2.try_into().ok(),
                latency_ms: Some(12.5),
            }),
            ..Default::default()
        }),
        r: vec![Reading {
            ts: Time(1.0),
            v: [("flow".try_into().unwrap(), Value::from(12.6))].into(),
        }],
        raw: None,
        rx: vec![],
        seq: Some(Seq(3)),
        v: v1(),
    };
    assert!(validator("up").is_valid(&serde_json::to_value(&up).unwrap()));
}

#[test]
fn cross_field_rules_live_in_the_schema() {
    let up = validator("up");
    assert!(
        !up.is_valid(&json!({"v":1,"r":[{"ts":1,"v":{"a":1}}]})),
        "needs seq or dedup"
    );
    assert!(!up.is_valid(&json!({"v":1,"seq":1})), "needs r or raw");
    let res = validator("cmdResult");
    assert!(
        !res.is_valid(&json!({"v":1,"id":"c7e1a2b3-0001","status":"refused"})),
        "refused needs a reason"
    );
    assert!(
        res.is_valid(&json!({"v":1,"id":"c7e1a2b3-0001","status":"refused","reason":"invalid"}))
    );
}

#[test]
fn constrained_strings_reject_bad_input() {
    assert!(
        serde_json::from_value::<CmdResult>(json!({"v":1,"id":"short","status":"ok"})).is_err()
    );
    assert!(serde_json::from_value::<Up>(json!({"v":1,"seq":-1})).is_err());
}
