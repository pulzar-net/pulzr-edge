//! `schema/register-map.schema.json` is generated from the Rust types; this keeps the
//! checked-in copy honest and checks the examples against it.

use pulzr_edge::{config::Config, modbus::regmap::RegisterMap};
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn checked_in_schema_is_current() {
    let generated =
        serde_json::to_string_pretty(&schemars::schema_for!(RegisterMap)).unwrap() + "\n";
    let path = root().join("schema/register-map.schema.json");
    if std::env::var_os("UPDATE_SCHEMA").is_some() {
        std::fs::write(&path, &generated).unwrap();
    }
    let on_disk = std::fs::read_to_string(&path).unwrap_or_default();
    assert_eq!(
        on_disk, generated,
        "schema/register-map.schema.json is stale: rerun with UPDATE_SCHEMA=1"
    );
}

#[test]
fn example_map_matches_schema_and_loads() {
    let schema: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(root().join("schema/register-map.schema.json")).unwrap(),
    )
    .unwrap();
    let v = jsonschema::validator_for(&schema).unwrap();
    let path = root().join("examples/maps/generic-energy-meter.toml");
    let as_json: serde_json::Value =
        toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    let errs: Vec<String> = v.iter_errors(&as_json).map(|e| e.to_string()).collect();
    assert!(errs.is_empty(), "{errs:?}");
    let map = RegisterMap::load(&path).unwrap();
    assert_eq!(map.fields.len(), 7);
    // a bad map fails the schema as well as validate()
    let bad = serde_json::json!({"schema": 0, "model": "x", "fields": [{"id": "a", "function": 3, "address": 0, "type": "f64"}]});
    assert!(!v.is_valid(&bad));
}

#[test]
fn example_config_loads() {
    let c = Config::load(&root().join("examples/config.toml")).unwrap();
    assert_eq!(c.buses.len(), 2);
    for b in &c.buses {
        for u in &b.units {
            RegisterMap::load(&root().join("examples").join(&u.map)).unwrap();
        }
    }
}
