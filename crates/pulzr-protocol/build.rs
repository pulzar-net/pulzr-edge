use std::{env, fs, path::Path};

fn main() {
    println!("cargo:rerun-if-changed=schema/protocol.schema.json");
    let text = fs::read_to_string("schema/protocol.schema.json").unwrap();
    let mut doc: serde_json::Value = serde_json::from_str(&text).unwrap();
    // Cross-field rules (`up` needs seq or dedup and r or raw; a refused or failed
    // result needs a reason) are not representable as Rust types. They stay in the
    // schema and are enforced by the jsonschema checks in this crate's tests.
    let defs = doc["$defs"].as_object_mut().unwrap();
    for (name, keys) in [
        ("up", &["anyOf", "oneOf"][..]),
        ("cmdResult", &["allOf"][..]),
    ] {
        let def = defs.get_mut(name).unwrap().as_object_mut().unwrap();
        for k in keys {
            def.remove(*k);
        }
    }
    let schema: schemars::schema::RootSchema = serde_json::from_value(doc).unwrap();
    let mut ts =
        typify::TypeSpace::new(typify::TypeSpaceSettings::default().with_struct_builder(false));
    ts.add_root_schema(schema).unwrap();
    let file = syn::parse2::<syn::File>(ts.to_stream()).unwrap();
    let out = Path::new(&env::var("OUT_DIR").unwrap()).join("protocol.rs");
    fs::write(out, prettyplease::unparse(&file)).unwrap();
}
