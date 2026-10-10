//! Collections and schemas from any JSON: the input is a definition (or a bare schema), a zero byte, and a value. A
//! definition is checked as a node's push would be, and taken unchecked as a restored entry would be; the value is
//! validated against both. Every value fits a schema of itself (`const`, `enum`) and the empty schema.

#![no_main]

use kharka::entry::Entry;
use kharka::schema::{Collections, check_definition, parse_duration, validate};
use libfuzzer_sys::fuzz_target;
use serde_json::{Value, json};

fuzz_target!(|data: &[u8]| {
    let (first, second) = match data.iter().position(|byte| *byte == 0) {
        Some(at) => (&data[..at], &data[at + 1..]),
        None => (data, &[][..]),
    };
    if let Ok(text) = std::str::from_utf8(second) {
        let _ = parse_duration(text);
    }
    let Ok(definition) = serde_json::from_slice::<Value>(first) else {
        return;
    };
    let value = serde_json::from_slice::<Value>(second).ok();

    let _ = check_definition("c", &definition);
    let entry = Entry::new("_collections/c", Some(definition.clone()), "1-0-fuzz".parse().unwrap());
    let collections = Collections::from_entries(std::iter::once(&entry), None);
    let _ = collections.check("c/fuzz", value.as_ref(), "fuzz");
    let _ = collections.check("c/other/x", value.as_ref(), "fuzz");
    let _ = collections.ttl("c/fuzz");

    let Some(value) = value else { return };
    let _ = validate(&definition, &value, "");
    assert_eq!(validate(&json!({}), &value, ""), Ok(()));
    assert_eq!(validate(&json!({ "const": value }), &value, ""), Ok(()));
    assert_eq!(validate(&json!({ "enum": [1, value] }), &value, ""), Ok(()));
});
