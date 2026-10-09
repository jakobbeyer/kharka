//! Collections: what may be written where.
//!
//! The collections live as entries, `_collections/<name>`, so every replica has them, also offline. The hub's
//! `kharka.toml` defines protected ones (and `strict`): the hub writes them as entries, and only the file changes them.
//! Any node defines and changes the others (`kharka collections set`). Without `strict`, a key outside every collection
//! goes unchecked; with it, it is refused.
//!
//! ```toml
//! strict = false          # true: every key needs a collection
//!
//! [collections.status]
//! kind = "state"          # status/<node>/… is written only by that node; stale after the TTL
//! ttl = "30s"
//!
//! [collections.tasks]
//! kind = "record"         # any node; the latest write wins
//! schema = "task.json"    # a JSON Schema file next to kharka.toml, or an inline table
//! ```
//!
//! Without any collection every key is allowed. The schemas are a subset of JSON Schema, checked by hand: `type`,
//! `enum`, `const`, `properties`, `required`, `additionalProperties`, `items`, `minimum`, `maximum`, `minLength`,
//! `maxLength`, `minItems`, `maxItems`. Other keywords are ignored, as JSON Schema does with unknown ones.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::entry::{COLLECTIONS, Entry};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Collections {
    /// Every key needs a collection.
    #[serde(default)]
    pub strict: bool,
    #[serde(default)]
    pub collections: BTreeMap<String, Collection>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Collection {
    #[serde(default)]
    pub kind: Kind,
    /// For state: how long an entry stays fresh without a new write, e.g. `30s`, `5m`, `1h`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Value>,
    /// From the hub's kharka.toml: only the file changes it.
    #[serde(default, skip_serializing_if = "is_false")]
    pub protected: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// What a node reports about itself: keys `<collection>/<node>/…`, written only by that node.
    State,
    /// Shared data: any node writes, the latest write wins.
    #[default]
    Record,
}

impl Collections {
    /// Reads `kharka.toml`; a schema given as a string is a JSON file next to it.
    pub fn load(path: &Path) -> anyhow::Result<Collections> {
        Collections::load_with_files(path).map(|(collections, _)| collections)
    }

    /// Reads `kharka.toml`, and names every file it read: the config and the schema files, so that a change to any of
    /// them can be noticed.
    pub fn load_with_files(path: &Path) -> anyhow::Result<(Collections, Vec<PathBuf>)> {
        let mut files = vec![path.to_path_buf()];
        let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let mut collections: Collections =
            toml::from_str(&text).with_context(|| format!("reading {}", path.display()))?;
        let base = path.parent().unwrap_or(Path::new("."));
        for (name, collection) in &mut collections.collections {
            check_name(name).map_err(|problem| anyhow::anyhow!("{}: {problem}", path.display()))?;
            if let Some(Value::String(file)) = &collection.schema {
                let file = base.join(file);
                files.push(file.clone());
                let text =
                    fs::read_to_string(&file).with_context(|| format!("reading the schema {}", file.display()))?;
                collection.schema = Some(
                    serde_json::from_str(&text).with_context(|| format!("reading the schema {}", file.display()))?,
                );
            }
            if let Some(schema) = &collection.schema {
                check_schema(schema, "").with_context(|| format!("{}: the schema of {name}", path.display()))?;
            }
            if let Some(ttl) = &collection.ttl {
                parse_duration(ttl).with_context(|| format!("{}: the ttl of {name}", path.display()))?;
            }
        }
        Ok((collections, files))
    }

    /// The names of the collections, or `none` without any; `(strict)` when every key needs one.
    pub fn summary(&self) -> String {
        let names = if self.collections.is_empty() {
            "none".to_string()
        } else {
            self.collections.keys().cloned().collect::<Vec<_>>().join(", ")
        };
        if self.strict {
            format!("{names} (strict)")
        } else {
            names
        }
    }

    /// The collections as the entries `_collections/<name>` and `_kharka/settings` describe them. A definition that
    /// does not read is left out.
    pub fn from_entries<'a>(definitions: impl Iterator<Item = &'a Entry>, settings: Option<&Entry>) -> Collections {
        let collections = definitions
            .filter(|entry| !entry.deleted)
            .filter_map(|entry| {
                let name = entry.key.strip_prefix(COLLECTIONS)?;
                let collection = serde_json::from_value(entry.value.clone()).ok()?;
                Some((name.to_string(), collection))
            })
            .collect();
        let strict = settings
            .and_then(Entry::live_value)
            .is_some_and(|value| value["strict"] == true);
        Collections { strict, collections }
    }

    /// Laid over others: these collections and `strict` win, and are protected (a daemon's own kharka.toml, without a
    /// hub).
    pub fn overlay(mut self, on_top: &Collections) -> Collections {
        for (name, collection) in &on_top.collections {
            let protected = Collection {
                protected: true,
                ..collection.clone()
            };
            self.collections.insert(name.clone(), protected);
        }
        self.strict |= on_top.strict;
        self
    }

    /// Whether `writer` may write this value (`None`: delete) at this key.
    pub fn check(&self, key: &str, value: Option<&Value>, writer: &str) -> Result<(), String> {
        if key.starts_with('_') {
            return Ok(());
        }
        let mut segments = key.split('/');
        let name = segments.next().unwrap_or_default();
        let Some(collection) = self.collections.get(name) else {
            if !self.strict {
                return Ok(());
            }
            return Err(format!(
                "{key}: no collection {name:?}, and every key needs one here (strict); there are: {}",
                self.summary()
            ));
        };
        if collection.kind == Kind::State && segments.next() != Some(writer) {
            return Err(format!(
                "{key}: {name} is state, each node writes only {name}/{writer}/…"
            ));
        }
        if let (Some(schema), Some(value)) = (&collection.schema, value) {
            validate(schema, value, "").map_err(|problem| format!("{key}: {problem}"))?;
        }
        Ok(())
    }

    /// How long an entry at this key stays fresh, for state collections with a TTL.
    pub fn ttl(&self, key: &str) -> Option<Duration> {
        let collection = self.collections.get(key.split('/').next()?)?;
        if collection.kind != Kind::State {
            return None;
        }
        parse_duration(collection.ttl.as_deref()?).ok()
    }
}

/// A collection's name: one key segment, not starting with `_`.
pub fn check_name(name: &str) -> Result<(), String> {
    if crate::entry::check_node(name).is_err() || name.starts_with('_') {
        return Err(format!(
            "not a collection name: {name:?} (A-Z a-z 0-9 . _ -, not starting with _)"
        ));
    }
    Ok(())
}

/// A definition a node sends for `_collections/<name>`: a valid name, kind, TTL and schema, and not marked protected
/// (only the hub's file protects).
pub fn check_definition(name: &str, value: &Value) -> Result<Collection, String> {
    check_name(name)?;
    let collection: Collection =
        serde_json::from_value(value.clone()).map_err(|err| format!("collection {name}: {err}"))?;
    if collection.protected {
        return Err(format!(
            "collection {name}: only the hub's kharka.toml protects a collection"
        ));
    }
    if let Some(ttl) = &collection.ttl {
        parse_duration(ttl).map_err(|err| format!("collection {name}: the ttl: {err}"))?;
    }
    if let Some(schema) = &collection.schema {
        check_schema(schema, "").map_err(|err| format!("collection {name}: the schema: {err:#}"))?;
    }
    Ok(collection)
}

/// `500ms`, `30s`, `5m`, `2h`, `1d`.
pub fn parse_duration(text: &str) -> anyhow::Result<Duration> {
    let digits = text.find(|c: char| !c.is_ascii_digit()).unwrap_or(text.len());
    let (number, unit) = text.split_at(digits);
    let Ok(number) = number.parse::<u64>() else {
        bail!("not a duration: {text:?} (e.g. 30s, 5m)")
    };
    let ms = match unit {
        "ms" => 1,
        "s" => 1000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => bail!("not a duration: {text:?} (units: ms, s, m, h, d)"),
    };
    Ok(Duration::from_millis(number.saturating_mul(ms)))
}

const TYPES: [&str; 7] = ["null", "boolean", "object", "array", "number", "integer", "string"];

/// The schema itself: an object whose known keywords have the right kind of value, all the way down. A keyword the
/// validator does not know is left alone, as JSON Schema does.
fn check_schema(schema: &Value, at: &str) -> anyhow::Result<()> {
    let Value::Object(map) = schema else {
        bail!("{}: a schema is an object", place(at))
    };
    let types: Vec<&Value> = match map.get("type") {
        None => vec![],
        Some(Value::Array(list)) if list.is_empty() => bail!("{}: type is an empty list, nothing would fit", place(at)),
        Some(Value::Array(list)) => list.iter().collect(),
        Some(one) => vec![one],
    };
    for kind in types {
        if !kind.as_str().is_some_and(|kind| TYPES.contains(&kind)) {
            bail!("{}: unknown type {kind}", place(at));
        }
    }
    let expect = |keyword: &str, fits: fn(&Value) -> bool, what: &str| -> anyhow::Result<()> {
        match map.get(keyword) {
            Some(value) if !fits(value) => bail!("{}: {keyword} must be {what}, not {value}", place(at)),
            _ => Ok(()),
        }
    };
    let count = |value: &Value| value.as_u64().is_some();
    expect(
        "required",
        |v| v.as_array().is_some_and(|names| names.iter().all(Value::is_string)),
        "a list of names",
    )?;
    expect("enum", Value::is_array, "a list")?;
    expect("properties", Value::is_object, "an object of schemas")?;
    expect("items", Value::is_object, "a schema")?;
    expect(
        "additionalProperties",
        |v| v.is_boolean() || v.is_object(),
        "true, false or a schema",
    )?;
    expect("minimum", Value::is_number, "a number")?;
    expect("maximum", Value::is_number, "a number")?;
    for keyword in ["minLength", "maxLength", "minItems", "maxItems"] {
        expect(keyword, count, "a whole number, 0 or more")?;
    }
    if let Some(Value::Object(properties)) = map.get("properties") {
        for (name, property) in properties {
            check_schema(property, &format!("{at}/{name}"))?;
        }
    }
    for keyword in ["items", "additionalProperties"] {
        if let Some(sub @ Value::Object(_)) = map.get(keyword) {
            check_schema(sub, &format!("{at}/{keyword}"))?;
        }
    }
    Ok(())
}

/// JSON equality as JSON Schema means it: numbers by their value (1 and 1.0 are the same), the rest as they are.
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x == y || x.as_f64() == y.as_f64(),
        (Value::Array(x), Value::Array(y)) => x.len() == y.len() && x.iter().zip(y).all(|(x, y)| same(x, y)),
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(key, x)| y.get(key).is_some_and(|y| same(x, y)))
        }
        _ => a == b,
    }
}

/// A value for a message: short, so that a refusal never repeats a large value.
fn shown(value: &Value) -> String {
    let text = value.to_string();
    if text.chars().count() <= 80 {
        return text;
    }
    let mut cut: String = text.chars().take(80).collect();
    cut.push('…');
    cut
}

fn place(at: &str) -> &str {
    if at.is_empty() { "the value" } else { at }
}

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Object(_) => "object",
        Value::Array(_) => "array",
        Value::Number(_) => "number",
        Value::String(_) => "string",
    }
}

fn has_type(value: &Value, kind: &str) -> bool {
    match kind {
        "integer" => {
            value.as_i64().is_some() || value.as_u64().is_some() || value.as_f64().is_some_and(|n| n.fract() == 0.0)
        }
        other => type_name(value) == other,
    }
}

/// Checks a value against a schema; the problem names the place, e.g. `/items/0/name`.
pub fn validate(schema: &Value, value: &Value, at: &str) -> Result<(), String> {
    let Value::Object(rules) = schema else { return Ok(()) };
    let fail = |message: String| Err(format!("{}: {message}", place(at)));
    match rules.get("type") {
        Some(Value::String(kind)) if !has_type(value, kind) => {
            return fail(format!("{} instead of {kind}", type_name(value)));
        }
        Some(Value::Array(kinds))
            if !kinds
                .iter()
                .any(|kind| kind.as_str().is_some_and(|kind| has_type(value, kind))) =>
        {
            return fail(format!(
                "{} is not one of the types {}",
                type_name(value),
                Value::Array(kinds.clone())
            ));
        }
        _ => {}
    }
    if let Some(Value::Array(options)) = rules.get("enum")
        && !options.iter().any(|option| same(option, value))
    {
        return fail(format!(
            "{} is not one of {}",
            shown(value),
            shown(&Value::Array(options.clone()))
        ));
    }
    if let Some(expected) = rules.get("const")
        && !same(expected, value)
    {
        return fail(format!("{} instead of {}", shown(value), shown(expected)));
    }
    let bound = |name: &str| rules.get(name).and_then(Value::as_f64);
    if let Some(number) = value.as_f64() {
        if let Some(min) = bound("minimum").filter(|min| number < *min) {
            return fail(format!("{number} is below {min}"));
        }
        if let Some(max) = bound("maximum").filter(|max| number > *max) {
            return fail(format!("{number} is above {max}"));
        }
    }
    let length = match value {
        Value::String(text) => Some((text.chars().count(), "minLength", "maxLength", "characters")),
        Value::Array(items) => Some((items.len(), "minItems", "maxItems", "items")),
        _ => None,
    };
    if let Some((len, min_name, max_name, unit)) = length {
        if let Some(min) = bound(min_name).filter(|min| (len as f64) < *min) {
            return fail(format!("{len} {unit}, at least {min} needed"));
        }
        if let Some(max) = bound(max_name).filter(|max| (len as f64) > *max) {
            return fail(format!("{len} {unit}, at most {max} allowed"));
        }
    }
    if let (Value::Array(items), Some(item_schema)) = (value, rules.get("items")) {
        for (index, item) in items.iter().enumerate() {
            validate(item_schema, item, &format!("{at}/{index}"))?;
        }
    }
    if let Value::Object(fields) = value {
        if let Some(Value::Array(required)) = rules.get("required") {
            for name in required.iter().filter_map(Value::as_str) {
                if !fields.contains_key(name) {
                    return fail(format!("{name} is missing"));
                }
            }
        }
        let properties = rules.get("properties").and_then(Value::as_object);
        for (name, field) in fields {
            match properties.and_then(|properties| properties.get(name)) {
                Some(field_schema) => validate(field_schema, field, &format!("{at}/{name}"))?,
                None => match rules.get("additionalProperties") {
                    Some(Value::Bool(false)) => return fail(format!("{name} is not allowed")),
                    Some(extra @ Value::Object(_)) => validate(extra, field, &format!("{at}/{name}"))?,
                    _ => {}
                },
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn topic_schema() -> Value {
        json!({
            "type": "object",
            "required": ["title", "status"],
            "properties": {
                "title": { "type": "string", "minLength": 1, "maxLength": 200 },
                "status": { "enum": ["now", "next", "waiting", "review", "later", "done"] },
                "updated": { "type": "integer", "minimum": 0 },
                "tags": { "type": "array", "items": { "type": "string" }, "maxItems": 3 },
                "assignee": { "type": ["string", "null"] }
            },
            "additionalProperties": false
        })
    }

    #[test]
    fn values_against_a_schema() {
        let schema = topic_schema();
        let good = json!({ "title": "A", "status": "now", "updated": 5, "tags": ["x"], "assignee": null });
        assert_eq!(validate(&schema, &good, ""), Ok(()));
        let cases = [
            (json!([]), "the value: array instead of object"),
            (json!({ "status": "now" }), "the value: title is missing"),
            (
                json!({ "title": "", "status": "now" }),
                "/title: 0 characters, at least 1 needed",
            ),
            (
                json!({ "title": "A", "status": "soon" }),
                "/status: \"soon\" is not one of",
            ),
            (
                json!({ "title": "A", "status": "now", "updated": 1.5 }),
                "/updated: number instead of integer",
            ),
            (
                json!({ "title": "A", "status": "now", "tags": ["x", 2] }),
                "/tags/1: number instead of string",
            ),
            (
                json!({ "title": "A", "status": "now", "colour": 1 }),
                "the value: colour is not allowed",
            ),
            (
                json!({ "title": "A", "status": "now", "assignee": 3 }),
                "/assignee: number is not one of the types",
            ),
        ];
        for (value, expected) in cases {
            let problem = validate(&schema, &value, "").unwrap_err();
            assert!(problem.starts_with(expected), "{value}: {problem}");
        }
    }

    #[test]
    fn collections_decide_where_who_writes() {
        let collections: Collections = toml::from_str(
            r#"
            [collections.status]
            kind = "state"
            ttl = "30s"
            [collections.topics]
            schema = { type = "object", required = ["title"] }
            "#,
        )
        .unwrap();
        assert_eq!(collections.check("status/mac", Some(&json!({})), "mac"), Ok(()));
        assert_eq!(collections.check("status/mac/battery", Some(&json!({})), "mac"), Ok(()));
        assert!(collections.check("status/server", Some(&json!({})), "mac").is_err());
        assert!(collections.check("status", Some(&json!({})), "mac").is_err());
        assert_eq!(
            collections.check("topics/1", Some(&json!({ "title": "x" })), "mac"),
            Ok(())
        );
        assert!(collections.check("topics/1", Some(&json!({})), "mac").is_err());
        assert_eq!(collections.check("topics/1", None, "mac"), Ok(()));
        assert_eq!(
            collections.check("other/1", Some(&json!(1)), "mac"),
            Ok(()),
            "open: no collection, no rules"
        );
        assert_eq!(collections.check("_nodes/mac", Some(&json!(1)), "hub"), Ok(()));
        assert_eq!(collections.ttl("status/mac"), Some(Duration::from_secs(30)));
        assert_eq!(collections.ttl("topics/1"), None);
        assert_eq!(Collections::default().check("any/thing", Some(&json!(1)), "x"), Ok(()));

        let strict = Collections {
            strict: true,
            ..collections
        };
        let refused = strict.check("other/1", Some(&json!(1)), "mac").unwrap_err();
        assert!(refused.contains("every key needs one here (strict)"), "{refused}");
        assert_eq!(strict.check("topics/1", Some(&json!({ "title": "x" })), "mac"), Ok(()));
    }

    #[test]
    fn schemas_are_checked_themselves() {
        for (schema, problem) in [
            (json!({ "type": [] }), "empty list"),
            (json!({ "required": "title" }), "required must be a list of names"),
            (json!({ "enum": "x" }), "enum must be a list"),
            (json!({ "minLength": -1 }), "minLength must be a whole number"),
            (json!({ "properties": { "a": { "type": "thing" } } }), "unknown type"),
        ] {
            let refused = check_schema(&schema, "").unwrap_err().to_string();
            assert!(refused.contains(problem), "{schema}: {refused}");
        }
        assert!(
            check_schema(&json!({ "title": "x", "format": "date", "x-own": 1 }), "").is_ok(),
            "unknown keywords stay"
        );
    }

    #[test]
    fn numbers_compare_by_value_and_messages_stay_short() {
        let schema = json!({ "enum": [1, 2.5, [1, { "a": 2 }]] });
        assert_eq!(validate(&schema, &json!(1.0), ""), Ok(()));
        assert_eq!(validate(&schema, &json!([1.0, { "a": 2.0 }]), ""), Ok(()));
        assert!(validate(&json!({ "const": 3 }), &json!(3.0), "").is_ok());
        let refused = validate(&schema, &json!("x".repeat(10_000)), "").unwrap_err();
        assert!(refused.len() < 300, "{} characters", refused.len());
    }

    #[test]
    fn a_misspelt_field_in_a_definition_is_refused() {
        assert!(check_definition("notes", &json!({ "kind": "state", "tll": "30s" })).is_err());
        assert!(toml::from_str::<Collections>("strickt = true\n").is_err());
    }

    #[test]
    fn definitions_sent_by_nodes() {
        let good = json!({ "kind": "state", "ttl": "1m", "schema": { "type": "object" } });
        assert_eq!(check_definition("notes", &good).unwrap().kind, Kind::State);
        assert!(check_definition("notes", &json!({})).is_ok(), "a record without rules");
        for (name, value, problem) in [
            ("_x", json!({}), "not a collection name"),
            ("a/b", json!({}), "not a collection name"),
            ("notes", json!({ "kind": "list" }), "unknown variant"),
            ("notes", json!({ "ttl": "soon" }), "the ttl"),
            ("notes", json!({ "schema": { "type": "thing" } }), "the schema"),
            (
                "notes",
                json!({ "protected": true }),
                "only the hub's kharka.toml protects",
            ),
        ] {
            let refused = check_definition(name, &value).unwrap_err();
            assert!(refused.contains(problem), "{name} {value}: {refused}");
        }
    }

    #[test]
    fn collections_from_their_entries() {
        let entry = |key: &str, value: Option<Value>| Entry::new(key, value, "1-0-n".parse().unwrap());
        let definitions = [
            entry(
                "_collections/tasks",
                Some(json!({ "schema": { "type": "object" }, "protected": true })),
            ),
            entry("_collections/notes", Some(json!({ "kind": "state" }))),
            entry("_collections/gone", None),
            entry("_collections/broken", Some(json!({ "kind": 3 }))),
        ];
        let settings = entry("_kharka/settings", Some(json!({ "strict": true })));
        let collections = Collections::from_entries(definitions.iter(), Some(&settings));
        assert!(collections.strict);
        assert_eq!(collections.collections.keys().collect::<Vec<_>>(), ["notes", "tasks"]);
        assert!(collections.collections["tasks"].protected);
        assert!(!Collections::from_entries(definitions.iter(), None).strict);

        let file: Collections = toml::from_str("[collections.notes]\nkind = \"record\"\n").unwrap();
        let laid = Collections::from_entries(definitions.iter(), None).overlay(&file);
        assert_eq!(laid.collections["notes"].kind, Kind::Record);
        assert!(laid.collections["notes"].protected);
    }

    #[test]
    fn a_config_file_with_a_schema_file() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("topic.json"), topic_schema().to_string()).unwrap();
        fs::write(
            dir.path().join("kharka.toml"),
            "[collections.topics]\nschema = \"topic.json\"\n",
        )
        .unwrap();
        let collections = Collections::load(&dir.path().join("kharka.toml")).unwrap();
        assert_eq!(collections.collections["topics"].schema, Some(topic_schema()));

        fs::write(
            dir.path().join("kharka.toml"),
            "[collections.x]\nschema = { type = \"thing\" }\n",
        )
        .unwrap();
        assert!(Collections::load(&dir.path().join("kharka.toml")).is_err());
        fs::write(
            dir.path().join("kharka.toml"),
            "[collections.x]\nkind = \"state\"\nttl = \"soon\"\n",
        )
        .unwrap();
        assert!(Collections::load(&dir.path().join("kharka.toml")).is_err());
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        for bad in ["", "s", "5", "5 s", "5w"] {
            assert!(parse_duration(bad).is_err(), "{bad}");
        }
    }
}
