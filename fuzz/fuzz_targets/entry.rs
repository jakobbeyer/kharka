//! Keys, node names, stamps and entries from any text: the checks agree with each other, a stamp prints as it parses,
//! and a clock fed with the stamps and wall times in the text only ever moves forward, past every stamp it sees that is
//! not too far ahead of its wall time.

#![no_main]

use kharka::entry::{Entry, check_key, check_node, check_prefix, check_writable};
use kharka::hlc::{Clock, Hlc};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(entry) = serde_json::from_slice::<Entry>(data) {
        let again: Entry = serde_json::from_str(&serde_json::to_string(&entry).unwrap()).unwrap();
        assert_eq!(
            (&again.key, &again.hlc, again.deleted, again.seq),
            (&entry.key, &entry.hlc, entry.deleted, entry.seq)
        );
    }
    let Ok(text) = std::str::from_utf8(data) else { return };
    check(text);
    let mut clock = Clock::new("fuzz");
    let mut now = 0;
    let mut last: Option<Hlc> = None;
    for line in text.lines() {
        check(line);
        let next = if let Ok(seen) = line.parse::<Hlc>() {
            clock.observe_at(&seen, now);
            let next = clock.tick_at(now);
            assert!(
                next > seen || seen.too_far_ahead(now) || at_the_end(&next),
                "{next} after seeing {seen}"
            );
            next
        } else if let Ok(wall) = line.parse::<u64>() {
            now = wall;
            clock.tick_at(now)
        } else {
            continue;
        };
        assert!(
            last.as_ref().is_none_or(|last| next > *last || at_the_end(&next)),
            "{next} after {last:?}"
        );
        last = Some(next);
    }
});

/// The last stamp there is, which repeats.
fn at_the_end(hlc: &Hlc) -> bool {
    (hlc.wall_ms, hlc.counter) == (u64::MAX, u32::MAX)
}

fn check(text: &str) {
    if let Ok(hlc) = text.parse::<Hlc>() {
        assert_eq!(hlc.to_string().parse::<Hlc>(), Ok(hlc.clone()));
        assert!(check_node(&hlc.node).is_ok());
    }
    if check_writable(text).is_ok() {
        assert!(check_key(text).is_ok());
    }
    if check_key(text).is_ok() {
        assert!(check_prefix(text).is_ok(), "a key is a prefix of itself: {text:?}");
    }
    if check_node(text).is_ok() {
        assert!(check_key(text).is_ok(), "a node name is a key segment: {text:?}");
    }
}
