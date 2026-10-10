//! Convergence, as properties: writes and deletions of several nodes, delivered to separate stores in any order and any
//! number of times, end in the same state, the latest stamp winning per key; and a node's stamps go strictly up, also
//! after it saw stamps of others.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};

use kharka::entry::Entry;
use kharka::hlc::{Clock, Hlc, wall_ms};
use kharka::storage::Storage;
use kharka::store::{Options, Store};
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::Index;
use serde_json::{Value, json};

const NODES: [&str; 3] = ["a", "b", "c"];
const KEYS: usize = 4;
/// The wall clocks show moments close to this one, close enough that stamps collide and counters count.
const NOW: u64 = 1_800_000_000_000;
/// Stores that take the writes, each in its own order.
const REPLICAS: usize = 3;

#[derive(Clone, Debug)]
enum Event {
    /// A node writes a key (`None`: deletes it), its wall clock showing `NOW + drift`.
    Write {
        node: usize,
        key: usize,
        value: Option<i64>,
        drift: u64,
    },
    /// A node sees an earlier write, of any node (the `nth` so far, counted round).
    See { node: usize, nth: usize },
    /// A node sees a stamp from elsewhere, perhaps with its counter spent.
    Stamp { node: usize, wall: u64, counter: u32 },
}

fn event() -> impl Strategy<Value = Event> {
    let node = 0..NODES.len();
    prop_oneof![
        4 => (node.clone(), 0..KEYS, proptest::option::weighted(0.8, -3i64..3), 0..20u64)
            .prop_map(|(node, key, value, drift)| Event::Write { node, key, value, drift }),
        2 => (node.clone(), any::<usize>()).prop_map(|(node, nth)| Event::See { node, nth }),
        1 => (node, 0..30u64, prop_oneof![0..3u32, u32::MAX - 2..=u32::MAX])
            .prop_map(|(node, wall, counter)| Event::Stamp { node, wall: NOW + wall, counter }),
    ]
}

/// The order a replica gets the writes in: each once, shuffled, and some again in between.
type Delivery = (Vec<usize>, Vec<(Index, usize)>);

fn scenario() -> impl Strategy<Value = (Vec<Event>, Vec<Delivery>)> {
    vec(event(), 1..60).prop_flat_map(|events| {
        let writes = events
            .iter()
            .filter(|event| matches!(event, Event::Write { .. }))
            .count();
        let order = Just((0..writes).collect::<Vec<_>>()).prop_shuffle();
        let again = vec((any::<Index>(), 0..writes.max(1)), 0..=writes);
        (Just(events), vec((order, again), REPLICAS))
    })
}

/// Storage in memory, which a store can be opened from again.
#[derive(Clone, Default)]
struct Memory {
    records: Arc<Mutex<Vec<Entry>>>,
    appended: usize,
}

impl Storage for Memory {
    fn load(&mut self) -> io::Result<Vec<Entry>> {
        Ok(self.records.lock().unwrap().clone())
    }
    fn append(&mut self, entries: &[Entry]) -> io::Result<()> {
        self.records.lock().unwrap().extend_from_slice(entries);
        self.appended += entries.len();
        Ok(())
    }
    fn compact(&mut self, entries: &mut dyn Iterator<Item = &Entry>) -> io::Result<()> {
        *self.records.lock().unwrap() = entries.cloned().collect();
        self.appended = 0;
        Ok(())
    }
    fn appended(&self) -> usize {
        self.appended
    }
    fn appended_bytes(&self) -> u64 {
        0
    }
    fn snapshot_bytes(&self) -> u64 {
        0
    }
    fn damaged(&self) -> bool {
        false
    }
}

fn open(storage: &Memory, node: &str) -> Store {
    let options = Options {
        sync: false,
        compact_min: 4,
        ..Options::default()
    };
    Store::with_storage(Box::new(storage.clone()), node, options).unwrap()
}

/// What a store holds per key: the value (or a tombstone) and its stamp.
fn state(store: &Store) -> BTreeMap<String, (Value, bool, Hlc)> {
    (0..KEYS)
        .filter_map(|key| store.held(&format!("k/{key}")))
        .map(|entry| {
            (
                entry.key.clone(),
                (entry.value.clone(), entry.deleted, entry.hlc.clone()),
            )
        })
        .collect()
}

/// Plays the events, each node with its own clock: the writes, in the order they were made. Every stamp a node makes
/// is after its last one and after every stamp it saw.
fn play(events: &[Event]) -> Result<Vec<Entry>, TestCaseError> {
    let mut clocks: Vec<Clock> = NODES.iter().map(|node| Clock::new(*node)).collect();
    let mut latest: Vec<Option<Hlc>> = vec![None; NODES.len()];
    let mut writes: Vec<Entry> = Vec::new();
    for event in events {
        let (node, seen) = match event {
            Event::Write {
                node,
                key,
                value,
                drift,
            } => {
                let hlc = clocks[*node].tick_at(NOW + drift);
                if let Some(before) = &latest[*node] {
                    prop_assert!(hlc > *before, "{} stamped {hlc} after {before}", NODES[*node]);
                }
                latest[*node] = Some(hlc.clone());
                writes.push(Entry::new(format!("k/{key}"), value.map(|value| json!(value)), hlc));
                continue;
            }
            Event::See { node, nth } => match writes.len() {
                0 => continue,
                len => (*node, writes[nth % len].hlc.clone()),
            },
            Event::Stamp { node, wall, counter } => (
                *node,
                Hlc {
                    wall_ms: *wall,
                    counter: *counter,
                    node: "elsewhere".into(),
                },
            ),
        };
        clocks[node].observe_at(&seen, NOW);
        if latest[node].as_ref().is_none_or(|latest| seen > *latest) {
            latest[node] = Some(seen);
        }
    }
    Ok(writes)
}

proptest! {
    #[test]
    fn every_order_ends_in_the_same_state((events, deliveries) in scenario()) {
        let writes = play(&events)?;
        // the latest stamp per key wins
        let mut expected = BTreeMap::new();
        for entry in &writes {
            let held = expected.entry(entry.key.clone()).or_insert_with(|| entry.clone());
            if entry.hlc > held.hlc {
                *held = entry.clone();
            }
        }
        let expected: BTreeMap<_, _> = expected
            .into_values()
            .map(|entry| (entry.key, (entry.value, entry.deleted, entry.hlc)))
            .collect();

        for (order, again) in deliveries {
            let mut sequence = order;
            for (at, nth) in again {
                if nth < writes.len() {
                    sequence.insert(at.index(sequence.len() + 1), nth);
                }
            }
            // as the hub takes pushes, and as a replica takes the hub's entries
            let (hub_storage, replica_storage) = (Memory::default(), Memory::default());
            let mut hub = open(&hub_storage, "hub");
            let mut replica = open(&replica_storage, "replica");
            for (number, &nth) in sequence.iter().enumerate() {
                hub.accept(writes[nth].clone()).unwrap();
                replica
                    .from_hub(Entry { seq: number as u64 + 1, ..writes[nth].clone() })
                    .unwrap();
            }
            prop_assert_eq!(&state(&hub), &expected);
            prop_assert_eq!(&state(&replica), &expected);
            prop_assert_eq!(hub.since(0).len(), expected.len(), "one numbered entry per key");
            // and the same again after loading what they stored
            prop_assert_eq!(&state(&open(&hub_storage, "hub")), &expected);
            prop_assert_eq!(&state(&open(&replica_storage, "replica")), &expected);
        }
    }

    #[test]
    fn a_store_stamps_after_everything_it_saw(
        steps in vec((any::<bool>(), -2_000i64..2_000, 0..3u32, 0..KEYS), 1..40)
    ) {
        let storage = Memory::default();
        let mut store = open(&storage, "a");
        let mut latest: Option<Hlc> = None;
        for (seq, (write, offset, counter, key)) in (1..).zip(steps) {
            let key = format!("k/{key}");
            let hlc = if write {
                let hlc = store.put(&key, json!(seq), None).unwrap().hlc;
                if let Some(latest) = &latest {
                    prop_assert!(hlc > *latest, "stamped {hlc} after {latest}");
                }
                hlc
            } else {
                let hlc = Hlc {
                    wall_ms: wall_ms().saturating_add_signed(offset),
                    counter,
                    node: "b".into(),
                };
                store
                    .from_hub(Entry { seq, ..Entry::new(key, Some(json!(seq)), hlc.clone()) })
                    .unwrap();
                hlc
            };
            if latest.as_ref().is_none_or(|latest| hlc > *latest) {
                latest = Some(hlc);
            }
        }
        // a store opened again stamps after everything it holds
        let mut again = open(&storage, "a");
        let next = again.put("k/next", json!(0), None).unwrap().hlc;
        prop_assert!(latest.is_none_or(|latest| next > latest));
    }
}
