//! The merge rule of the shared folder. It has no I/O.
//!
//! The mesh `state` document holds 1 signed slice for each author: `{"<author key>": {"files": …}}`.
//! A slice lists the full view of its author, 1 entry for each path:
//!
//! ```json
//! {"files": {"a.md": {"sha256": "…", "ticket": "…", "clock": 7}, "b.md": {"deleted": true, "clock": 8}}}
//! ```
//!
//! Each path is a last-write-wins register over all slices. The entry with the highest `clock`
//! wins. On a tie, a file beats a tombstone, and between 2 files the larger `sha256` wins. The
//! holder is not part of the rule, so a peer that adopts the winner cannot change the winner.

use std::collections::BTreeMap;

use serde_json::Value;

/// What 1 slice says about 1 path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub clock: u64,
    pub content: Content,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Content {
    File { sha256: String, ticket: String },
    Tombstone,
}

/// The winner of 1 path: the content, and a ticket from each slice that holds the same content at
/// the same clock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Winner {
    pub clock: u64,
    pub sha256: Option<String>,
    pub tickets: Vec<String>,
}

/// Every entry of every slice, grouped by path.
#[derive(Debug, Default)]
pub struct View {
    entries: BTreeMap<String, Vec<Entry>>,
}

impl View {
    /// Read the `state` document. A slice that is not an object, or has no valid `files` map, is
    /// empty. An entry that is not a file or a tombstone is skipped. A missing or invalid clock is 0.
    pub fn parse(document: &Value) -> Self {
        let mut entries: BTreeMap<String, Vec<Entry>> = BTreeMap::new();
        let Some(slices) = document.as_object() else {
            return Self::default();
        };
        for slice in slices.values() {
            let Some(files) = slice.get("files").and_then(Value::as_object) else {
                continue;
            };
            for (path, raw) in files {
                if let Some(entry) = parse_entry(raw) {
                    entries.entry(path.clone()).or_default().push(entry);
                }
            }
        }
        Self { entries }
    }

    pub fn paths(&self) -> impl Iterator<Item = &String> {
        self.entries.keys()
    }

    pub fn winner(&self, path: &str) -> Option<Winner> {
        let entries = self.entries.get(path)?;
        let best = entries.iter().max_by(|a, b| rank(a).cmp(&rank(b)))?;
        let sha256 = match &best.content {
            Content::File { sha256, .. } => Some(sha256.clone()),
            Content::Tombstone => None,
        };
        let tickets = entries
            .iter()
            .filter(|entry| entry.clock == best.clock)
            .filter_map(|entry| match (&entry.content, &sha256) {
                (Content::File { sha256: sha, ticket }, Some(want)) if sha == want => Some(ticket.clone()),
                _ => None,
            })
            .collect();
        Some(Winner { clock: best.clock, sha256, tickets })
    }

    /// The clock for a new local change to `path`: 1 above the highest clock that any slice holds.
    /// `None` if the clock would overflow.
    pub fn next_clock(&self, path: &str) -> Option<u64> {
        let highest = self.entries.get(path).and_then(|entries| entries.iter().map(|entry| entry.clock).max());
        highest.unwrap_or(0).checked_add(1)
    }
}

/// The order of the rule: clock, then a file over a tombstone, then the larger `sha256`.
fn rank(entry: &Entry) -> (u64, bool, &str) {
    match &entry.content {
        Content::File { sha256, .. } => (entry.clock, true, sha256.as_str()),
        Content::Tombstone => (entry.clock, false, ""),
    }
}

fn parse_entry(raw: &Value) -> Option<Entry> {
    let clock = raw.get("clock").and_then(Value::as_u64).unwrap_or(0);
    if raw.get("deleted").and_then(Value::as_bool) == Some(true) {
        return Some(Entry { clock, content: Content::Tombstone });
    }
    let sha256 = raw.get("sha256")?.as_str()?.to_owned();
    let ticket = raw.get("ticket")?.as_str()?.to_owned();
    Some(Entry { clock, content: Content::File { sha256, ticket } })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn file(sha: &str, ticket: &str, clock: u64) -> Value {
        json!({ "sha256": sha, "ticket": ticket, "clock": clock })
    }

    fn tombstone(clock: u64) -> Value {
        json!({ "deleted": true, "clock": clock })
    }

    fn view(slices: Value) -> View {
        View::parse(&slices)
    }

    #[test]
    fn the_highest_clock_wins() {
        let view = view(json!({
            "a": { "files": { "x": file("aa", "ta", 3) } },
            "b": { "files": { "x": file("bb", "tb", 5) } },
        }));
        let winner = view.winner("x").unwrap();
        assert_eq!((winner.clock, winner.sha256.as_deref()), (5, Some("bb")));
        assert_eq!(winner.tickets, ["tb"]);
    }

    #[test]
    fn on_a_tie_between_files_the_larger_sha256_wins_whatever_the_slice_order() {
        for (first, second) in [("a", "b"), ("b", "a")] {
            let view = view(json!({
                first: { "files": { "x": file("11", "t1", 4) } },
                second: { "files": { "x": file("22", "t2", 4) } },
            }));
            assert_eq!(view.winner("x").unwrap().sha256.as_deref(), Some("22"));
        }
    }

    #[test]
    fn on_a_tie_a_file_beats_a_tombstone() {
        let view = view(json!({
            "a": { "files": { "x": tombstone(4) } },
            "b": { "files": { "x": file("11", "t1", 4) } },
        }));
        assert_eq!(view.winner("x").unwrap().sha256.as_deref(), Some("11"));
    }

    #[test]
    fn a_higher_tombstone_deletes() {
        let view = view(json!({
            "a": { "files": { "x": file("11", "t1", 4) } },
            "b": { "files": { "x": tombstone(5) } },
        }));
        let winner = view.winner("x").unwrap();
        assert_eq!((winner.clock, winner.sha256, winner.tickets.len()), (5, None, 0));
    }

    #[test]
    fn an_adopter_adds_a_ticket_and_does_not_change_the_winner() {
        let before = view(json!({ "a": { "files": { "x": file("11", "ta", 4) } } }));
        let after = view(json!({
            "a": { "files": { "x": file("11", "ta", 4) } },
            "b": { "files": { "x": file("11", "tb", 4) } },
        }));
        let (before, after) = (before.winner("x").unwrap(), after.winner("x").unwrap());
        assert_eq!((before.clock, &before.sha256), (after.clock, &after.sha256));
        assert_eq!(after.tickets, ["ta", "tb"]);
    }

    #[test]
    fn a_ticket_of_another_content_or_clock_is_not_offered() {
        let view = view(json!({
            "a": { "files": { "x": file("11", "old", 3) } },
            "b": { "files": { "x": file("22", "same-clock-other-bytes", 4) } },
            "c": { "files": { "x": file("33", "win", 4) } },
        }));
        assert_eq!(view.winner("x").unwrap().tickets, ["win"]);
    }

    #[test]
    fn the_next_clock_is_1_above_the_highest_slice() {
        let view = view(json!({
            "a": { "files": { "x": file("11", "t", 4) } },
            "b": { "files": { "x": tombstone(9) } },
        }));
        assert_eq!(view.next_clock("x"), Some(10));
        assert_eq!(view.next_clock("unknown"), Some(1));
    }

    #[test]
    fn the_next_clock_does_not_overflow() {
        let view = view(json!({ "a": { "files": { "x": file("11", "t", u64::MAX) } } }));
        assert_eq!(view.next_clock("x"), None);
    }

    #[test]
    fn a_missing_clock_counts_as_0_and_bad_input_is_skipped() {
        let view = view(json!({
            "a": { "files": { "x": { "sha256": "11", "ticket": "t" }, "bad": { "sha256": 5 } } },
            "left": null,
            "odd": [1, 2],
        }));
        assert_eq!(view.winner("x").unwrap().clock, 0);
        assert!(view.winner("bad").is_none());
        assert_eq!(view.paths().count(), 1);
    }

    #[test]
    fn a_null_slice_removes_nothing() {
        let view = view(json!({
            "a": { "files": { "x": file("11", "t", 4) } },
            "b": null,
        }));
        assert_eq!(view.winner("x").unwrap().sha256.as_deref(), Some("11"));
    }
}
