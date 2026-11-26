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

use crate::writers::{Signed, Trust, verify};

/// What 1 slice says about 1 path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub clock: u64,
    pub sig: Option<String>,
    pub content: Content,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Content {
    File { sha256: String, ticket: String, size: Option<u64> },
    Tombstone,
}

/// The winner of 1 path: the content, and a ticket from each slice that holds the same bytes, at
/// any clock. A peer with the same bytes is a source whatever its clock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Winner {
    pub clock: u64,
    pub sha256: Option<String>,
    pub tickets: Vec<String>,
    pub sig: Option<String>,
    pub size: Option<u64>,
}

/// Every entry of every slice, grouped by path.
#[derive(Debug, Default)]
pub struct View {
    entries: BTreeMap<String, Vec<Entry>>,
}

impl View {
    /// Read the `state` document. A slice that is not an object, or has no valid `files` map, is
    /// empty. An entry that is not a file or a tombstone is skipped. A missing or invalid clock is 0.
    /// In creator mode, `trust` drops every entry that the creator did not sign.
    pub fn parse(document: &Value, trust: &Trust) -> Self {
        let mut entries: BTreeMap<String, Vec<Entry>> = BTreeMap::new();
        let Some(slices) = document.as_object() else {
            return Self::default();
        };
        for slice in slices.values() {
            let Some(files) = slice.get("files").and_then(Value::as_object) else {
                continue;
            };
            for (path, raw) in files {
                if let Some(entry) = parse_entry(raw).filter(|entry| is_trusted(trust, path, raw, entry)) {
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
        let (sha256, size) = match &best.content {
            Content::File { sha256, size, .. } => (Some(sha256.clone()), *size),
            Content::Tombstone => (None, None),
        };
        let tickets = entries
            .iter()
            .filter_map(|entry| match (&entry.content, &sha256) {
                (Content::File { sha256: sha, ticket, .. }, Some(want)) if sha == want => Some(ticket.clone()),
                _ => None,
            })
            .collect();
        Some(Winner { clock: best.clock, sha256, tickets, sig: best.sig.clone(), size })
    }

    /// The clock for a new local change to `path`: 1 above the highest clock that any slice holds,
    /// and above `own`, the clock that this peer last wrote or saw for the path. A peer that
    /// starts again can see an empty view, and `own` keeps its clock from falling behind.
    /// `None` if the clock would overflow.
    pub fn next_clock(&self, path: &str, own: u64) -> Option<u64> {
        let highest = self.entries.get(path).and_then(|entries| entries.iter().map(|entry| entry.clock).max());
        highest.unwrap_or(0).max(own).checked_add(1)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// The order of the rule: clock, then a file over a tombstone, then the larger `sha256`.
fn rank(entry: &Entry) -> (u64, bool, &str) {
    match &entry.content {
        Content::File { sha256, .. } => (entry.clock, true, sha256.as_str()),
        Content::Tombstone => (entry.clock, false, ""),
    }
}

/// In creator mode an entry must carry a clock, a signature, and for a file a size, and the
/// signature must verify over those parsed values. Everybody mode trusts every entry.
fn is_trusted(trust: &Trust, path: &str, raw: &Value, entry: &Entry) -> bool {
    if *trust == Trust::Everybody {
        return true;
    }
    let (Some(_), Some(sig)) = (raw.get("clock").and_then(Value::as_u64), entry.sig.as_deref()) else {
        return false;
    };
    let signed = match &entry.content {
        Content::File { sha256, size: Some(size), .. } => Signed::File { path, clock: entry.clock, sha256, size: *size },
        Content::File { size: None, .. } => return false,
        Content::Tombstone => Signed::Tombstone { path, clock: entry.clock },
    };
    verify(trust, &signed, sig)
}

fn parse_entry(raw: &Value) -> Option<Entry> {
    let clock = raw.get("clock").and_then(Value::as_u64).unwrap_or(0);
    let sig = raw.get("sig").and_then(Value::as_str).map(str::to_owned);
    if raw.get("deleted").and_then(Value::as_bool) == Some(true) {
        return Some(Entry { clock, sig, content: Content::Tombstone });
    }
    let sha256 = raw.get("sha256")?.as_str()?.to_owned();
    let ticket = raw.get("ticket")?.as_str()?.to_owned();
    let size = raw.get("size").and_then(Value::as_u64);
    Some(Entry { clock, sig, content: Content::File { sha256, ticket, size } })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::writers::{CreatorKey, encode_hash, parse_hash};

    fn file(sha: &str, ticket: &str, clock: u64) -> Value {
        json!({ "sha256": sha, "ticket": ticket, "clock": clock })
    }

    fn tombstone(clock: u64) -> Value {
        json!({ "deleted": true, "clock": clock })
    }

    fn view(slices: Value) -> View {
        View::parse(&slices, &Trust::Everybody)
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
    fn a_ticket_of_the_same_bytes_is_offered_at_any_clock_and_other_bytes_are_not() {
        let view = view(json!({
            "a": { "files": { "x": file("33", "older-same-bytes", 3) } },
            "b": { "files": { "x": file("22", "same-clock-other-bytes", 4) } },
            "c": { "files": { "x": file("33", "win", 4) } },
        }));
        assert_eq!(view.winner("x").unwrap().tickets, ["older-same-bytes", "win"]);
    }

    #[test]
    fn the_next_clock_is_1_above_the_highest_slice() {
        let view = view(json!({
            "a": { "files": { "x": file("11", "t", 4) } },
            "b": { "files": { "x": tombstone(9) } },
        }));
        assert_eq!(view.next_clock("x", 0), Some(10));
        assert_eq!(view.next_clock("unknown", 0), Some(1));
    }

    #[test]
    fn the_next_clock_is_above_the_own_clock_when_the_view_is_behind() {
        let empty = View::default();
        assert_eq!(empty.next_clock("x", 3), Some(4));
        let behind = view(json!({ "a": { "files": { "x": file("11", "t", 1) } } }));
        assert_eq!(behind.next_clock("x", 3), Some(4));
    }

    #[test]
    fn the_next_clock_does_not_overflow() {
        let view = view(json!({ "a": { "files": { "x": file("11", "t", u64::MAX) } } }));
        assert_eq!(view.next_clock("x", 0), None);
        assert_eq!(View::default().next_clock("x", u64::MAX), None);
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

    const MESH: &str = "mesh-1";

    fn signed_file(key: &CreatorKey, path: &str, clock: u64, sha: &str, size: u64, ticket: &str) -> Value {
        let sig = key.sign(MESH, &Signed::File { path, clock, sha256: sha, size });
        json!({ "sha256": sha, "ticket": ticket, "clock": clock, "size": size, "sig": sig })
    }

    fn signed_tombstone(key: &CreatorKey, path: &str, clock: u64) -> Value {
        let sig = key.sign(MESH, &Signed::Tombstone { path, clock });
        json!({ "deleted": true, "clock": clock, "sig": sig })
    }

    fn creator_trust(key: &CreatorKey) -> Trust {
        parse_hash(&encode_hash(MESH, Some(key))).unwrap().1
    }

    #[test]
    fn creator_mode_keeps_a_signed_file_and_a_signed_tombstone() {
        let key = CreatorKey::generate().unwrap();
        let document = json!({ "c": { "files": {
            "x": signed_file(&key, "x", 2, "11", 5, "t"),
            "y": signed_tombstone(&key, "y", 3),
        } } });
        let view = View::parse(&document, &creator_trust(&key));
        let winner = view.winner("x").unwrap();
        assert_eq!((winner.clock, winner.sha256.as_deref(), winner.size), (2, Some("11"), Some(5)));
        assert!(winner.sig.is_some());
        assert_eq!(view.winner("y").unwrap().sha256, None);
    }

    #[test]
    fn creator_mode_drops_an_entry_with_a_bad_or_missing_signature() {
        let key = CreatorKey::generate().unwrap();
        let forger = CreatorKey::generate().unwrap();
        let mut no_size = signed_file(&key, "nosize", 2, "11", 5, "t");
        no_size.as_object_mut().unwrap().remove("size");
        let mut no_clock = signed_file(&key, "noclock", 0, "11", 5, "t");
        no_clock.as_object_mut().unwrap().remove("clock");
        let mut no_sig = signed_file(&key, "nosig", 2, "11", 5, "t");
        no_sig.as_object_mut().unwrap().remove("sig");
        let document = json!({ "m": { "files": {
            "forged": signed_file(&forger, "forged", 2, "11", 5, "t"),
            "tampered": { "sha256": "22", "ticket": "t", "clock": 2, "size": 5,
                          "sig": key.sign(MESH, &Signed::File { path: "tampered", clock: 2, sha256: "11", size: 5 }) },
            "nosig": no_sig,
            "nosize": no_size,
            "noclock": no_clock,
            "forged-tombstone": signed_tombstone(&forger, "forged-tombstone", 9),
            "plain-tombstone": { "deleted": true, "clock": 9 },
            "good": signed_file(&key, "good", 1, "11", 5, "t"),
        } } });
        let view = View::parse(&document, &creator_trust(&key));
        assert_eq!(view.paths().collect::<Vec<_>>(), ["good"]);
    }

    #[test]
    fn creator_mode_accepts_a_copied_entry_with_another_ticket() {
        let key = CreatorKey::generate().unwrap();
        let original = signed_file(&key, "x", 2, "11", 5, "creator-ticket");
        let mut copy = original.clone();
        copy["ticket"] = json!("mounter-ticket");
        let document = json!({ "c": { "files": { "x": original } }, "m": { "files": { "x": copy } } });
        let winner = View::parse(&document, &creator_trust(&key)).winner("x").unwrap();
        assert_eq!(winner.tickets, ["creator-ticket", "mounter-ticket"]);
    }

    #[test]
    fn creator_mode_rejects_an_entry_replayed_under_another_path() {
        let key = CreatorKey::generate().unwrap();
        let document = json!({ "m": { "files": { "other": signed_file(&key, "x", 2, "11", 5, "t") } } });
        assert!(View::parse(&document, &creator_trust(&key)).is_empty());
    }

    #[test]
    fn creator_mode_rejects_an_entry_replayed_from_another_mesh() {
        let key = CreatorKey::generate().unwrap();
        let sig = key.sign("mesh-2", &Signed::File { path: "x", clock: 2, sha256: "11", size: 5 });
        let document = json!({ "m": { "files": { "x": { "sha256": "11", "ticket": "t", "clock": 2, "size": 5, "sig": sig } } } });
        assert!(View::parse(&document, &creator_trust(&key)).is_empty());
    }

    #[test]
    fn the_next_clock_ignores_an_unsigned_entry_in_creator_mode() {
        let key = CreatorKey::generate().unwrap();
        let document = json!({
            "c": { "files": { "x": signed_file(&key, "x", 2, "11", 5, "t") } },
            "m": { "files": { "x": { "sha256": "99", "ticket": "t", "clock": 900 } } },
        });
        let view = View::parse(&document, &creator_trust(&key));
        assert_eq!(view.next_clock("x", 0), Some(3));
        assert_eq!(View::parse(&document, &Trust::Everybody).next_clock("x", 0), Some(901));
    }

    #[test]
    fn everybody_mode_ignores_the_signature_and_carries_it_to_the_winner() {
        let document = json!({ "a": { "files": { "x": { "sha256": "11", "ticket": "t", "clock": 1, "size": 5, "sig": "junk" } } } });
        let winner = View::parse(&document, &Trust::Everybody).winner("x").unwrap();
        assert_eq!((winner.sig.as_deref(), winner.size), (Some("junk"), Some(5)));
    }
}
