//! Who may write to a share. It has no I/O.
//!
//! In `creator` mode the hash is `<mesh id>.<creator key>` and the creator signs each entry of the
//! shared folder. A peer applies an entry only if the signature verifies against the key in the hash.

use anyhow::{Result, anyhow, bail};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use zeroize::Zeroizing;

/// The `--writers` flag of `share`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum Writers {
    #[default]
    Everybody,
    Creator,
}

/// What a peer needs to check an entry. `Creator` carries the bare mesh id and the creator key.
// One `Trust` exists for each share and callers pass it by reference, so the size gap costs nothing.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Trust {
    Everybody,
    Creator { mesh: String, key: VerifyingKey },
}

/// The fields of an entry that the signature covers. The ticket is not one of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Signed<'a> {
    File { path: &'a str, clock: u64, sha256: &'a str, size: u64 },
    Tombstone { path: &'a str, clock: u64 },
}

/// The signing key of the creator of a share. It lives in memory for 1 `share` run.
pub struct CreatorKey(SigningKey);

const ENTRY_DOMAIN: &[u8] = b"agent-knowledge/entry/v1\0";
const KIND_TOMBSTONE: u8 = 0;
const KIND_FILE: u8 = 1;
const KEY_HEX_LEN: usize = 64;
const SIGNATURE_HEX_LEN: usize = 128;

impl CreatorKey {
    pub fn generate() -> Result<Self> {
        let mut secret = Zeroizing::new([0u8; 32]);
        getrandom::fill(secret.as_mut()).map_err(|error| anyhow!("no random bytes for the creator key: {error}"))?;
        Ok(Self(SigningKey::from_bytes(&secret)))
    }

    pub fn public_hex(&self) -> String {
        hex::encode(self.0.verifying_key().to_bytes())
    }

    /// The hex signature of `signed` in the mesh `mesh`.
    pub fn sign(&self, mesh: &str, signed: &Signed) -> String {
        hex::encode(self.0.sign(&message(mesh, signed)).to_bytes())
    }
}

/// `true` if `sig_hex` is a valid signature of `signed` under `trust`. Everybody mode accepts all.
pub fn verify(trust: &Trust, signed: &Signed, sig_hex: &str) -> bool {
    let Trust::Creator { mesh, key } = trust else {
        return true;
    };
    let Some(signature) = parse_signature(sig_hex) else {
        return false;
    };
    key.verify_strict(&message(mesh, signed), &signature).is_ok()
}

/// The hash of a share: the bare mesh id, or `<mesh id>.<creator key>` for a creator share.
pub fn encode_hash(mesh: &str, creator: Option<&CreatorKey>) -> String {
    match creator {
        Some(key) => format!("{mesh}.{}", key.public_hex()),
        None => mesh.to_owned(),
    }
}

/// Split a hash into the bare mesh id and the trust rule. A malformed suffix is an error.
pub fn parse_hash(hash: &str) -> Result<(String, Trust)> {
    let (mesh, suffix) = match hash.split_once('.') {
        Some((mesh, suffix)) => (mesh, Some(suffix)),
        None => (hash, None),
    };
    if mesh.is_empty() {
        bail!("the hash has no mesh id");
    }
    let Some(suffix) = suffix else {
        return Ok((mesh.to_owned(), Trust::Everybody));
    };
    let key = parse_key(suffix).ok_or_else(|| anyhow!("the creator key in the hash is not 64 lowercase hex digits of an Ed25519 key"))?;
    Ok((mesh.to_owned(), Trust::Creator { mesh: mesh.to_owned(), key }))
}

fn parse_key(hex_text: &str) -> Option<VerifyingKey> {
    let bytes: [u8; 32] = decode_lower_hex(hex_text, KEY_HEX_LEN)?.try_into().ok()?;
    VerifyingKey::from_bytes(&bytes).ok()
}

fn parse_signature(hex_text: &str) -> Option<Signature> {
    let bytes: [u8; 64] = decode_lower_hex(hex_text, SIGNATURE_HEX_LEN)?.try_into().ok()?;
    Some(Signature::from_bytes(&bytes))
}

fn decode_lower_hex(text: &str, len: usize) -> Option<Vec<u8>> {
    let is_lower_hex = text.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'));
    if text.len() != len || !is_lower_hex {
        return None;
    }
    hex::decode(text).ok()
}

fn message(mesh: &str, signed: &Signed) -> Vec<u8> {
    let mut bytes = ENTRY_DOMAIN.to_vec();
    push_text(&mut bytes, mesh);
    match signed {
        Signed::File { path, clock, sha256, size } => {
            push_text(&mut bytes, path);
            bytes.extend(clock.to_be_bytes());
            bytes.push(KIND_FILE);
            push_text(&mut bytes, sha256);
            bytes.extend(size.to_be_bytes());
        }
        Signed::Tombstone { path, clock } => {
            push_text(&mut bytes, path);
            bytes.extend(clock.to_be_bytes());
            bytes.push(KIND_TOMBSTONE);
        }
    }
    bytes
}

/// `u32be(len) || text`. A field longer than `u32::MAX` bytes cannot occur in a bounded document.
fn push_text(bytes: &mut Vec<u8>, text: &str) {
    bytes.extend(u32::try_from(text.len()).unwrap_or(u32::MAX).to_be_bytes());
    bytes.extend(text.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    const MESH: &str = "mesh-1";

    fn creator() -> (CreatorKey, Trust) {
        let key = CreatorKey::generate().unwrap();
        let (_, trust) = parse_hash(&encode_hash(MESH, Some(&key))).unwrap();
        (key, trust)
    }

    fn file<'a>(path: &'a str, clock: u64, sha256: &'a str, size: u64) -> Signed<'a> {
        Signed::File { path, clock, sha256, size }
    }

    #[test]
    fn the_signature_of_a_file_verifies() {
        let (key, trust) = creator();
        let signed = file("a.md", 3, "ab12", 10);
        assert!(verify(&trust, &signed, &key.sign(MESH, &signed)));
    }

    #[test]
    fn the_signature_of_a_tombstone_verifies() {
        let (key, trust) = creator();
        let signed = Signed::Tombstone { path: "a.md", clock: 4 };
        assert!(verify(&trust, &signed, &key.sign(MESH, &signed)));
    }

    #[test]
    fn a_changed_field_does_not_verify() {
        let (key, trust) = creator();
        let sig = key.sign(MESH, &file("a.md", 3, "ab12", 10));
        for changed in [file("a.md", 4, "ab12", 10), file("a.md", 3, "cd34", 10), file("a.md", 3, "ab12", 11)] {
            assert!(!verify(&trust, &changed, &sig), "{changed:?}");
        }
    }

    #[test]
    fn a_signature_replayed_under_another_path_or_mesh_does_not_verify() {
        let (key, trust) = creator();
        let signed = file("a.md", 3, "ab12", 10);
        assert!(!verify(&trust, &file("b.md", 3, "ab12", 10), &key.sign(MESH, &signed)));
        assert!(!verify(&trust, &signed, &key.sign("mesh-2", &signed)));
    }

    #[test]
    fn a_file_signature_is_not_a_tombstone_signature() {
        let (key, trust) = creator();
        let sig = key.sign(MESH, &file("a.md", 3, "ab12", 10));
        assert!(!verify(&trust, &Signed::Tombstone { path: "a.md", clock: 3 }, &sig));
    }

    #[test]
    fn a_tombstone_forged_by_another_key_does_not_verify() {
        let (_, trust) = creator();
        let forger = CreatorKey::generate().unwrap();
        let signed = Signed::Tombstone { path: "a.md", clock: 9 };
        assert!(!verify(&trust, &signed, &forger.sign(MESH, &signed)));
    }

    #[test]
    fn a_malformed_signature_does_not_verify() {
        let (key, trust) = creator();
        let signed = file("a.md", 3, "ab12", 10);
        let good = key.sign(MESH, &signed);
        for bad in ["", "zz", &good[..126], &good[2..], &format!("{good}00"), &good.to_uppercase()] {
            assert!(!verify(&trust, &signed, bad), "{bad:?}");
        }
    }

    #[test]
    fn everybody_mode_accepts_any_signature() {
        assert!(verify(&Trust::Everybody, &file("a.md", 1, "ab", 1), ""));
    }

    #[test]
    fn a_creator_hash_round_trips() {
        let key = CreatorKey::generate().unwrap();
        let hash = encode_hash(MESH, Some(&key));
        assert_eq!(hash, format!("{MESH}.{}", key.public_hex()));
        let (mesh, trust) = parse_hash(&hash).unwrap();
        assert_eq!(mesh, MESH);
        let signed = Signed::Tombstone { path: "a.md", clock: 1 };
        assert!(verify(&trust, &signed, &key.sign(MESH, &signed)));
    }

    #[test]
    fn a_bare_mesh_id_is_everybody_mode() {
        assert_eq!(encode_hash(MESH, None), MESH);
        assert_eq!(parse_hash(MESH).unwrap(), (MESH.to_owned(), Trust::Everybody));
    }

    #[test]
    fn a_malformed_suffix_is_an_error() {
        let key = CreatorKey::generate().unwrap().public_hex();
        let off_curve = (0..=u8::MAX)
            .map(|first| {
                let mut bytes = [0u8; 32];
                bytes[0] = first;
                bytes[31] = 0x80 | 0x7f;
                bytes
            })
            .find(|bytes| VerifyingKey::from_bytes(bytes).is_err())
            .map(hex::encode)
            .expect("some 32 bytes are not a curve point");
        let bad = [
            format!("{MESH}."),
            format!("{MESH}.{}", &key[..62]),
            format!("{MESH}.{key}00"),
            format!("{MESH}.{}", key.to_uppercase()),
            format!("{MESH}.{}", "zz".repeat(32)),
            format!("{MESH}.{key}.{key}"),
            format!("{MESH}.{off_curve}"),
            format!(".{key}"),
            String::new(),
        ];
        for hash in bad {
            assert!(parse_hash(&hash).is_err(), "{hash:?}");
        }
    }
}
