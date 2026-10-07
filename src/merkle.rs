//! A two-level Merkle tree over the applied state machine: 16 buckets and a
//! root. Its job is answering "have these three nodes actually converged?" in
//! one number, and if not, narrowing the disagreement to a bucket instead of
//! shipping every key across for comparison.
//!
//! Real systems (Dynamo, Cassandra, Riak) use this to repair replicas. Here the
//! log already does the repairing, so this is the *verifier* — the independent
//! check that Raft did what it claims. An independent check matters: a bug in
//! replication that also computed the check would happily agree with itself.
//!
//! crc32 is not a cryptographic hash. That is fine for detecting the accidental
//! divergence a bug causes, and useless against an adversary deliberately
//! crafting a collision. A real deployment would use SHA-256 here.

use std::collections::BTreeMap;

pub const BUCKETS: usize = 16;

/// Which bucket a key belongs to. Hash-based rather than alphabetical so a
/// sequential workload (`key1`, `key2`, ...) still spreads out.
pub fn bucket_of(key: &str) -> usize {
    (crc32fast::hash(key.as_bytes()) as usize) % BUCKETS
}

/// One hash per bucket. `BTreeMap` iterates in sorted order, so two nodes with
/// the same contents always fold them in the same sequence and get the same
/// number — no sorting step needed here.
pub fn bucket_hashes(kv: &BTreeMap<String, String>) -> [u32; BUCKETS] {
    let mut hashers: Vec<crc32fast::Hasher> =
        (0..BUCKETS).map(|_| crc32fast::Hasher::new()).collect();
    for (key, value) in kv {
        let h = &mut hashers[bucket_of(key)];
        h.update(key.as_bytes());
        h.update(b"=");
        h.update(value.as_bytes());
        h.update(b";");
    }
    let mut out = [0u32; BUCKETS];
    for (i, h) in hashers.into_iter().enumerate() {
        out[i] = h.finalize();
    }
    out
}

/// The single number two nodes compare. Equal roots at equal `last_applied`
/// means converged.
pub fn root(kv: &BTreeMap<String, String>) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    for h in bucket_hashes(kv) {
        hasher.update(&h.to_le_bytes());
    }
    hasher.finalize()
}

/// Which buckets disagree. This is the step that makes a Merkle tree worth the
/// trouble: the answer is usually one or two buckets, so only those keys need
/// to be exchanged to find the actual culprit.
pub fn differing_buckets(a: &[u32; BUCKETS], b: &[u32; BUCKETS]) -> Vec<usize> {
    (0..BUCKETS).filter(|i| a[*i] != b[*i]).collect()
}

/// The keys living in the given buckets, for the final narrowing step.
pub fn keys_in_buckets(kv: &BTreeMap<String, String>, buckets: &[usize]) -> Vec<String> {
    kv.keys()
        .filter(|k| buckets.contains(&bucket_of(k)))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn identical_state_hashes_to_an_identical_root() {
        let a = map(&[("name", "mohit"), ("city", "pune"), ("lang", "rust")]);
        let b = map(&[("lang", "rust"), ("name", "mohit"), ("city", "pune")]);
        assert_eq!(root(&a), root(&b), "insertion order must not matter");
    }

    #[test]
    fn one_changed_value_changes_the_root() {
        let a = map(&[("name", "mohit")]);
        let b = map(&[("name", "rakh")]);
        assert_ne!(root(&a), root(&b));
    }

    #[test]
    fn a_disagreement_is_narrowed_to_the_bucket_holding_the_bad_key() {
        let a = map(&[("name", "mohit"), ("city", "pune"), ("lang", "rust")]);
        let b = map(&[("name", "mohit"), ("city", "mumbai"), ("lang", "rust")]);
        let diff = differing_buckets(&bucket_hashes(&a), &bucket_hashes(&b));
        assert_eq!(diff, vec![bucket_of("city")]);
        assert_eq!(keys_in_buckets(&a, &diff), vec!["city".to_string()]);
    }

    #[test]
    fn a_missing_key_is_detected_not_just_a_changed_one() {
        let a = map(&[("name", "mohit"), ("city", "pune")]);
        let b = map(&[("name", "mohit")]);
        assert_ne!(root(&a), root(&b));
        assert_eq!(
            differing_buckets(&bucket_hashes(&a), &bucket_hashes(&b)),
            vec![bucket_of("city")]
        );
    }

    #[test]
    fn two_empty_state_machines_agree() {
        assert_eq!(root(&BTreeMap::new()), root(&BTreeMap::new()));
        assert!(
            differing_buckets(
                &bucket_hashes(&BTreeMap::new()),
                &bucket_hashes(&BTreeMap::new())
            )
            .is_empty()
        );
    }

    #[test]
    fn a_key_whose_value_moved_to_another_key_does_not_hash_the_same() {
        // Guards the delimiter: without the "=" and ";" separators, {"ab":"c"}
        // and {"a":"bc"} would fold to identical bytes.
        let a = map(&[("ab", "c")]);
        let b = map(&[("a", "bc")]);
        assert_ne!(root(&a), root(&b));
    }
}
