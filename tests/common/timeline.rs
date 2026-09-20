//! A run of the ring, a fork of one, and what either costs to keep.
//!
//! `tests/fork.rs` and `tests/time_travel.rs` ask the same two questions of the same system — does a
//! run fork, and does a store hold the timelines' prefix once — the first at a point and the second
//! everywhere. They reached those questions through one set of helpers written out twice, which is
//! two places for an edit meant for both to land in one of them.
//!
//! # Why `#[path]` rather than `common/mod.rs`
//!
//! Six other integration tests write `mod common;`, and an integration test is its own crate: a
//! helper none of that crate's cases calls is dead code, and `-D warnings` makes dead code a failed
//! build. Declaring the ring's helpers from `common/mod.rs` would put them in all six. Reaching this
//! file by its path instead compiles it into the two crates that use it and no others.

use std::collections::BTreeSet;

use chronoloop::fork::{Fork, fork};
use chronoloop::store::StateStore;
use chronoloop::systems::ring;
use chronoloop::trace::Trace;
use chronoloop::world::{Node, StateHash};

/// The seed the recorded cases run, since none of them is about a particular one.
pub const SEED: u64 = 20_260_919;

/// The seed a fork sends a run off to, where a case wants a run that parts from the original.
pub const OTHER: u64 = 99;

/// How many steps a run of the ring takes, which is what a fork point is chosen out of.
pub const STEPS: usize = 20;

/// Runs the ring, failing the test rather than returning an error no case expects.
pub fn run(seed: u64) -> (Trace, StateStore) {
    let (trace, store) =
        ring::run(seed).unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"));
    assert_eq!(trace.steps().len(), STEPS, "a run of seed {seed}");
    (trace, store)
}

/// Runs `trace`'s seed again, drawing from `to` once the run is past the instant of `step`.
///
/// The original is taken rather than re-run, so a case sweeping every step of a run pays for that
/// run once.
pub fn forked(trace: &Trace, step: usize, to: u64) -> (Fork, Trace, StateStore) {
    let at = fork(trace, step, to)
        .unwrap_or_else(|e| panic!("a run of {STEPS} steps reached step {step}: {e}"));
    let (forked, store) = ring::forked(trace.seed(), at)
        .unwrap_or_else(|e| panic!("the fork at step {step} did not finish: {e}"));
    (at, forked, store)
}

/// The tree the store holds under `hash`, failing the test if it holds none.
///
/// Private on purpose: both files reach a state through [`merge`] or [`nodes`], so exporting this
/// would widen the surface for nobody.
fn state(store: &StateStore, hash: StateHash) -> Node {
    store
        .get(hash)
        .unwrap_or_else(|| panic!("a run's store keeps every state the run passed through"))
}

/// Puts every state `trace` names into `store`, reading them out of the one that kept them.
pub fn merge(store: &mut StateStore, trace: &Trace, kept: &StateStore) {
    for step in trace.steps() {
        store.insert(&state(kept, step.state()));
    }
}

/// Every distinct node in the trees of the states `trace` names, walked rather than counted.
///
/// The store's own arithmetic done the other way round: the trees come back out of the run's own
/// store and are hashed again here, so a count checked against this is not one store agreeing with
/// itself.
pub fn nodes(trace: &Trace, kept: &StateStore) -> BTreeSet<StateHash> {
    fn walk(tree: &Node, seen: &mut BTreeSet<StateHash>) {
        seen.insert(tree.state_hash());
        if let Node::Branch(children) = tree {
            for child in children.values() {
                walk(child, seen);
            }
        }
    }

    let mut seen = BTreeSet::new();
    for step in trace.steps() {
        walk(&state(kept, step.state()), &mut seen);
    }
    seen
}
