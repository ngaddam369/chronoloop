//! The two claims Phase 3's state history rests on, at a scale where neither can be an accident.
//!
//! The files beside the modules hold each piece to account on its own: `tests/fork.rs` forks one run
//! of one seed at four of its steps, and `tests/store.rs` counts what one run costs to keep. Both
//! claims are made about *any* run and *any* step, and both were checked at a point. This one asks
//! them at the scale they are made at:
//!
//! - **A fork back to the seed the run already had is the run again** — at every step of every seed
//!   tried, not at four steps of one. It is the cheapest check there is on the whole mechanism, and
//!   the one that fails the loudest: a fork that cannot reproduce its own run has nothing to say
//!   about any other. It holds because changing key keeps the generator's place in the stream, so
//!   nothing here is a special case — which is exactly why it is worth asking everywhere.
//! - **A run and every fork of it cost one prefix between them** — the shape time travel actually
//!   produces. A fork at step *N* re-uses the run's first *N* states outright and the forks re-use
//!   each other's, so twenty-one timelines in one store cost far less than twenty-one stores. That
//!   is asserted as a node **count**, since a store that shared nothing would hand every state back
//!   just as correctly.
//!
//! What each layer holds, and what it does not:
//!
//! - The **cheap cases** run in CI and are a spread of seeds rather than a scale.
//! - The **pinned counts** are recorded from actual runs, so they fail if anything upstream of the
//!   states moves — the encoding, what the wire draws, the order events fire in — and
//!   `make local-validation` holds debug and release against the same numbers.
//! - The **sweeps** are the same checks at a scale where one seed's run cannot be a coincidence.
//!   Note what they do not say: a sweep of the no-op says nothing about where a fork to a
//!   *different* seed parts from the run, which `tests/fork.rs` holds; and a node count says
//!   nothing about the states being the right states, which the pinned traces hold.
//!
//! The two sides of the no-op reach their steps by different routes through [`ring`]: [`ring::run`]
//! draws from a plain generator throughout, [`ring::forked`] from one that asks the clock before
//! every draw and changes key partway through. Neither is the other compared with itself.

use std::collections::BTreeSet;

use chronoloop::fork::{Fork, fork};
use chronoloop::store::StateStore;
use chronoloop::systems::ring;
use chronoloop::trace::Trace;
use chronoloop::world::{Node, StateHash};

/// The seed the recorded cases run, since none of them is about a particular one.
const SEED: u64 = 20_260_919;

/// The seed a fork sends a run off to, where a case wants a run that parts from the original.
const OTHER: u64 = 99;

/// How many steps a run of the ring takes, which is what a fork point is chosen out of.
const STEPS: usize = 20;

/// The spread the cheap cases run, rather than a scale: either end of the range, the two adjacent
/// seeds the crate's distinctness cases use, and the seed every recorded number here comes from.
const SPREAD: [u64; 5] = [0, 1, 42, SEED, u64::MAX];

/// Runs the ring, failing the test rather than returning an error no case expects.
fn run(seed: u64) -> (Trace, StateStore) {
    let (trace, store) =
        ring::run(seed).unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"));
    assert_eq!(trace.steps().len(), STEPS, "a run of seed {seed}");
    (trace, store)
}

/// Runs `trace`'s seed again, drawing from `to` once the run is past the instant of `step`.
///
/// The original is taken rather than re-run, so a case sweeping every step of a run pays for that
/// run once.
fn forked(trace: &Trace, step: usize, to: u64) -> (Fork, Trace, StateStore) {
    let at = fork(trace, step, to)
        .unwrap_or_else(|e| panic!("a run of {STEPS} steps reached step {step}: {e}"));
    let (forked, store) = ring::forked(trace.seed(), at)
        .unwrap_or_else(|e| panic!("the fork at step {step} did not finish: {e}"));
    (at, forked, store)
}

/// The tree the store holds under `hash`, failing the test if it holds none.
fn state(store: &StateStore, hash: StateHash) -> Node {
    store
        .get(hash)
        .unwrap_or_else(|| panic!("a run's store keeps every state the run passed through"))
}

/// Puts every state `trace` names into `store`, reading them out of the one that kept them.
fn merge(store: &mut StateStore, trace: &Trace, kept: &StateStore) {
    for step in trace.steps() {
        store.insert(&state(kept, step.state()));
    }
}

/// Every distinct node in the trees of the states `trace` names, walked rather than counted.
///
/// The store's own arithmetic done the other way round: the trees come back out of the run's own
/// store and are hashed again here, so a count checked against this is not one store agreeing with
/// itself.
fn nodes(trace: &Trace, kept: &StateStore) -> BTreeSet<StateHash> {
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

/// Checks that forking `seed`'s run back to `seed` at every one of its steps leaves it unchanged.
fn no_fork_of_its_own_seed_changes_a_run(seed: u64) {
    let (original, _) = run(seed);
    for step in 0..STEPS {
        let (at, again, _) = forked(&original, step, seed);
        assert_eq!(
            again.steps(),
            original.steps(),
            "seed {seed} forked at step {step} back to itself"
        );
        assert_eq!(
            again.fork(),
            Some(at),
            "seed {seed} forked at step {step} still says it forked"
        );
    }
}

/// What a run of `seed` and a fork of it at every step cost one store, and what they cost apart.
///
/// The first number is the sum of what each of the twenty-one timelines would cost a store of its
/// own, walked out of the trees rather than asked of any store. The second is what the store holding
/// all of them actually costs, which is the thing under test.
fn shared_and_apart(seed: u64) -> (usize, usize) {
    let (original, kept) = run(seed);
    let mut store = StateStore::new();
    let mut walked = BTreeSet::new();
    let mut apart = 0;

    let mut keep = |trace: &Trace, kept: &StateStore, store: &mut StateStore| {
        merge(store, trace, kept);
        let own = nodes(trace, kept);
        apart += own.len();
        walked.extend(own);
    };

    keep(&original, &kept, &mut store);
    for step in 0..STEPS {
        let (_, forked, forked_kept) = forked(&original, step, OTHER);
        keep(&forked, &forked_kept, &mut store);
    }

    // What this is worth, and what it is not. A store keyed on a node's own hash holds the distinct
    // nodes by construction, so this cannot fail while that is how it keys — it is a guard against
    // that being traded for something keyed on where a node hangs, and it is the walk rather than
    // the store that the numbers below are checked against. What holds the sharing is the ratio and
    // the recorded pair.
    assert_eq!(
        store.len(),
        walked.len(),
        "seed {seed}: the store holds the distinct nodes of the twenty-one timelines and nothing \
         besides"
    );
    (apart, store.len())
}

#[test]
fn forking_anywhere_in_a_run_back_to_its_own_seed_is_the_run_again() {
    // What takes this red is anything that moves where the new stream starts: a generator that
    // begins the new key at the head of its stream rather than where it stood, or one handed the
    // fork's seed outright instead of the seed drawn for it.
    //
    // What does *not* is a fork in force from the very first draw. It cannot: when a run forks back
    // to its own seed the key it changes to is the key it is already on, so changing early changes
    // nothing. That case belongs to a fork that goes somewhere else, and
    // `tests/fork.rs::every_step_up_to_the_fork_is_the_step_the_run_took` is where it goes red.
    for seed in SPREAD {
        no_fork_of_its_own_seed_changes_a_run(seed);
    }
}

#[test]
fn a_run_and_every_fork_of_it_cost_one_prefix_between_them() {
    let (apart, shared) = shared_and_apart(SEED);

    assert!(
        shared * 2 < apart,
        "twenty-one timelines cost {apart} nodes in stores of their own and {shared} sharing one"
    );
    // Recorded from an actual run rather than derived here, so a change anywhere upstream of the
    // states says so. `make local-validation` holds both profiles against the same pair.
    assert_eq!(
        (apart, shared),
        (1_470, 475),
        "seed {SEED}, forked at every one of its {STEPS} steps"
    );
}

#[test]
#[ignore = "forks every step of a hundred runs; run it with `make local-validation`"]
fn no_run_of_any_seed_is_changed_by_a_fork_back_to_that_seed() {
    for seed in 0..100 {
        no_fork_of_its_own_seed_changes_a_run(seed);
    }
}

#[test]
#[ignore = "forks every step of fifty runs; run it with `make local-validation`"]
fn every_run_and_its_forks_cost_one_prefix_between_them() {
    // The claim above at a scale where it cannot be a fact about one seed's run. The totals are
    // recorded from an actual sweep, so they hold the content of every one of those stores and not
    // only the ratio — a sweep asserting the ratio alone would stay green with the counts moving.
    //
    // What the two totals are worth is not the same. The first is the ring's shape rather than a
    // fact about any seed: every run of it costs seventy distinct nodes whatever it draws, because
    // every run writes the same resources with the same fields and no two of its steps land on one
    // instant — both checked across these fifty seeds before being written down here. The second is
    // the one the sweep is for.
    let mut apart = 0;
    let mut shared = 0;
    for seed in 0..50 {
        let (seed_apart, seed_shared) = shared_and_apart(seed);
        assert!(
            seed_shared * 2 < seed_apart,
            "seed {seed}: {seed_apart} nodes apart and {seed_shared} sharing one store"
        );
        apart += seed_apart;
        shared += seed_shared;
    }
    assert_eq!(
        (apart, shared),
        (73_500, 20_306),
        "fifty seeds forked everywhere"
    );
}
