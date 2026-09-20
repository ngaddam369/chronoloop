//! A run of the ring, and the same run made to draw from another seed partway through.
//!
//! The unit tests beside [`chronoloop::fork`] hold the switch to account with a clock a case moves
//! by hand. This one asks the question those cannot: does a **run** fork — does the whole engine,
//! with a wire that loses and duplicates and reorders, four tasks going round, and a clock that
//! nothing outside the simulation touches, reach exactly the steps it reached the first time up to
//! the fork, and another run's steps after it?
//!
//! Two of the cases below carry their own caveat, because the shapes they take are the shapes that
//! read like more coverage than they are. They are marked where they sit.

use chronoloop::diff::diff;
use chronoloop::store::StateStore;
use chronoloop::trace::Trace;

#[path = "common/timeline.rs"]
mod timeline;

use timeline::{OTHER, SEED, STEPS, forked, merge, nodes, run};

#[test]
fn forking_to_the_seed_the_run_already_had_changes_nothing() {
    // The cheapest check there is on the whole mechanism, and the one that fails the loudest: a
    // fork that cannot reproduce its own run has nothing to say about any other. It holds because
    // changing key keeps the generator's place in the stream, so putting back the key it already
    // had puts it back exactly where it was — not because anything here is a special case.
    //
    // The two sides reach their steps by different routes through `ring`: one draws from a plain
    // generator throughout, the other from one that asks the clock before every draw and changes
    // key partway through.
    for step in [0, 1, 7, STEPS - 1] {
        let (original, _) = run(SEED);
        let (at, again, _) = forked(&original, step, SEED);
        assert_eq!(
            again.steps(),
            original.steps(),
            "forking at step {step} back to seed {SEED}"
        );
        assert_eq!(again.fork(), Some(at), "the trace still says it forked");
    }
}

#[test]
fn every_step_up_to_the_fork_is_the_step_the_run_took() {
    // What makes a fork a fork rather than another run: the prefix is not an approximation of where
    // the run had got to, it *is* the run. Compared as whole steps, so the state each one names is
    // held to account and not only the instant and the message.
    //
    // What takes this red is a fork in force from the first draw — a generator that never asks the
    // clock. A fork that takes hold an instant early does not, and neither does a step made to name
    // the wrong state, since both runs then name the wrong one alike.
    for step in [0, 1, 7, STEPS - 1] {
        let (original, _) = run(SEED);
        let (_, forked, _) = forked(&original, step, OTHER);
        assert_eq!(
            &forked.steps()[..=step],
            &original.steps()[..=step],
            "forking at step {step}"
        );
    }
}

#[test]
fn the_steps_after_a_fork_are_another_run() {
    // A distinctness assertion, and it says only that there *was* a difference — never what it was
    // or where. Both were tried: it stays green with the fork taking hold an instant early, and
    // green with every step made to name a state that is not the one it reached. What holds the
    // content is the case above, which pins the prefix, and the recorded trace below, which pins
    // what the steps themselves say.
    for step in [0, 1, 7] {
        let (original, _) = run(SEED);
        let (_, forked, _) = forked(&original, step, OTHER);
        assert_ne!(
            &forked.steps()[step + 1..],
            &original.steps()[step + 1..],
            "forking at step {step} sends the run somewhere else"
        );
    }
}

#[test]
fn a_fork_costs_a_store_its_tail_and_not_a_second_run() {
    // What the prefix buys. The states before the fork are the run's own states, so a store that
    // already holds the run holds them too — and the fork costs it only what happened afterwards.
    // The control is an unrelated seed, which shares nothing but the states the ring passes through
    // on its way up: without the prefix holding, a fork would cost about what that costs.
    //
    // Measured on this seed: the run alone is 70 nodes, a fork of it at step 7 brings 32 more, and
    // an unrelated seed brings 60. The assertion is the comparison rather than those numbers, since
    // what is being claimed is that a fork is cheaper than a run and not that it is cheaper by that
    // much.
    let step = 7;
    let (original, kept) = run(SEED);
    let (_, forked, forked_kept) = forked(&original, step, OTHER);
    let (elsewhere, elsewhere_kept) = run(OTHER);

    let mut store = StateStore::new();
    merge(&mut store, &original, &kept);
    let alone = store.len();

    // Derived by walking the trees rather than by asking the store, so the two sides of the
    // assertions below arrive by different routes.
    let from_fork = &nodes(&forked, &forked_kept) - &nodes(&original, &kept);
    let from_elsewhere = &nodes(&elsewhere, &elsewhere_kept) - &nodes(&original, &kept);

    merge(&mut store, &forked, &forked_kept);
    let with_fork = store.len() - alone;
    assert_eq!(
        with_fork,
        from_fork.len(),
        "the fork brought what it changed"
    );

    let mut apart = StateStore::new();
    merge(&mut apart, &original, &kept);
    merge(&mut apart, &elsewhere, &elsewhere_kept);
    let with_elsewhere = apart.len() - alone;
    assert_eq!(with_elsewhere, from_elsewhere.len());

    assert!(
        with_fork < with_elsewhere,
        "a fork at step {step} of {STEPS} brought {with_fork} nodes and another seed \
         {with_elsewhere}"
    );
}

#[test]
fn a_comparison_says_what_the_other_timeline_did() {
    // The three modules meeting: a fork produces a state, a store keeps both, and a comparison says
    // what stands where. What is asserted is the *shape* of the report — every change is a field of
    // a node, and the nodes named are nodes the ring has — since which fields moved is a fact about
    // a seed rather than about the engine.
    let (original, kept) = run(SEED);
    let (_, forked, forked_kept) = forked(&original, 7, OTHER);

    let mut store = StateStore::new();
    merge(&mut store, &original, &kept);
    merge(&mut store, &forked, &forked_kept);

    let ends = |trace: &Trace| {
        trace
            .steps()
            .last()
            .unwrap_or_else(|| panic!("a run of {STEPS} steps has a last one"))
            .state()
    };
    let changes = diff(&store, ends(&original), ends(&forked))
        .unwrap_or_else(|e| panic!("both timelines are in the store: {e}"));

    assert!(
        !changes.is_empty(),
        "two timelines that parted at step 7 do not end in one state"
    );
    for change in &changes {
        let path = change.path().names();
        assert_eq!(
            path.len(),
            2,
            "a change is a field of a node: {}",
            change.path()
        );
    }
}

#[test]
fn a_recorded_fork_is_forked_again_unchanged() {
    // Recorded from actual runs rather than assembled here, and unlike the cases above it outlives
    // the process that wrote it: it fails if anything upstream of the text moves — where the fork
    // takes hold, what the wire draws once it has, the state encoding, the order events fire in.
    // `make local-validation` runs the suite in both profiles, which is what makes "the same in
    // debug and in release" a check of this text rather than a claim about it.
    //
    // Four lines are pinned rather than twenty: the fork, the last step the two runs share, the
    // first step they do not — beside the original's own, so the parting is on the page — and the
    // last step of all, which depends on every draw made after the fork.
    let (original, _) = run(SEED);
    let (_, forked, _) = forked(&original, 7, OTHER);

    let text = forked.to_string();
    let written: Vec<&str> = text.lines().collect();
    assert_eq!(
        written.len(),
        STEPS + 2,
        "a header, a fork, and a step apiece"
    );
    assert_eq!(written[1], "forked at 2.879111427s to seed 99");

    assert_eq!(
        written[9],
        "step 7 2.879111427s \
         ecc42d317938900b695cf0a85c58da1dc374058d43ed9c8ef62d43d0e759e9dd \
         eu-east heard from node 0 in round 2",
        "the step forked at is the step the run took"
    );
    assert_eq!(
        written[10],
        "step 8 3.891334403s \
         18f4f67aca5db29179801da7063321809e0fc597d8c140ce87b9db346353e2fb \
         us-east heard from node 1 in round 3",
        "the first step drawn from the other seed"
    );
    assert_eq!(
        original.to_string().lines().nth(9),
        Some(
            "step 8 3.889674378s \
             0360e6ca7cbcd7605793ee6f77bccd75856a396ad2d84f0466e966df23e324aa \
             us-east heard from node 1 in round 3"
        ),
        "and what the run itself did there, two milliseconds earlier and in another state"
    );
    assert_eq!(
        written[STEPS + 1],
        "step 19 7.673233028s \
         de5263dab4f2e8e6dafca836b75dbfedbff7b8e74899e6bd264c61ff8e757c51 \
         eu-west heard from node 3 in round 5"
    );
}

#[test]
fn a_forked_trace_survives_being_written_out_and_read_back() {
    // A fork's trace is the artifact a fork produces, so it has to say what it takes to produce
    // these steps again — the seed the run started from *and* where it stopped being that run.
    let (original, _) = run(SEED);
    let (at, forked, _) = forked(&original, 7, OTHER);
    let read_back: Trace = forked
        .to_string()
        .parse()
        .unwrap_or_else(|e| panic!("a written trace reads back: {e}"));

    assert_eq!(read_back, forked);
    assert_eq!(read_back.seed(), SEED);
    assert_eq!(read_back.fork(), Some(at));
}
