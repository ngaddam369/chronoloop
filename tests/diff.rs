//! What changed between two steps of a run that actually happened.
//!
//! The unit tests beside the module hold the comparison to account with worlds assembled by hand,
//! one small change at a time. This one asks the question those cannot: over a **run**, where the
//! world is written by nodes talking across the simulated network, does a comparison of two
//! consecutive steps name the one thing that moved and nothing else?
//!
//! The expectation is derived by a different route from the comparison. Every step of this run is
//! one node writing down what it now knows, and the step's own event says which node that was — so
//! the test reads the node out of the message and asserts the paths the comparison names are that
//! node's. Nothing about the world's shape is consulted to work out the answer.
//!
//! Two fields are the point of it. `waiting` is written the same on every round, and `peer` stops
//! changing after a node has heard from its neighbour once, since a ring gives every node the same
//! neighbour forever. Neither may ever appear in a comparison again, and a walk that did not stop
//! where two hashes agree would still not report them — what would report them is a world that
//! folded something of its own into a resource's name, which is the mistake this guards.

use core::cell::RefCell;
use core::time::Duration;
use std::collections::BTreeSet;
use std::rc::Rc;

use chronoloop::clock::{Clock, VirtualTime};
use chronoloop::diff::diff;
use chronoloop::executor::Executor;
use chronoloop::history::Entry;
use chronoloop::net::{Network, VirtualNetwork};
use chronoloop::rng::{Rng, SeededRng};
use chronoloop::store::StateStore;
use chronoloop::trace::{Step, Trace};
use chronoloop::world::{Name, Resource, Snapshot, Value, World};

/// How long a node thinks before it says anything.
const THINKING: core::ops::RangeInclusive<Duration> =
    Duration::from_millis(1)..=Duration::from_secs(2);

/// What the nodes of a run are called, as many of them as a case asks for.
const NODES: [&str; 4] = ["eu-west", "eu-east", "us-east", "ap-south"];

/// The seed every case here runs, since none of them is about a particular seed.
const SEED: u64 = 20_260_919;

/// A name, failing the test rather than returning an error no case expects.
fn name(text: &str) -> Name {
    Name::new(text).unwrap_or_else(|e| panic!("a test name is a name: {e}"))
}

/// An entry, failing the test rather than returning an error no case expects.
fn entry(at: VirtualTime, message: String) -> Entry {
    Entry::new(at, message).unwrap_or_else(|e| panic!("a recorded message is one line: {e}"))
}

/// Runs a ring of `nodes` nodes for `rounds` rounds each, returning what happened and what followed.
///
/// Each node waits, says something to its neighbour, hears from the other one, and writes down what
/// it now knows. One resource of the world changes per step and the rest of it stands, which is the
/// shape a control loop leaves behind — and the shape a comparison of two steps exists to show.
fn observed(seed: u64, nodes: usize, rounds: u64) -> Vec<(Entry, World)> {
    let mut executor = Executor::new();
    let mut seeds = SeededRng::from_seed(seed);
    let network: VirtualNetwork<&'static str, SeededRng> =
        VirtualNetwork::new(executor.handle(), SeededRng::from_seed(seeds.next_u64()));
    let endpoints: Vec<_> = NODES[..nodes].iter().map(|_| network.add_node()).collect();
    let ids: Vec<_> = endpoints.iter().map(Network::id).collect();
    let world = Rc::new(RefCell::new(World::new()));
    let log = Rc::new(RefCell::new(Vec::new()));

    for (index, endpoint) in endpoints.into_iter().enumerate() {
        let neighbour = ids[(index + 1) % ids.len()];
        let called = name(NODES[index]);
        let clock = executor.handle();
        let mut rng = SeededRng::from_seed(seeds.next_u64());
        let observing = Rc::clone(&world);
        let observed = Rc::clone(&log);
        executor.spawn(async move {
            for round in 1..=rounds {
                clock.sleep(rng.duration_in(THINKING.clone())).await;
                endpoint.send(neighbour, "tick");
                let heard = endpoint.recv().await;
                let resource = Resource::new()
                    .with_field(name("peer"), Value::Text(heard.sender().to_string()))
                    .with_field(name("rounds"), Value::Count(round))
                    .with_field(name("heard"), Value::Instant(clock.now()))
                    .with_field(name("waiting"), Value::Flag(false));
                let event = entry(
                    clock.now(),
                    format!("{called} heard from {} in round {round}", heard.sender()),
                );
                // Both borrows end with the statement that takes them, so neither is held across a
                // poll and neither is held while the other is taken.
                let step = {
                    let mut world = observing.borrow_mut();
                    world.insert(called.clone(), resource);
                    world.clone()
                };
                observed.borrow_mut().push((event, step));
            }
        });
    }

    executor
        .run()
        .unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"));
    Rc::try_unwrap(log)
        .unwrap_or_else(|_| panic!("the tasks are finished and have let the log go"))
        .into_inner()
}

/// Traces a run of `seed`, keeping the states it passed through in a store beside the trace.
fn traced(seed: u64, nodes: usize, rounds: u64) -> (Trace, StateStore) {
    let mut store = StateStore::new();
    let steps = observed(seed, nodes, rounds)
        .into_iter()
        .map(|(event, world)| Step::new(event, store.insert(&world.snapshot())))
        .collect();
    (Trace::new(seed, steps), store)
}

/// Returns the node that wrote `step`, read out of what the step says happened.
///
/// This is the different route: the answer comes from the event rather than from anything the
/// comparison looked at.
fn wrote(step: &Step) -> &str {
    step.event()
        .message()
        .split(' ')
        .next()
        .unwrap_or_else(|| panic!("every step of this run opens with the node that wrote it"))
}

/// Returns the state the run was in at `step`, and fails the test if the trace is shorter.
fn state_at(trace: &Trace, step: usize) -> chronoloop::world::StateHash {
    trace
        .at(step)
        .unwrap_or_else(|| panic!("the run reached step {step}"))
        .state()
}

#[test]
fn a_step_of_a_run_changes_the_one_resource_that_wrote_it() {
    let (trace, mut store) = traced(SEED, 4, 5);
    assert_eq!(trace.steps().len(), 20, "four nodes, five rounds each");
    // A trace holds one record per event and so has none for the world before anything happened.
    // The run started in an empty one, and step zero is compared against that.
    let started = store.insert(&World::new().snapshot());

    let mut seen_before = BTreeSet::new();
    for step in 0..trace.steps().len() {
        let who = wrote(
            trace
                .at(step)
                .unwrap_or_else(|| panic!("the run reached step {step}")),
        );
        // The world before the first step is the world the run started in, which no step names.
        let before = if step == 0 {
            started
        } else {
            state_at(&trace, step - 1)
        };
        let changes = diff(&store, before, state_at(&trace, step))
            .unwrap_or_else(|e| panic!("step {step} names a state the store kept: {e}"));

        let first_time = seen_before.insert(who.to_owned());
        let paths: Vec<String> = changes
            .iter()
            .map(|change| change.path().to_string())
            .collect();
        let want: Vec<String> = if first_time {
            // A node that has not spoken before puts a resource in the world, and a whole subtree
            // appearing is one change at the name it appeared under.
            vec![who.to_owned()]
        } else {
            // And afterwards only the two fields that move. `peer` is fixed by the ring and
            // `waiting` is written the same every round, so neither is ever named again.
            vec![format!("{who}.heard"), format!("{who}.rounds")]
        };
        assert_eq!(paths, want, "step {step}, written by {who}");
    }

    assert_eq!(
        seen_before.len(),
        4,
        "every node of the ring wrote at least one step"
    );
}

#[test]
fn two_steps_far_apart_name_every_resource_that_moved_between_them() {
    // The comparison is not only for neighbours. Five rounds apart, every node of the ring has
    // written again, so every one of them is named — and still only in the fields that moved.
    let (trace, store) = traced(SEED, 4, 5);
    let changes = diff(
        &store,
        state_at(&trace, 3),
        state_at(&trace, trace.steps().len() - 1),
    )
    .unwrap_or_else(|e| panic!("both steps name states the store kept: {e}"));

    let paths: Vec<String> = changes
        .iter()
        .map(|change| change.path().to_string())
        .collect();
    let mut want: Vec<String> = NODES
        .iter()
        .flat_map(|node| [format!("{node}.heard"), format!("{node}.rounds")])
        .collect();
    want.sort();
    assert_eq!(paths, want);
}

#[test]
fn a_recorded_comparison_reads_the_same_way_again() {
    // Recorded from an actual run of SEED rather than assembled here, so it outlives the process
    // that wrote it. It fails if anything upstream of the text moves — what the wire draws, the
    // order the executor fires events in, the state encoding, or how a value is shown.
    let (trace, store) = traced(SEED, 2, 2);
    let changes = diff(&store, state_at(&trace, 1), state_at(&trace, 3))
        .unwrap_or_else(|e| panic!("both steps name states the store kept: {e}"));
    assert_eq!(
        changes.to_string(),
        "~ eu-east.heard 1.173105286s -> 2.842439169s\n\
         ~ eu-east.rounds 1 -> 2\n\
         ~ eu-west.heard 1.124312165s -> 2.828616271s\n\
         ~ eu-west.rounds 1 -> 2\n"
    );
}

#[test]
fn two_seeds_change_the_same_keys_and_not_the_same_states() {
    // Which keys move is the system's doing and is the same whatever the seed; what they move to
    // is the seed's. Asserted on the changes rather than on the text, since two runs' text differs
    // in the instants alone and that is not what this is about.
    //
    // What this does *not* check is that the keys are the right keys. A comparison made to report
    // at the resource instead of descending into it leaves this green, because four seeds still
    // name four different things — it is the cases above and the recorded text below that go red.
    // Distinctness at any scale says nothing about where a change is, only that there was one.
    let mut paths = BTreeSet::new();
    let mut reports = BTreeSet::new();
    for seed in [0, 1, 42, u64::MAX] {
        let (trace, store) = traced(seed, 4, 3);
        let changes = diff(&store, state_at(&trace, 4), state_at(&trace, 11))
            .unwrap_or_else(|e| panic!("both steps name states the store kept: {e}"));
        paths.insert(
            changes
                .iter()
                .map(|change| change.path().to_string())
                .collect::<Vec<_>>(),
        );
        reports.insert(changes.to_string());
    }
    assert_eq!(paths.len(), 1, "every seed moves the same keys");
    assert_eq!(
        reports.len(),
        4,
        "and no two of them move them the same way"
    );
}

#[test]
#[ignore = "compares two hundred steps of a run; run it with `make local-validation`"]
fn every_step_of_a_long_run_changes_one_resource() {
    // The claim at a scale where nothing about it can be an accident. A world of four resources of
    // four fields stands for two hundred steps and never once is a comparison of two neighbours
    // more than the two fields that moved.
    let (trace, store) = traced(SEED, 4, 50);
    assert_eq!(trace.steps().len(), 200, "four nodes, fifty rounds each");

    for step in 4..trace.steps().len() {
        let who = wrote(
            trace
                .at(step)
                .unwrap_or_else(|| panic!("the run reached step {step}")),
        );
        let changes = diff(&store, state_at(&trace, step - 1), state_at(&trace, step))
            .unwrap_or_else(|e| panic!("step {step} names a state the store kept: {e}"));
        assert_eq!(
            changes
                .iter()
                .map(|change| change.path().to_string())
                .collect::<Vec<_>>(),
            vec![format!("{who}.heard"), format!("{who}.rounds")],
            "step {step}, written by {who}"
        );
    }

    // The first state against the last: every node moved, and still nothing else did.
    let whole = diff(&store, state_at(&trace, 3), state_at(&trace, 199))
        .unwrap_or_else(|e| panic!("both steps name states the store kept: {e}"));
    assert_eq!(whole.len(), 8, "two fields apiece across four nodes");
}
