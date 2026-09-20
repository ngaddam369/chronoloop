//! A trace of a run that actually happened, written out and read back.
//!
//! The unit tests beside the module hold the written form to account with steps assembled by hand.
//! This one asks the question those cannot: does a trace of a **run** survive the trip to a file and
//! back, and are the states it names states the store can still produce? The steps below are written
//! by nodes talking over the simulated network — the instants from the virtual clock, the delays from
//! the seed, the addresses from the network — so every part of the engine sits between the seed and
//! the text.
//!
//! The states are put in a [`StateStore`] on the way past and looked up again out of the trace that
//! came back from the text, so the hash doing the looking up arrived by a different route from the
//! one `insert` handed back.
//!
//! [`StateStore`]: chronoloop::store::StateStore

use core::cell::RefCell;
use core::time::Duration;
use std::rc::Rc;

use chronoloop::clock::{Clock, VirtualTime};
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
/// shape a control loop leaves behind — and each change is paired with the entry describing it, since
/// a step of a trace is one event and the state it left the run in.
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

#[test]
fn a_trace_of_a_run_survives_being_written_out_and_read_back() {
    let (trace, _) = traced(SEED, 4, 5);
    assert_eq!(trace.steps().len(), 20, "four nodes, five rounds each");
    assert_eq!(trace.to_string().parse(), Ok(trace));
}

#[test]
fn every_state_a_written_trace_names_comes_back_out_of_the_store() {
    // The point of the form: a trace read out of a file is an index into the states a run passed
    // through. The hashes doing the looking up here have been through the text and back, so they are
    // not the ones `insert` handed over.
    let (trace, store) = traced(SEED, 4, 5);
    let read_back: Trace = trace
        .to_string()
        .parse()
        .unwrap_or_else(|e| panic!("a written trace reads back: {e}"));

    for (number, step) in read_back.steps().iter().enumerate() {
        let state = store
            .get(step.state())
            .unwrap_or_else(|| panic!("step {number} names a state the store never saw"));
        assert_eq!(
            state.state_hash(),
            step.state(),
            "the state step {number} names answers to that name"
        );
    }
}

#[test]
fn a_recorded_trace_is_traced_again_unchanged() {
    // Recorded from an actual run of SEED rather than assembled here. Unlike the round trip above,
    // this outlives the process that wrote it: it fails if anything upstream of the text moves —
    // the state encoding, what the wire draws, the order the executor fires events in — and
    // `make local-validation` holds both profiles against this one text.
    let (trace, _) = traced(SEED, 2, 2);
    assert_eq!(
        trace.to_string(),
        "chronoloop trace seed 20260919\n\
         step 0 1.124312165s \
         74ca32fb603910a5fbb50097878bb58e155ad84f9daba6af393a351d8171d7c9 \
         eu-west heard from node 1 in round 1\n\
         step 1 1.173105286s \
         ddfd9d7189b97caab45703e747b3b7e9249b14d376b7b5ee3dcb49c0f3794a33 \
         eu-east heard from node 0 in round 1\n\
         step 2 2.828616271s \
         6a726f5699e0489330377b57ec2990fbd990f20bb6bd276fc67b3d974e5b919e \
         eu-west heard from node 1 in round 2\n\
         step 3 2.842439169s \
         b0873ab87bd0e56bc6953b9126c369ed5180d6cf66cc54f4d50afa921f31faea \
         eu-east heard from node 0 in round 2\n"
    );
}

#[test]
fn two_seeds_trace_two_different_runs() {
    // Compared on the steps and never on the written form: a trace's header names its seed, so the
    // text of two runs differs however little the runs themselves did — and the assertion could not
    // fail even with the engine cut off from its seed entirely.
    //
    // What this does *not* check is that the states a trace names are the states the run reached. A
    // step made to carry the wrong hash altogether leaves this green, because the instants and the
    // messages alone keep two runs' steps apart. The pinned text above and the walk back through the
    // store are what hold that, and they were the cases that went red when it was tried.
    struct Case {
        name: &'static str,
        left: u64,
        right: u64,
    }
    let cases = [
        Case {
            name: "adjacent seeds",
            left: 0,
            right: 1,
        },
        Case {
            name: "either end of the range",
            left: 42,
            right: u64::MAX,
        },
    ];
    for case in cases {
        let (left, _) = traced(case.left, 4, 3);
        let (right, _) = traced(case.right, 4, 3);
        assert_ne!(left.steps(), right.steps(), "{}", case.name);
    }
}

#[test]
#[ignore = "traces two hundred steps; run it with `make local-validation`"]
fn a_long_run_is_traced_step_for_step() {
    // The claim at a scale where nothing about it can be an accident: every step of a long run is
    // numbered, written, read back, and found in the store under the name the text carried.
    let (trace, store) = traced(SEED, 4, 50);
    assert_eq!(trace.steps().len(), 200, "four nodes, fifty rounds each");

    let read_back: Trace = trace
        .to_string()
        .parse()
        .unwrap_or_else(|e| panic!("a written trace reads back: {e}"));
    assert_eq!(read_back, trace);
    for (number, step) in read_back.steps().iter().enumerate() {
        assert!(
            store.get(step.state()).is_some(),
            "step {number} names a state the store never saw"
        );
    }

    // Recorded from an actual run, like the text pinned above. The last state of a long run depends
    // on every draw that came before it, so this is the cheapest way to hold a whole run to account
    // without pinning two hundred lines of it.
    let last = read_back
        .steps()
        .last()
        .unwrap_or_else(|| panic!("a run of two hundred steps has a last one"));
    assert_eq!(
        last.to_string(),
        "70.275277230s \
         436b3b4a03da09e792f938cd38149f154d91aa0e4eccbe58dc13e8e0aadec6f9 \
         ap-south heard from node 2 in round 50"
    );
}
