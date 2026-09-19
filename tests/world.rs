//! A state hashed from what a real run observed, rather than from values a test chose.
//!
//! The unit tests beside the module hold the encoding to account with worlds assembled by hand.
//! This one asks the question those cannot: does a state a **run** produced answer to one name? The
//! world below is filled in by two tasks on the simulated network, stamped with the instants the
//! virtual clock actually reached and addressed to nodes the network actually handed out, so every
//! part of the engine is between the seed and the hash.
//!
//! The comparison across seeds is of **hashes**, never of anything carrying the seed: a form that
//! names its own input differs however little two runs did, and the assertion could not fail.

use core::cell::RefCell;
use core::time::Duration;
use std::rc::Rc;

use chronoloop::clock::Clock;
use chronoloop::executor::Executor;
use chronoloop::net::{Network, VirtualNetwork};
use chronoloop::rng::{Rng, SeededRng};
use chronoloop::world::{Name, Resource, Snapshot, Value, World};

/// How long a node thinks before it says anything.
const THINKING: core::ops::RangeInclusive<Duration> =
    Duration::from_millis(1)..=Duration::from_secs(2);

/// A name, failing the test rather than returning an error no case expects.
fn name(text: &str) -> Name {
    Name::new(text).unwrap_or_else(|e| panic!("a test name is a name: {e}"))
}

/// Runs an exchange under `seed` and returns the world the two nodes were observed to leave behind.
///
/// Every value in it comes from the run: the instants from the virtual clock, the addresses from
/// the network, the delays from the seed.
fn observed(seed: u64) -> World {
    let mut executor = Executor::new();
    let mut seeds = SeededRng::from_seed(seed);
    let network: VirtualNetwork<&'static str, SeededRng> =
        VirtualNetwork::new(executor.handle(), SeededRng::from_seed(seeds.next_u64()));
    let opener = network.add_node();
    let answerer = network.add_node();
    let peer = answerer.id();
    let world = Rc::new(RefCell::new(World::new()));

    let clock = executor.handle();
    let mut rng = SeededRng::from_seed(seeds.next_u64());
    let observing = Rc::clone(&world);
    executor.spawn(async move {
        clock.sleep(rng.duration_in(THINKING.clone())).await;
        opener.send(peer, "ping");
        let sent = clock.now();
        let reply = opener.recv().await;
        let resource = Resource::new()
            .with_field(name("peer"), Value::Text(peer.to_string()))
            .with_field(name("sent"), Value::Instant(sent))
            .with_field(name("heard"), Value::Text((*reply.message()).to_string()))
            .with_field(name("settled"), Value::Instant(clock.now()))
            .with_field(name("waiting"), Value::Flag(false));
        // The borrow ends with the statement that takes it, so nothing holds it across a poll.
        observing.borrow_mut().insert(name("opener"), resource);
    });

    let clock = executor.handle();
    let mut rng = SeededRng::from_seed(seeds.next_u64());
    let observing = Rc::clone(&world);
    executor.spawn(async move {
        let asked = answerer.recv().await;
        let heard = clock.now();
        clock.sleep(rng.duration_in(THINKING.clone())).await;
        answerer.send(asked.sender(), "pong");
        let resource = Resource::new()
            .with_field(name("peer"), Value::Text(asked.sender().to_string()))
            .with_field(name("heard"), Value::Instant(heard))
            .with_field(name("answers"), Value::Count(1))
            .with_field(name("settled"), Value::Instant(clock.now()))
            .with_field(name("waiting"), Value::Flag(false));
        observing.borrow_mut().insert(name("answerer"), resource);
    });

    executor
        .run()
        .unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"));
    Rc::try_unwrap(world)
        .unwrap_or_else(|_| panic!("the tasks are finished and have let the world go"))
        .into_inner()
}

#[test]
fn one_seed_is_observed_as_one_state() {
    for seed in [0, 7, 20_260_919, u64::MAX] {
        assert_eq!(
            observed(seed).state_hash(),
            observed(seed).state_hash(),
            "seed {seed}"
        );
    }
}

#[test]
fn different_seeds_are_observed_as_different_states() {
    struct Case {
        name: &'static str,
        left: u64,
        right: u64,
    }
    // Without this, a change that stopped the seed reaching the world — the delays replaced by
    // constants, the instants left out of the state — would leave every other case here passing.
    let cases = [
        Case {
            name: "adjacent seeds",
            left: 0,
            right: 1,
        },
        Case {
            name: "small seeds",
            left: 7,
            right: 8,
        },
        Case {
            name: "either end of the range",
            left: 42,
            right: u64::MAX,
        },
    ];
    for case in cases {
        assert_ne!(
            observed(case.left).state_hash(),
            observed(case.right).state_hash(),
            "{}",
            case.name
        );
    }
}

#[test]
fn a_state_hash_outlives_the_process_that_produced_it() {
    // Recorded from an actual run and committed, so it is not the same run compared with itself:
    // `make local-validation` builds this twice and holds both profiles against this one text.
    // Every part of the engine is upstream of it, so a change to the encoding, to what the wire
    // draws, or to the order events fire in, moves it.
    assert_eq!(
        observed(7).state_hash().to_string(),
        "79d631b3589020a5fdaedf4721bc1e38a15d6261792415d292845db3ffdde396",
        "the state seed 7 is observed to leave behind"
    );
}

#[test]
fn a_state_read_back_through_its_own_accessors_is_the_same_state() {
    // The world a run leaves behind is inspected through `resources`, `fields` and `get`, so those
    // have to hand back everything the hash was taken over — a field reachable only from inside
    // would make a state that could be named but not read.
    let run = observed(7);
    let mut rebuilt = World::new();
    for (name, resource) in run.resources().rev() {
        let mut copy = Resource::new();
        for (field, value) in resource.fields().rev() {
            assert_eq!(resource.get(field), Some(value));
            copy.insert(field.clone(), value.clone());
        }
        rebuilt.insert(name.clone(), copy);
    }

    assert_eq!(rebuilt.state_hash(), run.state_hash());
    assert_eq!(rebuilt, run);
}
