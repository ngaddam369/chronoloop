//! What a run actually costs to keep, measured on states a run actually produced.
//!
//! The unit tests beside the module hold the sharing to account with worlds assembled by hand, a
//! step at a time. This one asks the question those cannot: a **run** goes through many states, each
//! one a small change to the last — does keeping all of them cost what changed, or does it cost a
//! world per step? The states below are written by four nodes talking over the simulated network,
//! stamped with the instants the virtual clock reached and addressed to nodes the network handed
//! out, so the seed is upstream of every part of what is stored.
//!
//! The expected node count is derived by a different route from the store's own: the test walks
//! every step's tree itself and collects the hashes it finds, so the two sides of the assertion are
//! not the store agreeing with itself.

use core::cell::RefCell;
use core::time::Duration;
use std::collections::BTreeSet;
use std::rc::Rc;

use chronoloop::clock::Clock;
use chronoloop::executor::Executor;
use chronoloop::net::{Network, VirtualNetwork};
use chronoloop::rng::{Rng, SeededRng};
use chronoloop::store::StateStore;
use chronoloop::world::{Name, Node, Resource, Snapshot, StateHash, Value, World};

/// How long a node thinks before it says anything.
const THINKING: core::ops::RangeInclusive<Duration> =
    Duration::from_millis(1)..=Duration::from_secs(2);

/// What the nodes of the run are called, one apiece.
const NODES: [&str; 4] = ["eu-west", "eu-east", "us-east", "ap-south"];

/// The seed every case here runs, since none of them is about a particular seed.
const SEED: u64 = 20_260_919;

/// A name, failing the test rather than returning an error no case expects.
fn name(text: &str) -> Name {
    Name::new(text).unwrap_or_else(|e| panic!("a test name is a name: {e}"))
}

/// Runs a ring of nodes for `rounds` rounds each and returns the state after every change.
///
/// Each node waits, says something to its neighbour, hears from the other one, and writes down what
/// it now knows — so one resource of the world changes per step and the rest of it stands. That is
/// the shape a control loop leaves behind, and the shape the store exists to keep cheaply.
fn observed(seed: u64, rounds: u64) -> Vec<World> {
    let mut executor = Executor::new();
    let mut seeds = SeededRng::from_seed(seed);
    let network: VirtualNetwork<&'static str, SeededRng> =
        VirtualNetwork::new(executor.handle(), SeededRng::from_seed(seeds.next_u64()));
    let endpoints: Vec<_> = NODES.iter().map(|_| network.add_node()).collect();
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
                // Both borrows end with the statement that takes them, so neither is held across a
                // poll and neither is held while the other is taken.
                let step = {
                    let mut world = observing.borrow_mut();
                    world.insert(called.clone(), resource);
                    world.clone()
                };
                observed.borrow_mut().push(step);
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

/// Counts every node of `tree`, repeats and all, and puts each one's name in `seen`.
///
/// This is the store's own arithmetic done the other way round: the walk is here, over the trees as
/// they came back from the run, rather than over anything the store built.
fn walk(tree: &Node, seen: &mut BTreeSet<StateHash>) -> usize {
    seen.insert(tree.state_hash());
    match tree {
        Node::Leaf(_) => 1,
        Node::Branch(children) => {
            1 + children
                .values()
                .map(|child| walk(child, seen))
                .sum::<usize>()
        }
    }
}

/// What a run's states are made of: how many nodes in total, and how many of them are distinct.
fn cost(steps: &[World]) -> (usize, usize) {
    let mut distinct = BTreeSet::new();
    let whole = steps
        .iter()
        .map(|step| walk(&step.snapshot(), &mut distinct))
        .sum();
    (whole, distinct.len())
}

#[test]
fn every_step_of_a_run_comes_back_out_of_the_store() {
    let steps = observed(SEED, 5);
    assert_eq!(steps.len(), NODES.len() * 5, "four nodes, five rounds each");

    let mut store = StateStore::new();
    let names: Vec<_> = steps
        .iter()
        .map(|step| store.insert(&step.snapshot()))
        .collect();

    for (step, hash) in steps.iter().zip(names) {
        assert_eq!(
            store.get(hash),
            Some(step.snapshot()),
            "the state stored under {hash}"
        );
    }
}

#[test]
fn a_run_costs_what_it_changed_rather_than_what_it_kept() {
    let steps = observed(SEED, 5);
    let (whole, distinct) = cost(&steps);

    let mut store = StateStore::new();
    for step in &steps {
        store.insert(&step.snapshot());
    }

    assert_eq!(
        store.len(),
        distinct,
        "the store holds the distinct nodes of the run and nothing besides"
    );
    assert!(
        store.len() * 2 < whole,
        "keeping every step whole costs {whole} nodes and the store costs {}",
        store.len()
    );
    // Recorded from an actual run rather than derived here, so a change anywhere upstream of the
    // states — the encoding, what the wire draws, the order events fire in — moves them and says
    // so. `make local-validation` holds both profiles against the same pair.
    assert_eq!((whole, store.len()), (390, 70), "seed {SEED} over 20 steps");
}

#[test]
#[ignore = "runs a thousand steps; run it with `make local-validation`"]
fn a_thousand_steps_do_not_cost_a_thousand_worlds() {
    // The claim the store exists for, at the scale it was made about. Twenty steps could be argued
    // to share by accident; a thousand of them cannot.
    let steps = observed(SEED, 250);
    assert_eq!(
        steps.len(),
        1000,
        "four nodes, two hundred and fifty rounds"
    );
    let (whole, distinct) = cost(&steps);

    let mut store = StateStore::new();
    for step in &steps {
        store.insert(&step.snapshot());
    }

    assert_eq!(store.len(), distinct);
    // Recorded from an actual run, like the pair in the case above. The whole is a world of four
    // resources of four fields — twenty-one nodes — a thousand times over, less the thirty missing
    // from the first three steps, before every node had reported for the first time. What the store
    // costs against that is a little over three nodes a step: the field that changed, the resource
    // holding it, and the world holding that.
    assert_eq!(
        (whole, store.len()),
        (20_970, 3_255),
        "seed {SEED} over 1000 steps"
    );
}
