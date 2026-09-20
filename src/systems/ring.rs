//! A ring of nodes talking to their neighbours, leaving a state behind at every step.
//!
//! [`crate::systems::pingpong`] is the smallest whole simulation there is and it keeps nothing: two
//! tasks exchange a message and the run is over. A control loop is the other shape — it goes round,
//! it holds what it has observed, and what it holds changes a little at a time. This is that shape
//! at its smallest: each node waits, says something to the node on its left, hears from the one on
//! its right, and writes down what it now knows.
//!
//! One resource of the [`World`] changes per step and the rest of it stands, so a run leaves behind
//! a state per step of which almost all is the state before it. That is what a [`StateStore`] is
//! built for, what a [`Trace`] puts in order, and what [`crate::diff`] tells apart — and it is what
//! [`crate::fork`] needs before it has anything to fork.
//!
//! # A fork is a second way to run the same nodes
//!
//! [`run`] and [`forked`] drive the same tasks over the same wire; what differs is where the draws
//! come from. [`run`] gives the wire and each node a generator of the run's own seed. [`forked`]
//! wraps each of those in a [`ForkedRng`], and hands each one a second seed drawn from the fork's
//! own seed in the order the first ones were drawn from the run's — so no two of them end up on one
//! key, and a fork back to the seed the run already had is the run again, draw for draw.
//!
//! They are two entry points rather than one taking an optional fork on purpose: a run and a fork of
//! it then reach their steps by different routes through this file, so a comparison of the two is
//! not a comparison of one route with itself.

use core::cell::RefCell;
use core::ops::RangeInclusive;
use core::time::Duration;
use std::rc::Rc;

use crate::clock::{Clock, VirtualTime};
use crate::executor::{Executor, Handle};
use crate::fork::{Fork, ForkedRng};
use crate::history::Entry;
use crate::net::{Network, NodeId, VirtualNetwork};
use crate::rng::{Rng, SeededRng};
use crate::store::StateStore;
use crate::systems::RunError;
use crate::trace::{Step, Trace};
use crate::world::{Name, NameError, Resource, Snapshot, Value, World};

/// What the nodes are called. The ring runs one node per name, in this order.
const NODES: [&str; 4] = ["eu-west", "eu-east", "us-east", "ap-south"];

/// How many times round the ring each node goes.
const ROUNDS: u64 = 5;

/// How long a node takes to think before it says anything.
const THINKING: RangeInclusive<Duration> = Duration::from_millis(1)..=Duration::from_secs(2);

/// What a node sends its neighbour. The ring is about what the nodes record, not about what they say.
const TICK: &str = "tick";

/// The field a node writes the neighbour it last heard from into.
const PEER: &str = "peer";

/// The field a node writes its round count into.
const ROUND: &str = "round";

/// The field a node writes the instant it last heard something at into.
const HEARD: &str = "heard";

/// The field a node writes whether it is still expecting an answer into.
///
/// A node has heard by the time it writes anything down, so this is `false` every round. It is here
/// on purpose: a field that never moves is what a comparison of two steps has to leave unmentioned,
/// and a system with no such field would never put that to the test.
const WAITING: &str = "waiting";

/// What a node writes down, once it has heard from its neighbour.
struct Observation {
    at: VirtualTime,
    message: String,
    world: World,
}

/// The names a run writes, made once up front so that no task is left holding a name it cannot make.
///
/// Every one of them is a constant above and none of them could fail, but making them is fallible
/// and a task has nowhere to report a failure to. Making them before the run starts puts the one
/// place it could go wrong where there is still a caller to tell.
struct Names {
    nodes: Vec<Name>,
    peer: Name,
    round: Name,
    heard: Name,
    waiting: Name,
}

impl Names {
    /// Makes every name a run writes, or says which of them is not a name.
    fn new() -> Result<Self, NameError> {
        let mut nodes = Vec::with_capacity(NODES.len());
        for called in NODES {
            nodes.push(Name::new(called)?);
        }
        Ok(Self {
            nodes,
            peer: Name::new(PEER)?,
            round: Name::new(ROUND)?,
            heard: Name::new(HEARD)?,
            waiting: Name::new(WAITING)?,
        })
    }

    /// What a node has to say about itself once it has heard from `peer` in round `round`.
    fn observed(&self, peer: NodeId, round: u64, at: VirtualTime) -> Resource {
        Resource::new()
            .with_field(self.peer.clone(), Value::Text(peer.to_string()))
            .with_field(self.round.clone(), Value::Count(round))
            .with_field(self.heard.clone(), Value::Instant(at))
            .with_field(self.waiting.clone(), Value::Flag(false))
    }
}

/// Runs the ring under `seed` and returns what it passed through and the states it passed through.
///
/// The same seed always leaves the same trace, and the store beside it hands back any state the
/// trace names:
///
/// ```
/// use chronoloop::systems::ring;
///
/// let (trace, store) = ring::run(20_260_919)?;
///
/// assert_eq!(trace.seed(), 20_260_919);
/// assert_eq!(trace.steps().len(), 20);
/// assert_eq!(trace.fork(), None);
/// assert!(store.get(trace.steps()[0].state()).is_some());
/// # Ok::<(), chronoloop::systems::RunError>(())
/// ```
pub fn run(seed: u64) -> Result<(Trace, StateStore), RunError> {
    let (steps, store) = collect(observe(seed, |_, rng| rng)?)?;
    Ok((Trace::new(seed, steps), store))
}

/// Runs the ring under `seed`, drawing from `fork`'s seed once the run is past `fork`'s instant.
///
/// Everything decided up to that instant is decided by the draws the run made the first time, so the
/// trace this returns opens with the steps [`run`] produced and parts from them after the fork. A
/// fork to the seed the run already had therefore changes nothing at all:
///
/// ```
/// use chronoloop::fork::fork;
/// use chronoloop::systems::ring;
///
/// let (trace, _) = ring::run(20_260_919)?;
/// let back_to_itself = fork(&trace, 7, trace.seed())?;
/// let (again, _) = ring::forked(trace.seed(), back_to_itself)?;
///
/// assert_eq!(again.steps(), trace.steps());
/// assert_eq!(again.fork(), Some(back_to_itself));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn forked(seed: u64, fork: Fork) -> Result<(Trace, StateStore), RunError> {
    // A generator of the fork's own seed, handing out one seed per generator of the run in the order
    // the run's seed handed out theirs. When the two seeds are the same it hands out the same ones.
    let mut after = SeededRng::from_seed(fork.seed());
    let observed = observe(seed, move |clock: &Handle, rng| {
        ForkedRng::new(clock.clone(), fork, rng, &mut after)
    })?;
    let (steps, store) = collect(observed)?;
    Ok((Trace::forked(seed, fork, steps), store))
}

/// Runs the ring, giving the wire and each node a generator `wrap` has had the making of.
///
/// `wrap` is handed the run's clock and the generator the seed produced for that part of the run,
/// and hands back whatever the run is to draw from — the generator itself, or one that changes key
/// partway through.
fn observe<R: Rng + 'static>(
    seed: u64,
    mut wrap: impl FnMut(&Handle, SeededRng) -> R,
) -> Result<Vec<Observation>, RunError> {
    let names = Rc::new(Names::new()?);
    let mut executor = Executor::new();
    let mut seeds = SeededRng::from_seed(seed);

    // The wire and each node draw from generators of their own, all seeded from the run's. Sharing
    // one would make a node's delays depend on how often its neighbour — or the network — drew.
    let clock = executor.handle();
    let wire = wrap(&clock, SeededRng::from_seed(seeds.next_u64()));
    let network: VirtualNetwork<&'static str, R> = VirtualNetwork::new(clock, wire);
    let endpoints: Vec<_> = NODES.iter().map(|_| network.add_node()).collect();
    let ids: Vec<_> = endpoints.iter().map(Network::id).collect();

    let log = Rc::new(RefCell::new(Vec::new()));
    for (index, endpoint) in endpoints.into_iter().enumerate() {
        let neighbour = ids[(index + 1) % ids.len()];
        let clock = executor.handle();
        let rng = wrap(&clock, SeededRng::from_seed(seeds.next_u64()));
        let names = Rc::clone(&names);
        let called = names.nodes[index].clone();
        let observations = Rc::clone(&log);
        executor.spawn(async move {
            node(
                &clock,
                rng,
                &endpoint,
                &called,
                neighbour,
                &names,
                &observations,
            )
            .await;
        });
    }

    executor.run()?;
    // Every task is finished and nothing is polling, so nothing else holds the log open.
    Ok(core::mem::take(&mut *log.borrow_mut()))
}

/// One node of the ring: think, speak, listen, write down what is now known, and go round again.
///
/// Written against the capabilities alone — it can ask the simulation for the time, for a delay and
/// for its neighbour, and it has no way to reach a real clock, the machine's entropy or a socket.
async fn node<C, R, N>(
    clock: &C,
    mut rng: R,
    endpoint: &N,
    called: &Name,
    neighbour: NodeId,
    names: &Names,
    observations: &RefCell<Vec<Observation>>,
) where
    C: Clock,
    R: Rng,
    N: Network<Message = &'static str>,
{
    for round in 1..=ROUNDS {
        clock.sleep(rng.duration_in(THINKING)).await;
        endpoint.send(neighbour, TICK);
        let heard = endpoint.recv().await;
        let at = clock.now();

        // A step is the world as it stood once the change was made, so it carries what the other
        // nodes have written as well. Nothing below waits, so the borrow is given up before the
        // round comes round again and is never held across a poll.
        let mut log = observations.borrow_mut();
        let mut world = log
            .last()
            .map_or_else(World::new, |last| last.world.clone());
        world.insert(called.clone(), names.observed(heard.sender(), round, at));
        log.push(Observation {
            at,
            message: format!(
                "{} heard from {} in round {round}",
                called.as_str(),
                heard.sender()
            ),
            world,
        });
    }
}

/// Turns what a run observed into steps, keeping every state it passed through in a store.
///
/// The entries are made here rather than inside the tasks, so that a message a recording could not
/// read back is reported to whoever asked for the run instead of to a task with nowhere to put it.
fn collect(observed: Vec<Observation>) -> Result<(Vec<Step>, StateStore), RunError> {
    let mut store = StateStore::new();
    let mut steps = Vec::with_capacity(observed.len());
    for step in observed {
        let event = Entry::new(step.at, step.message)?;
        steps.push(Step::new(event, store.insert(&step.world.snapshot())));
    }
    Ok((steps, store))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::world::Node;

    /// The seed the cases run, since none of them is about a particular one.
    const SEED: u64 = 20_260_919;

    /// Runs the ring, failing the test rather than returning an error no case expects.
    fn ring(seed: u64) -> (Trace, StateStore) {
        run(seed).unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"))
    }

    /// The names of a branch's children, or nothing if the node is a leaf.
    fn children(node: &Node) -> Option<Vec<String>> {
        match node {
            Node::Leaf(_) => None,
            Node::Branch(children) => Some(
                children
                    .keys()
                    .map(|name| name.as_str().to_string())
                    .collect(),
            ),
        }
    }

    #[test]
    fn a_run_takes_one_step_per_node_per_round() {
        let (trace, _) = ring(SEED);
        let rounds = usize::try_from(ROUNDS).unwrap_or_else(|e| panic!("a count fits: {e}"));
        assert_eq!(trace.steps().len(), NODES.len() * rounds);
    }

    #[test]
    fn every_step_names_a_state_the_store_hands_back() {
        // The trace is an index and the store is what it indexes, so a run has to leave the two
        // agreeing: a step naming a state the store never saw is a step nothing can be shown for.
        let (trace, store) = ring(SEED);
        for (number, step) in trace.steps().iter().enumerate() {
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
    fn a_step_early_on_knows_about_fewer_nodes_than_the_last_one() {
        // A control loop's world fills up as its parts report for the first time. The first step is
        // one node writing itself down and nothing else; by the end every node is in there, each
        // with the four fields it writes.
        let (trace, store) = ring(SEED);
        let state = |step: &Step| {
            store
                .get(step.state())
                .unwrap_or_else(|| panic!("the store kept every state the run passed through"))
        };

        let first = state(&trace.steps()[0]);
        assert_eq!(
            children(&first).map(|names| names.len()),
            Some(1),
            "the first step is the first node writing itself down"
        );

        let last = state(trace.steps().last().unwrap_or_else(|| {
            panic!("a run of twenty steps has a last one");
        }));
        let mut expected: Vec<String> = NODES.iter().map(|name| (*name).to_string()).collect();
        expected.sort_unstable();
        assert_eq!(
            children(&last),
            Some(expected),
            "by the end every node has written itself down"
        );

        let Node::Branch(resources) = last else {
            panic!("a world is a branch of its resources, never a leaf");
        };
        for (name, resource) in &resources {
            assert_eq!(
                children(resource),
                Some(vec![
                    HEARD.to_string(),
                    PEER.to_string(),
                    ROUND.to_string(),
                    WAITING.to_string(),
                ]),
                "{name} wrote down everything a node writes down"
            );
        }
    }

    #[test]
    fn different_seeds_run_different_rings() {
        // Compared on the steps rather than on the written form: a trace's header names its seed, so
        // the text of two runs differs however little the runs themselves did. What this says is
        // only that there *was* a difference — the recorded trace in the integration tests is what
        // says the steps are the steps this seed produces.
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
            assert_ne!(
                ring(case.left).0.steps(),
                ring(case.right).0.steps(),
                "{}",
                case.name
            );
        }
    }
}
