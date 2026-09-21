//! A coordinator asking a set of replicas to acknowledge it, round after round.
//!
//! [`crate::systems::ring`] leaves a state behind at every step, which is what time travel needed.
//! What it cannot do is go wrong: every run of it succeeds, and a run that cannot fail is nothing
//! for a reduction to test against. This is the system that can, and it is the first in the crate
//! to take a [`FaultSchedule`] — because what Phase 4 reduces is the trouble a run was put through.
//!
//! Each round the coordinator sends the round's number to every replica and waits, against one
//! deadline, for the acknowledgements to come back. A round that closes with fewer than a quorum of
//! them is a failure, and the run's [`Outcome`] names the first such round and the step it closed
//! at.
//!
//! # A failure here is the schedule's doing and nothing else's
//!
//! The wire this runs on is **dependable**: it takes time, and that is all. It loses nothing and
//! repeats nothing, so an injected fault is the only thing that can keep an acknowledgement from
//! coming back. A system whose wire dropped messages of its own accord would fail for reasons a
//! reduction could never remove, and reducing a schedule against it would be reducing noise.
//!
//! The seed still reaches everything that is genuinely undecided — how long each message spends on
//! the wire, how long a replica thinks before it answers, and so which acknowledgements land first.
//! What it does not reach is **when a round opens**: a round opens at a fixed multiple of the
//! coordinator's period, the way a control loop on a resync interval does. That is what makes a
//! fault able to name a round — a window over one period cuts exactly that round and no other —
//! and it is what keeps the round a failure falls in from moving when the schedule around it is cut
//! down.
//!
//! # The outcome comes out of what the run wrote down
//!
//! A task has nowhere to report a failure to, so the coordinator records what it observed and the
//! verdict is read off that same list afterwards, beside the steps that are read off it. The step
//! an [`Outcome::Fail`] names is therefore a step the trace has, by construction rather than by
//! agreement between two pieces of code.

use core::cell::RefCell;
use core::ops::RangeInclusive;
use core::time::Duration;
use std::rc::Rc;

use crate::clock::{Clock, VirtualTime};
use crate::executor::Executor;
use crate::fault::FaultSchedule;
use crate::history::Entry;
use crate::net::{Link, Network, NodeId, VirtualNetwork};
use crate::outcome::{Outcome, Reason, ReasonError};
use crate::rng::{Rng, SeededRng};
use crate::store::StateStore;
use crate::systems::RunError;
use crate::trace::{Step, Trace};
use crate::world::{Name, NameError, Resource, Snapshot, Value, World};

/// How many replicas the coordinator is talking to.
const REPLICAS: u64 = 5;

/// How many acknowledgements a round needs, which is a majority of the replicas.
const QUORUM: u64 = 3;

/// How many rounds the coordinator goes through.
const ROUNDS: u64 = 5;

/// How long apart the rounds are. Round `n` opens at `n` of these after the start of the run.
const PERIOD: Duration = Duration::from_secs(5);

/// How long the coordinator waits for a round's acknowledgements before closing it.
///
/// Comfortably longer than a message across and a reply back, so no amount of ordinary jitter can
/// lose a round its quorum, and comfortably shorter than [`PERIOD`], so no round outlives its own.
const PATIENCE: Duration = Duration::from_secs(2);

/// How long a replica waits to be asked something before deciding nothing more is coming.
const QUIET: Duration = Duration::from_secs(30);

/// How long a replica takes to think about what it was asked.
const THINKING: RangeInclusive<Duration> = Duration::from_millis(1)..=Duration::from_millis(200);

/// What the dependable wire takes to carry a message.
const LATENCY: RangeInclusive<Duration> = Duration::from_millis(10)..=Duration::from_millis(100);

/// What the coordinator is called where the run writes down what it knows.
const COORDINATOR: &str = "coordinator";

/// What each replica is called there, with its number after it.
const REPLICA: &str = "replica-";

/// The field holding the round something is about.
const ROUND: &str = "round";

/// The field the coordinator writes a round's acknowledgement count into.
const ACKS: &str = "acks";

/// The field the coordinator writes whether a round reached a quorum into.
const REACHED: &str = "quorum";

/// The field a replica's last acknowledgement is stamped with.
const AT: &str = "at";

/// Returns the instant round `round` opens at.
fn opens(round: u64) -> VirtualTime {
    VirtualTime::from_nanos(round.saturating_mul(as_nanos(PERIOD)))
}

/// Returns `duration` as a whole number of nanoseconds, capped at the end of virtual time.
fn as_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Returns how long there is left until `deadline`, which is nothing once it has passed.
fn until(now: VirtualTime, deadline: VirtualTime) -> Duration {
    Duration::from_nanos(deadline.as_nanos().saturating_sub(now.as_nanos()))
}

/// Something the coordinator wrote down, and the state the run was in once it had.
///
/// `lost` is the round this observation closed below a quorum, if it closed one at all. The verdict
/// and the steps are both read off the same list of these, which is what makes the step a failure
/// names one the trace has.
struct Observation {
    at: VirtualTime,
    message: String,
    world: World,
    lost: Option<u64>,
}

/// The names a run writes, made once up front so no task is left holding a name it cannot make.
///
/// Every one of them is built from a constant above and none of them could fail, but making them is
/// fallible and a task has nowhere to report a failure to. Making them before the run starts puts
/// the one place it could go wrong where there is still a caller to tell.
struct Names {
    replicas: Vec<Name>,
    coordinator: Name,
    round: Name,
    acks: Name,
    reached: Name,
    at: Name,
}

impl Names {
    /// Makes every name a run writes, or says which of them is not a name.
    fn new() -> Result<Self, NameError> {
        let mut replicas = Vec::new();
        for index in 0..REPLICAS {
            replicas.push(Name::new(format!("{REPLICA}{index}"))?);
        }
        Ok(Self {
            replicas,
            coordinator: Name::new(COORDINATOR)?,
            round: Name::new(ROUND)?,
            acks: Name::new(ACKS)?,
            reached: Name::new(REACHED)?,
            at: Name::new(AT)?,
        })
    }

    /// What the coordinator knows about a replica that has just acknowledged round `round`.
    fn acknowledged(&self, round: u64, at: VirtualTime) -> Resource {
        Resource::new()
            .with_field(self.round.clone(), Value::Count(round))
            .with_field(self.at.clone(), Value::Instant(at))
    }

    /// What the coordinator knows about itself once round `round` has closed with `acks` in.
    fn closed(&self, round: u64, acks: u64) -> Resource {
        Resource::new()
            .with_field(self.round.clone(), Value::Count(round))
            .with_field(self.acks.clone(), Value::Count(acks))
            .with_field(self.reached.clone(), Value::Flag(acks >= QUORUM))
    }
}

/// Runs the coordinator and its replicas under `seed`, with `faults` to get in their way.
///
/// Returns what the run passed through, the states it passed through, and how it went. A run that
/// meets no faults reaches every replica every round and holds up:
///
/// ```
/// use chronoloop::fault::FaultSchedule;
/// use chronoloop::outcome::Outcome;
/// use chronoloop::systems::quorum;
///
/// let (trace, store, outcome) = quorum::run(20_260_921, &FaultSchedule::default())?;
///
/// assert_eq!(outcome, Outcome::Pass);
/// assert_eq!(trace.seed(), 20_260_921);
/// assert!(store.get(trace.steps()[0].state()).is_some());
/// # Ok::<(), chronoloop::systems::RunError>(())
/// ```
///
/// # Errors
///
/// Returns [`RunError`] if the simulation could not finish, or if the run wrote down something that
/// could not be read back.
pub fn run(seed: u64, faults: &FaultSchedule) -> Result<(Trace, StateStore, Outcome), RunError> {
    let observed = observe(seed, faults)?;
    let outcome = verdict(&observed)?;
    let (steps, store) = collect(observed)?;
    Ok((Trace::new(seed, steps), store, outcome))
}

/// Runs the whole thing and returns what the coordinator wrote down.
fn observe(seed: u64, faults: &FaultSchedule) -> Result<Vec<Observation>, RunError> {
    let names = Rc::new(Names::new()?);
    let mut executor = Executor::new();
    let mut seeds = SeededRng::from_seed(seed);

    // The wire and every replica draw from generators of their own, all seeded from the run's, so
    // what one of them does never depends on how often another drew.
    let network: VirtualNetwork<u64, SeededRng> =
        VirtualNetwork::new(executor.handle(), SeededRng::from_seed(seeds.next_u64()))
            .with_default_link(Link::new(LATENCY));

    // The coordinator is added first, so it is node 0 and the replicas are the nodes after it.
    let endpoint = network.add_node();
    let replicas: Vec<_> = (0..REPLICAS).map(|_| network.add_node()).collect();
    let addresses: Vec<NodeId> = replicas.iter().map(Network::id).collect();
    network.set_faults(faults.clone());

    for replica in replicas {
        let clock = executor.handle();
        let mut rng = SeededRng::from_seed(seeds.next_u64());
        executor.spawn(async move {
            answer(&clock, &mut rng, &replica).await;
        });
    }

    let log = Rc::new(RefCell::new(Vec::new()));
    let clock = executor.handle();
    let observations = Rc::clone(&log);
    executor.spawn(async move {
        ask(&clock, &endpoint, &addresses, &names, &observations).await;
    });

    executor.run()?;
    // Every task is finished and nothing is polling, so nothing else holds the log open.
    Ok(core::mem::take(&mut *log.borrow_mut()))
}

/// The coordinator: open a round, ask everyone, count what comes back, close the round.
///
/// Written against the capabilities alone — it can ask the simulation for the time, arm a deadline
/// and talk to a replica, and it has no way to reach a real clock or a socket.
async fn ask<C, N>(
    clock: &C,
    endpoint: &N,
    replicas: &[NodeId],
    names: &Names,
    observations: &RefCell<Vec<Observation>>,
) where
    C: Clock,
    N: Network<Message = u64>,
{
    for round in 1..=ROUNDS {
        // A round opens on the period rather than whenever the last one finished, so which round a
        // window of simulated time falls in is not something the run's draws can move.
        clock.sleep_until(opens(round)).await;
        for replica in replicas {
            endpoint.send(*replica, round);
        }

        let deadline = opens(round)
            .checked_add(PATIENCE)
            .unwrap_or(VirtualTime::from_nanos(u64::MAX));
        let mut acks = 0;
        while acks < REPLICAS {
            let Ok(ack) = clock
                .timeout(until(clock.now(), deadline), endpoint.recv())
                .await
            else {
                break;
            };
            // An acknowledgement of a round gone by is not this round's, and counting it would let
            // a late answer stand in for one that never came.
            //
            // Nothing in the tests reaches this, and it is worth saying so rather than letting the
            // line read as covered: a round gives up well inside its own period, so an answer to
            // the round before it cannot still be on the wire. What it guards is the constants
            // above being changed, not something that happens today.
            if *ack.message() != round {
                continue;
            }
            acks += 1;
            let at = clock.now();
            let replica = names
                .replicas
                .get(index_of(ack.sender(), replicas))
                .unwrap_or(&names.coordinator)
                .clone();
            write(
                observations,
                at,
                format!("{} acknowledged round {round}", replica.as_str()),
                replica,
                names.acknowledged(round, at),
                None,
            );
        }

        let at = clock.now();
        write(
            observations,
            at,
            format!("round {round} closed with {acks} of {REPLICAS} acknowledgements"),
            names.coordinator.clone(),
            names.closed(round, acks),
            // What the round is judged on, written down with it rather than worked out afterwards
            // from the text: the verdict and the steps then come off one list.
            (acks < QUORUM).then_some(round),
        );
    }
}

/// Returns where `sender` sits among `replicas`, or past the end if it is not one of them.
fn index_of(sender: NodeId, replicas: &[NodeId]) -> usize {
    replicas
        .iter()
        .position(|replica| *replica == sender)
        .unwrap_or(replicas.len())
}

/// Writes down what the coordinator now knows, on top of what it knew before.
///
/// A step is the world as it stood once the change was made, so it carries what every other part of
/// the world holds as well. Nothing here waits, so the borrow is given up before the coordinator
/// goes round again and is never held across a poll.
fn write(
    observations: &RefCell<Vec<Observation>>,
    at: VirtualTime,
    message: String,
    name: Name,
    resource: Resource,
    lost: Option<u64>,
) {
    let mut log = observations.borrow_mut();
    let mut world = log
        .last()
        .map_or_else(World::new, |last: &Observation| last.world.clone());
    world.insert(name, resource);
    log.push(Observation {
        at,
        message,
        world,
        lost,
    });
}

/// A replica: answer whatever is asked, and stop once the wire has been quiet long enough.
///
/// Every wait is bounded by a deadline and the coordinator's loop by a count, so no seed and no
/// schedule can leave this run waiting for something that is never coming.
async fn answer<C, R, N>(clock: &C, rng: &mut R, endpoint: &N)
where
    C: Clock,
    R: Rng,
    N: Network<Message = u64>,
{
    while let Ok(asked) = clock.timeout(QUIET, endpoint.recv()).await {
        let round = *asked.message();
        clock.sleep(rng.duration_in(THINKING)).await;
        endpoint.send(asked.sender(), round);
    }
}

/// Reads the run's verdict off what it wrote down: the first round that closed below a quorum.
fn verdict(observed: &[Observation]) -> Result<Outcome, ReasonError> {
    let Some((step, round)) = observed
        .iter()
        .enumerate()
        .find_map(|(step, seen)| seen.lost.map(|round| (step, round)))
    else {
        return Ok(Outcome::Pass);
    };
    Ok(Outcome::Fail {
        reason: Reason::new(format!("round {round} lost quorum"))?,
        step,
    })
}

/// Turns what the run observed into steps, keeping every state it passed through in a store.
///
/// The entries are made here rather than inside the tasks, so that a message a trace could not read
/// back is reported to whoever asked for the run instead of to a task with nowhere to put it.
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
    use crate::fault::{Fault, Window};

    /// The seed the cases run, since none of them is about a particular one.
    const SEED: u64 = 20_260_921;

    /// Runs the system, failing the test rather than returning an error no case expects.
    fn runs(seed: u64, faults: &FaultSchedule) -> (Trace, StateStore, Outcome) {
        run(seed, faults).unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"))
    }

    /// The coordinator's address, which is the first node the run adds.
    fn coordinator() -> NodeId {
        NodeId::from_index(0)
    }

    /// The address of the replica the run adds `index`th, counting from zero.
    fn replica(index: u64) -> NodeId {
        NodeId::from_index(index + 1)
    }

    /// A schedule cutting the coordinator's way out to the first `cut` replicas, for `during`.
    fn cutting(cut: u64, during: Window) -> FaultSchedule {
        FaultSchedule::new(
            (0..cut)
                .map(|index| Fault::Partition {
                    from: coordinator(),
                    to: replica(index),
                    during,
                })
                .collect(),
        )
    }

    /// The round and the count every round-closing step of `trace` reports, in order.
    ///
    /// Read out of what the run wrote rather than asked of the run again, so a case comparing
    /// against this is not comparing the counting with itself.
    fn closes(trace: &Trace) -> Vec<(u64, u64)> {
        trace
            .steps()
            .iter()
            .filter_map(|step| {
                let words: Vec<&str> = step.event().message().split(' ').collect();
                match words.as_slice() {
                    ["round", round, "closed", "with", acks, "of", ..] => {
                        Some((round.parse().ok()?, acks.parse().ok()?))
                    }
                    _ => None,
                }
            })
            .collect()
    }

    #[test]
    fn a_run_that_meets_no_faults_hears_from_every_replica_every_round() {
        // The baseline the whole system rests on: the wire is dependable, so with nothing injected
        // every round is answered in full and there is nothing for a reduction to chase.
        let (trace, _, outcome) = runs(SEED, &FaultSchedule::default());

        assert_eq!(outcome, Outcome::Pass);
        assert_eq!(
            closes(&trace),
            (1..=ROUNDS)
                .map(|round| (round, REPLICAS))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_round_that_loses_a_majority_of_its_replicas_loses_quorum() {
        // Three of five cut off leaves two answering, which is one short of a majority.
        let cut = REPLICAS - QUORUM + 1;
        let (trace, _, outcome) =
            runs(SEED, &cutting(cut, Window::forever_from(VirtualTime::ZERO)));

        assert_eq!(
            outcome,
            Outcome::Fail {
                reason: Reason::new("round 1 lost quorum")
                    .unwrap_or_else(|e| panic!("a reason is a reason: {e}")),
                step: 2,
            },
            "the first round to close short is the one reported"
        );
        assert_eq!(
            closes(&trace),
            (1..=ROUNDS)
                .map(|round| (round, REPLICAS - cut))
                .collect::<Vec<_>>(),
            "the cut lasts the whole run, so every round closes short"
        );
    }

    #[test]
    fn a_round_one_replica_short_of_a_majority_still_holds() {
        // The other side of the threshold, and what keeps the case above from passing for any
        // number of faults at all.
        let cut = REPLICAS - QUORUM;
        let (trace, _, outcome) =
            runs(SEED, &cutting(cut, Window::forever_from(VirtualTime::ZERO)));

        assert_eq!(outcome, Outcome::Pass);
        assert_eq!(
            closes(&trace),
            (1..=ROUNDS)
                .map(|round| (round, QUORUM))
                .collect::<Vec<_>>(),
            "a quorum answered and no more"
        );
    }

    #[test]
    fn a_window_over_one_period_costs_that_round_and_no_other() {
        // What a round opening on the period buys. A fault can name a round, because the instants a
        // round occupies are not something the run's draws can move.
        let lost = 3;
        let during = Window::new(opens(lost), opens(lost + 1))
            .unwrap_or_else(|e| panic!("a window over one period is sound: {e}"));
        let (trace, _, outcome) = runs(SEED, &cutting(REPLICAS - QUORUM + 1, during));

        assert_eq!(
            outcome,
            Outcome::Fail {
                reason: Reason::new(format!("round {lost} lost quorum"))
                    .unwrap_or_else(|e| panic!("a reason is a reason: {e}")),
                step: 14,
            }
        );
        assert_eq!(
            closes(&trace),
            (1..=ROUNDS)
                .map(|round| (round, if round == lost { QUORUM - 1 } else { REPLICAS }))
                .collect::<Vec<_>>(),
            "every round but the one the window covers is answered in full"
        );
    }

    #[test]
    fn the_step_a_failure_names_is_a_step_the_trace_has() {
        // The verdict and the steps are read off one list, so this holds by construction. It is
        // pinned because the construction is what a later reader would be tempted to take apart.
        let (trace, store, outcome) = runs(
            SEED,
            &cutting(REPLICAS, Window::forever_from(VirtualTime::ZERO)),
        );
        let Outcome::Fail { step, .. } = outcome else {
            panic!("a run no replica answered lost every round");
        };

        let reached = trace
            .at(step)
            .unwrap_or_else(|| panic!("step {step} is a step the trace has"));
        assert!(
            store.get(reached.state()).is_some(),
            "and the state it names is one the store kept"
        );
    }

    #[test]
    fn different_seeds_run_different_quorums() {
        // Compared on the steps rather than on the written form: a trace's header names its seed,
        // so the text of two runs differs however little the runs themselves did. What this says is
        // only that there *was* a difference — the recorded trace in `tests/outcome.rs` is what
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
                runs(case.left, &FaultSchedule::default()).0.steps(),
                runs(case.right, &FaultSchedule::default()).0.steps(),
                "{}",
                case.name
            );
        }
    }
}
