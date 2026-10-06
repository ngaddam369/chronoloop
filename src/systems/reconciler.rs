//! A level-triggered control loop driving an API server towards what it has been asked for.
//!
//! The systems before this one are protocols: an exchange, a ring, a quorum. This is the first
//! **control loop**, which is the thing chronoloop exists to run — an operator that is told what
//! should exist and keeps making it so.
//!
//! # Level-triggered, and what that buys
//!
//! The decision is [`reconcile`], and it reads exactly two things: what is [`Desired`] and what is
//! [`Observed`]. It keeps nothing between calls, so it cannot be told *what changed* — only what
//! is. That is what "level-triggered" means, and two properties fall out of it rather than being
//! built in:
//!
//! - **A lost action is retried without anything deciding to retry it.** The next pass looks again,
//!   finds the world still short of what was asked for, and asks for the same thing again. A
//!   controller that acted on *changes* would have seen nothing change and done nothing, forever.
//! - **A controller can start from nothing.** There is no memory to lose, so a controller restarted
//!   halfway through, or one that never saw the beginning, reaches the same decisions as one that
//!   was there all along.
//!
//! Both depend on the other half of the contract: [`Observed::apply`] is idempotent. Creating
//! something that already exists, resizing or deleting something that is not there, changes
//! nothing — so an action decided on a listing that has since gone stale does no harm the next pass
//! cannot undo.
//!
//! # The run
//!
//! [`run`] starts two tasks over the simulated network. The **API server** holds what is desired and
//! what exists. What is desired changes on a fixed timeline — three resources are asked for, one is
//! resized, one is torn down — and the server applies whatever it is asked to. The **controller**
//! wakes on a fixed resync period, asks for a listing, works out what to do and sends it. Every
//! change the server makes is a step of the trace.
//!
//! The wire is **dependable**, for the reason `quorum` gives: it takes time and nothing else, so an
//! injected [`FaultSchedule`] is the only thing that can keep a request from arriving, and a failure
//! is the schedule's doing. A run holds up when, once the loop has finished, what exists is exactly
//! what was asked for.

use core::cell::RefCell;
use core::fmt;
use core::ops::RangeInclusive;
use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet, btree_map};
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

/// How long apart the controller's passes are. Pass `n` opens at `n` of these after the start.
const PERIOD: Duration = Duration::from_secs(5);

/// How many passes the controller makes before it stops.
const PASSES: u64 = 8;

/// How long the controller waits for a listing before giving the pass up.
///
/// Comfortably longer than a request across and a listing back, and comfortably shorter than
/// [`PERIOD`], so no pass outlives its own.
const PATIENCE: Duration = Duration::from_secs(2);

/// How long the server waits for a request, once nothing more is going to be asked of it, before
/// deciding nothing more is coming.
const QUIET: Duration = Duration::from_secs(30);

/// What the dependable wire takes to carry a message.
const LATENCY: RangeInclusive<Duration> = Duration::from_millis(10)..=Duration::from_millis(100);

/// What is asked for over the run: at a whole number of seconds, a resource and the size wanted of
/// it, or nothing to say it is no longer wanted.
///
/// Each change falls between two passes, so the pass after it is the first to see it.
const TIMELINE: &[(u64, &str, Option<u64>)] = &[
    (0, "alpha", Some(2)),
    (0, "beta", Some(1)),
    (0, "gamma", Some(3)),
    (12, "alpha", Some(4)),
    (22, "beta", None),
];

/// The field holding the size a resource is wanted at.
const DESIRED: &str = "desired";

/// The field holding the size a resource exists at.
const ACTUAL: &str = "actual";

/// What a resource is wanted to be, or is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Spec {
    size: u64,
}

impl Spec {
    /// A resource of `size`.
    pub fn new(size: u64) -> Self {
        Self { size }
    }

    /// How big the resource is.
    pub fn size(self) -> u64 {
        self.size
    }
}

/// What should exist: every resource that is wanted, and what it is wanted to be.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Desired {
    resources: BTreeMap<Name, Spec>,
}

impl Desired {
    /// Wants nothing at all.
    pub fn new() -> Self {
        Self::default()
    }

    /// Wants `name` as `spec`, returning what it was wanted as before.
    pub fn insert(&mut self, name: Name, spec: Spec) -> Option<Spec> {
        self.resources.insert(name, spec)
    }

    /// Stops wanting `name`, returning what it was wanted as.
    pub fn remove(&mut self, name: &Name) -> Option<Spec> {
        self.resources.remove(name)
    }

    /// Every resource that is wanted, in name order.
    pub fn resources(&self) -> btree_map::Iter<'_, Name, Spec> {
        self.resources.iter()
    }
}

/// What does exist: every resource the server holds, and what it is.
///
/// There is no way to change one other than [`Observed::apply`], which is the way a server changes
/// what it holds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Observed {
    resources: BTreeMap<Name, Spec>,
}

impl Observed {
    /// Holds nothing at all.
    pub fn new() -> Self {
        Self::default()
    }

    /// Carries out `action`, returning whether anything changed.
    ///
    /// Idempotent: creating something that already exists, and resizing or deleting something that
    /// does not, changes nothing. That is what makes it safe to act on a listing that has gone
    /// stale — the worst a stale action can do is nothing, or something the next pass undoes.
    pub fn apply(&mut self, action: &Action) -> bool {
        match action {
            Action::Create { name, spec } => {
                if self.resources.contains_key(name) {
                    return false;
                }
                self.resources.insert(name.clone(), *spec);
                true
            }
            Action::Resize { name, spec } => match self.resources.get_mut(name) {
                Some(current) if current != spec => {
                    *current = *spec;
                    true
                }
                _ => false,
            },
            Action::Delete { name } => self.resources.remove(name).is_some(),
        }
    }

    /// Every resource that exists, in name order.
    pub fn resources(&self) -> btree_map::Iter<'_, Name, Spec> {
        self.resources.iter()
    }
}

/// One thing the controller asks the server to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Bring a resource that does not exist into being.
    Create {
        /// The resource.
        name: Name,
        /// What it is to be.
        spec: Spec,
    },
    /// Change a resource that exists into something else.
    Resize {
        /// The resource.
        name: Name,
        /// What it is to become.
        spec: Spec,
    },
    /// Take a resource that exists away.
    Delete {
        /// The resource.
        name: Name,
    },
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Create { name, spec } => {
                write!(f, "create {} at size {}", name.as_str(), spec.size())
            }
            Self::Resize { name, spec } => {
                write!(f, "resize {} to size {}", name.as_str(), spec.size())
            }
            Self::Delete { name } => write!(f, "delete {}", name.as_str()),
        }
    }
}

/// Works out what to ask for to turn what is `observed` into what is `desired`.
///
/// Reads the two states and nothing else, and keeps nothing for next time: the answer is a function
/// of what *is*, never of what changed. The actions come back in name order, at most one a resource,
/// and carrying all of them out leaves nothing more to do:
///
/// ```
/// use chronoloop::systems::reconciler::{Desired, Observed, Spec, reconcile};
/// use chronoloop::world::Name;
///
/// let mut desired = Desired::new();
/// desired.insert(Name::new("alpha")?, Spec::new(2));
/// let mut observed = Observed::new();
///
/// let actions = reconcile(&desired, &observed);
/// assert_eq!(actions.len(), 1);
/// assert_eq!(actions[0].to_string(), "create alpha at size 2");
///
/// for action in &actions {
///     observed.apply(action);
/// }
/// assert!(reconcile(&desired, &observed).is_empty());
/// # Ok::<(), chronoloop::world::NameError>(())
/// ```
pub fn reconcile(desired: &Desired, observed: &Observed) -> Vec<Action> {
    let names: BTreeSet<&Name> = desired
        .resources
        .keys()
        .chain(observed.resources.keys())
        .collect();
    names
        .into_iter()
        .filter_map(
            |name| match (desired.resources.get(name), observed.resources.get(name)) {
                (Some(&spec), None) => Some(Action::Create {
                    name: name.clone(),
                    spec,
                }),
                (Some(&spec), Some(&current)) if spec != current => Some(Action::Resize {
                    name: name.clone(),
                    spec,
                }),
                (None, Some(_)) => Some(Action::Delete { name: name.clone() }),
                _ => None,
            },
        )
        .collect()
}

/// What travels between the controller and the server.
#[derive(Debug, Clone)]
enum Message {
    /// The controller asking what is wanted and what exists.
    List,
    /// The server's answer.
    Listed(Desired, Observed),
    /// The controller asking for one thing to be done.
    Apply(Action),
}

/// Returns the instant pass `pass` opens at.
fn opens(pass: u64) -> VirtualTime {
    VirtualTime::from_nanos(pass.saturating_mul(as_nanos(PERIOD)))
}

/// Returns `duration` as a whole number of nanoseconds, capped at the end of virtual time.
fn as_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Returns how long there is left until `deadline`, which is nothing once it has passed.
fn until(now: VirtualTime, deadline: VirtualTime) -> Duration {
    Duration::from_nanos(deadline.as_nanos().saturating_sub(now.as_nanos()))
}

/// One change to what is wanted, with its name already made.
struct Change {
    at: VirtualTime,
    name: Name,
    spec: Option<Spec>,
}

/// The timeline, with every name made up front.
///
/// Making a name is fallible and a task has nowhere to report a failure to, so the one place it
/// could go wrong is put where there is still a caller to tell — the reason `quorum` makes its
/// names before its run starts.
fn timeline() -> Result<Vec<Change>, NameError> {
    TIMELINE
        .iter()
        .map(|&(seconds, name, size)| {
            Ok(Change {
                at: opens(0)
                    .checked_add(Duration::from_secs(seconds))
                    .unwrap_or(VirtualTime::from_nanos(u64::MAX)),
                name: Name::new(name)?,
                spec: size.map(Spec::new),
            })
        })
        .collect()
}

/// The fields the run writes every resource's state into.
struct Fields {
    desired: Name,
    actual: Name,
}

impl Fields {
    /// Makes both field names, or says which of them is not a name.
    fn new() -> Result<Self, NameError> {
        Ok(Self {
            desired: Name::new(DESIRED)?,
            actual: Name::new(ACTUAL)?,
        })
    }

    /// The world as the server holds it: one resource for each name that is wanted or exists, with
    /// the size it is wanted at and the size it is at, each where there is one.
    fn world(&self, desired: &Desired, observed: &Observed) -> World {
        let mut world = World::new();
        for (name, spec) in desired.resources() {
            world.insert(
                name.clone(),
                Resource::new().with_field(self.desired.clone(), Value::Count(spec.size())),
            );
        }
        for (name, spec) in observed.resources() {
            let mut resource = world.remove(name).unwrap_or_default();
            resource.insert(self.actual.clone(), Value::Count(spec.size()));
            world.insert(name.clone(), resource);
        }
        world
    }
}

/// Something the server changed, and the state the run was in once it had.
struct Observation {
    at: VirtualTime,
    message: String,
    world: World,
    converged: bool,
}

/// Runs the controller and the server under `seed`, with `faults` to get in their way.
///
/// Returns what the run passed through, the states it passed through, and how it went. A run that
/// meets no faults ends with what exists matching what was asked for:
///
/// ```
/// use chronoloop::fault::FaultSchedule;
/// use chronoloop::outcome::Outcome;
/// use chronoloop::systems::reconciler;
///
/// let (trace, store, outcome) = reconciler::run(20_261_005, &FaultSchedule::default())?;
///
/// assert_eq!(outcome, Outcome::Pass);
/// assert_eq!(trace.seed(), 20_261_005);
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

/// Runs the whole thing and returns what the server wrote down.
fn observe(seed: u64, faults: &FaultSchedule) -> Result<Vec<Observation>, RunError> {
    let changes = timeline()?;
    let fields = Fields::new()?;
    let mut executor = Executor::new();
    let mut seeds = SeededRng::from_seed(seed);

    // Only the wire draws anything: what is asked for and when the controller wakes are both fixed,
    // so a run's seed reaches it through how long each message spends on the way.
    let network: VirtualNetwork<Message, SeededRng> =
        VirtualNetwork::new(executor.handle(), SeededRng::from_seed(seeds.next_u64()))
            .with_default_link(Link::new(LATENCY));

    // The controller is added first, so it is node 0 and the server is node 1.
    let controller = network.add_node();
    let server = network.add_node();
    let address = server.id();
    network.set_faults(faults.clone());

    let log = Rc::new(RefCell::new(Vec::new()));
    let clock = executor.handle();
    let observations = Rc::clone(&log);
    executor.spawn(async move {
        serve(&clock, &server, changes, &fields, &observations).await;
    });

    let clock = executor.handle();
    executor.spawn(async move {
        control(&clock, &controller, address).await;
    });

    executor.run()?;
    // Every task is finished and nothing is polling, so nothing else holds the log open.
    Ok(core::mem::take(&mut *log.borrow_mut()))
}

/// The controller: on every pass, look, decide, and ask.
///
/// Written against the capabilities alone, and holding nothing from one pass to the next — the
/// listing a pass acts on is the one it asked for itself. A pass whose listing does not come back
/// in time does nothing, and the next pass looks again.
async fn control<C, N>(clock: &C, endpoint: &N, server: NodeId)
where
    C: Clock,
    N: Network<Message = Message>,
{
    for pass in 1..=PASSES {
        // A pass opens on the period rather than whenever the last one finished, so which pass a
        // window of simulated time falls in is not something the run's draws can move.
        clock.sleep_until(opens(pass)).await;
        endpoint.send(server, Message::List);

        let deadline = opens(pass)
            .checked_add(PATIENCE)
            .unwrap_or(VirtualTime::from_nanos(u64::MAX));
        // Only the server talks to the controller and it only ever sends a listing, so the first
        // thing to arrive is this pass's. A listing from a pass gone by could not still be on the
        // wire — a pass gives up well inside its period — and acting on one would do no harm
        // anyway, which is the point of deciding on what is rather than on what changed.
        let Ok(delivery) = clock
            .timeout(until(clock.now(), deadline), endpoint.recv())
            .await
        else {
            continue;
        };
        let Message::Listed(desired, observed) = delivery.into_message() else {
            continue;
        };
        for action in reconcile(&desired, &observed) {
            endpoint.send(server, Message::Apply(action));
        }
    }
}

/// The server: change what is wanted on the timeline, and do whatever it is asked.
///
/// It waits for whichever comes first, the next change or the next request — selecting between the
/// two rather than spawning a task for each — and once every change is made it stops when it has
/// been quiet for long enough. Every wait is bounded and the controller's passes by a count, so no
/// seed and no schedule can leave the run waiting for something that is never coming.
async fn serve<C, N>(
    clock: &C,
    endpoint: &N,
    changes: Vec<Change>,
    fields: &Fields,
    observations: &RefCell<Vec<Observation>>,
) where
    C: Clock,
    N: Network<Message = Message>,
{
    let mut desired = Desired::new();
    let mut observed = Observed::new();
    let mut changes = changes.into_iter().peekable();
    loop {
        let wait = changes
            .peek()
            .map_or(QUIET, |change| until(clock.now(), change.at));
        let Ok(delivery) = clock.timeout(wait, endpoint.recv()).await else {
            let Some(change) = changes.next() else {
                break;
            };
            let message = if let Some(spec) = change.spec {
                desired.insert(change.name.clone(), spec);
                format!("{} wanted at size {}", change.name.as_str(), spec.size())
            } else {
                desired.remove(&change.name);
                format!("{} no longer wanted", change.name.as_str())
            };
            write(
                observations,
                clock.now(),
                message,
                fields,
                &desired,
                &observed,
            );
            continue;
        };
        let sender = delivery.sender();
        match delivery.into_message() {
            Message::List => {
                endpoint.send(sender, Message::Listed(desired.clone(), observed.clone()));
            }
            Message::Apply(action) => {
                if observed.apply(&action) {
                    let message = action.to_string();
                    write(
                        observations,
                        clock.now(),
                        message,
                        fields,
                        &desired,
                        &observed,
                    );
                }
            }
            // Nothing sends the server a listing; one arriving is not a request for anything.
            Message::Listed(..) => {}
        }
    }
}

/// Writes down a change the server has just made.
///
/// Nothing here waits, so the borrow is given up before the server goes round again and is never
/// held across a poll.
fn write(
    observations: &RefCell<Vec<Observation>>,
    at: VirtualTime,
    message: String,
    fields: &Fields,
    desired: &Desired,
    observed: &Observed,
) {
    observations.borrow_mut().push(Observation {
        at,
        message,
        world: fields.world(desired, observed),
        converged: reconcile(desired, observed).is_empty(),
    });
}

/// Reads the run's verdict off what the server wrote down.
///
/// The last change the server made is the state the run ended in, so the run held up exactly when
/// nothing was left to do after it. A run that broke names that last step, since there is no one
/// earlier step a failure to get somewhere can be said to have happened at.
fn verdict(observed: &[Observation]) -> Result<Outcome, ReasonError> {
    match observed.iter().enumerate().next_back() {
        Some((step, last)) if !last.converged => Ok(Outcome::Fail {
            reason: Reason::new("did not converge")?,
            step,
        }),
        _ => Ok(Outcome::Pass),
    }
}

/// Turns what the server observed into steps, keeping every state it passed through in a store.
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

    /// A name, failing the test if it is not one.
    fn name(name: &str) -> Name {
        Name::new(name).unwrap_or_else(|e| panic!("{name:?} is a name: {e}"))
    }

    /// What is wanted, built from names and sizes.
    fn desired(resources: &[(&str, u64)]) -> Desired {
        let mut desired = Desired::new();
        for &(resource, size) in resources {
            desired.insert(name(resource), Spec::new(size));
        }
        desired
    }

    /// What exists, built the only way a server builds it: by creating each resource.
    fn observed(resources: &[(&str, u64)]) -> Observed {
        let mut observed = Observed::new();
        for &(resource, size) in resources {
            observed.apply(&Action::Create {
                name: name(resource),
                spec: Spec::new(size),
            });
        }
        observed
    }

    /// The actions as a person is shown them.
    fn shown(actions: &[Action]) -> Vec<String> {
        actions.iter().map(ToString::to_string).collect()
    }

    /// What a state holds, as names and sizes, so two states of different types can be compared.
    fn sizes<'a>(resources: impl Iterator<Item = (&'a Name, &'a Spec)>) -> Vec<(String, u64)> {
        resources
            .map(|(name, spec)| (name.as_str().to_owned(), spec.size()))
            .collect()
    }

    #[test]
    fn reconcile_asks_for_exactly_the_difference() {
        struct Case {
            name: &'static str,
            desired: &'static [(&'static str, u64)],
            observed: &'static [(&'static str, u64)],
            expected: &'static [&'static str],
        }
        let cases = [
            Case {
                name: "nothing wanted and nothing there",
                desired: &[],
                observed: &[],
                expected: &[],
            },
            Case {
                name: "everything already as wanted",
                desired: &[("alpha", 2), ("beta", 1)],
                observed: &[("alpha", 2), ("beta", 1)],
                expected: &[],
            },
            Case {
                name: "wanted and missing",
                desired: &[("alpha", 2)],
                observed: &[],
                expected: &["create alpha at size 2"],
            },
            Case {
                name: "there at the wrong size",
                desired: &[("alpha", 4)],
                observed: &[("alpha", 2)],
                expected: &["resize alpha to size 4"],
            },
            Case {
                name: "there and not wanted",
                desired: &[],
                observed: &[("beta", 1)],
                expected: &["delete beta"],
            },
            Case {
                name: "one of each, in name order whatever the kind",
                desired: &[("alpha", 4), ("gamma", 3)],
                observed: &[("alpha", 2), ("beta", 1)],
                expected: &[
                    "resize alpha to size 4",
                    "delete beta",
                    "create gamma at size 3",
                ],
            },
        ];
        for case in cases {
            assert_eq!(
                shown(&reconcile(&desired(case.desired), &observed(case.observed))),
                case.expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn carrying_out_one_pass_leaves_nothing_to_do_from_any_state() {
        // The level-triggered claim, asked of every state a controller could find the world in: each
        // of four resources missing, at the wrong size or right, wanted or not. A controller that
        // restarts finds one of these with no memory of how it got there, so one pass from each of
        // them reaching what is wanted is what "restartable" comes to.
        let want = desired(&[("alpha", 1), ("beta", 2), ("gamma", 3)]);
        let universe = [("alpha", 1), ("beta", 2), ("gamma", 3), ("delta", 4)];
        let mut tried = 0;
        for mut code in 0..3_u32.pow(4) {
            let mut there = Vec::new();
            for &(resource, size) in &universe {
                match code % 3 {
                    1 => there.push((resource, size)),
                    2 => there.push((resource, size + 10)),
                    _ => {}
                }
                code /= 3;
            }
            let mut world = observed(&there);
            for action in reconcile(&want, &world) {
                world.apply(&action);
            }
            assert_eq!(
                sizes(world.resources()),
                sizes(want.resources()),
                "from {there:?}"
            );
            assert!(reconcile(&want, &world).is_empty(), "from {there:?}");
            tried += 1;
        }
        assert_eq!(tried, 81, "every one of the states, not a corner of them");
    }

    #[test]
    fn applying_an_action_twice_is_applying_it_once() {
        struct Case {
            name: &'static str,
            before: &'static [(&'static str, u64)],
            action: Action,
            after: &'static [(&'static str, u64)],
        }
        let cases = [
            Case {
                name: "a create",
                before: &[],
                action: Action::Create {
                    name: name("alpha"),
                    spec: Spec::new(2),
                },
                after: &[("alpha", 2)],
            },
            Case {
                name: "a resize",
                before: &[("alpha", 2)],
                action: Action::Resize {
                    name: name("alpha"),
                    spec: Spec::new(4),
                },
                after: &[("alpha", 4)],
            },
            Case {
                name: "a delete",
                before: &[("alpha", 2)],
                action: Action::Delete {
                    name: name("alpha"),
                },
                after: &[],
            },
        ];
        for case in cases {
            let mut world = observed(case.before);
            assert!(
                world.apply(&case.action),
                "{}: the first one changes",
                case.name
            );
            let once = world.clone();
            assert!(
                !world.apply(&case.action),
                "{}: the second one does not",
                case.name
            );
            assert_eq!(world, once, "{}", case.name);
            assert_eq!(
                sizes(world.resources()),
                sizes(observed(case.after).resources()),
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn an_action_on_a_world_it_no_longer_fits_changes_nothing() {
        // What makes a stale listing harmless: an action decided against a world that has moved on
        // since is either already done or no longer possible, and either way does nothing.
        struct Case {
            name: &'static str,
            before: &'static [(&'static str, u64)],
            action: Action,
        }
        let cases = [
            Case {
                name: "creating what is already there, even at another size",
                before: &[("alpha", 2)],
                action: Action::Create {
                    name: name("alpha"),
                    spec: Spec::new(5),
                },
            },
            Case {
                name: "resizing what is not there",
                before: &[],
                action: Action::Resize {
                    name: name("alpha"),
                    spec: Spec::new(4),
                },
            },
            Case {
                name: "deleting what is not there",
                before: &[("beta", 1)],
                action: Action::Delete {
                    name: name("alpha"),
                },
            },
        ];
        for case in cases {
            let mut world = observed(case.before);
            assert!(!world.apply(&case.action), "{}", case.name);
            assert_eq!(world, observed(case.before), "{}", case.name);
        }
    }

    #[test]
    fn a_run_that_meets_no_faults_ends_with_what_was_asked_for() {
        let (trace, _, outcome) = run(20_261_005, &FaultSchedule::default())
            .unwrap_or_else(|e| panic!("the run finishes: {e}"));

        assert_eq!(outcome, Outcome::Pass);
        let messages: Vec<&str> = trace
            .steps()
            .iter()
            .map(|step| step.event().message())
            .collect();
        for expected in [
            "create alpha at size 2",
            "resize alpha to size 4",
            "delete beta",
        ] {
            assert!(messages.contains(&expected), "{expected} in {messages:?}");
        }
    }
}
