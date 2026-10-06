//! What must stay true of a run, checked against the run it was true of.
//!
//! An [`crate::outcome::Outcome`] says how a run went, and until this module only the system that
//! ran could say it: each verdict was read off notes the system kept for itself while it ran. A
//! system nobody had written such notes into could be traced, scrubbed and compared, but never
//! judged — and a run that cannot be judged cannot be shrunk, because a reduction keeps a candidate
//! only when it still fails.
//!
//! An [`Invariant`] is a judgement written **outside** the system. It reads the [`World`] a step
//! left the run in — the state the trace already names, rebuilt from the store the trace already
//! points into — so it asks nothing of a system beyond what every traced run leaves behind, and
//! [`check`] can judge any of them.
//!
//! # Two kinds of promise
//!
//! A **safety** invariant is something that must hold after every step. It is broken at the first
//! step whose state it does not hold of.
//!
//! A **liveness** invariant is something that may stop holding for a while and must come back
//! within a span of **virtual time**. A stretch opens at the instant of the step it first fails at,
//! and it closes at the first step it holds at again, provided that step is no later than the
//! stretch's opening plus the span. A step landing exactly on that deadline is in time — the rule
//! the engine's own `Timeout` keeps, where work ready at the instant its deadline falls has
//! finished. Two things break one:
//!
//! - **A step after the deadline with the stretch still open.** What is named is the step *before*
//!   it, since that step's state is the one standing when the span ran out. The later step may well
//!   be the one where it holds again, and naming a state it holds of would point a reader at the one
//!   place nothing is wrong.
//! - **The end of the run with the stretch still open.** A run that stopped before recovering has
//!   not recovered, however little of the span it used, so the last step is named. This is what a
//!   control loop's "did not converge" is.
//!
//! The span is a time rather than a count of steps because a step is whatever the system chose to
//! write down: a requeued look, or a message a fault happened to move, would spend a budget counted
//! in steps on something that has nothing to do with recovering. A span of simulated time is the
//! promise an operator actually makes, and a fault taken away by a reduction does not move it.
//!
//! # Every invariant's first breach, not only the first one
//!
//! [`check`] hands back, for each invariant that broke, the first step it broke at — ordered by step,
//! and among breaches at one step by where the invariants stand in the slice it was given. A fixed
//! rule rather than whichever was looked at first, for the reason two events at one instant order
//! by a sequence number.
//!
//! It reports every one rather than the earliest alone because a report naming only the first
//! failure is blind to every other wherever that one fails first, and fixing the first is exactly
//! when the second appears. Whoever wants a single verdict takes the first breach and turns it into
//! an [`Outcome`]; it is a step and a reason like any other failure, so a reduction, a repro and a
//! sweep all take it as they are.
//!
//! # A report, and never read back
//!
//! A [`Violation`] is written as one line and has no reader, for the reason a comparison has none:
//! it is an output, and nothing takes one back in. What a later run is checked against is the
//! [`Outcome`] it becomes, which does read back.

use core::fmt;
use core::time::Duration;

use crate::clock::VirtualTime;
use crate::outcome::{Outcome, Reason};
use crate::store::StateStore;
use crate::trace::Trace;
use crate::world::{DecodeWorldError, StateHash, World};

/// Which promise an [`Invariant`] makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// It holds after every step.
    Safety,
    /// Whenever it stops holding, it holds again within `within` of virtual time, and the run does
    /// not end while it does not.
    Liveness {
        /// How long it may go without holding.
        within: Duration,
    },
}

/// Something that must stay true of a run's recorded world.
///
/// Its name is a [`Reason`], because the name is what a breach of it is reported as and what a
/// failure it causes is told from another by: it says what broke. What it checks is a plain
/// function of one world, so an invariant carries nothing a run could change and judges every run
/// alike.
#[derive(Clone)]
pub struct Invariant {
    name: Reason,
    kind: Kind,
    holds: fn(&World) -> bool,
}

impl Invariant {
    /// Creates an invariant that `holds` after every step.
    pub fn safety(name: Reason, holds: fn(&World) -> bool) -> Self {
        Self {
            name,
            kind: Kind::Safety,
            holds,
        }
    }

    /// Creates an invariant that, whenever it stops holding, `holds` again within `within` of
    /// virtual time — and holds at the end of the run.
    pub fn liveness(name: Reason, within: Duration, holds: fn(&World) -> bool) -> Self {
        Self {
            name,
            kind: Kind::Liveness { within },
            holds,
        }
    }

    /// Returns what a breach of this invariant is reported as.
    pub fn name(&self) -> &Reason {
        &self.name
    }

    /// Returns which promise this invariant makes.
    pub fn kind(&self) -> Kind {
        self.kind
    }
}

impl fmt::Debug for Invariant {
    /// Leaves out the function it checks: what a function pointer prints is its address, which
    /// moves from run to run, and nothing an engine writes down may.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Invariant")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// An invariant broken, the first step it was broken at, and the state that step left the run in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    invariant: Reason,
    step: usize,
    state: StateHash,
}

impl Violation {
    /// Returns the name of the invariant that was broken.
    pub fn invariant(&self) -> &Reason {
        &self.invariant
    }

    /// Returns the first step of the trace the invariant was broken at.
    pub fn step(&self) -> usize {
        self.step
    }

    /// Returns the name of the state that step left the run in.
    pub fn state(&self) -> StateHash {
        self.state
    }
}

impl fmt::Display for Violation {
    /// Writes `step <n> in state <hash> broke: <invariant>`, the name last since it is the one part
    /// whose length is not fixed — the order a trace's own records keep.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "step {} in state {} broke: {}",
            self.step, self.state, self.invariant
        )
    }
}

impl From<Violation> for Outcome {
    /// A breach is a failure like any other: what broke, and the step it broke at.
    fn from(violation: Violation) -> Self {
        Self::Fail {
            reason: violation.invariant,
            step: violation.step,
        }
    }
}

/// Why a trace could not be checked.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CheckError {
    /// The store does not hold the state a step names.
    Missing {
        /// The step naming it.
        step: usize,
        /// The state it names.
        state: StateHash,
    },
    /// The state a step names is not a world.
    Decode {
        /// The step naming it.
        step: usize,
        /// What is wrong with it.
        error: DecodeWorldError,
    },
}

impl fmt::Display for CheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { step, state } => {
                write!(
                    f,
                    "step {step} names state {state}, which the store does not hold"
                )
            }
            Self::Decode { step, error } => write!(f, "step {step} is not a world: {error}"),
        }
    }
}

impl std::error::Error for CheckError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Missing { .. } => None,
            Self::Decode { error, .. } => Some(error),
        }
    }
}

/// Checks every step of `trace` against `invariants`, reading each step's world out of `store`.
///
/// Returns each broken invariant's first breach, ordered by step and then by where the invariant
/// stands in `invariants`; an empty list means every one held. A trace of no steps breaks nothing,
/// since there is no state for anything to fail of.
///
/// ```
/// use chronoloop::clock::VirtualTime;
/// use chronoloop::history::Entry;
/// use chronoloop::invariant::{self, Invariant};
/// use chronoloop::outcome::{Outcome, Reason};
/// use chronoloop::store::StateStore;
/// use chronoloop::trace::{Step, Trace};
/// use chronoloop::world::{Name, Resource, Snapshot, Value, World};
///
/// /// The cache is never cold.
/// fn warm(world: &World) -> bool {
///     let (Ok(cache), Ok(warm)) = (Name::new("cache"), Name::new("warm")) else {
///         return false;
///     };
///     world.get(&cache).and_then(|cache| cache.get(&warm)) != Some(&Value::Flag(false))
/// }
///
/// let mut store = StateStore::new();
/// let mut step = |at, warm| -> Result<Step, Box<dyn std::error::Error>> {
///     let cache = Resource::new().with_field(Name::new("warm")?, Value::Flag(warm));
///     let world = World::new().with_resource(Name::new("cache")?, cache);
///     let event = Entry::new(VirtualTime::from_nanos(at), "the cache moved")?;
///     Ok(Step::new(event, store.insert(&world.snapshot())))
/// };
/// let trace = Trace::new(7, vec![step(0, true)?, step(5, false)?, step(9, true)?]);
///
/// let never_cold = Invariant::safety(Reason::new("the cache went cold")?, warm);
/// let broken = invariant::check(&trace, &store, &[never_cold])?;
///
/// assert_eq!(broken.len(), 1);
/// assert_eq!(broken[0].step(), 1);
/// assert_eq!(broken[0].state(), trace.steps()[1].state());
/// assert_eq!(
///     Outcome::from(broken[0].clone()).to_string(),
///     "failed at step 1: the cache went cold"
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Errors
///
/// Returns [`CheckError`] if `store` does not hold a state the trace names, or holds one that is not
/// a world.
pub fn check(
    trace: &Trace,
    store: &StateStore,
    invariants: &[Invariant],
) -> Result<Vec<Violation>, CheckError> {
    let mut watches: Vec<Watch> = invariants.iter().map(|_| Watch::Holding).collect();
    let mut last = None;
    for (step, recorded) in trace.steps().iter().enumerate() {
        let state = recorded.state();
        let node = store
            .get(state)
            .ok_or(CheckError::Missing { step, state })?;
        let world = World::try_from(&node).map_err(|error| CheckError::Decode { step, error })?;
        let at = recorded.event().at();
        for (invariant, watch) in invariants.iter().zip(&mut watches) {
            watch.step(invariant, &world, (step, state), at, last);
        }
        last = Some((step, state));
    }

    let mut broken: Vec<Violation> = invariants
        .iter()
        .zip(watches)
        .filter_map(|(invariant, watch)| {
            let (step, state) = match watch {
                Watch::Broken(step, state) => (step, state),
                // A stretch still open when the run stopped is a recovery that never came.
                Watch::Down(_) => last?,
                Watch::Holding => return None,
            };
            Some(Violation {
                invariant: invariant.name.clone(),
                step,
                state,
            })
        })
        .collect();
    // A stable sort of a list built in the slice's order, so breaches at one step keep that order.
    broken.sort_by_key(Violation::step);
    Ok(broken)
}

/// Where one invariant stands, partway through a trace.
#[derive(Debug, Clone, Copy)]
enum Watch {
    /// Nothing is wrong.
    Holding,
    /// A liveness stretch is open, and must close by this instant — or by the end of the run, if
    /// the span reaches past the end of virtual time.
    Down(Option<VirtualTime>),
    /// Broken at this step, in this state; nothing later changes that.
    Broken(usize, StateHash),
}

impl Watch {
    /// Moves on to `now`, a step and the state it names, which happened at `at` and left the run in
    /// `world`; `last` is the step before it, if there was one.
    fn step(
        &mut self,
        invariant: &Invariant,
        world: &World,
        now: (usize, StateHash),
        at: VirtualTime,
        last: Option<(usize, StateHash)>,
    ) {
        if let Self::Broken(..) = self {
            return;
        }
        // A deadline that has gone by was gone by while the last step's state stood.
        if let (Self::Down(Some(deadline)), Some((step, state))) = (*self, last)
            && at > deadline
        {
            *self = Self::Broken(step, state);
            return;
        }
        let holds = (invariant.holds)(world);
        *self = match (invariant.kind, holds, *self) {
            (_, true, _) => Self::Holding,
            (Kind::Safety, false, _) => Self::Broken(now.0, now.1),
            (Kind::Liveness { .. }, false, Self::Down(deadline)) => Self::Down(deadline),
            (Kind::Liveness { within }, false, _) => Self::Down(at.checked_add(within)),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::Entry;
    use crate::trace::Step;
    use crate::world::{Name, Node, Resource, Snapshot, Value};

    /// A name, failing the test if it is not one.
    fn name(text: &str) -> Name {
        Name::new(text).unwrap_or_else(|e| panic!("{text:?} is a name: {e}"))
    }

    /// A reason, failing the test if it is not one.
    fn reason(text: &str) -> Reason {
        Reason::new(text).unwrap_or_else(|e| panic!("{text:?} is a reason: {e}"))
    }

    /// A world whose one resource holds the flags `a` and `b`.
    fn world(a: bool, b: bool) -> World {
        World::new().with_resource(
            name("subject"),
            Resource::new()
                .with_field(name("a"), Value::Flag(a))
                .with_field(name("b"), Value::Flag(b)),
        )
    }

    /// Whether the world's flag `field` is up.
    fn flag(world: &World, field: &str) -> bool {
        world
            .get(&name("subject"))
            .and_then(|subject| subject.get(&name(field)))
            == Some(&Value::Flag(true))
    }

    fn a(world: &World) -> bool {
        flag(world, "a")
    }

    fn b(world: &World) -> bool {
        flag(world, "b")
    }

    /// A trace of one step per `(nanosecond, a, b)`, with every state it names kept in the store.
    fn traced(steps: &[(u64, bool, bool)]) -> (Trace, StateStore) {
        let mut store = StateStore::new();
        let steps = steps
            .iter()
            .map(|&(at, a, b)| {
                let event = Entry::new(VirtualTime::from_nanos(at), "moved")
                    .unwrap_or_else(|e| panic!("a one-line message: {e}"));
                Step::new(event, store.insert(&world(a, b).snapshot()))
            })
            .collect();
        (Trace::new(0, steps), store)
    }

    /// A trace of one step per `(nanosecond, a)`, with `b` always up.
    fn traced_a(steps: &[(u64, bool)]) -> (Trace, StateStore) {
        let steps: Vec<_> = steps.iter().map(|&(at, a)| (at, a, true)).collect();
        traced(&steps)
    }

    /// The steps and invariants each breach in `broken` names, in order.
    fn named(broken: &[Violation]) -> Vec<(usize, &str)> {
        broken
            .iter()
            .map(|violation| (violation.step(), violation.invariant().as_str()))
            .collect()
    }

    fn checked(trace: &Trace, store: &StateStore, invariants: &[Invariant]) -> Vec<Violation> {
        check(trace, store, invariants).unwrap_or_else(|e| panic!("the trace can be checked: {e}"))
    }

    #[test]
    fn safety_is_broken_at_the_first_step_it_does_not_hold_at() {
        // Two steps break it, so a check naming the last breach rather than the first goes red.
        let (trace, store) = traced_a(&[(0, true), (1, false), (2, true), (3, false)]);
        let broken = checked(&trace, &store, &[Invariant::safety(reason("a fell"), a)]);

        assert_eq!(named(&broken), vec![(1, "a fell")]);
        assert_eq!(broken[0].state(), trace.steps()[1].state());
    }

    #[test]
    fn safety_that_always_holds_is_never_broken() {
        let (trace, store) = traced_a(&[(0, true), (1, true)]);
        assert!(checked(&trace, &store, &[Invariant::safety(reason("a fell"), a)]).is_empty());
    }

    #[test]
    fn liveness_is_broken_only_when_a_stretch_outlasts_its_span() {
        struct Case {
            name: &'static str,
            steps: &'static [(u64, bool)],
            within: u64,
            expected: Option<usize>,
        }
        let cases = [
            Case {
                name: "never stops holding",
                steps: &[(0, true), (10, true)],
                within: 0,
                expected: None,
            },
            Case {
                name: "holds again inside the span",
                steps: &[(0, true), (10, false), (15, true)],
                within: 10,
                expected: None,
            },
            Case {
                name: "holds again exactly at the deadline",
                steps: &[(0, true), (10, false), (20, true)],
                within: 10,
                expected: None,
            },
            Case {
                // Neither the step the stretch opened at (1) nor the one it holds again at (3): the
                // state standing when the span ran out is step 2's.
                name: "holds again a nanosecond late",
                steps: &[(0, true), (10, false), (15, false), (21, true)],
                within: 10,
                expected: Some(2),
            },
            Case {
                name: "still failing when the span runs out",
                steps: &[(0, false), (11, false), (12, true)],
                within: 10,
                expected: Some(0),
            },
            Case {
                // The run uses almost none of its span and still breaks it: it stopped before it
                // recovered. The last step is named rather than the one the stretch opened at.
                name: "the run ends with a stretch open",
                steps: &[(0, true), (10, false), (12, false)],
                within: 1_000,
                expected: Some(2),
            },
            Case {
                // Measured from where the first stretch opened, the second runs out at 10; from
                // where it opened itself, at 20.
                name: "a second stretch is timed from its own opening",
                steps: &[(0, false), (5, true), (10, false), (19, true)],
                within: 10,
                expected: None,
            },
            Case {
                name: "a span too long for virtual time still ends with the run",
                steps: &[(0, true), (u64::MAX, false)],
                within: u64::MAX,
                expected: Some(1),
            },
        ];
        for case in cases {
            let (trace, store) = traced_a(case.steps);
            let late = Invariant::liveness(
                reason("a stayed down"),
                Duration::from_nanos(case.within),
                a,
            );
            let broken = checked(&trace, &store, &[late]);

            assert_eq!(
                broken.iter().map(Violation::step).collect::<Vec<_>>(),
                case.expected.into_iter().collect::<Vec<_>>(),
                "{}",
                case.name
            );
            if let Some(step) = case.expected {
                assert_eq!(
                    broken[0].state(),
                    trace.steps()[step].state(),
                    "{}",
                    case.name
                );
            }
        }
    }

    #[test]
    fn every_invariant_reports_its_own_first_breach_in_order_of_step() {
        // `a` is broken for good at step 3 and `b` is down past its span from step 1, so a check
        // that stopped at the first breach it met — or named only the earliest — loses one of them.
        let (trace, store) = traced(&[
            (0, true, true),
            (10, true, false),
            (30, true, false),
            (40, false, true),
            (50, false, true),
        ]);
        let invariants = [
            Invariant::safety(reason("a fell"), a),
            Invariant::liveness(reason("b stayed down"), Duration::from_nanos(5), b),
        ];

        assert_eq!(
            named(&checked(&trace, &store, &invariants)),
            vec![(1, "b stayed down"), (3, "a fell")]
        );
    }

    #[test]
    fn breaches_at_one_step_order_by_where_their_invariants_stand() {
        // Both break at step 1, one a safety and one a liveness, so an order taken from the kind of
        // promise rather than from the slice turns one of the two orders red.
        let (trace, store) = traced(&[(0, true, true), (10, false, false), (20, false, true)]);
        let fell = Invariant::safety(reason("a fell"), a);
        let late = Invariant::liveness(reason("b stayed down"), Duration::from_nanos(5), b);

        assert_eq!(
            named(&checked(&trace, &store, &[fell.clone(), late.clone()])),
            vec![(1, "a fell"), (1, "b stayed down")]
        );
        assert_eq!(
            named(&checked(&trace, &store, &[late, fell])),
            vec![(1, "b stayed down"), (1, "a fell")]
        );
    }

    #[test]
    fn nothing_is_broken_by_a_trace_of_no_steps() {
        let late = Invariant::liveness(reason("a stayed down"), Duration::ZERO, a);
        assert!(checked(&Trace::new(0, Vec::new()), &StateStore::new(), &[late]).is_empty());
    }

    #[test]
    fn a_state_the_check_cannot_read_is_an_error_naming_its_step() {
        let event = || {
            Entry::new(VirtualTime::ZERO, "moved")
                .unwrap_or_else(|e| panic!("a one-line message: {e}"))
        };
        let fell = [Invariant::safety(reason("a fell"), a)];

        let unknown = world(true, true).state_hash();
        let trace = Trace::new(0, vec![Step::new(event(), unknown)]);
        assert_eq!(
            check(&trace, &StateStore::new(), &fell),
            Err(CheckError::Missing {
                step: 0,
                state: unknown
            })
        );

        let mut store = StateStore::new();
        let (good, _) = traced_a(&[(0, true)]);
        let leaf: Node = Value::Flag(true).snapshot();
        let trace = Trace::new(
            0,
            vec![
                good.steps()[0].clone(),
                Step::new(event(), store.insert(&leaf)),
            ],
        );
        store.insert(&world(true, true).snapshot());
        assert_eq!(
            check(&trace, &store, &fell),
            Err(CheckError::Decode {
                step: 1,
                error: DecodeWorldError::NotAWorld
            })
        );
    }

    #[test]
    fn a_breach_is_written_on_one_line_and_fails_the_run_at_its_step() {
        let (trace, store) = traced_a(&[(0, true), (1, false)]);
        let broken = checked(&trace, &store, &[Invariant::safety(reason("a fell"), a)]);
        let violation = broken[0].clone();

        assert_eq!(
            violation.to_string(),
            format!("step 1 in state {} broke: a fell", trace.steps()[1].state())
        );
        assert_eq!(
            Outcome::from(violation),
            Outcome::Fail {
                reason: reason("a fell"),
                step: 1
            }
        );
    }

    #[test]
    fn an_invariant_is_shown_without_the_address_of_what_it_checks() {
        let late = Invariant::liveness(reason("a stayed down"), Duration::from_secs(30), a);
        assert_eq!(late.name(), &reason("a stayed down"));
        assert_eq!(
            late.kind(),
            Kind::Liveness {
                within: Duration::from_secs(30)
            }
        );
        assert_eq!(
            format!("{late:?}"),
            "Invariant { name: Reason(\"a stayed down\"), kind: Liveness { within: 30s }, .. }"
        );
        assert_eq!(Invariant::safety(reason("a fell"), a).kind(), Kind::Safety);
    }
}
