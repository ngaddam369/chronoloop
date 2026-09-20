//! Running on from a recorded step under another seed.
//!
//! [`crate::trace`] says what a run passed through and [`crate::diff`] says what moved between two
//! of its states. Both look backwards. What a recorded failure invites is the other question —
//! *what would have happened if the wire had rolled differently from step eleven onwards?* — and
//! nothing so far can answer it.
//!
//! # A fork is a re-run, not a continuation
//!
//! The executor cannot be handed a half-finished run and told to carry on: a run is tasks, waits,
//! messages in flight and a queue of events, none of which a recorded trace holds. So a fork runs
//! the seed again from the beginning and **changes what it draws from partway through**. Everything
//! up to the fork is decided exactly as it was decided the first time, because it is decided by the
//! same draws; everything after it is decided by another seed.
//!
//! That is what makes the prefix real rather than approximate. A fork is not a guess at where the
//! run had got to — it *is* the run, up to the instant the fork names.
//!
//! # Strictly after
//!
//! A [`Fork`] names an instant, not a step. A step is the system's notion and the engine has none;
//! what the engine has is the clock, and a trace's step carries the instant it happened at, which is
//! what [`fork`] reads out of it. Every generator in the run then changes at the same instant, so
//! "everything decided after this instant comes from another seed" is one run-wide claim rather than
//! a different one per generator.
//!
//! A draw made **at** that instant still comes from the original seed. Every draw that decided
//! anything up to and including the step forked at was made at an instant no later than it, so the
//! prefix is preserved by construction rather than by inspection. Two consequences, neither of them
//! hidden:
//!
//! - A later step that happens to share the instant is preserved too. A fork takes hold at the
//!   first draw after the instant, not at the step number.
//! - A delay drawn before the instant still lands where it was going to land. A fork changes what is
//!   decided after it, not what was already decided and is still on its way — the same rule as a
//!   partition being consulted when a message is sent rather than when it arrives.
//!
//! # The key moves and the place in the stream does not
//!
//! [`ForkedRng`] changes seed through [`SeededRng::reseeded`], which replaces the key and leaves the
//! generator standing where it stood. So forking to the seed the run already had draws precisely
//! what the run would have drawn, and the fork is a no-op — not because a case says so, but because
//! that is what the cipher does. It is the cheapest check there is on the whole mechanism: a fork
//! that cannot reproduce its own run has nothing to say about any other.
//!
//! Each generator is given a post-fork seed of its own, drawn from the fork's seed in the order the
//! run drew its first ones. Handing every generator the fork's seed outright would put them all on
//! one key at different places in one stream, which is the correlation the run already avoids by
//! giving the wire and each task a generator apiece. Deriving the second set the same way as the
//! first is also what keeps the no-op: when the fork's seed is the run's own, the second set is the
//! first set.
//!
//! # No written form
//!
//! [`Recording`], [`FaultSchedule`] and [`Trace`] all read back from the form they write, because
//! each is an input somewhere — a run replays one, a wire consults one, a scrubber indexes one. A
//! fork is an input too, but it arrives as a step and a seed someone asked for, not as a file. There
//! is no [`FromStr`] here, and no [`Display`] either, until something reads or writes one.
//!
//! [`Display`]: fmt::Display
//! [`FaultSchedule`]: crate::fault::FaultSchedule
//! [`FromStr`]: core::str::FromStr
//! [`Recording`]: crate::history::Recording
//! [`Trace`]: crate::trace::Trace

use core::fmt;
use core::ops::RangeInclusive;
use core::time::Duration;

use crate::clock::{Clock, VirtualTime};
use crate::rng::{Rng, SeededRng};
use crate::trace::Trace;

/// Where a run's entropy changes, and what it changes to.
///
/// The instant is the one a step of a recorded trace happened at, which [`fork`] is the way to get.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fork {
    at: VirtualTime,
    seed: u64,
}

impl Fork {
    /// Creates a fork that changes the run's seed to `seed` after `at`.
    pub fn new(at: VirtualTime, seed: u64) -> Self {
        Self { at, seed }
    }

    /// Returns the instant after which the run draws from the fork's seed.
    pub fn at(&self) -> VirtualTime {
        self.at
    }

    /// Returns the seed the run draws from once it is past the instant.
    pub fn seed(&self) -> u64 {
        self.seed
    }
}

/// The step a fork was asked for is one the run never reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnreachedStep {
    /// The step that was asked for.
    pub step: usize,
    /// How many steps the run took, so the last of them is one fewer.
    pub steps: usize,
}

impl fmt::Display for UnreachedStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the run never reached step {}; it took {} steps",
            self.step, self.steps
        )
    }
}

impl std::error::Error for UnreachedStep {}

/// Returns the fork that runs `trace`'s seed again and draws from `seed` after step `step`.
///
/// The step is where the two timelines part: it and everything before it are the run's own, and what
/// follows is drawn from elsewhere. What comes back is a value a run takes; running it is the
/// system's job, since only a system knows how to build itself.
///
/// # Errors
///
/// Returns [`UnreachedStep`] if the run never got as far as `step`.
pub fn fork(trace: &Trace, step: usize, seed: u64) -> Result<Fork, UnreachedStep> {
    trace
        .at(step)
        .map(|reached| Fork::new(reached.event().at(), seed))
        .ok_or(UnreachedStep {
            step,
            steps: trace.steps().len(),
        })
}

/// A generator that changes key once the run has gone past an instant.
///
/// Before the instant it is the generator it was given; after it, one of another seed standing in
/// the same place in the stream. A run forks by building one of these in place of each of its
/// [`SeededRng`]s, the second seed for each drawn from the fork's seed in the order the first ones
/// were drawn from the run's:
///
/// ```
/// use chronoloop::clock::VirtualTime;
/// use chronoloop::fork::{Fork, ForkedRng};
/// use chronoloop::executor::Executor;
/// use chronoloop::rng::{Rng, SeededRng};
///
/// let fork = Fork::new(VirtualTime::from_nanos(1_000), 99);
/// let mut seeds = SeededRng::from_seed(7);
/// let mut after = SeededRng::from_seed(fork.seed());
/// let base = seeds.next_u64();
///
/// // The run has not started, so it is not past the instant and draws what it always would.
/// let executor = Executor::new();
/// let mut wire = ForkedRng::new(executor.handle(), fork, base.into(), &mut after);
/// assert_eq!(wire.next_u64(), SeededRng::from_seed(base).next_u64());
/// ```
pub struct ForkedRng<C> {
    clock: C,
    at: VirtualTime,
    rng: SeededRng,
    /// The seed to change to, taken the first time the run is found to be past the instant.
    ///
    /// Clearing it says the fork has happened, and saves rebuilding the generator on every draw
    /// after it. It is not what keeps the new stream going: changing key keeps the place, so
    /// putting the same key back where it already stands would change nothing anyway.
    after: Option<u64>,
}

impl<C> fmt::Debug for ForkedRng<C> {
    /// Written without the clock, which a run's handle does not describe.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForkedRng")
            .field("at", &self.at)
            .field("seed", &self.rng.seed())
            .field("after", &self.after)
            .finish_non_exhaustive()
    }
}

impl<C: Clock> ForkedRng<C> {
    /// Creates a generator that draws from `before` until `clock` is past `fork`'s instant, and
    /// from the next seed `after` hands out once it is.
    ///
    /// `after` is a generator of the fork's own seed, shared by every generator in the run so that
    /// each takes a different seed from it — in the order the run took their first ones. That is
    /// what keeps two of them off one key, and what makes a fork to the run's own seed change
    /// nothing at all.
    pub fn new(clock: C, fork: Fork, before: SeededRng, after: &mut SeededRng) -> Self {
        Self {
            clock,
            at: fork.at(),
            rng: before,
            after: Some(after.next_u64()),
        }
    }

    /// Returns the generator to draw from, changing key first if the run is past the instant.
    ///
    /// A draw made at the instant itself still comes from the seed the run started with — see the
    /// module's docs for why the prefix depends on it.
    fn drawing(&mut self) -> &mut SeededRng {
        if let Some(seed) = self.after
            && self.clock.now() > self.at
        {
            self.rng = self.rng.reseeded(seed);
            self.after = None;
        }
        &mut self.rng
    }
}

impl<C: Clock> Rng for ForkedRng<C> {
    fn next_u64(&mut self) -> u64 {
        self.drawing().next_u64()
    }

    fn range(&mut self, bounds: RangeInclusive<u64>) -> u64 {
        self.drawing().range(bounds)
    }

    fn chance(&mut self, numerator: u32, denominator: u32) -> bool {
        self.drawing().chance(numerator, denominator)
    }

    fn duration_in(&mut self, bounds: RangeInclusive<Duration>) -> Duration {
        self.drawing().duration_in(bounds)
    }
}

#[cfg(test)]
mod tests {
    use core::cell::Cell;
    use std::rc::Rc;

    use super::*;
    use crate::history::Entry;
    use crate::trace::Step;
    use crate::world::{Snapshot, World};

    /// How many draws a case compares before it is satisfied two streams are the same stream.
    const DRAWS: usize = 20;

    /// The seed the run below was given.
    const SEED: u64 = 7;

    /// The seed a fork changes it to.
    const OTHER: u64 = 99;

    /// A clock whose hands a case moves, so a draw can be made on either side of a fork.
    ///
    /// Nothing here waits, so the wait it hands back is a stub: the cases are about what a generator
    /// does when it is asked the time, not about what happens when a task sleeps.
    #[derive(Clone)]
    struct Dial(Rc<Cell<VirtualTime>>);

    impl Dial {
        fn at(nanos: u64) -> Self {
            Self(Rc::new(Cell::new(VirtualTime::from_nanos(nanos))))
        }

        fn set(&self, nanos: u64) {
            self.0.set(VirtualTime::from_nanos(nanos));
        }
    }

    impl Clock for Dial {
        type Sleep = core::future::Ready<()>;

        fn now(&self) -> VirtualTime {
            self.0.get()
        }

        fn sleep_until(&self, deadline: VirtualTime) -> Self::Sleep {
            let _ = deadline;
            core::future::ready(())
        }
    }

    /// A trace of `instants`, each step in a state of its own so no two of them are alike.
    fn trace(instants: &[u64]) -> Trace {
        let steps = instants
            .iter()
            .enumerate()
            .map(|(number, nanos)| {
                let at = VirtualTime::from_nanos(*nanos);
                let event = Entry::new(at, format!("step {number} happened"))
                    .unwrap_or_else(|e| panic!("a test message is one line: {e}"));
                Step::new(event, World::new().snapshot().state_hash())
            })
            .collect();
        Trace::new(SEED, steps)
    }

    /// The seed a fork of `seed` hands to the first generator that asks for one.
    fn handed_out(seed: u64) -> u64 {
        SeededRng::from_seed(seed).next_u64()
    }

    /// A generator wrapped so that it changes to `fork`'s seed once `dial` is past the instant.
    fn forked(dial: &Dial, fork: Fork) -> ForkedRng<Dial> {
        let mut after = SeededRng::from_seed(fork.seed());
        ForkedRng::new(dial.clone(), fork, SeededRng::from_seed(SEED), &mut after)
    }

    /// `count` draws from a plain generator of `seed`, the route a run that never forked takes.
    fn plain(seed: u64, count: usize) -> Vec<u64> {
        let mut rng = SeededRng::from_seed(seed);
        (0..count).map(|_| rng.next_u64()).collect()
    }

    #[test]
    fn a_fork_names_the_instant_the_step_happened_at() {
        struct Case {
            name: &'static str,
            step: usize,
            want: u64,
        }
        let instants = [0, 1_500, 1_500, 90_000];
        let cases = [
            Case {
                name: "the first step",
                step: 0,
                want: 0,
            },
            Case {
                name: "a step partway through",
                step: 1,
                want: 1_500,
            },
            Case {
                name: "a step sharing the instant before it",
                step: 2,
                want: 1_500,
            },
            Case {
                name: "the last step",
                step: 3,
                want: 90_000,
            },
        ];
        let trace = trace(&instants);
        for case in cases {
            assert_eq!(
                fork(&trace, case.step, OTHER),
                Ok(Fork::new(VirtualTime::from_nanos(case.want), OTHER)),
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn a_step_the_run_never_reached_is_named_rather_than_guessed_at() {
        struct Case {
            name: &'static str,
            instants: &'static [u64],
            step: usize,
            /// What the report says, which is what a person asking for the step is given.
            want: &'static str,
        }
        let cases = [
            Case {
                name: "one past the end",
                instants: &[0, 1_500, 90_000],
                step: 3,
                want: "the run never reached step 3; it took 3 steps",
            },
            Case {
                name: "far past the end",
                instants: &[0, 1_500, 90_000],
                step: 4_000,
                want: "the run never reached step 4000; it took 3 steps",
            },
            Case {
                name: "any step of a run that took none",
                instants: &[],
                step: 0,
                want: "the run never reached step 0; it took 0 steps",
            },
        ];
        for case in cases {
            let reported = fork(&trace(case.instants), case.step, OTHER)
                .expect_err("a step past the end is not a fork");
            assert_eq!(reported.to_string(), case.want, "{}", case.name);
        }
    }

    #[test]
    fn a_draw_at_the_fork_is_the_draw_the_run_would_have_made() {
        // The prefix rests on this. Everything that decided a step was drawn no later than the
        // instant the step happened at, so a draw made *at* the instant has to be the run's own; one
        // made after it is the fork's.
        //
        // This case is most of what holds the rule. Taking the fork at the instant instead of after
        // it leaves every run-level case green but one — a draw at the instant only reaches what
        // comes after the instant, so the prefix survives it and only the recorded text notices.
        let dial = Dial::at(1_000);
        let mut rng = forked(&dial, Fork::new(VirtualTime::from_nanos(1_000), OTHER));

        let before: Vec<u64> = (0..DRAWS).map(|_| rng.next_u64()).collect();
        assert_eq!(
            before,
            plain(SEED, DRAWS),
            "a draw at the instant comes from the seed the run started with"
        );

        dial.set(1_001);
        let after: Vec<u64> = (0..DRAWS).map(|_| rng.next_u64()).collect();
        // Reached by drawing forward from the new seed rather than by seeking, so neither side of
        // this comes from the code under test.
        let carried_on: Vec<u64> = plain(handed_out(OTHER), DRAWS * 2)[DRAWS..].to_vec();
        assert_eq!(
            after, carried_on,
            "the first draw past the instant comes from the fork's seed, in the place the run had \
             reached"
        );
    }

    #[test]
    fn a_fork_the_run_never_reaches_changes_nothing() {
        struct Case {
            name: &'static str,
            at: u64,
            reached: u64,
        }
        let cases = [
            Case {
                name: "an instant the run stopped short of",
                at: 90_000,
                reached: 89_999,
            },
            Case {
                name: "the end of virtual time",
                at: u64::MAX,
                reached: u64::MAX - 1,
            },
        ];
        for case in cases {
            let dial = Dial::at(case.reached);
            let mut rng = forked(&dial, Fork::new(VirtualTime::from_nanos(case.at), OTHER));
            let drawn: Vec<u64> = (0..DRAWS).map(|_| rng.next_u64()).collect();
            assert_eq!(drawn, plain(SEED, DRAWS), "{}", case.name);
        }
    }

    #[test]
    fn the_fork_takes_up_the_stream_where_the_run_left_it() {
        // What this holds is the place, not the number of times the key is taken up. Changing key
        // on *every* draw past the instant was tried and leaves the whole suite green, because
        // reseeding keeps the position: the second change puts the same key back where it already
        // stood. So taking the seed once is intent and cost rather than correctness, and the case
        // below cannot be made to say otherwise. What it does catch is a fork that never happens.
        let dial = Dial::at(0);
        let mut rng = forked(&dial, Fork::new(VirtualTime::ZERO, OTHER));

        dial.set(1);
        let drawn: Vec<u64> = (1..=DRAWS as u64)
            .map(|step| {
                // The clock keeps moving, which is what a run's clock does; the key must not keep
                // moving with it.
                dial.set(step);
                rng.next_u64()
            })
            .collect();

        assert_eq!(
            drawn,
            plain(handed_out(OTHER), DRAWS),
            "every draw past the instant comes from the fork's seed, one after another"
        );
    }

    #[test]
    fn every_kind_of_draw_goes_through_the_fork() {
        // Four draws reach a generator and each of them has to ask the clock. One that did not would
        // leave a run forking in its delays and not in its coin flips, which is a run neither seed
        // produced.
        let bounds = Duration::from_millis(1)..=Duration::from_secs(2);
        let dial = Dial::at(1);
        let fork = Fork::new(VirtualTime::ZERO, OTHER);

        let mut forked_range = forked(&dial, fork);
        let mut forked_chance = forked(&dial, fork);
        let mut forked_duration = forked(&dial, fork);

        let mut want_range = SeededRng::from_seed(handed_out(OTHER));
        let mut want_chance = SeededRng::from_seed(handed_out(OTHER));
        let mut want_duration = SeededRng::from_seed(handed_out(OTHER));
        let mut unforked_range = SeededRng::from_seed(SEED);
        let mut unforked_chance = SeededRng::from_seed(SEED);
        let mut unforked_duration = SeededRng::from_seed(SEED);

        let ranges: Vec<u64> = (0..DRAWS).map(|_| forked_range.range(1..=1_000)).collect();
        let chances: Vec<bool> = (0..DRAWS).map(|_| forked_chance.chance(1, 2)).collect();
        let durations: Vec<Duration> = (0..DRAWS)
            .map(|_| forked_duration.duration_in(bounds.clone()))
            .collect();

        assert_eq!(
            ranges,
            (0..DRAWS)
                .map(|_| want_range.range(1..=1_000))
                .collect::<Vec<_>>(),
            "a bounded draw comes from the fork's seed"
        );
        assert_eq!(
            chances,
            (0..DRAWS)
                .map(|_| want_chance.chance(1, 2))
                .collect::<Vec<_>>(),
            "a coin flip comes from the fork's seed"
        );
        assert_eq!(
            durations,
            (0..DRAWS)
                .map(|_| want_duration.duration_in(bounds.clone()))
                .collect::<Vec<_>>(),
            "a delay comes from the fork's seed"
        );

        assert_ne!(
            ranges,
            (0..DRAWS)
                .map(|_| unforked_range.range(1..=1_000))
                .collect::<Vec<_>>(),
            "a bounded draw that ignored the fork would be the run's own"
        );
        assert_ne!(
            chances,
            (0..DRAWS)
                .map(|_| unforked_chance.chance(1, 2))
                .collect::<Vec<_>>(),
            "a coin flip that ignored the fork would be the run's own"
        );
        assert_ne!(
            durations,
            (0..DRAWS)
                .map(|_| unforked_duration.duration_in(bounds.clone()))
                .collect::<Vec<_>>(),
            "a delay that ignored the fork would be the run's own"
        );
    }
}
