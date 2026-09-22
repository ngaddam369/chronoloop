//! Reducing a failing run to the faults it actually needed.
//!
//! [`crate::outcome`] gave a run a verdict, and [`crate::systems::quorum`] a system that produces
//! one. Neither of them makes a failure *readable*. A blind sweep finds a seed by throwing trouble at
//! a run, and what it hands back is whatever trouble it happened to throw — a list in which the few
//! faults that caused the failure sit among many that did not. This module is what turns that list
//! into a repro: it drops entries, narrows windows and lowers odds, running the system again after
//! every candidate and keeping only the candidates whose run still fails **the same way**.
//!
//! # What a candidate has to be
//!
//! Every move here is a smaller [`FaultSchedule`], which is the whole reason a schedule is a value
//! written to a file rather than a sequence of calls on a network: reducing works by making a
//! *smaller thing of the same kind*, and there is no smaller version of a method call. Two properties
//! of the schedule carry this module and must not be quietly changed — it is an ordered **list**, so
//! dropping the nth entry is well defined; and where two faults overlap the **first** one applies, so
//! dropping one has a predictable effect on the next.
//!
//! # Same failure, not same step
//!
//! A candidate is kept when its outcome [`reproduces`] the failure being reduced, which compares the
//! reasons and not the steps. That is not a convenience: the faults a failure never needed still cost
//! the run draws and still move messages about, so almost every sound reduction moves the step it
//! surfaces at. [`crate::outcome`] carries the argument in full.
//!
//! # What is reduced, and what is not
//!
//! The **seed is not reduced.** There is no smaller version of a seed, and the run under one is what
//! the whole reduction is defined against; the caller keeps hold of it and the predicate closes over
//! it.
//!
//! **Nothing here draws.** The reduction is a pure function of the schedule it is given and the
//! answers the predicate gives, so a repro it produces is reached the same way on every machine that
//! runs it. What a *run* draws is the run's business.
//!
//! # A bisection finds a narrow window, not the narrowest
//!
//! A run is not monotone in the span of a window: narrowing an outage changes which messages get
//! through, and so changes every draw after it. The bisections below therefore find *a* small value
//! rather than proving the smallest one, which is true of every shrinker there is. What they do
//! guarantee is that whatever comes back was run and still failed the same way.
//!
//! [`reproduces`]: Outcome::reproduces

use core::ops::Range;

use crate::clock::VirtualTime;
use crate::fault::{Fault, FaultSchedule, Window};
use crate::net::Odds;
use crate::outcome::Outcome;

/// A failing run reduced to the faults it needs.
///
/// The outcome is the failure of the run the **kept** schedule produces, not of the run that was
/// handed in. The two differ: reducing a schedule moves the step a failure surfaces at, and the step
/// worth reporting is the one belonging to the schedule being kept, since that is the run a reader
/// will go and look at.
///
/// There is deliberately no written form here. A repro is a seed, a schedule and the failure to
/// expect, and a format for one invented before anything writes one would be a format shaped by
/// guesswork; [`FaultSchedule`] and [`Outcome`] already write themselves.
#[derive(Debug)]
pub struct Reduction {
    schedule: FaultSchedule,
    outcome: Outcome,
}

impl Reduction {
    /// Returns the faults the failure still needs.
    pub fn schedule(&self) -> &FaultSchedule {
        &self.schedule
    }

    /// Returns the failure the kept schedule produces.
    pub fn outcome(&self) -> &Outcome {
        &self.outcome
    }
}

/// Reduces `faults` to the fewest and smallest that still fail the way they did.
///
/// `run` is the run under reduction with its seed already closed over: it takes a candidate schedule
/// and says how that run went. Returns [`None`] when the run under `faults` held up, which is a
/// genuine absence rather than a failure — there is no failure to reduce.
///
/// The failure being reduced towards is the one this function observes itself, through the same
/// predicate every candidate goes through, so a caller cannot ask it to chase a failure the predicate
/// does not produce.
///
/// ```
/// use chronoloop::fault::FaultSchedule;
/// use chronoloop::outcome::{Outcome, Reason};
/// use chronoloop::shrink::shrink;
///
/// // Two outages, of which the failure below only cares about the first.
/// let faults: FaultSchedule = "chronoloop faults\n\
///                              partition on node 0 -> node 1 from 0.000000000s until forever\n\
///                              partition on node 2 -> node 3 from 0.000000000s until forever\n"
///     .parse()?;
/// let cut = Reason::new("the way out is cut")?;
///
/// let reduced = shrink(&faults, |candidate| {
///     Ok::<_, core::convert::Infallible>(
///         if candidate.to_string().contains("node 0 -> node 1") {
///             Outcome::Fail { reason: cut.clone(), step: 0 }
///         } else {
///             Outcome::Pass
///         },
///     )
/// })?
/// .ok_or("the run failed, so there is something to reduce")?;
///
/// // The outage the failure never needed is gone, and the one it did keep has been narrowed as far
/// // as this predicate lets it — which is all the way, since it only ever looks for the link.
/// assert_eq!(
///     reduced.schedule().to_string(),
///     "chronoloop faults\n\
///      partition on node 0 -> node 1 from 0.000000000s until 0.000000000s\n"
/// );
/// assert_eq!(reduced.outcome().to_string(), "failed at step 0: the way out is cut");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn shrink<E, F>(faults: &FaultSchedule, mut run: F) -> Result<Option<Reduction>, E>
where
    F: FnMut(&FaultSchedule) -> Result<Outcome, E>,
{
    let failure = run(faults)?;
    if failure == Outcome::Pass {
        return Ok(None);
    }
    let mut reducing = Reducing {
        run,
        failure: failure.clone(),
        kept: faults.faults().to_vec(),
        outcome: failure,
    };

    // Round and round until a whole pass changes nothing. This terminates because every candidate
    // that is kept strictly lowers (entries, total span, total odds) read left to right: dropping
    // takes an entry out, narrowing leaves the count alone and shortens a span, and lowering leaves
    // both alone and takes an occurrence off.
    //
    // The loop is not decoration. A window narrowed until it holds nothing is a fault that does
    // nothing, and only a later pass can drop it. It is also cheap: a bisection over a span already
    // down to one instant costs a single candidate.
    loop {
        let before = reducing.kept.clone();
        reducing.drop_entries()?;
        reducing.narrow_windows()?;
        reducing.lower_odds()?;
        if reducing.kept == before {
            break;
        }
    }

    Ok(Some(Reduction {
        schedule: FaultSchedule::new(reducing.kept),
        outcome: reducing.outcome,
    }))
}

/// A reduction in progress: the failure to keep hold of, and the smallest schedule that still gets it.
struct Reducing<F> {
    /// The run under reduction, with its seed already closed over.
    run: F,
    /// The failure every candidate is measured against. It never moves.
    failure: Outcome,
    /// The smallest schedule so far that still fails that way.
    kept: Vec<Fault>,
    /// The failure `kept` produces, which is the one reported at the end.
    outcome: Outcome,
}

impl<E, F> Reducing<F>
where
    F: FnMut(&FaultSchedule) -> Result<Outcome, E>,
{
    /// Runs `candidate` and keeps it if the failure came back, saying whether it did.
    ///
    /// This is the only thing that writes the kept schedule, and it writes it only for a candidate
    /// that was run and still failed. That is what makes the whole reduction sound rather than any of
    /// the searching below: a bisection left with a bound in the wrong place costs an instant of
    /// accuracy and can still only hand back a schedule that was tried.
    ///
    /// The kept schedule and the outcome move together, which is what makes the outcome handed back
    /// at the end the outcome of the schedule handed back with it.
    fn keeps(&mut self, candidate: Vec<Fault>) -> Result<bool, E> {
        let outcome = (self.run)(&FaultSchedule::new(candidate.clone()))?;
        if !outcome.reproduces(&self.failure) {
            return Ok(false);
        }
        self.kept = candidate;
        self.outcome = outcome;
        Ok(true)
    }

    /// Drops the entries the failure does not need, by delta debugging over the list.
    ///
    /// Delta debugging after Zeller and Hildebrandt, over **contiguous** chunks so that the order the
    /// schedule was given in survives — the first of two overlapping faults is the one that applies,
    /// so a reduction that reordered the list would change what the entries it kept mean. Each chunk
    /// is offered for removal in turn; a removal that keeps the failure is taken and the shorter list
    /// is cut into one piece fewer, and a round in which none is taken doubles the number of chunks.
    /// At the finest
    /// granularity a chunk is one entry, and reaching the end of that round is what leaves a list from
    /// which no single entry can be taken.
    ///
    /// The published algorithm has a coarser step before this one — try each chunk *on its own*, and
    /// when one of them carries the whole failure everything outside it goes in a single move. It is
    /// **not** here, and that was settled by measurement rather than by argument: it reduces the same
    /// schedules to the same entries, and it cost more runs on every schedule it was tried against,
    /// clustered faults and scattered ones alike. The reason it cannot pay for itself here is worth
    /// keeping — if a chunk on its own still fails, then removing the other chunks one at a time gets
    /// to the same place, so the coarse step mostly pays for a first guess that was wrong. Re-open it
    /// with new measurements, never with an argument.
    fn drop_entries(&mut self) -> Result<(), E> {
        // The empty schedule, which the partitions below never produce: a failure that comes back
        // with nothing injected at all is a failure the schedule never caused, and it is the cheapest
        // question there is.
        if !self.kept.is_empty() && self.keeps(Vec::new())? {
            return Ok(());
        }

        let mut chunks = 2;
        'reducing: while self.kept.len() > 1 {
            chunks = chunks.min(self.kept.len());
            let spans = spans(self.kept.len(), chunks);

            for span in &spans {
                let mut without = self.kept.clone();
                without.drain(span.clone());
                if self.keeps(without)? {
                    // One piece fewer to cut the shorter list into, and never fewer than two, so the
                    // next round is no coarser than the one that just worked.
                    chunks = (chunks - 1).max(2);
                    continue 'reducing;
                }
            }

            if chunks >= self.kept.len() {
                break;
            }
            chunks = chunks.saturating_mul(2).min(self.kept.len());
        }
        Ok(())
    }

    /// Narrows every window towards the span the failure needs.
    ///
    /// The end comes in first and the start after it, because narrowing the end is what leaves the
    /// start a small range to be searched in rather than the whole of the original window.
    fn narrow_windows(&mut self) -> Result<(), E> {
        for index in 0..self.kept.len() {
            self.narrow_end(index)?;
            self.narrow_start(index)?;
        }
        Ok(())
    }

    /// Brings the end of the window at `index` in as far as the failure allows.
    ///
    /// A bisection, so a window that never closes costs the sixty-four candidates of the whole span
    /// of virtual time rather than being cut an instant at a time. The bounds are a search and
    /// nothing more — what comes back is whatever [`Reducing::keeps`] last accepted.
    fn narrow_end(&mut self, index: usize) -> Result<(), E> {
        let Some(during) = self.kept.get(index).map(window_of) else {
            return Ok(());
        };
        let mut low = during.start().as_nanos();
        let mut high = during.end().as_nanos();
        while low < high {
            let mid = low + (high - low) / 2;
            if self.keeps(narrowed(&self.kept, index, during.start(), t(mid)))? {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        Ok(())
    }

    /// Moves the start of the window at `index` up as far as the failure allows.
    fn narrow_start(&mut self, index: usize) -> Result<(), E> {
        let Some(during) = self.kept.get(index).map(window_of) else {
            return Ok(());
        };
        let mut low = during.start().as_nanos();
        let mut high = during.end().as_nanos();
        while low < high {
            // The upper midpoint, since the answer wanted here is the highest start that still
            // fails and the lower one would never reach it.
            let mid = low + (high - low).div_ceil(2);
            if self.keeps(narrowed(&self.kept, index, t(mid), during.end()))? {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        Ok(())
    }

    /// Lowers every fault's odds to the fewest occurrences the failure needs.
    ///
    /// Only the numerator comes down. The denominator is how many trials the odds are counted in, so
    /// moving it would change the odds in either direction rather than reduce them.
    ///
    /// Note that odds of none are not the same as no fault at all: the draw still happens, so the run
    /// spends the same amount of its seed, and a fault in force still shadows both a later fault on
    /// the same direction and whatever the link itself would have done. A fault reduced to odds of
    /// none is dropped, if it can be, by the pass that comes after this one.
    fn lower_odds(&mut self) -> Result<(), E> {
        for index in 0..self.kept.len() {
            let Some(odds) = self.kept.get(index).copied().and_then(odds_of) else {
                continue;
            };
            let mut low = 0;
            let mut high = odds.numerator();
            while low < high {
                let mid = low + (high - low) / 2;
                if self.keeps(lowered(&self.kept, index, mid))? {
                    high = mid;
                } else {
                    low = mid + 1;
                }
            }
        }
        Ok(())
    }
}

/// An instant, in the shorthand the bisections above are written in.
fn t(nanos: u64) -> VirtualTime {
    VirtualTime::from_nanos(nanos)
}

/// `len` items in `chunks` contiguous pieces, as the ranges they occupy.
///
/// The last piece is short when the two do not divide, and asking for more pieces than there is room
/// for gives as many as there is room for — which costs nothing, because the granularity rises until
/// it reaches the length and every entry is a piece of its own.
fn spans(len: usize, chunks: usize) -> Vec<Range<usize>> {
    let size = len.div_ceil(chunks.max(1)).max(1);
    (0..len)
        .step_by(size)
        .map(|start| start..start.saturating_add(size).min(len))
        .collect()
}

/// The span of simulated time a fault is in force for.
fn window_of(fault: &Fault) -> Window {
    match fault {
        Fault::Partition { during, .. } | Fault::Loss { during, .. } => *during,
    }
}

/// The odds a fault draws against, for the faults that draw.
fn odds_of(fault: Fault) -> Option<Odds> {
    match fault {
        Fault::Loss { odds, .. } => Some(odds),
        Fault::Partition { .. } => None,
    }
}

/// `kept`, with the window of the fault at `index` narrowed to `start` until `end`.
fn narrowed(kept: &[Fault], index: usize, start: VirtualTime, end: VirtualTime) -> Vec<Fault> {
    let mut candidate = kept.to_vec();
    if let Some(fault) = candidate.get_mut(index) {
        let during = window_of(fault).narrowed_to(start, end);
        *fault = match *fault {
            Fault::Partition { from, to, .. } => Fault::Partition { from, to, during },
            Fault::Loss { from, to, odds, .. } => Fault::Loss {
                from,
                to,
                during,
                odds,
            },
        };
    }
    candidate
}

/// `kept`, with the odds of the fault at `index` lowered to `numerator` occurrences.
fn lowered(kept: &[Fault], index: usize, numerator: u32) -> Vec<Fault> {
    let mut candidate = kept.to_vec();
    if let Some(Fault::Loss { odds, .. }) = candidate.get_mut(index) {
        *odds = odds.lowered_to(numerator);
    }
    candidate
}

#[cfg(test)]
mod tests {
    use core::cell::Cell;
    use core::convert::Infallible;

    use super::*;
    use crate::net::NodeId;
    use crate::outcome::Reason;

    /// The faults the reductions below start from, when a case is not about a particular shape.
    const SPREAD: u64 = 9;

    fn node(index: u64) -> NodeId {
        NodeId::from_index(index)
    }

    /// A window from `start` to `end`, failing the test rather than returning an error.
    fn window(start: u64, end: u64) -> Window {
        Window::new(t(start), t(end)).unwrap_or_else(|e| panic!("a test window is sound: {e}"))
    }

    /// Odds of `numerator` in `denominator`, failing the test rather than returning an error.
    fn odds(numerator: u32, denominator: u32) -> Odds {
        Odds::new(numerator, denominator).unwrap_or_else(|e| panic!("test odds are sound: {e}"))
    }

    fn partition(from: u64, to: u64, during: Window) -> Fault {
        Fault::Partition {
            from: node(from),
            to: node(to),
            during,
        }
    }

    fn loss(from: u64, to: u64, during: Window, odds: Odds) -> Fault {
        Fault::Loss {
            from: node(from),
            to: node(to),
            during,
            odds,
        }
    }

    /// A reason, failing the test rather than returning an error no case expects.
    fn reason(text: &str) -> Reason {
        Reason::new(text).unwrap_or_else(|e| panic!("a test reason is a reason: {e}"))
    }

    /// The failure the scripted predicates below report, at `step`.
    fn broke_at(step: usize) -> Outcome {
        Outcome::Fail {
            reason: reason("the schedule the predicate asked for"),
            step,
        }
    }

    /// Reduces `faults` against a predicate saying whether a candidate still fails.
    ///
    /// The predicate is **scripted** rather than a seeded run, which is the same choice the network's
    /// own unit tests make: a predicate that declares exactly which faults it needs makes the smallest
    /// schedule an exact expectation, where hunting for a seed whose failure happens to need three
    /// faults would pin the system instead of the reduction. `tests/shrink.rs` is where a real run
    /// answers.
    fn reduce(
        faults: &FaultSchedule,
        mut fails: impl FnMut(&FaultSchedule) -> bool,
    ) -> Option<Reduction> {
        shrink(faults, |candidate| {
            Ok::<_, Infallible>(if fails(candidate) {
                broke_at(0)
            } else {
                Outcome::Pass
            })
        })
        .unwrap_or_else(|never| match never {})
    }

    /// As `reduce`, for the cases that expect there to be something to reduce.
    fn reduced(faults: &FaultSchedule, fails: impl FnMut(&FaultSchedule) -> bool) -> FaultSchedule {
        reduce(faults, fails)
            .unwrap_or_else(|| panic!("the predicate fails under the schedule it was given"))
            .schedule
    }

    /// Whether `candidate` carries every one of `needed`, windows and odds and all.
    fn carries(candidate: &FaultSchedule, needed: &[Fault]) -> bool {
        needed
            .iter()
            .all(|fault| candidate.faults().contains(fault))
    }

    /// Nine faults on nine directions, none of which overlaps another.
    fn spread() -> Vec<Fault> {
        (0..SPREAD)
            .map(|index| partition(index, index + 1, window(0, 100)))
            .collect()
    }

    #[test]
    fn a_schedule_reduces_to_the_faults_the_failure_needs() {
        // The three the failure needs are deliberately not next to each other, so no contiguous chunk
        // holds all three and only dropping — one entry or one chunk's complement at a time — can get
        // there. The predicate names the faults down to their windows, so narrowing one is a candidate
        // it turns away: this case is about the list and nothing else.
        let all = spread();
        let needed = [all[0], all[4], all[8]];

        let reduced = reduced(&FaultSchedule::new(all), |candidate| {
            carries(candidate, &needed)
        });

        assert_eq!(
            reduced.faults(),
            needed,
            "the three it needs, still in the order they were given in"
        );
    }

    #[test]
    fn a_window_narrows_to_the_instant_the_failure_needs() {
        // A window that never closes, against a failure that needs one instant of it. The pinned text
        // is what a person reads: the outage only ever had to exist for a single nanosecond.
        let faults = FaultSchedule::new(vec![partition(
            0,
            1,
            Window::forever_from(VirtualTime::ZERO),
        )]);

        let reduced = reduced(&faults, |candidate| {
            candidate.partitioned(node(0), node(1), t(10))
        });

        assert_eq!(
            reduced.to_string(),
            "chronoloop faults\n\
             partition on node 0 -> node 1 from 0.000000010s until 0.000000011s\n"
        );
    }

    #[test]
    fn an_odds_lowers_to_the_fewest_occurrences_the_failure_needs() {
        // Four in four is always; the failure needs two of the four. The denominator does not move,
        // because it is the trials and not the odds.
        let faults = FaultSchedule::new(vec![loss(
            0,
            1,
            Window::forever_from(VirtualTime::ZERO),
            odds(4, 4),
        )]);

        let reduced = reduced(&faults, |candidate| {
            candidate
                .loss(node(0), node(1), t(10))
                .is_some_and(|odds| odds.numerator() >= 2)
        });

        assert_eq!(
            reduced.to_string(),
            "chronoloop faults\n\
             loss 2 in 4 on node 0 -> node 1 from 0.000000010s until 0.000000011s\n"
        );
    }

    #[test]
    fn a_schedule_a_run_holds_up_under_has_no_reduction() {
        // Not an error: a schedule under which nothing broke has no failure to reduce, and saying so
        // is the honest answer rather than handing back the schedule as though it had been reduced.
        let reduced = reduce(&FaultSchedule::new(spread()), |_| false);

        assert!(reduced.is_none());
    }

    #[test]
    fn a_failure_that_needs_no_faults_reduces_to_none() {
        // The one candidate delta debugging's own partitions never produce, and the one that says the
        // schedule was never the cause. Without it the reduction stops at a single fault it has no
        // reason to keep.
        let reduced = reduced(&FaultSchedule::new(spread()), |_| true);

        assert!(reduced.is_empty(), "{reduced}");
    }

    #[test]
    fn a_reduction_that_needs_a_second_pass_gets_one() {
        // Written to need one, and admitted to be contrived: it is the only way to hold the loop to
        // account, since every other case here is finished by a single pass. The failure needs the
        // way out cut at one instant, and needs the second outage only while the first is still wide
        // — so narrowing the first is what makes the second droppable, and only a later pass can drop
        // it. One pass leaves both.
        let first = partition(0, 1, Window::forever_from(VirtualTime::ZERO));
        let second = partition(2, 3, window(0, 100));

        let reduced = reduced(&FaultSchedule::new(vec![first, second]), |candidate| {
            let cut = candidate.partitioned(node(0), node(1), t(10));
            let narrowed = candidate.faults().iter().any(|fault| span_of(fault) <= 1);
            cut && (narrowed || carries(candidate, &[second]))
        });

        assert_eq!(
            reduced.to_string(),
            "chronoloop faults\n\
             partition on node 0 -> node 1 from 0.000000010s until 0.000000011s\n"
        );
    }

    /// How many instants a fault's window holds, as the cases above measure it.
    fn span_of(fault: &Fault) -> u64 {
        let during = window_of(fault);
        during.end().as_nanos() - during.start().as_nanos()
    }

    #[test]
    fn reducing_a_reduction_changes_nothing() {
        // What the fixed point is worth: a reduced schedule put through the same reduction comes back
        // as itself. Not a pure function compared with itself — the two sides are one reduction and
        // two, which is a property the loop has to earn.
        struct Case {
            name: &'static str,
            faults: Vec<Fault>,
            needs: fn(&FaultSchedule) -> bool,
        }
        let cases = [
            Case {
                name: "three of nine faults",
                faults: spread(),
                needs: |candidate| {
                    let all = spread();
                    carries(candidate, &[all[0], all[4], all[8]])
                },
            },
            Case {
                name: "one outage narrowed to an instant",
                faults: vec![partition(0, 1, Window::forever_from(VirtualTime::ZERO))],
                needs: |candidate| candidate.partitioned(node(0), node(1), t(10)),
            },
            Case {
                name: "odds lowered as far as they go",
                faults: vec![loss(0, 1, window(0, 100), odds(4, 4))],
                needs: |candidate| {
                    candidate
                        .loss(node(0), node(1), t(10))
                        .is_some_and(|odds| odds.numerator() >= 2)
                },
            },
        ];

        for case in cases {
            let once = reduced(&FaultSchedule::new(case.faults), case.needs);
            let twice = reduced(&once, case.needs);
            assert_eq!(twice.to_string(), once.to_string(), "{}", case.name);
        }
    }

    #[test]
    fn a_reduction_reports_the_failure_of_the_run_it_kept() {
        // The step belongs to the schedule handed back, not to the one handed in. A reader takes the
        // step to `inspect`, and a step from a run that is no longer on offer sends them to the wrong
        // place. Here the step counts the faults, so the two cannot be confused.
        let all = spread();
        let needed = [all[0]];
        let reduction = shrink(&FaultSchedule::new(all), |candidate| {
            Ok::<_, Infallible>(if carries(candidate, &needed) {
                broke_at(candidate.len())
            } else {
                Outcome::Pass
            })
        })
        .unwrap_or_else(|never| match never {})
        .unwrap_or_else(|| panic!("the predicate fails under the schedule it was given"));

        assert_eq!(reduction.schedule().len(), 1);
        assert_eq!(
            reduction.outcome(),
            &broke_at(1),
            "the run of one fault, not the run of nine"
        );
    }

    #[test]
    fn a_run_that_could_not_finish_stops_the_reduction() {
        // A predicate that cannot answer is not a candidate that failed to reproduce. Treating the two
        // alike would quietly drop every fault a broken run was asked about.
        let calls = Cell::new(0_usize);
        let stopped = shrink(&FaultSchedule::new(spread()), |_| {
            let call = calls.get();
            calls.set(call + 1);
            if call == 0 {
                Ok(broke_at(0))
            } else {
                Err("the run did not finish")
            }
        });

        assert_eq!(stopped.err(), Some("the run did not finish"));
        assert_eq!(calls.get(), 2, "it stopped at the candidate that broke");
    }

    #[test]
    fn a_list_is_cut_into_the_pieces_a_reduction_drops() {
        // The partitions the dropping works over. They have to cover the list exactly and in order:
        // a piece that overlapped its neighbour would have a complement that is not the list without
        // that piece, and a reordering would change what the faults kept mean.
        struct Case {
            name: &'static str,
            len: usize,
            chunks: usize,
            want: Vec<Range<usize>>,
        }
        let cases = [
            Case {
                name: "in half",
                len: 6,
                chunks: 2,
                want: vec![0..3, 3..6],
            },
            Case {
                name: "in half, with one left over",
                len: 5,
                chunks: 2,
                want: vec![0..3, 3..5],
            },
            Case {
                name: "one piece each",
                len: 3,
                chunks: 3,
                want: vec![0..1, 1..2, 2..3],
            },
            Case {
                name: "more pieces than there is room for",
                len: 2,
                chunks: 4,
                want: vec![0..1, 1..2],
            },
            Case {
                name: "nothing to cut",
                len: 0,
                chunks: 2,
                want: vec![],
            },
        ];

        for case in cases {
            let spans = spans(case.len, case.chunks);
            assert_eq!(spans, case.want, "{}", case.name);
            assert_eq!(
                spans.iter().map(ExactSizeIterator::len).sum::<usize>(),
                case.len,
                "{}: the pieces cover the list exactly once",
                case.name
            );
        }
    }
}
