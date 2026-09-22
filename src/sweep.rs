//! Running many seeds under one schedule of faults, looking for the ones that break.
//!
//! Everything [`crate::outcome`], [`crate::shrink`] and [`crate::repro`] do is about **one** seed:
//! whether it held up, what its faults come down to, and how to put it back. None of them finds the
//! seed. That is what this is — the blind half of the wedge, which throws one schedule of trouble at
//! a range of seeds and hands back the ones that could not take it, each of which is something
//! [`crate::shrink::shrink`] can then cut down.
//!
//! # A sweep is the only place in the engine that uses real threads
//!
//! Seeds are independent: a run of one draws nothing from a run of another, and each is an ordinary
//! single-threaded simulation on an executor of its own. So the seeds can be spread across workers
//! without the fourth determinism rule — *no threads inside a simulation* — being bent at all.
//! Nothing is shared between the workers: each builds its own list and the lists are merged
//! afterwards.
//!
//! What the rule does demand is that **nothing about the answer comes from the split**. Three things
//! together are what make that true rather than likely:
//!
//! - the report is merged **by seed**, never in the order the workers finished;
//! - every seed is swept even once one has broken, because "the first failure" under several workers
//!   is whichever worker got there first;
//! - a sweep that cannot finish reports the **lowest** seed it could not finish, for the same reason.
//!
//! So `--jobs` buys wall-clock time and changes nothing else. `--jobs 1` is not a separate path
//! through this module — it is one worker through the same one — which is what makes comparing the
//! two a comparison of one implementation at two settings.
//!
//! That was measured rather than argued, and the result is worth writing down: a version of this
//! that ignores `jobs` and always runs one worker reddens **nothing** in the suite. It cannot be
//! otherwise, since the whole point is that the count does not reach the answer — so what the cases
//! comparing two counts hold is the merge, not the count. The only thing that notices a sweep
//! running its seeds one after another is the barrier case below, and it notices by hanging.
//!
//! [`tests/audit.rs`]'s scan forbids a thread anywhere in `src/` and `tests/`, and names this file
//! as the single exception, for one rule, in a table that says why.

use core::num::{NonZeroU64, NonZeroUsize};
use std::panic::resume_unwind;
use std::thread;

use crate::outcome::{Broke, Outcome};

/// What a sweep found.
///
/// There is deliberately no written form here, and no reader. Nothing takes a survey as input — a
/// person reads one and a script reads the exit code — which is the rule [`crate::diff`]'s report
/// already follows: a form parses when something reads it back, and inventing one before anything
/// does is inventing it by guesswork. What the command line prints is assembled where the faults it
/// swept under are also in hand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Survey {
    swept: u64,
    broke: Vec<Broke>,
}

impl Survey {
    /// Returns how many seeds were run.
    ///
    /// Worth having beside the failures: "nothing broke" and "nothing was run" are different
    /// answers, and only one of them is good news.
    pub fn swept(&self) -> u64 {
        self.swept
    }

    /// Returns the seeds that broke, in seed order.
    pub fn broke(&self) -> &[Broke] {
        &self.broke
    }
}

/// Runs the seeds `0..seeds` across `jobs` workers, and returns the ones that broke.
///
/// `run` is the run under sweep with its faults already closed over: it takes a seed and says how
/// that run went, the way [`crate::shrink::shrink`]'s predicate takes a candidate schedule. So this
/// knows nothing about which system it is sweeping.
///
/// `jobs` is a cost and not a choice: every count produces the same survey, and more workers than
/// there are seeds is capped rather than started and joined for nothing.
///
/// ```
/// use core::num::{NonZeroU64, NonZeroUsize};
///
/// use chronoloop::outcome::{Outcome, Reason};
/// use chronoloop::sweep::sweep;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let seeds = NonZeroU64::new(10).ok_or("ten is not zero")?;
/// let jobs = NonZeroUsize::new(4).ok_or("four is not zero")?;
/// let every_third = Reason::new("every third seed breaks")?;
///
/// // A run that breaks on the seeds divisible by three, and holds up on the rest.
/// let survey = sweep(seeds, jobs, |seed| {
///     Ok::<_, core::convert::Infallible>(if seed % 3 == 0 {
///         Outcome::Fail { reason: every_third.clone(), step: 0 }
///     } else {
///         Outcome::Pass
///     })
/// })?;
///
/// assert_eq!(survey.swept(), 10);
/// assert_eq!(
///     survey.broke().iter().map(|broke| broke.seed()).collect::<Vec<_>>(),
///     vec![0, 3, 6, 9],
/// );
/// assert_eq!(
///     survey.broke()[1].to_string(),
///     "seed 3: failed at step 0: every third seed breaks",
/// );
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns whatever `run` returned for the **lowest** seed it could not finish, which is the same
/// seed however the work was split. A run that broke is not an error — it is what a sweep is for.
///
/// # Panics
///
/// Propagates a panic from a worker, once every worker has been joined. A simulation that panicked
/// is a bug belonging to whoever asked for the sweep, not a verdict to fold into the report.
pub fn sweep<E, F>(seeds: NonZeroU64, jobs: NonZeroUsize, run: F) -> Result<Survey, E>
where
    F: Fn(u64) -> Result<Outcome, E> + Sync,
    E: Send,
{
    let swept = seeds.get();
    // A worker with no seed to run has nothing to do. `try_from` cannot fail on any target this
    // builds for, and the fallback is capped by the line it is on rather than trusted.
    let workers = u64::try_from(jobs.get()).unwrap_or(u64::MAX).min(swept);
    let run = &run;

    let collected: Vec<Worked<E>> = thread::scope(|region| {
        // Every worker is started before any of them is joined. Joining inside this map would run
        // the seeds one after another while looking exactly like this does.
        let started: Vec<_> = (0..workers)
            .map(|worker| region.spawn(move || stride(swept, worker, workers, run)))
            .collect();
        started
            .into_iter()
            .map(|worker| {
                worker
                    .join()
                    .unwrap_or_else(|panicked| resume_unwind(panicked))
            })
            .collect()
    });

    let mut broke = Vec::new();
    let mut unfinished: Option<(u64, E)> = None;
    for worked in collected {
        match worked {
            Ok(found) => broke.extend(found),
            Err((seed, error)) => {
                if unfinished.as_ref().is_none_or(|(lowest, _)| seed < *lowest) {
                    unfinished = Some((seed, error));
                }
            }
        }
    }
    if let Some((_, error)) = unfinished {
        return Err(error);
    }

    // The merge, and the reason a survey says the same thing at every count of workers: what comes
    // back is ordered by seed, not by which worker filled it in or by how the seeds were handed out.
    broke.sort_by_key(Broke::seed);
    Ok(Survey { swept, broke })
}

/// What one worker came back with: the seeds of its own that broke, or the lowest it could not run.
type Worked<E> = Result<Vec<Broke>, (u64, E)>;

/// Runs every `workers`th seed from `worker`, which is one worker's whole share of `0..seeds`.
///
/// A stride rather than a block of its own: the cost of a seed is not the same as the cost of its
/// neighbour, so interleaving the shares keeps one worker from drawing all the slow ones. It also
/// means the order the seeds are visited in is nothing like the order they are reported in, so the
/// merge above has to do its job rather than appear to.
fn stride<E, F>(seeds: u64, worker: u64, workers: u64, run: &F) -> Worked<E>
where
    F: Fn(u64) -> Result<Outcome, E>,
{
    let mut broke = Vec::new();
    let mut seed = worker;
    while seed < seeds {
        match run(seed) {
            // A seed that held up is not a seed this names, which `Broke` settles rather than the
            // caller of it.
            Ok(outcome) => broke.extend(Broke::new(seed, outcome)),
            Err(error) => return Err((seed, error)),
        }
        seed += workers;
    }
    Ok(broke)
}

#[cfg(test)]
mod tests {
    use core::convert::Infallible;
    use std::sync::Barrier;

    use super::*;
    use crate::outcome::Reason;

    /// A count of seeds, failing the test rather than returning an error no case expects.
    fn seeds(count: u64) -> NonZeroU64 {
        NonZeroU64::new(count).unwrap_or_else(|| panic!("a sweep covers at least one seed"))
    }

    /// A count of workers, the same way.
    fn jobs(count: usize) -> NonZeroUsize {
        NonZeroUsize::new(count).unwrap_or_else(|| panic!("a sweep runs on at least one worker"))
    }

    /// A failure naming the seed it belongs to, so a case can tell two of them apart.
    fn broke_at(seed: u64) -> Outcome {
        Outcome::Fail {
            reason: Reason::new(format!("seed {seed} broke"))
                .unwrap_or_else(|e| panic!("a test reason is a reason: {e}")),
            step: 0,
        }
    }

    /// The seeds a survey says broke, which is what the cases below are written in terms of.
    fn seeds_that_broke(survey: &Survey) -> Vec<u64> {
        survey.broke().iter().map(Broke::seed).collect()
    }

    /// Sweeps `count` seeds over `workers` workers, failing the test rather than returning.
    fn swept<F>(count: u64, workers: usize, run: F) -> Survey
    where
        F: Fn(u64) -> Result<Outcome, Infallible> + Sync,
    {
        match sweep(seeds(count), jobs(workers), run) {
            Ok(survey) => survey,
            Err(never) => match never {},
        }
    }

    /// How many seeds the cases below sweep, and how many workers they spread them over.
    const SEEDS: u64 = 50;

    /// More than one worker, and not a divisor of [`SEEDS`], so no split lines up with the range.
    const WORKERS: usize = 7;

    #[test]
    fn every_seed_in_the_range_is_swept_exactly_once() {
        // A predicate that fails on everything, so the survey has to name the whole range. That is
        // a stronger statement than counting what the workers visited — and it needs no lock, since
        // the answer is the thing under test rather than something the case has to collect itself.
        let survey = swept(SEEDS, WORKERS, |seed| Ok(broke_at(seed)));

        assert_eq!(survey.swept(), SEEDS);
        assert_eq!(seeds_that_broke(&survey), (0..SEEDS).collect::<Vec<_>>());
    }

    #[test]
    fn the_seeds_that_broke_come_back_in_seed_order() {
        // The half of the range that breaks is scattered through it, so a report assembled in the
        // order the workers finished — or in the order a stride hands the seeds out — is not this.
        let breaks = |seed: u64| seed % 3 == 1;
        let survey = swept(SEEDS, WORKERS, |seed| {
            Ok(if breaks(seed) {
                broke_at(seed)
            } else {
                Outcome::Pass
            })
        });

        assert_eq!(survey.swept(), SEEDS);
        assert_eq!(
            seeds_that_broke(&survey),
            (0..SEEDS).filter(|seed| breaks(*seed)).collect::<Vec<_>>()
        );
    }

    #[test]
    fn how_many_workers_a_sweep_uses_does_not_change_what_it_found() {
        // The claim `--jobs` rests on. It holds because the workers are merged by seed rather than
        // in the order they finished, so this is the merge being asserted and not a race being
        // caught: a sweep that concatenated its workers' answers would fail this every time rather
        // than now and then.
        let breaks = |seed: u64| seed % 7 == 3;
        let run = |seed: u64| {
            Ok::<_, Infallible>(if breaks(seed) {
                broke_at(seed)
            } else {
                Outcome::Pass
            })
        };
        let alone = swept(SEEDS, 1, run);

        for workers in [2, 3, WORKERS, 64] {
            assert_eq!(
                swept(SEEDS, workers, run),
                alone,
                "{workers} workers and one worker found different things"
            );
        }
    }

    #[test]
    fn more_workers_than_seeds_costs_a_sweep_nothing() {
        // A worker with no seed to run has nothing to do, so the workers are capped at the seeds
        // rather than started and joined for the sake of it.
        let survey = swept(3, 64, |seed| Ok(broke_at(seed)));

        assert_eq!(survey.swept(), 3);
        assert_eq!(seeds_that_broke(&survey), vec![0, 1, 2]);
    }

    #[test]
    fn a_sweep_that_found_nothing_still_says_how_much_it_looked_at() {
        // "Nothing broke" and "nothing was run" are different answers, and a sweep that gave the
        // first when it meant the second would be green for the wrong reason.
        let survey = swept(SEEDS, WORKERS, |_| Ok(Outcome::Pass));

        assert_eq!(survey.swept(), SEEDS);
        assert!(survey.broke().is_empty());
    }

    #[test]
    fn a_sweep_that_could_not_finish_reports_the_lowest_seed_it_could_not() {
        // Which seed is reported must not depend on which worker got there first. Seeds 3 and 6 are
        // in different workers at four of the five splits below and in one worker at the fifth, so
        // the answer being the same at all five is the answer not coming from the split.
        let run = |seed: u64| match seed {
            3 | 6 => Err(seed),
            other => Ok(broke_at(other)),
        };

        for workers in [1, 2, 3, 4, WORKERS] {
            assert_eq!(
                sweep(seeds(SEEDS), jobs(workers), run).err(),
                Some(3),
                "over {workers} workers"
            );
        }
    }

    #[test]
    #[ignore = "hangs rather than failing if the workers do not really run at once; `make local-validation` runs it"]
    fn the_workers_of_a_sweep_run_at_the_same_time() {
        // The one thing `--jobs` promises that nothing above can hold: that asking for four workers
        // gets four. It cannot be shown by timing anything — the first determinism rule forbids a
        // wall clock, and a bound on a loaded machine flakes — so it is shown by a predicate that
        // only returns once every worker has arrived. One seed per worker, so all four have to be
        // in flight at once for any of them to finish.
        //
        // The cost of proving it this way, said plainly: a sweep that ran its seeds one after
        // another deadlocks here instead of reddening. That is why the case is gated out of CI, and
        // it is also what makes it the only thing here that would notice a sweep joining each
        // worker before starting the next.
        let workers = 4;
        let arrived = Barrier::new(workers);
        let survey = swept(4, workers, |seed| {
            arrived.wait();
            Ok(broke_at(seed))
        });

        assert_eq!(seeds_that_broke(&survey), vec![0, 1, 2, 3]);
    }
}
