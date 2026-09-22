//! A sweep over the real coordinator, wired to the system and the faults it exists to look through.
//!
//! `src/sweep.rs`'s own cases drive a scripted predicate, which is how a case about *the sweep*
//! avoids pinning whatever the coordinator happened to do. This is the other half: the predicate is
//! a real run of a real system under a real schedule, and what is asserted is which seeds it finds.
//!
//! # The schedule these cases run is chosen, not inherited
//!
//! The outage schedule the rest of the suite reduces reaches the same failure under every seed,
//! because three replicas cut off cost round 3 its quorum whatever anything draws. A sweep asserted
//! against it would find every seed and would go on finding every seed with the engine cut off from
//! its seed entirely — a case blind to the one thing it is about. The lossy schedule below is the
//! one whose failures the draws take part in: half of the first twenty seeds break under it and half
//! do not, and *which* half is a fact about the entropy.
//!
//! # What a sweep does and does not promise
//!
//! It promises that the seeds it names are the seeds that broke, in seed order, whatever `jobs` is
//! set to. It does not promise that `jobs` workers ran at once — nothing here can say that without
//! timing something, which the first determinism rule forbids, and the case that does say it is the
//! barrier in `src/sweep.rs`, gated out of CI because it hangs rather than failing.

use core::num::{NonZeroU64, NonZeroUsize};

use chronoloop::fault::FaultSchedule;
use chronoloop::outcome::Broke;
use chronoloop::sweep::{Survey, sweep};
use chronoloop::systems::{RunError, quorum};

/// Faults whose failure the run's draws take part in, so one seed breaks under them and another does
/// not.
///
/// The same schedule `tests/cli.rs`, `tests/repro.rs` and `tests/shrink.rs` run, deliberately: what
/// those reach through one seed, this reaches through a range of them.
const LOSSY: &str = "chronoloop faults\n\
                     loss 4 in 4 on node 0 -> node 2 from 10.000000000s until 15.000000001s\n\
                     loss 1 in 2 on node 0 -> node 3 from 15.000000000s until 15.000000001s\n\
                     partition on node 0 -> node 4 from 15.000000000s until 15.000000001s\n";

/// Faults no seed breaks under: a link nobody uses, and losses between two replicas that never
/// speak to each other.
const MILD: &str = "chronoloop faults\n\
                    partition on node 4 -> node 5 from 0.000000000s until forever\n\
                    loss 1 in 2 on node 2 -> node 3 from 0.000000000s until forever\n";

/// How many seeds the cases here cover, which is few enough to run in CI.
const SEEDS: u64 = 20;

/// The seeds of `0..SEEDS` that break under [`LOSSY`], recorded from an actual run.
///
/// Pinned rather than counted: a case asserting only *how many* broke would stay green with the
/// sweep naming the wrong ten, and one asserting only that some broke would stay green with a
/// schedule that breaks everything.
const BROKE: [u64; 10] = [0, 3, 4, 5, 7, 8, 12, 16, 17, 18];

/// A schedule read back from the text it is written in, failing the test rather than returning.
///
/// A node has no public constructor, so the written form is how a schedule arrives from outside the
/// crate — and it is how one arrives at a sweep too.
fn faults(text: &str) -> FaultSchedule {
    text.parse()
        .unwrap_or_else(|e| panic!("a test schedule is a schedule: {e}"))
}

/// A range of seeds, failing the test rather than returning an error no case expects.
fn range(count: u64) -> NonZeroU64 {
    NonZeroU64::new(count).unwrap_or_else(|| panic!("a sweep covers at least one seed"))
}

/// A count of workers, the same way.
fn workers(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).unwrap_or_else(|| panic!("a sweep runs on at least one worker"))
}

/// Sweeps `count` seeds of the coordinator under `schedule`, over `jobs` workers.
fn swept(count: u64, jobs: usize, schedule: &FaultSchedule) -> Survey {
    sweep(range(count), workers(jobs), |seed| {
        quorum::run(seed, schedule).map(|(_, _, outcome)| outcome)
    })
    .unwrap_or_else(|e: RunError| panic!("every run of the coordinator finishes: {e}"))
}

/// The seeds a survey says broke.
fn seeds_that_broke(survey: &Survey) -> Vec<u64> {
    survey.broke().iter().map(Broke::seed).collect()
}

#[test]
fn a_sweep_finds_the_seeds_that_break_under_the_faults_and_leaves_the_rest() {
    let survey = swept(SEEDS, 4, &faults(LOSSY));

    assert_eq!(survey.swept(), SEEDS);
    assert_eq!(seeds_that_broke(&survey), BROKE);
}

#[test]
fn a_sweep_under_faults_that_cost_nothing_finds_nothing() {
    // The other side of the threshold, and what keeps the case above from passing for any schedule
    // at all: these faults are in force for the whole run and no seed is any the worse for them.
    let survey = swept(SEEDS, 4, &faults(MILD));

    assert_eq!(survey.swept(), SEEDS);
    assert!(
        survey.broke().is_empty(),
        "nothing broke: {:?}",
        seeds_that_broke(&survey)
    );
}

#[test]
fn every_seed_a_sweep_named_is_a_seed_that_breaks_on_its_own() {
    // The sweep's answer checked against the system one seed at a time — a different route to the
    // same fact, rather than the sweep agreeing with itself. It holds both directions: a seed the
    // sweep named breaks, and a seed it passed over does not.
    let lossy = faults(LOSSY);
    let broke = |seed: u64| {
        let (_, _, outcome) =
            quorum::run(seed, &lossy).unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"));
        Broke::new(seed, outcome).is_some()
    };

    for seed in 0..SEEDS {
        assert_eq!(
            BROKE.contains(&seed),
            broke(seed),
            "seed {seed} is named by the sweep exactly when it breaks"
        );
    }
}

#[test]
fn how_many_workers_a_sweep_uses_does_not_change_which_seeds_it_finds() {
    // The claim `--jobs` rests on, over a real system this time. It holds by the merge rather than
    // by the scheduler behaving: the workers are put back together in seed order, so a sweep that
    // concatenated them would fail this every time rather than now and then.
    let lossy = faults(LOSSY);
    let alone = swept(SEEDS, 1, &lossy);

    for jobs in [2, 3, 8, 64] {
        assert_eq!(
            swept(SEEDS, jobs, &lossy),
            alone,
            "{jobs} workers and one worker found different things"
        );
    }
}

#[test]
#[ignore = "sweeps two hundred seeds over eight workers; run it with `make local-validation`"]
fn a_wider_sweep_finds_the_same_seeds_however_it_is_split() {
    // The same property at a scale where the workers genuinely overlap, which is the only thing
    // about it a small case cannot say. The total is pinned as well as the agreement: a case
    // asserting only that the splits agree would stay green with all of them finding nothing.
    const WIDE: u64 = 200;

    let lossy = faults(LOSSY);
    let alone = swept(WIDE, 1, &lossy);

    assert_eq!(alone.swept(), WIDE);
    assert_eq!(
        alone.broke().len(),
        106,
        "how many of the first {WIDE} seeds these faults are enough to break"
    );
    assert_eq!(
        &seeds_that_broke(&alone)[..BROKE.len()],
        BROKE,
        "and the first twenty of them are the seeds the cases above pin"
    );

    for jobs in [2, 7, 8, 32] {
        assert_eq!(
            swept(WIDE, jobs, &lossy),
            alone,
            "{jobs} workers and one worker found different things"
        );
    }
}
