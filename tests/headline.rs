//! A bug found blind, cut down to what caused it, and kept.
//!
//! The reconciler decides every difference it sees at once and says nothing about the order they
//! land in, and its timeline moves `users`' primary out of west into east at 23 seconds. If the
//! delete of west lands while east is still catching up, the data goes with it. Nothing here was
//! told that. Every seed is thrown its own trouble — six faults on the link between controller and
//! server, drawn from the seed by [`reconciler::drawn_faults`] knowing only the link and when the
//! passes are — and the sweep reports whatever broke.
//!
//! What it finds is the data loss, and only the data loss: no seed in the gated five hundred fails
//! to converge, so a sweep reporting something is a sweep reporting the hazard. The lowest seed it
//! finds is cut down to the faults its failure needed, and that repro is committed beside this file,
//! written by `chronoloop corner` and never edited by hand. Its schedule is one fault, a lossy
//! stretch eating the first pass's creates — the same thing the hand-written schedule in
//! `tests/reconciler.rs` was aimed at, arrived at here without aiming.
//!
//! What these cases can and cannot feel. The hunt and the reduction both feel the seed twice over,
//! through the run and through the faults drawn for it, so cutting the engine off from its seed
//! moves the seed they find. The committed repro feels it through its run alone, which is the
//! weaker claim, and is held by [`HOLDS`]: the same one fault, under another seed, keeps the data.
//! The heal rule is held at scale by the gated sweep and per draw by the generator's own unit cases;
//! none of these says anything about the state encoding, which `tests/world.rs` holds.

use core::num::{NonZeroU64, NonZeroUsize};

use chronoloop::fault::FaultSchedule;
use chronoloop::outcome::Outcome;
use chronoloop::repro::Repro;
use chronoloop::shrink::shrink;
use chronoloop::sweep::sweep;
use chronoloop::systems::{System, reconciler};

/// The repro the hunt and the reduction produced, as `chronoloop corner --seed 5` wrote it.
const FOUND: &str = include_str!("fixtures/lost-data.repro");

/// The lowest seed whose own drawn faults cost the reconciler a database's data, found by hunting.
const LOWEST: u64 = 5;

/// A seed that keeps the data under the committed repro's one fault, found by running: its delete
/// of west lands **before** its promote of east, so what saves it is east having caught up rather
/// than west having already stepped down — the distinction `tests/reconciler.rs`'s pair draws.
const HOLDS: u64 = 1;

/// How many of the gated hunt's seeds lose data under their own faults, recorded from an actual run.
const LOST_IN_HUNT: usize = 50;

/// How many seeds the gated hunt covers.
const HUNT: u64 = 500;

/// The committed repro, read back.
fn found() -> Repro {
    FOUND
        .parse()
        .unwrap_or_else(|e| panic!("the committed repro is a repro: {e}"))
}

/// How the reconciler's run of `seed` under `faults` went.
fn outcome(seed: u64, faults: &FaultSchedule) -> Outcome {
    System::Reconciler
        .run(seed, faults)
        .unwrap_or_else(|e| panic!("seed {seed} finishes: {e}"))
}

/// A count, failing the test rather than returning an error no case expects.
fn seeds(count: u64) -> NonZeroU64 {
    NonZeroU64::new(count).unwrap_or_else(|| panic!("a hunt covers at least one seed"))
}

/// Runs the reconciler over `0..count`, each seed under the faults drawn for it.
fn hunt(count: u64) -> Vec<String> {
    let jobs = NonZeroUsize::new(4).unwrap_or_else(|| panic!("four is not zero"));
    sweep(seeds(count), jobs, |seed| {
        System::Reconciler.run(seed, &reconciler::drawn_faults(seed))
    })
    .unwrap_or_else(|e| panic!("every seed finishes: {e}"))
    .broke()
    .iter()
    .map(ToString::to_string)
    .collect()
}

#[test]
fn a_blind_hunt_finds_the_lost_data() {
    assert_eq!(
        hunt(LOWEST + 1),
        ["seed 5: failed at step 16: lost data"],
        "every seed below the one it finds holds up, and the one it finds lost data"
    );
}

#[test]
fn cutting_the_found_seed_down_writes_the_committed_repro() {
    let drawn = reconciler::drawn_faults(LOWEST);
    let reduction = shrink(&drawn, |candidate| {
        System::Reconciler.run(LOWEST, candidate)
    })
    .unwrap_or_else(|e| panic!("every candidate finishes: {e}"))
    .unwrap_or_else(|| panic!("seed {LOWEST} breaks under its own faults"));
    let repro = Repro::new(
        System::Reconciler,
        LOWEST,
        reduction.schedule().clone(),
        reduction.outcome().clone(),
    )
    .unwrap_or_else(|e| panic!("a failure is something to reproduce: {e}"));

    assert_eq!(repro.to_string(), FOUND);
    assert_eq!(
        (drawn.len(), repro.faults().len()),
        (6, 1),
        "six faults drawn, one needed"
    );
}

#[test]
fn the_committed_repro_still_reproduces() {
    let repro = found();
    assert_eq!(repro.system(), System::Reconciler);

    let produced = outcome(repro.seed(), repro.faults());
    assert!(
        produced.reproduces(repro.expected()),
        "expected {}, produced {produced}",
        repro.expected()
    );
}

#[test]
fn the_committed_repro_is_already_as_small_as_it_goes() {
    // Its own tail handed back to the reduction comes back as the same repro. The other route to
    // the same bytes from the one above: that case reduces six drawn faults, this one reduces the
    // committed one.
    let repro = found();
    let again = shrink(repro.faults(), |candidate| {
        System::Reconciler.run(repro.seed(), candidate)
    })
    .unwrap_or_else(|e| panic!("every candidate finishes: {e}"))
    .unwrap_or_else(|| panic!("the committed repro still breaks"));

    assert_eq!(again.schedule(), repro.faults());
    assert_eq!(again.outcome(), repro.expected());
}

#[test]
fn the_seed_in_the_committed_repro_is_doing_work() {
    // The repro's one fault does not lose the data on its own: under another seed it holds up. So
    // the seed in the file is part of what makes it a repro, and a run that ignored it would be
    // noticed here — the case `tests/repro.rs`'s lossy pair exists for, on this system.
    let repro = found();
    assert_eq!(outcome(HOLDS, repro.faults()), Outcome::Pass);

    let (trace, _, _) = reconciler::run(HOLDS, repro.faults())
        .unwrap_or_else(|e| panic!("seed {HOLDS} finishes: {e}"));
    let at = |message: &str| {
        trace
            .steps()
            .iter()
            .position(|step| step.event().message() == message)
            .unwrap_or_else(|| panic!("seed {HOLDS} runs {message:?}"))
    };
    assert!(
        at("delete users in west") < at("promote users in east"),
        "west is deleted while it is still the primary, and east being in sync is what keeps it"
    );
}

#[test]
#[ignore = "a sweep of five hundred runs; `make local-validation` runs it in both profiles"]
fn a_hunt_finds_data_loss_and_nothing_else() {
    // The blindness claim at scale: whatever the drawn trouble does to the run, the only failure it
    // produces is the hazard. A schedule still cutting the link on the last pass would turn up here
    // as a run that did not converge, which is what the heal rule is for.
    let broke = hunt(HUNT);
    let lost = broke
        .iter()
        .filter(|line| line.ends_with(": lost data"))
        .count();
    assert_eq!(lost, broke.len(), "{broke:#?}");
    assert_eq!(lost, LOST_IN_HUNT);
}
