//! A bug found blind, kept as a file, and fixed — with the file still saying what it was.
//!
//! The reconciler's timeline moves `users`' primary out of west into east at 23 seconds. A controller
//! that asks for every difference at once asks for the promote of east and the delete of west in one
//! pass, and each is a message of its own, so the wire lands them in either order; if the delete
//! lands while east is still catching up, the data goes with it. A sweep found that with no hint —
//! every seed thrown its own trouble by [`reconciler::drawn_faults`], knowing only the link and when
//! the passes are — on 50 of 500 seeds, and the lowest of them, seed 5, cut down to one fault. That
//! repro is committed beside this file, written by `chronoloop corner --seed 5` against `712ae86`,
//! the last commit whose controller asked for everything at once; checking that commit out is how
//! anyone reproduces the bug.
//!
//! The controller here does not ask for the delete of a still-wanted database's primary at all. It
//! promotes the standby once the standby is in sync, looks again a moment later, and takes the old
//! primary away only once that look shows it stepped down. These cases are the other half: the
//! committed repro no longer reproduces, its seed now promotes before it deletes, and the same hunt
//! — the same seeds under the very same drawn faults — comes back with nothing in it.
//!
//! What these cases can and cannot feel. A hunt reporting nothing is the weakest kind of claim: a
//! hunt that ran nothing would report nothing too, which is why the run of seed 5 is held to its
//! order of events and not only to its verdict. None of them feels the world's rule that a primary
//! deleted with nothing in sync behind it destroys the data — no run of this controller makes that
//! move — and the unit cases beside the module are what hold it. Nor do they feel the seed: every
//! seed holds up now, so a run cut off from its seed holds up too, and it is the pinned trace in
//! `tests/reconciler.rs` that notices.

use core::num::{NonZeroU64, NonZeroUsize};

use chronoloop::outcome::Outcome;
use chronoloop::repro::Repro;
use chronoloop::sweep::sweep;
use chronoloop::systems::{System, reconciler};

/// The repro the hunt and the reduction produced, as `chronoloop corner --seed 5` wrote it against
/// the controller that asked for every difference at once.
const FOUND: &str = include_str!("fixtures/lost-data.repro");

/// How many seeds the gated hunt covers — the same five hundred under which the controller that
/// asked for everything at once lost data on fifty.
const HUNT: u64 = 500;

/// The committed repro, read back.
fn found() -> Repro {
    FOUND
        .parse()
        .unwrap_or_else(|e| panic!("the committed repro is a repro: {e}"))
}

/// A count, failing the test rather than returning an error no case expects.
fn seeds(count: u64) -> NonZeroU64 {
    NonZeroU64::new(count).unwrap_or_else(|| panic!("a hunt covers at least one seed"))
}

/// Runs the reconciler over `0..count`, each seed under the faults drawn for it, and returns how
/// many seeds it ran and the failures it found.
fn hunt(count: u64) -> (u64, Vec<String>) {
    let jobs = NonZeroUsize::new(4).unwrap_or_else(|| panic!("four is not zero"));
    let survey = sweep(seeds(count), jobs, |seed| {
        System::Reconciler.run(seed, &reconciler::drawn_faults(seed))
    })
    .unwrap_or_else(|e| panic!("every seed finishes: {e}"));
    (
        survey.swept(),
        survey.broke().iter().map(ToString::to_string).collect(),
    )
}

#[test]
fn the_committed_repro_no_longer_reproduces() {
    // Not merely a failure of some other kind: the run the file names now holds up outright.
    let repro = found();
    assert_eq!(repro.system(), System::Reconciler);

    let produced = System::Reconciler
        .run(repro.seed(), repro.faults())
        .unwrap_or_else(|e| panic!("seed {} finishes: {e}", repro.seed()));
    assert!(!produced.reproduces(repro.expected()));
    assert_eq!(produced, Outcome::Pass);
}

#[test]
fn the_seed_that_lost_data_now_promotes_before_it_deletes() {
    // Why it holds up, read off the run rather than its verdict: under the committed fault, west is
    // deleted only after east has been promoted.
    let repro = found();
    let (trace, _, _) = reconciler::run(repro.seed(), repro.faults())
        .unwrap_or_else(|e| panic!("seed {} finishes: {e}", repro.seed()));
    let at = |message: &str| {
        trace
            .steps()
            .iter()
            .position(|step| step.event().message() == message)
            .unwrap_or_else(|| panic!("seed {} runs {message:?}", repro.seed()))
    };
    // Not east being ready before its promote: the world refuses an early promote and writes no
    // step for it, so that order holds of every trace and says nothing about the controller.
    assert!(
        at("promote users in east") < at("delete users in west"),
        "west goes as a standby, after the promote has stepped it down"
    );
}

#[test]
fn the_seeds_up_to_the_one_that_was_found_all_hold_up() {
    assert_eq!(hunt(6), (6, Vec::new()), "seed 5 among them");
}

#[test]
#[ignore = "a sweep of five hundred runs; `make local-validation` runs it in both profiles"]
fn the_same_hunt_finds_nothing() {
    // The before and after on one range: the same seeds under the same drawn faults, and nothing —
    // neither lost data nor a loop that ran out of passes before it got where it was asked.
    assert_eq!(hunt(HUNT), (HUNT, Vec::new()));
}
