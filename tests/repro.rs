//! A failing run written down, and a later run held to it.
//!
//! The unit tests beside [`chronoloop::repro`] pin the form against schedules written by hand, which
//! is what makes the text an exact expectation. This one asks the question those cannot: take a
//! **real** failing run — five replicas answering a coordinator over a wire that takes time and does
//! nothing else — cut its schedule down, write the three things that are left into a repro, and then
//! put another run through what the *text* says. If the file is a repro, that run fails the same way.
//!
//! The first text pinned below is 295 bytes long — counted off that literal rather than asserted
//! beside it, since the literal already fixes it. That is the size the artifact is claimed at: a
//! failing run of a five-node system, in under a third of a kilobyte.
//!
//! One limit, stated rather than left to be found. The outage schedule below reduces to the same
//! three partitions under every seed, so the cases built on it stay green with the engine cut off
//! from its seed entirely — `tests/shrink.rs` records the same thing about the same schedule. What
//! holds the *seed* in the file to account is the lossy repro, whose failure the draws take part in:
//! the identical faults under another seed hold up.
//!
//! Two routes, deliberately. The first two cases build a repro from a reduction and pin what it
//! writes; every case after them starts from the **pinned literal** rather than from that value, so
//! what is being run is what a file holds rather than something already in hand. The reduced schedule and the
//! failure in it are the ones `tests/shrink.rs` recorded from the same run, so the two files' literals
//! have to agree.

use chronoloop::fault::FaultSchedule;
use chronoloop::outcome::Outcome;
use chronoloop::repro::Repro;
use chronoloop::shrink::shrink;
use chronoloop::systems::quorum;
use chronoloop::trace::Trace;

/// The seed the recorded cases run, since none of them is about a particular one.
const SEED: u64 = 20_260_921;

/// Three faults the failure does not need and three that cost round 3 its quorum.
const FAULTS: &str = "chronoloop faults\n\
                      partition on node 4 -> node 5 from 0.000000000s until forever\n\
                      partition on node 0 -> node 1 from 0.000000000s until 1.000000000s\n\
                      partition on node 0 -> node 4 from 5.000000000s until 10.000000000s\n\
                      partition on node 0 -> node 1 from 15.000000000s until 20.000000000s\n\
                      partition on node 0 -> node 2 from 15.000000000s until 20.000000000s\n\
                      partition on node 0 -> node 3 from 15.000000000s until 20.000000000s\n";

/// The repro that comes of reducing it, recorded from an actual run.
///
/// The seed, the failure to expect, and then the three one-nanosecond outages `tests/shrink.rs` pins
/// as the whole of what went wrong — the same text, reached here through a repro rather than through
/// a reduction.
const REPRO: &str = "chronoloop repro seed 20260921\n\
                     failed at step 14: round 3 lost quorum\n\
                     chronoloop faults\n\
                     partition on node 0 -> node 1 from 15.000000000s until 15.000000001s\n\
                     partition on node 0 -> node 2 from 15.000000000s until 15.000000001s\n\
                     partition on node 0 -> node 3 from 15.000000000s until 15.000000001s\n";

/// A schedule whose faults are odds rather than outages, so what fails depends on what is drawn.
const LOSSY: &str = "chronoloop faults\n\
                     loss 1 in 1 on node 0 -> node 1 from 10.000000000s until 20.000000000s\n\
                     loss 4 in 4 on node 0 -> node 2 from 0.000000000s until forever\n\
                     loss 1 in 2 on node 0 -> node 3 from 15.000000000s until 20.000000000s\n\
                     partition on node 0 -> node 4 from 15.000000000s until 20.000000000s\n";

/// The repro that comes of reducing that, recorded from an actual run.
const LOSSY_REPRO: &str = "chronoloop repro seed 20260921\n\
     failed at step 13: round 3 lost quorum\n\
     chronoloop faults\n\
     loss 4 in 4 on node 0 -> node 2 from 10.000000000s until 15.000000001s\n\
     loss 1 in 2 on node 0 -> node 3 from 15.000000000s until 15.000000001s\n\
     partition on node 0 -> node 4 from 15.000000000s until 15.000000001s\n";

/// A seed the lossy repro's faults are not enough to break, found by running them under it.
const HOLDS_UP: u64 = 1;

/// A schedule read back from the text it is written in, failing the test rather than returning.
///
/// A node has no public constructor, so this is how a schedule arrives from outside the crate — and
/// it is how a repro's schedule arrives too.
fn schedule(text: &str) -> FaultSchedule {
    text.parse()
        .unwrap_or_else(|e| panic!("the schedule these cases are written in is a schedule: {e}"))
}

/// A repro read back from the text it is written in, failing the test rather than returning.
fn read(text: &str) -> Repro {
    text.parse()
        .unwrap_or_else(|e| panic!("the repro these cases are written in is a repro: {e}"))
}

/// What the run of `faults` under `seed` passed through, and how it went.
fn runs(seed: u64, faults: &FaultSchedule) -> (Trace, Outcome) {
    let (trace, _, outcome) =
        quorum::run(seed, faults).unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"));
    (trace, outcome)
}

/// The repro a reduction of `faults` under `SEED` produces.
fn reduced(faults: &str) -> Repro {
    let reduction = shrink(&schedule(faults), |candidate| {
        quorum::run(SEED, candidate).map(|(_, _, outcome)| outcome)
    })
    .unwrap_or_else(|e| panic!("seed {SEED} did not finish: {e}"))
    .unwrap_or_else(|| panic!("seed {SEED} held up under the schedule it was given"));

    Repro::new(
        SEED,
        reduction.schedule().clone(),
        reduction.outcome().clone(),
    )
    .unwrap_or_else(|e| panic!("a reduced failure is a failure: {e}"))
}

#[test]
fn a_reduction_becomes_a_repro_of_a_few_hundred_bytes() {
    // The seed a `Reduction` never held, the faults it kept, and the failure the run under them
    // produces — in one text that outlives the process that found them.
    assert_eq!(reduced(FAULTS).to_string(), REPRO);
}

#[test]
fn a_repro_of_a_failure_the_draws_take_part_in_is_written_the_same_way() {
    // The same form over odds rather than outages, and the repro the seed-sensitive cases below run
    // from. Its faults let some messages through, so what it fails for is partly what it drew.
    assert_eq!(reduced(LOSSY).to_string(), LOSSY_REPRO);
}

#[test]
fn a_repro_read_back_puts_a_later_run_through_the_same_failure() {
    // The whole point of the artifact, and the reason the run below is driven from the parsed text
    // rather than from the value the case above built: what is being checked is that a *file* is
    // enough to put a run back where it was.
    let repro = read(REPRO);
    let (_, outcome) = runs(repro.seed(), repro.faults());

    assert!(
        outcome.reproduces(repro.expected()),
        "the run of what the file says went {outcome}, and the file expects {}",
        repro.expected()
    );
}

#[test]
fn the_step_a_repro_names_is_a_step_of_the_run_it_names() {
    // A repro's failure carries the step to go and look at, which is an index into the trace of the
    // run the repro names — so the artifact points somewhere, rather than merely being consistent.
    let repro = read(REPRO);
    let (trace, _) = runs(repro.seed(), repro.faults());
    let Outcome::Fail { step, .. } = repro.expected() else {
        panic!("a repro names a failure, which the type refuses to be made without");
    };

    assert!(
        trace.at(*step).is_some(),
        "the repro names step {step} of a trace of {} steps",
        trace.steps().len()
    );
}

#[test]
fn a_repro_without_the_faults_it_carries_does_not_reproduce() {
    // What holds the schedule in the file to account. Without it, every case above would stay green
    // with the faults left out of the artifact altogether and the seed carrying it alone — this run
    // is the same seed, one outage short, and it holds up.
    let repro = read(REPRO);
    let short = FaultSchedule::new(repro.faults().faults()[1..].to_vec());
    let (_, outcome) = runs(repro.seed(), &short);

    assert!(
        !outcome.reproduces(repro.expected()),
        "the run one outage short of the file's faults went {outcome}"
    );
}

#[test]
fn a_repro_carries_its_seed_because_the_faults_alone_need_not_fail() {
    // What holds the seed in the file to account, and the one thing here that can feel the engine's
    // entropy at all. These faults break the run they were reduced from and let another seed's run
    // through untouched, so a repro that dropped its seed — or an engine that stopped drawing from
    // one — would be caught right here.
    let repro = read(LOSSY_REPRO);
    let (_, its_own) = runs(repro.seed(), repro.faults());
    let (_, another) = runs(HOLDS_UP, repro.faults());

    assert!(
        its_own.reproduces(repro.expected()),
        "the seed the file names went {its_own}"
    );
    assert_eq!(
        another,
        Outcome::Pass,
        "seed {HOLDS_UP} meets the same faults and is not the run the file is of"
    );
}
