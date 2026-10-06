//! A control loop run against an API server over the simulated network.
//!
//! The unit tests beside [`chronoloop::systems::reconciler`] hold the decision to account on states
//! a case builds by hand — every state of four resources, every action twice. This one asks what
//! those cannot: does a **run** of the loop, with a wire that takes time and a server whose idea of
//! what is wanted changes underneath it, end where it was asked to — and does it get there when the
//! wire loses what it was asked?
//!
//! Schedules are read back from the text a schedule is written in, for the reason
//! `tests/outcome.rs` gives: a node has no public constructor, and a file is the way a schedule
//! arrives anyway. The controller is node 0 and the server node 1.
//!
//! What these cases can and cannot feel: the controller's passes and the timeline of what is wanted
//! are both fixed, so the seed reaches a run only through how long each message spends on the wire.
//! The recorded trace and the two-seed case feel that. The partition cases do not — a teardown asked
//! for while the way to the server is cut is lost on every seed, and a server never heard from is
//! short of everything on every seed — and the gated sweep is there to say so at scale rather than
//! letting one seed imply it.

use chronoloop::fault::FaultSchedule;
use chronoloop::outcome::{Outcome, Reason};
use chronoloop::systems::reconciler;
use chronoloop::trace::{Step, Trace};

/// The seed the recorded cases run, since none of them is about a particular one.
const SEED: u64 = 20_261_005;

/// The run `SEED` produces with nothing in its way, recorded from an actual run.
///
/// Beta is created before alpha although alpha was asked for first: each action is a message of its
/// own and draws a delay of its own, so the wire reorders them without anything asking it to.
const RECORDED: &str = "chronoloop trace seed 20261005\n\
step 0 0.000000000s f073ee88308e94d982c11fa9a2c6fc295c41f38b6b60f30671fb424d62df8707 alpha wanted at size 2\n\
step 1 0.000000000s 254ca02d2290c3014965b8e26507c8c02879de22f3278806b91022ae410ba943 beta wanted at size 1\n\
step 2 0.000000000s bb5c37019492b92cc04ce263adfc3b7bc92c366fa6fec624e5a646a75bbc4096 gamma wanted at size 3\n\
step 3 5.241029260s e36a1a2fcde072c9ca9814460ef772c5295d62181a1430508a378587c8e16904 create beta at size 1\n\
step 4 5.258549900s a80c9f09e310301b608b017503daf4963f6f8de377b16acf1afc4383151a8501 create alpha at size 2\n\
step 5 5.269098541s 1566ed55465fb958a0ca1e426357f18f555e3db6bc73d4a74f44465c4205fda9 create gamma at size 3\n\
step 6 12.000000000s d5e60b77749e6bba8de6f642db028d4df8961295b048748d00bd3ecfb7ffa76c alpha wanted at size 4\n\
step 7 15.151974057s e2174f31ba3047fc32f9ab0e00dc1a414a78e2c9994e36cf454cb3604e148c02 resize alpha to size 4\n\
step 8 22.000000000s 34b4679407a03ff0aaa87273cf56eb4f0307b56f663268b0756e94f81a0b031a beta no longer wanted\n\
step 9 25.164143502s 101fa55f683fe86784bee27b4885b2b0cb72efa2afbfa04c93ac53ae6393ed40 delete beta\n";

/// A schedule losing everything the controller sends in the second after the fifth pass asks.
///
/// The pass asks for its listing at exactly 25 seconds, so the listing goes through and the window
/// opens a nanosecond later — in time to lose the teardown the pass decides on, which is the first
/// pass to see beta is no longer wanted.
const LOST_TEARDOWN: &str = "chronoloop faults\n\
                             partition on node 0 -> node 1 from 25.000000001s until 26.000000000s\n";

/// A schedule cutting the controller off from the server for the whole run.
const NEVER_HEARD: &str = "chronoloop faults\n\
                           partition on node 0 -> node 1 from 0.000000000s until forever\n";

/// The pass after the one whose teardown is lost, which is the next to see beta still there.
const NEXT_PASS_SECONDS: u64 = 30;

/// How many seeds the gated sweep walks.
const SWEEP: u64 = 500;

/// Reads a schedule from its written form, failing the test if it is not one.
fn faults(text: &str) -> FaultSchedule {
    text.parse()
        .unwrap_or_else(|e| panic!("a schedule a test writes is a schedule: {e}"))
}

/// Runs the loop, failing the test rather than returning an error no case expects.
fn runs(seed: u64, faults: &FaultSchedule) -> (Trace, Outcome) {
    let (trace, _, outcome) =
        reconciler::run(seed, faults).unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"));
    (trace, outcome)
}

/// The instant, in nanoseconds, of the step recording `message`.
fn when(trace: &Trace, message: &str) -> u64 {
    trace
        .steps()
        .iter()
        .find(|step| step.event().message() == message)
        .unwrap_or_else(|| panic!("seed {} recorded {message:?}", trace.seed()))
        .event()
        .at()
        .as_nanos()
}

#[test]
fn a_recorded_run_is_the_run_this_seed_produces() {
    let (trace, outcome) = runs(SEED, &FaultSchedule::default());

    assert_eq!(trace.to_string(), RECORDED);
    assert_eq!(outcome, Outcome::Pass);
}

#[test]
fn a_lost_teardown_is_asked_for_again_on_the_next_pass() {
    // What being level-triggered is for. Nothing tells the controller its teardown was lost and
    // nothing in it decides to retry: the next pass looks, finds beta still there and unwanted, and
    // asks again. A controller acting on what *changed* would see nothing change and never ask.
    let (trace, outcome) = runs(SEED, &faults(LOST_TEARDOWN));
    let (clean, _) = runs(SEED, &FaultSchedule::default());

    assert_eq!(outcome, Outcome::Pass);
    assert!(
        when(&trace, "delete beta") >= NEXT_PASS_SECONDS * 1_000_000_000,
        "the teardown lands on the pass after the one that lost it"
    );
    assert!(
        when(&clean, "delete beta") < NEXT_PASS_SECONDS * 1_000_000_000,
        "and without the fault it lands a pass earlier, so the fault is what moved it"
    );
    // Reached by a different route — another run, with a fault in it — and the same state, which is
    // what converging means.
    assert_eq!(
        trace.steps().last().map(Step::state),
        clean.steps().last().map(Step::state),
        "the run ends in the state it would have reached anyway"
    );
}

#[test]
fn a_controller_the_server_never_hears_never_converges() {
    // The other side of the case above, and what keeps it from passing because nothing could fail:
    // a loop that is never heard is still short of everything when it stops.
    let (trace, outcome) = runs(SEED, &faults(NEVER_HEARD));

    assert_eq!(
        outcome,
        Outcome::Fail {
            reason: Reason::new("did not converge")
                .unwrap_or_else(|e| panic!("a reason is a reason: {e}")),
            step: 4,
        },
        "named at the last step, the state the run ended in"
    );
    assert!(
        trace
            .steps()
            .iter()
            .all(|step| step.event().message().contains("wanted")),
        "only what was asked for changed, and nothing the controller asked"
    );
}

#[test]
fn different_seeds_run_different_loops() {
    // Compared on the steps rather than the written form, whose header names the seed. This says
    // only that there *was* a difference; the recorded trace is what says what the steps are.
    assert_ne!(
        runs(0, &FaultSchedule::default()).0.steps(),
        runs(1, &FaultSchedule::default()).0.steps()
    );
}

#[test]
#[ignore = "a sweep over many seeds; `make local-validation` runs it"]
fn every_seed_converges_and_a_lost_teardown_always_waits_a_pass() {
    // The partition cases above are the same on every seed by construction, and this is where that
    // is checked rather than assumed: whatever the wire draws, a loop left alone converges, and a
    // loop that loses its teardown converges a pass later and no sooner.
    let lost = faults(LOST_TEARDOWN);
    for seed in 0..SWEEP {
        let (_, outcome) = runs(seed, &FaultSchedule::default());
        assert_eq!(
            outcome,
            Outcome::Pass,
            "seed {seed} with nothing in its way"
        );

        let (trace, outcome) = runs(seed, &lost);
        assert_eq!(outcome, Outcome::Pass, "seed {seed} with its teardown lost");
        assert!(
            when(&trace, "delete beta") >= NEXT_PASS_SECONDS * 1_000_000_000,
            "seed {seed} retried on the next pass"
        );
    }
}
