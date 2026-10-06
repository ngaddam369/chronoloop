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
//! are both fixed, so the seed reaches a run through how long each message spends on the wire and
//! how long each replica takes to be provisioned and to catch up. The recorded trace feels both, and
//! is the only case here that does: the two-seed case says only that *something* differed. The
//! partition cases do not feel the seed at all — a teardown asked for while the way to the server is
//! cut is lost on every seed, and a server never heard from is short of everything on every seed —
//! and the gated sweep is there to say so at scale rather than letting one seed imply it.
//!
//! Nothing on the timeline moves a primary, so no case here reaches the one move that can destroy a
//! database's data; the unit cases beside the module are what hold the world to that. What the sweep
//! does say is that the timeline as it stands never loses anything on any seed it tries, which is
//! what a run that does will be measured against.

use chronoloop::fault::FaultSchedule;
use chronoloop::outcome::{Outcome, Reason};
use chronoloop::systems::reconciler;
use chronoloop::trace::{Step, Trace};

/// The seed the recorded cases run, since none of them is about a particular one.
const SEED: u64 = 20_261_005;

/// The run `SEED` produces with nothing in its way, recorded from an actual run.
///
/// The standby in west is created before the primary in east although the primary was asked for
/// first: each action is a message of its own and draws a delay of its own, so the wire reorders
/// them without anything asking it to. And every replica takes a time of its own to get ready — the
/// primary in east serves from 9.4s while the standby beside it in west does not catch up until
/// 15.3s — which is the server's own generator at work rather than the wire's.
const RECORDED: &str = "chronoloop trace seed 20261005\n\
step 0 0.000000000s ee469f37ecf97403fd2f856276b7fff698932a84fadbbb6f337235d5462db557 orders wanted with primary east and standbys west\n\
step 1 0.000000000s c72e887f3dbc774d23199b55fe1438e2e4d80d027a6d359b96a9d5faa40fa522 users wanted with primary west and standbys east\n\
step 2 6.882367947s 9ef28aa214b30a1bd42fd53319de67ebaa32946c6e5babb8790827a3f3bb43ba create orders in west as standby\n\
step 3 6.961705411s f572e0efd37c2343d1e940671a7e603f0b13c5f51d2def0db24e85343a9ab0c3 create users in west as primary\n\
step 4 7.036160234s 5df309537d9c3953ef13972fa116f61ac36479539af75370cf3839797d214b3e create orders in east as primary\n\
step 5 7.128753860s 3452a2ba0a3c36808587a63e3a5c08d85f53b33f314e5f7f969f6e2505e765d4 create users in east as standby\n\
step 6 9.422648332s d50f3be8ccbacb6bee25cca1d994497f48d1d65c89e7c75f471098025723b3cb orders in east ready\n\
step 7 9.728370453s 0751025d8c7ec16e69e2a449fef3eb08dfe46a0e77b226d652e2cd78e77252c9 users in west ready\n\
step 8 10.103673954s 179992608b49055f392de2102d3c95b671b9198a195400eecc3c0fa0149bc1f1 users in east catching up\n\
step 9 10.635338194s cfab8224f407327e2915ff1cc26df1a011fa52ca2e71c478ad40a81491a44f45 orders in west catching up\n\
step 10 12.000000000s 1d5c7a815242ed5733c7113e35acdc14b9425e58153c33dd54b0459d8d91572d orders wanted with primary east and standbys south, west\n\
step 11 14.303210345s 7ac57dd664b1602673946466b34faaa4e53b9460de709bafe83fcb7ce467c7f7 users in east ready\n\
step 12 15.341333989s c60e635f41c874c319968ddd35843e9130f3e7622ba268b8bf423af87a6164d3 orders in west ready\n\
step 13 16.119259949s 8e2715526e0cfee73fad557422664d1c82e438d9b39ebc079b3c0e9899a5b803 create orders in south as standby\n\
step 14 18.526590425s 700f59f4eb63ed4781b9aff758f2a4c3ee4d29b270ffcac85edd1d0386516106 orders in south catching up\n\
step 15 22.000000000s 659abc6cadfb4e0e1b1b023d063263899a73b85004eb7183bb896c8b4dccb3a0 orders wanted with primary east and standbys south\n\
step 16 24.373838850s 7a927a93bd1c301280a1923c907a32a99f2a5026a7c58338d2f9b3a3aa05d29c orders in south ready\n\
step 17 26.209087666s f02d5ca4811191e7b24769308fc04b669e4407e847f6986cd07ae73853ac3474 delete orders in west\n";

/// A schedule losing everything the controller sends in the two seconds after the fifth pass asks.
///
/// The pass asks for its listing at exactly 25 seconds, so the listing goes through and the window
/// opens a nanosecond later — in time to lose the teardown the pass decides on, which is the first
/// pass to see the standby in west is no longer wanted. Two seconds because a listing can take most
/// of one to come back and the teardown sent on it has to fall inside the window on every seed.
const LOST_TEARDOWN: &str = "chronoloop faults\n\
                             partition on node 0 -> node 1 from 25.000000001s until 27.000000000s\n";

/// A schedule cutting the controller off from the server for the whole run.
const NEVER_HEARD: &str = "chronoloop faults\n\
                           partition on node 0 -> node 1 from 0.000000000s until forever\n";

/// The pass after the one whose teardown is lost, which is the next to see the standby in west
/// still there.
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
    // nothing in it decides to retry: the next pass looks, finds west still there and unwanted, and
    // asks again. A controller acting on what *changed* would see nothing change and never ask.
    let (trace, outcome) = runs(SEED, &faults(LOST_TEARDOWN));
    let (clean, _) = runs(SEED, &FaultSchedule::default());

    assert_eq!(outcome, Outcome::Pass);
    assert!(
        when(&trace, "delete orders in west") >= NEXT_PASS_SECONDS * 1_000_000_000,
        "the teardown lands on the pass after the one that lost it"
    );
    assert!(
        when(&clean, "delete orders in west") < NEXT_PASS_SECONDS * 1_000_000_000,
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
            step: 3,
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
fn every_seed_converges_without_loss_and_a_lost_teardown_always_waits_a_pass() {
    // The partition cases above are the same on every seed by construction, and this is where that
    // is checked rather than assumed: whatever the wire draws, a loop left alone converges, and a
    // loop that loses its teardown converges a pass later and no sooner.
    let lost = faults(LOST_TEARDOWN);
    for seed in 0..SWEEP {
        // A pass is the verdict's word that no wanted database lost its data and that every
        // replica ended where it was asked for, ready.
        let (_, outcome) = runs(seed, &FaultSchedule::default());
        assert_eq!(
            outcome,
            Outcome::Pass,
            "seed {seed} with nothing in its way"
        );

        let (trace, outcome) = runs(seed, &lost);
        assert_eq!(outcome, Outcome::Pass, "seed {seed} with its teardown lost");
        assert!(
            when(&trace, "delete orders in west") >= NEXT_PASS_SECONDS * 1_000_000_000,
            "seed {seed} retried on the next pass"
        );
    }
}
