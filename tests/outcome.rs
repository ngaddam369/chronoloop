//! A run that goes wrong, and the verdict it leaves behind.
//!
//! The unit tests beside [`chronoloop::outcome`] hold the verdict to account on values a case
//! builds by hand. This one asks the question those cannot: does a **run** produce one — does the
//! whole engine, with a wire that takes time, five replicas answering at their own pace and an
//! injected schedule cutting three of them off, come out saying the thing a person would otherwise
//! have to go and work out for themselves?
//!
//! The schedule is built by **reading one back from the text a schedule is written in**, rather
//! than by naming nodes in Rust. A node has no public constructor — `VirtualNetwork::add_node` is
//! the only source of an address from outside the crate — so the written form is the way in, and it
//! is the same way a reduced schedule will arrive from a file. That is the argument
//! `tests/fault.rs` already makes: a schedule that only ever agreed with itself inside one process
//! says nothing about what a file holds.

use chronoloop::fault::FaultSchedule;
use chronoloop::outcome::{Outcome, Reason};
use chronoloop::systems::quorum;
use chronoloop::trace::Trace;

/// The seed the recorded cases run, since none of them is about a particular one.
const SEED: u64 = 20_260_921;

/// How many acknowledgements a round needs to keep its quorum.
///
/// Not how many replicas there are: a round that keeps its quorum need not have heard from all of
/// them, and one of the entries below is there precisely because it costs a round an answer without
/// costing it the round.
const QUORUM: u64 = 3;

/// The round the schedule below is aimed at.
const AIMED_AT: u64 = 3;

/// Where in the schedule the faults the failure actually needs begin.
///
/// The three before them are not needed for it. Two do nothing whatever — replicas never talk to
/// one another, and nothing at all is sent in the first second of the run. The third is the
/// interesting one: it costs round 1 an acknowledgement without costing it its quorum, so the run
/// still fails at round 3 and fails there **one step earlier**.
const NEEDED_FROM: usize = 3;

/// The entry of the schedule that moves the failure without causing it.
const MOVES_IT: usize = 2;

/// A schedule aimed at two rounds at once, so a case can say which of them is reported.
const TWICE: &str = "chronoloop faults\n\
                     partition on node 0 -> node 1 from 10.000000000s until 15.000000000s\n\
                     partition on node 0 -> node 2 from 10.000000000s until 15.000000000s\n\
                     partition on node 0 -> node 3 from 10.000000000s until 15.000000000s\n\
                     partition on node 0 -> node 1 from 20.000000000s until 25.000000000s\n\
                     partition on node 0 -> node 2 from 20.000000000s until 25.000000000s\n\
                     partition on node 0 -> node 3 from 20.000000000s until 25.000000000s\n";

/// A schedule aimed at the round before it, so that a case has two *different* failures to compare.
const EARLIER: &str = "chronoloop faults\n\
                       partition on node 0 -> node 1 from 10.000000000s until 15.000000000s\n\
                       partition on node 0 -> node 2 from 10.000000000s until 15.000000000s\n\
                       partition on node 0 -> node 3 from 10.000000000s until 15.000000000s\n";

/// A schedule of three faults the failure does not need and three that cost a round its quorum.
const FAULTS: &str = "chronoloop faults\n\
                      partition on node 4 -> node 5 from 0.000000000s until forever\n\
                      partition on node 0 -> node 1 from 0.000000000s until 1.000000000s\n\
                      partition on node 0 -> node 4 from 5.000000000s until 10.000000000s\n\
                      partition on node 0 -> node 1 from 15.000000000s until 20.000000000s\n\
                      partition on node 0 -> node 2 from 15.000000000s until 20.000000000s\n\
                      partition on node 0 -> node 3 from 15.000000000s until 20.000000000s\n";

/// When the round the schedule is aimed at gives up waiting, written the way a trace writes it.
///
/// A round opens at its own multiple of the coordinator's period and waits a fixed time for what it
/// asked for, so a round that is never going to hear back closes at exactly that instant — on every
/// seed, whatever anything drew. It is the sharpest thing a round opening on the period can be held
/// to: a round that opened whenever the last one happened to finish would close a little later each
/// time.
const GIVES_UP_AT: &str = "17.000000000s";

/// How many seeds the gated sweeps walk.
const SWEEP: u64 = 500;

/// The schedule the cases run under, read back from the text above.
fn faults() -> FaultSchedule {
    FAULTS
        .parse()
        .unwrap_or_else(|e| panic!("the schedule these cases are written in is a schedule: {e}"))
}

/// The schedule above without the entry at `index`, which is the move a reduction makes.
fn without(index: usize) -> FaultSchedule {
    let mut fewer = faults().faults().to_vec();
    fewer.remove(index);
    FaultSchedule::new(fewer)
}

/// Runs the system, failing the test rather than returning an error no case expects.
fn runs(seed: u64, faults: &FaultSchedule) -> (Trace, Outcome) {
    let (trace, _, outcome) =
        quorum::run(seed, faults).unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"));
    (trace, outcome)
}

/// A reason, failing the test rather than returning an error no case expects.
fn reason(text: &str) -> Reason {
    Reason::new(text).unwrap_or_else(|e| panic!("a test reason is a reason: {e}"))
}

/// The step an outcome failed at, failing the test if it did not fail at all.
fn failed_at(outcome: &Outcome) -> usize {
    match outcome {
        Outcome::Fail { step, .. } => *step,
        Outcome::Pass => panic!("three of five replicas cut off costs a round its quorum"),
    }
}

/// Every round-closing step of `trace`, as its position and the count it reported.
///
/// Read out of the text the run wrote down, which is a different route from the one the verdict
/// took: the coordinator counted acknowledgements as they landed, and this reads back afterwards
/// what it said about them. An expectation derived this way can disagree with the verdict, which is
/// the whole point of deriving it rather than asking for it.
fn closes(trace: &Trace) -> Vec<(usize, u64)> {
    trace
        .steps()
        .iter()
        .enumerate()
        .filter_map(|(step, took)| {
            let words: Vec<&str> = took.event().message().split(' ').collect();
            match words.as_slice() {
                ["round", _, "closed", "with", acks, "of", ..] => Some((step, acks.parse().ok()?)),
                _ => None,
            }
        })
        .collect()
}

#[test]
fn a_recorded_run_says_how_it_went_and_where_to_look() {
    // Recorded from an actual run, so it outlives the process that wrote it: `make local-validation`
    // runs this file in debug and again in release, and both compare against this same text. It
    // fails if anything upstream of it moves — what the wire draws, the state encoding, the order
    // events fire in, when a round opens, or what the coordinator writes down.
    //
    // Four lines are pinned rather than twenty-six: round 1 closing short of everyone but not short
    // of a quorum, the last acknowledgement round 3 got, the close that fell short, and the close of
    // the round after it, which is what says the run went on rather than stopping at the failure.
    // The line count is pinned before any of them is indexed, so a run that wrote fewer lines is
    // reported as that rather than as a panic.
    let (trace, outcome) = runs(SEED, &faults());

    assert_eq!(
        outcome.to_string(),
        "failed at step 13: round 3 lost quorum"
    );

    let text = trace.to_string();
    let written: Vec<&str> = text.lines().collect();
    assert_eq!(written.len(), 27, "a header and a step apiece");
    assert_eq!(
        written[5],
        "step 4 7.000000000s \
         479f2aa4e8822591e137f50a9aeaff4a9b4f72d5a34b2d74663779ff443f38af \
         round 1 closed with 4 of 5 acknowledgements",
        "a round can lose a replica and keep its quorum"
    );
    assert_eq!(
        written[13],
        "step 12 15.362123492s \
         b3ca4dc3b66386fc9f4bb5cd4420a1ce0ab25fc51c1469016a8768fae938f27e \
         replica-4 acknowledged round 3"
    );
    assert_eq!(
        written[14],
        "step 13 17.000000000s \
         d223dbc7f25c791ffd20a5f35228fb81b1f5819d090eb3e4ac0911773c31d058 \
         round 3 closed with 2 of 5 acknowledgements",
        "the round closes at its deadline, because what it is waiting for is never coming"
    );
    assert_eq!(
        written[20],
        "step 19 20.257618557s \
         a4e8c30241fff57bea21115a68fa29711381568069c3b18d4daa138eb849a0d1 \
         round 4 closed with 5 of 5 acknowledgements",
        "the window closed with the round, and the run went on"
    );

    assert_eq!(
        text.parse(),
        Ok(trace),
        "and what it wrote is what a trace reads back"
    );
}

#[test]
fn the_step_a_failure_names_is_the_first_round_that_closed_short() {
    // What makes the verdict worth reading: the step is where to look, and `inspect --step` is what
    // a person types next. The expectation is walked out of the trace's own text rather than asked
    // of the run again, so the two can disagree — a verdict naming the run's last step, or the last
    // round to fall short rather than the first, is caught here and nowhere else.
    let (trace, outcome) = runs(SEED, &faults());
    let step = failed_at(&outcome);

    let closes = closes(&trace);
    let short: Vec<(usize, u64)> = closes
        .iter()
        .copied()
        .filter(|(_, acks)| *acks < QUORUM)
        .collect();
    assert_eq!(
        short.len(),
        1,
        "one round of the five fell short: {closes:?}"
    );
    assert_eq!(step, short[0].0);
    assert!(
        closes
            .iter()
            .take_while(|(at, _)| *at < step)
            .all(|(_, acks)| *acks >= QUORUM),
        "every round that closed before it kept its quorum: {closes:?}"
    );
    assert_eq!(
        outcome,
        Outcome::Fail {
            reason: reason(&format!("round {AIMED_AT} lost quorum")),
            step,
        }
    );
}

#[test]
fn a_failure_that_surfaces_at_another_step_is_the_same_failure() {
    // The decision the whole reduction turns on, put to a run rather than argued about. Dropping the
    // entry that cost round 1 an acknowledgement gives round 1 its fifth step back, so round 3 —
    // still cut off, still short — now falls one step later. The failure did not change; where it
    // surfaced did, and a rule that compared steps would throw this reduction away.
    let original = runs(SEED, &faults()).1;
    let candidate = runs(SEED, &without(MOVES_IT)).1;

    assert_ne!(
        failed_at(&candidate),
        failed_at(&original),
        "dropping it moves the step: {original} -> {candidate}"
    );
    assert!(
        candidate.reproduces(&original),
        "and leaves the failure standing: {original} -> {candidate}"
    );
}

#[test]
fn a_run_that_falls_short_twice_is_reported_at_the_first_of_them() {
    // A run does not stop at a failure — it goes round again, and may fall short again. Which of
    // them the verdict names is what sends a reader to the right place, and it is the earliest,
    // because that is the one nothing before it explains. The case above cannot say this: only one
    // of its rounds ever falls short, so "the first" and "the last" are one round there.
    let (trace, outcome) = runs(
        SEED,
        &TWICE
            .parse()
            .unwrap_or_else(|e| panic!("the twice-aimed schedule is a schedule: {e}")),
    );

    let short: Vec<(usize, u64)> = closes(&trace)
        .into_iter()
        .filter(|(_, acks)| *acks < QUORUM)
        .collect();
    assert_eq!(short.len(), 2, "two rounds of the five fell short");
    assert_eq!(
        outcome,
        Outcome::Fail {
            reason: reason(&format!("round {} lost quorum", AIMED_AT - 1)),
            step: short[0].0,
        }
    );
}

#[test]
fn a_failure_of_another_round_is_another_failure() {
    // The other side of the rule, and the side the cases above cannot reach: in every one of them
    // the run that does not reproduce the failure is a run that held up, so none of them would
    // notice a comparison that called any two failures the same. These are two runs that both
    // broke, in the same way, one round apart.
    let aimed = runs(SEED, &faults()).1;
    let earlier = runs(
        SEED,
        &EARLIER
            .parse()
            .unwrap_or_else(|e| panic!("the earlier schedule is a schedule: {e}")),
    )
    .1;

    assert_eq!(
        earlier,
        Outcome::Fail {
            reason: reason(&format!("round {} lost quorum", AIMED_AT - 1)),
            step: failed_at(&earlier),
        }
    );
    assert!(!earlier.reproduces(&aimed), "{aimed} is not {earlier}");
    assert!(!aimed.reproduces(&earlier), "{earlier} is not {aimed}");
}

#[test]
fn a_fault_the_failure_never_needed_leaves_the_same_failure() {
    // The premise a reduction rests on, under test before there is a reduction. Dropping an entry
    // the failure never needed has to leave a run failing the same way, and dropping one it did has
    // to leave a run that does not — otherwise dropping entries one at a time says nothing at all
    // about which of them caused anything.
    let original = runs(SEED, &faults()).1;

    for (index, dropped) in faults().faults().iter().enumerate() {
        let candidate = runs(SEED, &without(index)).1;
        let needed = index >= NEEDED_FROM;
        assert_eq!(
            candidate.reproduces(&original),
            !needed,
            "dropping {dropped}: {original} -> {candidate}"
        );
    }
}

#[test]
#[ignore = "a sweep of seeds, run by `make local-validation` rather than by CI"]
fn no_seed_fails_a_run_that_meets_no_faults() {
    // The claim the system's own docs make, asked at the scale it is claimed at: the wire is
    // dependable, so nothing but an injected fault can cost a round its quorum, and a reduction
    // against this system is therefore never reducing noise. One seed passing says only that one
    // seed did.
    //
    // What this does not say: nothing here holds the *contents* of a run to account. The recorded
    // text above is what does, and only that.
    for seed in 0..SWEEP {
        assert_eq!(
            runs(seed, &FaultSchedule::default()).1,
            Outcome::Pass,
            "seed {seed} met no faults"
        );
    }
}

#[test]
#[ignore = "a sweep of seeds, run by `make local-validation` rather than by CI"]
fn a_window_over_one_period_names_a_round_whatever_the_run_draws() {
    // What a round opening on the period buys, asked everywhere rather than at one seed. A round
    // that opened whenever the last one happened to finish would drift with the draws, and a window
    // aimed at it would cut a different round — or none at all — from one seed to the next. The
    // step is deliberately not asserted: it moves with the draws, which is the point.
    let aimed = reason(&format!("round {AIMED_AT} lost quorum"));
    for seed in 0..SWEEP {
        let (trace, outcome) = runs(seed, &faults());
        let Outcome::Fail { reason, step } = &outcome else {
            panic!("seed {seed} held up under a schedule aimed at round {AIMED_AT}");
        };
        assert_eq!(reason, &aimed, "seed {seed}");

        let closed = trace
            .at(*step)
            .unwrap_or_else(|| panic!("seed {seed}: step {step} is a step the trace has"));
        assert_eq!(
            closed.event().at().to_string(),
            GIVES_UP_AT,
            "seed {seed}: the round gave up waiting at the instant it was always going to"
        );
    }
}
