//! A failing run cut down to the faults it could not do without.
//!
//! The unit tests beside [`chronoloop::shrink`] hold the reduction to account against scripted
//! predicates, which is what makes the smallest schedule an exact expectation. This one asks the
//! question those cannot: put the reduction in front of a **real run** — five replicas answering over
//! a wire that takes time, a coordinator counting acknowledgements round after round — and does the
//! schedule it hands back name the outage that actually caused the failure?
//!
//! The schedules are built by **reading them back from the text a schedule is written in**, for the
//! reason `tests/outcome.rs` and `tests/fault.rs` already give: a node has no public constructor, and
//! a file is how a schedule arrives. It is also how a reduced one will leave.
//!
//! The six-fault schedule below is the one `tests/outcome.rs` runs, deliberately. That file
//! establishes, one drop at a time, that the failure needs the last three entries and none of the
//! first three; this one asks what the reduction makes of the same schedule without being told any of
//! it.
//!
//! The fifty-fault schedule scatters those same three among forty-seven that cannot cause anything,
//! which is the size the dropping was measured at and the size at which a reduction that stops
//! short stops being invisible. It reduces to the same recorded text the six do.

use chronoloop::clock::VirtualTime;
use chronoloop::fault::{Fault, FaultSchedule, Window};
use chronoloop::outcome::Outcome;
use chronoloop::shrink::{Reduction, shrink};
use chronoloop::systems::quorum;
use chronoloop::trace::Step;

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

/// What the reduction makes of it, recorded from an actual run.
///
/// Three outages of a single nanosecond each, at the instant round 3 opens. That is the whole of what
/// went wrong, and it is what a five-second window over three links was hiding: the coordinator asks
/// every replica the moment the round opens, so an outage only ever had to exist for that one instant.
const REDUCED: &str = "chronoloop faults\n\
                       partition on node 0 -> node 1 from 15.000000000s until 15.000000001s\n\
                       partition on node 0 -> node 2 from 15.000000000s until 15.000000001s\n\
                       partition on node 0 -> node 3 from 15.000000000s until 15.000000001s\n";

/// The same three outages, scattered among forty-seven the failure cannot use.
///
/// The measurements that settled how the dropping works were taken at fifty faults, and the three
/// that matter sit at the tenth, twenty-sixth and forty-second entries — no contiguous chunk holds
/// them, so nothing but dropping entry by entry can arrive at them.
///
/// Three rules make the forty-seven decoration rather than cause, and all three are structural
/// rather than lucky:
///
/// - **At most two of the five replicas are impaired in any round but the third, whatever is
///   drawn.** Round 1 loses node 4's question and node 5's answer, round 2 node 1's question and
///   node 2's answer, round 4 node 2's and node 3's, round 5 node 1's and node 5's. Three
///   acknowledgements is a quorum, so every one of those rounds closes with exactly the three it
///   needs. The faults that are drawn against only ever fall on a replica an outage has already
///   silenced, so no draw can make a third.
/// - **Nothing but the three touches the third round.** No decoration falls on a direction between
///   the coordinator and a replica between 15s, when the round opens, and 17s, when it gives up —
///   so no decoration can join the set the failure needs.
/// - **No decoration shadows one of the three.** Where two faults are in force on one direction the
///   first of them applies, so every other window on `node 0 -> node 1`, `-> node 2` and `-> node 3`
///   is disjoint from the three's.
///
/// They are not inert, though: outages and real odds sit on links carrying real traffic in four of
/// the five rounds, so the run under all fifty is not the run under the three. The case below says
/// so rather than leaving it to be assumed. The two written `loss 0 in 4` are the exception that
/// makes the point — odds of none are still a fault in force, drawing exactly what the link would
/// have drawn, so there is nothing to lower and the reduction can only drop them.
const FIFTY: &str = "chronoloop faults\n\
                     partition on node 1 -> node 2 from 0.000000000s until forever\n\
                     partition on node 0 -> node 1 from 0.000000000s until 5.000000000s\n\
                     loss 1 in 2 on node 1 -> node 4 from 0.000000000s until forever\n\
                     partition on node 0 -> node 4 from 5.000000000s until 7.000000000s\n\
                     partition on node 2 -> node 1 from 0.000000000s until forever\n\
                     partition on node 5 -> node 0 from 5.000000000s until 7.000000000s\n\
                     partition on node 0 -> node 2 from 0.000000000s until 5.000000000s\n\
                     loss 1 in 2 on node 0 -> node 5 from 5.000000000s until 7.000000000s\n\
                     partition on node 1 -> node 3 from 0.000000000s until forever\n\
                     partition on node 0 -> node 1 from 15.000000000s until 20.000000000s\n\
                     partition on node 3 -> node 1 from 0.000000000s until forever\n\
                     partition on node 0 -> node 1 from 10.000000000s until 12.000000000s\n\
                     loss 3 in 4 on node 4 -> node 1 from 0.000000000s until forever\n\
                     partition on node 2 -> node 0 from 10.000000000s until 12.000000000s\n\
                     partition on node 0 -> node 3 from 0.000000000s until 5.000000000s\n\
                     loss 1 in 2 on node 0 -> node 2 from 10.000000000s until 12.000000000s\n\
                     partition on node 2 -> node 4 from 0.000000000s until forever\n\
                     loss 0 in 4 on node 0 -> node 4 from 10.000000000s until 12.000000000s\n\
                     partition on node 4 -> node 2 from 0.000000000s until forever\n\
                     partition on node 0 -> node 4 from 0.000000000s until 5.000000000s\n\
                     loss 1 in 3 on node 2 -> node 5 from 0.000000000s until forever\n\
                     partition on node 3 -> node 5 from 0.000000000s until forever\n\
                     partition on node 0 -> node 5 from 0.000000000s until 5.000000000s\n\
                     loss 1 in 1 on node 1 -> node 0 from 0.000000000s until 5.000000000s\n\
                     partition on node 5 -> node 3 from 0.000000000s until forever\n\
                     partition on node 0 -> node 2 from 15.000000000s until 20.000000000s\n\
                     loss 1 in 1 on node 2 -> node 0 from 0.000000000s until 5.000000000s\n\
                     partition on node 4 -> node 5 from 0.000000000s until forever\n\
                     partition on node 0 -> node 2 from 20.000000000s until 22.000000000s\n\
                     loss 2 in 3 on node 5 -> node 2 from 0.000000000s until forever\n\
                     partition on node 3 -> node 0 from 20.000000000s until 22.000000000s\n\
                     partition on node 5 -> node 4 from 0.000000000s until forever\n\
                     loss 3 in 4 on node 0 -> node 3 from 20.000000000s until 22.000000000s\n\
                     loss 0 in 4 on node 4 -> node 0 from 20.000000000s until 22.000000000s\n\
                     partition on node 1 -> node 5 from 0.000000000s until forever\n\
                     partition on node 0 -> node 1 from 25.000000000s until 27.000000000s\n\
                     partition on node 5 -> node 1 from 0.000000000s until forever\n\
                     partition on node 5 -> node 0 from 25.000000000s until 27.000000000s\n\
                     partition on node 2 -> node 3 from 0.000000000s until forever\n\
                     loss 1 in 2 on node 0 -> node 5 from 25.000000000s until 27.000000000s\n\
                     partition on node 3 -> node 2 from 0.000000000s until forever\n\
                     partition on node 0 -> node 3 from 15.000000000s until 20.000000000s\n\
                     partition on node 3 -> node 4 from 0.000000000s until forever\n\
                     partition on node 0 -> node 1 from 30.000000000s until forever\n\
                     partition on node 4 -> node 3 from 0.000000000s until forever\n\
                     partition on node 2 -> node 0 from 30.000000000s until forever\n\
                     loss 1 in 1 on node 0 -> node 3 from 30.000000000s until forever\n\
                     loss 1 in 2 on node 4 -> node 0 from 30.000000000s until forever\n\
                     partition on node 0 -> node 5 from 30.000000000s until forever\n\
                     loss 1 in 4 on node 3 -> node 0 from 30.000000000s until forever\n";

/// A schedule whose faults are odds rather than outages, so the lowering reaches a real wire.
const LOSSY: &str = "chronoloop faults\n\
                     loss 1 in 1 on node 0 -> node 1 from 10.000000000s until 20.000000000s\n\
                     loss 4 in 4 on node 0 -> node 2 from 0.000000000s until forever\n\
                     loss 1 in 2 on node 0 -> node 3 from 15.000000000s until 20.000000000s\n\
                     partition on node 0 -> node 4 from 15.000000000s until 20.000000000s\n";

/// What the reduction makes of that, recorded from an actual run.
const REDUCED_LOSSY: &str = "chronoloop faults\n\
                             loss 4 in 4 on node 0 -> node 2 from 10.000000000s until 15.000000001s\n\
                             loss 1 in 2 on node 0 -> node 3 from 15.000000000s until 15.000000001s\n\
                             partition on node 0 -> node 4 from 15.000000000s until 15.000000001s\n";

/// How many seeds the sweep over the fifty-fault schedule walks.
///
/// Wider than the sweep below because a run is cheap where a reduction is not: this one runs the
/// system once a seed rather than the hundred and fifty-odd times a reduction asks for.
const FIFTY_SWEEP: u64 = 500;

/// How many seeds the gated sweep walks.
///
/// Every seed costs the reduction well over a hundred runs of the system, so this is small where the
/// sweeps in `tests/outcome.rs` are five hundred — kept proportionate to the gate it sits in rather
/// than to the number that reads best.
const SWEEP: u64 = 20;

/// A schedule read back from the text it is written in, failing the test rather than returning.
fn schedule(text: &str) -> FaultSchedule {
    text.parse()
        .unwrap_or_else(|e| panic!("the schedule these cases are written in is a schedule: {e}"))
}

/// How the run of `faults` under `seed` went, failing the test rather than returning an error.
fn runs(seed: u64, faults: &FaultSchedule) -> Outcome {
    quorum::run(seed, faults)
        .unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"))
        .2
}

/// What the run of `faults` under `seed` passed through, failing the test rather than returning.
fn steps(seed: u64, faults: &FaultSchedule) -> Vec<Step> {
    quorum::run(seed, faults)
        .unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"))
        .0
        .steps()
        .to_vec()
}

/// Reduces `faults` against a real run of the system under `seed`.
fn reduce(seed: u64, faults: &FaultSchedule) -> Reduction {
    shrink(faults, |candidate| {
        quorum::run(seed, candidate).map(|(_, _, outcome)| outcome)
    })
    .unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"))
    .unwrap_or_else(|| panic!("seed {seed} held up under the schedule it was given"))
}

/// The span of simulated time a fault is in force for, in nanoseconds.
fn span(fault: &Fault) -> u64 {
    let during = window(fault);
    during.end().as_nanos() - during.start().as_nanos()
}

/// The window a fault is in force for.
fn window(fault: &Fault) -> Window {
    match fault {
        Fault::Partition { during, .. } | Fault::Loss { during, .. } => *during,
        _ => panic!("a schedule read back from text holds the faults text can write"),
    }
}

/// `fault`, in force for `during` instead of the window it holds.
fn moved(fault: &Fault, during: Window) -> Fault {
    match *fault {
        Fault::Partition { from, to, .. } => Fault::Partition { from, to, during },
        Fault::Loss { from, to, odds, .. } => Fault::Loss {
            from,
            to,
            during,
            odds,
        },
        _ => panic!("a schedule read back from text holds the faults text can write"),
    }
}

/// `faults`, with the fault at `index` replaced by `fault`.
fn replacing(faults: &FaultSchedule, index: usize, fault: Fault) -> FaultSchedule {
    let mut candidate = faults.faults().to_vec();
    candidate[index] = fault;
    FaultSchedule::new(candidate)
}

/// `faults`, without the one at `index`.
fn without(faults: &FaultSchedule, index: usize) -> FaultSchedule {
    let mut fewer = faults.faults().to_vec();
    fewer.remove(index);
    FaultSchedule::new(fewer)
}

#[test]
fn a_failing_run_reduces_to_the_outage_that_caused_it() {
    // Recorded from an actual run, so it outlives the process that wrote it: `make local-validation`
    // runs this file in debug and again in release and both compare against this same text. It fails
    // if anything upstream of it moves — what the wire draws, when a round opens, or which candidates
    // the reduction offers and in which order.
    let reduction = reduce(SEED, &schedule(FAULTS));

    assert_eq!(reduction.schedule().to_string(), REDUCED);
    assert_eq!(
        reduction.schedule().len(),
        3,
        "three of the six, and the three `tests/outcome.rs` shows the failure needs"
    );

    // The step moved, which is the whole reason two failures are the same failure when their reasons
    // agree. It also means the outcome the reduction reports has to be the reduced run's: a reader
    // takes that step to `inspect`, and the original's step 13 is a step of a run no longer on offer.
    assert_eq!(
        runs(SEED, &schedule(FAULTS)).to_string(),
        "failed at step 13: round 3 lost quorum"
    );
    assert_eq!(
        reduction.outcome().to_string(),
        "failed at step 14: round 3 lost quorum",
        "round 1 has its fifth acknowledgement back, so the failure surfaces a step later"
    );
}

#[test]
fn no_fault_of_a_reduction_can_be_dropped() {
    // The property the dropping claims, checked by a route the reduction did not take: every single
    // drop is tried by hand and every one of them has to stop reproducing. Pinning the text alone
    // would say what came back and not that it is minimal.
    let reduction = reduce(SEED, &schedule(FAULTS));
    let reduced = reduction.schedule();

    for index in 0..reduced.len() {
        let fewer = without(reduced, index);
        let outcome = runs(SEED, &fewer);
        assert!(
            !outcome.reproduces(reduction.outcome()),
            "dropping {} leaves {outcome}",
            reduced.faults()[index]
        );
    }
}

#[test]
fn the_window_a_reduction_keeps_is_the_shortest_that_still_cuts_the_round() {
    // The other half of the same argument, for the narrowing rather than the dropping. A single
    // nanosecond is as short as a window that holds anything can be, and the bound is derived rather
    // than admired: a window of no span at all holds nothing, and the run then holds up.
    let reduction = reduce(SEED, &schedule(FAULTS));
    let reduced = reduction.schedule();

    for (index, fault) in reduced.faults().iter().enumerate() {
        assert_eq!(span(fault), 1, "{fault}");

        let start = window(fault).start();
        let holds_nothing = Window::new(start, start)
            .unwrap_or_else(|e| panic!("a window of no span is still a window: {e}"));
        let outcome = runs(
            SEED,
            &replacing(reduced, index, moved(fault, holds_nothing)),
        );
        assert!(
            !outcome.reproduces(reduction.outcome()),
            "an outage of no span at all leaves {outcome}: {fault}"
        );
    }
}

#[test]
fn a_failing_run_of_fifty_faults_reduces_to_the_three_that_caused_it() {
    // The scale the dropping was measured at, against the real system. What makes this more than
    // the six-fault case with padding on the end is the answer: two schedules with nothing in
    // common but three entries reduce to one recorded text, so what comes back is a fact about the
    // failure rather than about the list it was found in.
    let reduction = reduce(SEED, &schedule(FIFTY));

    assert_eq!(reduction.schedule().to_string(), REDUCED);
    assert_eq!(
        reduction.outcome().to_string(),
        "failed at step 14: round 3 lost quorum",
        "the run of the three, which is the run the six-fault schedule also comes down to"
    );

    // And the forty-seven are not padding. They cut real questions and real answers in four of the
    // five rounds, so the run they are part of passes through different steps from the run of the
    // three alone — a reduction that had merely dropped forty-seven faults nothing ever consulted
    // would leave this equal.
    assert_ne!(
        steps(SEED, &schedule(FIFTY)),
        steps(SEED, reduction.schedule()),
        "the decorations reach the run, and the reduction takes them out anyway"
    );

    // What this case cannot feel, said rather than implied by its size: three replicas cut at the
    // instant the round opens costs that round its quorum whatever the wire draws, so the answer
    // above is the same under every seed and the engine could be cut off from its entropy without
    // reddening a line of it. The lossy cases below are what feel the draws.
}

#[test]
fn reducing_a_reduction_changes_nothing() {
    // What the reduction's own loop is for, through a real system rather than a scripted predicate.
    // The two sides are one reduction and two of them, not one value compared with itself.
    //
    // The second schedule is what makes this able to fail at all, and it took a mutation to find
    // out: a reduction made to stop after the first entries it takes out still lands exactly on
    // the six-fault schedule's three, because those three are one contiguous half of it. Fifty is
    // where stopping short shows — the result is then not one no single entry can be taken from,
    // and reducing it again gets further.
    struct Case {
        name: &'static str,
        faults: &'static str,
    }
    let cases = [
        Case {
            name: "six faults, three of them needed",
            faults: FAULTS,
        },
        Case {
            name: "fifty faults, the same three needed",
            faults: FIFTY,
        },
    ];

    for case in cases {
        let once = reduce(SEED, &schedule(case.faults));
        let twice = reduce(SEED, once.schedule());

        assert_eq!(
            twice.schedule().to_string(),
            once.schedule().to_string(),
            "{}",
            case.name
        );
        assert_eq!(twice.outcome(), once.outcome(), "{}", case.name);
    }
}

#[test]
fn a_reduction_of_lossy_faults_keeps_what_the_draws_need() {
    // Odds rather than outages, so the lowering reaches a real wire. Two things here are recorded
    // facts about this seed's draws rather than anything the reduction decides, and both are worth
    // saying out loud:
    //
    // Neither odds comes down. A quarter of the messages getting through is enough for a round to
    // keep its quorum, and a coin that never comes up is no loss at all, so for this seed the fewest
    // occurrences that still fail are the ones the schedule arrived with. What pins the lowering
    // *working* is the unit case beside `shrink`, where a predicate says which odds it needs; what
    // this says is that the move reaches the wire and leaves a schedule that still fails.
    //
    // And one window keeps a round the failure is not about. The case below is why.
    let reduction = reduce(SEED, &schedule(LOSSY));

    assert_eq!(reduction.schedule().to_string(), REDUCED_LOSSY);
    assert_eq!(
        reduction.outcome().to_string(),
        "failed at step 13: round 3 lost quorum"
    );
}

#[test]
fn a_window_a_reduction_will_not_narrow_is_one_the_draws_are_standing_in() {
    // The reduction leaves the first lossy fault's window open from round 2, three rounds before the
    // one that fails, and it is not timidity: a message that is lost costs the wire's generator a
    // delay it would otherwise have drawn, so where that generator stands when round 3 tosses its
    // coin depends on round 2 having lost one. Start the window a single nanosecond later and the run
    // holds up.
    //
    // This is what a bisection is *for*, and it is the thing a person reading the original schedule
    // would never have guessed. It is also the honest limit of the claim the module makes: a narrow
    // window is one that still fails, not one whose every instant is needed for the reason the reader
    // assumes.
    let reduced = reduce(SEED, &schedule(LOSSY));
    let reduced = reduced.schedule();
    let earliest = window(&reduced.faults()[0]);

    let later = Window::new(
        VirtualTime::from_nanos(earliest.start().as_nanos() + 1),
        earliest.end(),
    )
    .unwrap_or_else(|e| panic!("a window one nanosecond shorter is a window: {e}"));
    assert_eq!(
        runs(
            SEED,
            &replacing(reduced, 0, moved(&reduced.faults()[0], later))
        ),
        Outcome::Pass,
        "the round that fails needs the round before it to have lost a message"
    );
}

#[test]
#[ignore = "a sweep of seeds, run by `make local-validation` rather than by CI"]
fn every_seed_reduces_to_the_same_outage() {
    // The reduction is not a fact about one seed's draws. Whatever the wire draws, three of five
    // replicas cut off at the instant round 3 opens costs that round its quorum and two of five does
    // not, because the wire is dependable and a round waits comfortably longer than any jitter — so
    // the same three one-nanosecond outages are what every seed comes down to.
    //
    // What this does not say: nothing here holds the *contents* of a reduction to account beyond the
    // schedule. The recorded outcomes above are what do that.
    for seed in 0..SWEEP {
        assert_eq!(
            reduce(seed, &schedule(FAULTS)).schedule().to_string(),
            REDUCED,
            "seed {seed}"
        );
    }
}

#[test]
#[ignore = "a sweep of seeds, run by `make local-validation` rather than by CI"]
fn no_seed_finds_a_failure_among_the_fifty_but_the_one_they_are_scattered_around() {
    // The premise the fifty-fault fixture rests on, held rather than asserted in prose: the
    // forty-seven leave at most two of the five replicas impaired in any round but the third, and
    // three of five answering is a quorum, so no draw can turn one of them into a cause. A
    // decoration that could cost a round its quorum on some seed would be a fault the reduction is
    // entitled to keep, and the case above would be pinning a fact about one seed's draws.
    //
    // Compared through `reproduces`, which asks the reason and not the step: the step a failure
    // surfaces at moves with every draw, and what must not move is which round went short.
    //
    // It is the only thing holding that premise, which was established by breaking it: moving one
    // decoration onto a link that would silence a third replica in round 1 turns this red at seed
    // 0 and leaves every other case in the repository green — seed 20260921 does not happen to
    // lose that coin, so the case above cannot tell a fixture that is sound from one that is
    // lucky.
    let faults = schedule(FIFTY);
    let expected = runs(SEED, &faults);

    for seed in 0..FIFTY_SWEEP {
        let outcome = runs(seed, &faults);
        assert!(
            outcome.reproduces(&expected),
            "seed {seed} went {outcome}, where {SEED} went {expected}"
        );
    }
}
