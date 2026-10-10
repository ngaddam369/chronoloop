//! Invariants written outside a system, judging the runs it records.
//!
//! The unit tests beside [`chronoloop::invariant`] hold the checker to account on traces a case
//! builds by hand. This one hands it real runs — a wire that takes time, a schedule cutting nodes
//! off, a store the run filled itself — and asks whether a judgement that knows nothing but the
//! recorded world lands where the system's own verdict does.
//!
//! That agreement is the point of every case here, and it is worth what the two sides' routes
//! differ by. Both systems' verdicts are their own invariants, checked by this same checker, but
//! over predicates written in their modules; the ones below are written here, from what the module
//! docs say the world holds, and share no code with them. A predicate reading the world wrong on
//! either side disagrees with the other one. What these cases **cannot** catch is a fault in the
//! checker itself, since both routes run through it — the coordinator's verdict used to be read off
//! notes it kept for itself, and that was the route that held the checker here, until its verdict
//! became its invariants too. The checker is held elsewhere now: by its own unit tests on traces
//! built by hand, and by the coordinator's pinned outcomes in `tests/outcome.rs`, `tests/cli.rs`,
//! `tests/shrink.rs` and `tests/repro.rs`, every one of which a checker naming the wrong step moves.
//!
//! What none of it pins is the content of any one run: each expectation is the run's own verdict,
//! so a change upstream that moved every step would move both sides alike. Pinned histories live in
//! `tests/outcome.rs` and `tests/reconciler.rs`.

use core::time::Duration;

use chronoloop::fault::FaultSchedule;
use chronoloop::invariant::{self, Invariant, Violation};
use chronoloop::outcome::{Outcome, Reason};
use chronoloop::store::StateStore;
use chronoloop::systems::{quorum, reconciler};
use chronoloop::trace::Trace;
use chronoloop::world::{Name, Value, World};

/// The seed the coordinator's cases run, as `tests/outcome.rs` runs them.
const QUORUM_SEED: u64 = 20_260_921;

/// The seed the controller's cases run, as the dispatch case in `systems` runs it.
const RECONCILER_SEED: u64 = 20_261_005;

/// Three of five replicas cut off for one round, so exactly that round closes short.
const ONCE: &str = "chronoloop faults\n\
                    partition on node 0 -> node 1 from 10.000000000s until 15.000000000s\n\
                    partition on node 0 -> node 2 from 10.000000000s until 15.000000000s\n\
                    partition on node 0 -> node 3 from 10.000000000s until 15.000000000s\n";

/// The same, and again two rounds later, so a case can tell the first breach from the last.
const TWICE: &str = "chronoloop faults\n\
                     partition on node 0 -> node 1 from 10.000000000s until 15.000000000s\n\
                     partition on node 0 -> node 2 from 10.000000000s until 15.000000000s\n\
                     partition on node 0 -> node 3 from 10.000000000s until 15.000000000s\n\
                     partition on node 0 -> node 1 from 20.000000000s until 25.000000000s\n\
                     partition on node 0 -> node 2 from 20.000000000s until 25.000000000s\n\
                     partition on node 0 -> node 3 from 20.000000000s until 25.000000000s\n";

/// The controller cut off from its server for the whole run, so nothing it wants ever happens.
const CUT: &str = "chronoloop faults\n\
                   partition on node 0 -> node 1 from 0.000000000s until forever\n";

/// Longer than any run here lasts, so only the end of a run can break a liveness bound of it.
const AN_HOUR: Duration = Duration::from_secs(3_600);

/// A name, failing the test if it is not one.
fn name(text: &str) -> Name {
    Name::new(text).unwrap_or_else(|e| panic!("{text:?} is a name: {e}"))
}

/// A reason, failing the test if it is not one.
fn reason(text: &str) -> Reason {
    Reason::new(text).unwrap_or_else(|e| panic!("{text:?} is a reason: {e}"))
}

/// A schedule, read back from the text it is written in.
fn faults(text: &str) -> FaultSchedule {
    text.parse()
        .unwrap_or_else(|e| panic!("a test schedule is a schedule: {e}"))
}

/// Checks a run, failing the test if it cannot be checked at all.
fn checked(trace: &Trace, store: &StateStore, invariants: &[Invariant]) -> Vec<Violation> {
    invariant::check(trace, store, invariants)
        .unwrap_or_else(|e| panic!("a recorded run can be checked: {e}"))
}

/// The step a verdict failed at, failing the test if it held up.
fn failed_at(outcome: &Outcome) -> usize {
    match outcome {
        Outcome::Fail { step, .. } => *step,
        Outcome::Pass => panic!("the run was expected to break"),
    }
}

/// The coordinator never closes a round short of its quorum.
///
/// Read off the `quorum` flag of the `coordinator` resource, which a round sets as it closes.
fn every_round_reaches_quorum(world: &World) -> bool {
    world
        .get(&name("coordinator"))
        .and_then(|coordinator| coordinator.get(&name("quorum")))
        != Some(&Value::Flag(false))
}

/// Every database has exactly the replicas wanted of it, each in its role and ready.
///
/// Read off the fields the controller's world names for a region: `<region>-wanted` for the role
/// asked of it, `<region>-role` and `<region>-phase` for the replica there. The controller judges
/// convergence with a predicate of its own over the same fields; this one shares no code with it.
fn converged(world: &World) -> bool {
    world.resources().all(|(_, database)| {
        let at = |region: &str, ending: &str| {
            Name::new(format!("{region}-{ending}"))
                .ok()
                .and_then(|field| database.get(&field))
        };
        database.fields().all(|(field, value)| {
            if let Some(region) = field.as_str().strip_suffix("-wanted") {
                at(region, "role") == Some(value)
                    && at(region, "phase") == Some(&Value::Text("ready".into()))
            } else if let Some(region) = field.as_str().strip_suffix("-role") {
                at(region, "wanted").is_some()
            } else {
                true
            }
        })
    })
}

#[test]
fn a_broken_safety_invariant_names_the_step_the_run_blamed_and_the_state_it_left() {
    for (case, schedule) in [("one round short", ONCE), ("two rounds short", TWICE)] {
        let (trace, store, outcome) = quorum::run(QUORUM_SEED, &faults(schedule))
            .unwrap_or_else(|e| panic!("{case}: the run finishes: {e}"));
        let short = Invariant::safety(reason("a round closed short"), every_round_reaches_quorum);

        let broken = checked(&trace, &store, &[short]);

        // Under two short rounds the verdict names the first; a check naming the last disagrees.
        let step = failed_at(&outcome);
        assert_eq!(broken.len(), 1, "{case}: {broken:?}");
        assert_eq!(broken[0].step(), step, "{case}");
        assert_eq!(
            Some(broken[0].state()),
            trace.at(step).map(chronoloop::trace::Step::state),
            "{case}"
        );
        assert_eq!(
            Outcome::from(broken[0].clone()).to_string(),
            format!("failed at step {step}: a round closed short"),
            "{case}"
        );
    }
}

#[test]
fn a_run_that_holds_up_breaks_no_safety_invariant() {
    let (trace, store, outcome) = quorum::run(QUORUM_SEED, &FaultSchedule::default())
        .unwrap_or_else(|e| panic!("the run finishes: {e}"));
    let short = Invariant::safety(reason("a round closed short"), every_round_reaches_quorum);

    assert_eq!(outcome, Outcome::Pass);
    assert!(checked(&trace, &store, &[short]).is_empty());
}

#[test]
fn a_loop_that_never_converges_breaks_liveness_at_the_last_step_its_run_took() {
    // The span is longer than the run, so the only thing that can break it is the run ending with
    // the stretch still open — and the controller's own verdict, reached through a predicate of its
    // own over the same world, names the last step for not converging.
    let (trace, store, outcome) = reconciler::run(RECONCILER_SEED, &faults(CUT))
        .unwrap_or_else(|e| panic!("the run finishes: {e}"));
    let settles = Invariant::liveness(reason("did not converge"), AN_HOUR, converged);

    let broken = checked(&trace, &store, &[settles]);

    assert_eq!(failed_at(&outcome), trace.steps().len() - 1);
    assert_eq!(
        broken.iter().map(Violation::step).collect::<Vec<_>>(),
        vec![failed_at(&outcome)]
    );
    assert_eq!(Outcome::from(broken[0].clone()), outcome);
}

#[test]
fn a_loop_left_alone_converges_within_its_span_and_not_within_none() {
    // Every seed holds up without faults, so the generous span is broken by nothing. The same runs
    // under no span at all break it, which is what shows the span is what was being measured: the
    // world is not converged while the controller works, and no change of it lands in an instant.
    for seed in 0..5 {
        let (trace, store, outcome) = reconciler::run(seed, &FaultSchedule::default())
            .unwrap_or_else(|e| panic!("seed {seed} finishes: {e}"));
        let within = |span| Invariant::liveness(reason("did not converge"), span, converged);

        assert_eq!(outcome, Outcome::Pass, "seed {seed}");
        assert!(
            checked(&trace, &store, &[within(AN_HOUR)]).is_empty(),
            "seed {seed}"
        );
        assert_eq!(
            checked(&trace, &store, &[within(Duration::ZERO)]).len(),
            1,
            "seed {seed}"
        );
    }
}
