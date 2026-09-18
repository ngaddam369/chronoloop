//! The crown jewel, asserted with the whole engine running at once.
//!
//! Every component here has its own test proving that *it* is a function of the seed. This one asks
//! the question the project rests on: is a **run**? The system below is deliberately the widest one
//! in the repository — per-task generators, an unreliable wire that loses and duplicates and
//! reorders, tasks spawned mid-run and joined, deadlines armed and abandoned, and an injected fault
//! schedule on top. Something outside the seed reaching behaviour — an iteration order, an address,
//! a leftover static — can hide indefinitely in a run that never touches the machinery holding it.
//!
//! What each layer of this proves, and what it does not:
//!
//! - Running one seed **twice in one process** catches state leaking between runs and anything
//!   derived from an address. It cannot catch a difference between two builds, because there is
//!   only one build.
//! - The **pinned history** below can. It is a committed literal, so `make local-validation` running
//!   this file in debug and again in release compares both against the same text.
//! - The **sweep** is the same in-process check at scale, plus the one assertion that collapses the
//!   moment the seed stops reaching behaviour: every seed writes a history of its own. Note what
//!   that does *not* say — it takes only one live source of entropy to keep every run distinct, so
//!   the sweep would still pass with some of them cut off. Pinning a whole history is what holds
//!   every source to account at once.
//!
//! Anything comparing two different seeds compares their **entries**, never their written form: a
//! recording opens with `chronoloop history seed <n>`, so the text of two seeds differs however
//! little their runs did, and an assertion against it could not fail.
//!
//! Nothing here asserts how long anything took. A run's cost is a fact about a machine; what is
//! asserted is what the run wrote.

use core::ops::RangeInclusive;
use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet};

use chronoloop::clock::{Clock, VirtualTime};
use chronoloop::executor::Executor;
use chronoloop::fault::{Fault, FaultSchedule, Window};
use chronoloop::history::{Recorder, Recording};
use chronoloop::net::{Link, Network, NodeId, Odds, VirtualNetwork};
use chronoloop::rng::{Rng, SeededRng};

const SECOND: u64 = 1_000_000_000;

/// How many asker-and-follower pairs the cluster has.
const PAIRS: usize = 3;

/// How many times an asker tries before it gives up.
const ATTEMPTS: u32 = 3;

/// How long an asker waits for an answer before trying again.
const PATIENCE: Duration = Duration::from_secs(4);

/// How long a follower waits for work before deciding there is none coming.
const QUIET: Duration = Duration::from_secs(20);

/// How long a follower takes to think about what it was asked.
const THINKING: RangeInclusive<Duration> = Duration::from_millis(100)..=Duration::from_secs(2);

/// The seeds the cheap cases use. Spread across the range, and including both of its ends.
const SEEDS: [u64; 8] = [0, 1, 2, 7, 42, 20_260_918, u64::MAX - 1, u64::MAX];

/// How many seeds the gated sweep walks.
const SWEEP: u64 = 20_000;

/// What a run recorded, without the header naming the seed that produced it.
///
/// Anything comparing two *different* seeds has to leave the header out. A recording's written form
/// opens with `chronoloop history seed <n>`, so two seeds differ in their text whatever their runs
/// did — an assertion against the whole thing would hold even with the engine cut off from the seed
/// entirely, which is worse than no assertion at all.
fn body(recording: &Recording) -> String {
    recording
        .entries()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<String>>()
        .join("\n")
}

/// Odds of `numerator` in `denominator`, failing the test rather than returning an error.
fn odds(numerator: u32, denominator: u32) -> Odds {
    Odds::new(numerator, denominator).unwrap_or_else(|e| panic!("test odds are sound: {e}"))
}

/// A window between two instants, failing the test rather than returning an error.
fn span(start: u64, end: u64) -> Window {
    Window::new(VirtualTime::from_nanos(start), VirtualTime::from_nanos(end))
        .unwrap_or_else(|e| panic!("a test window is sound: {e}"))
}

/// A wire slow and unreliable enough to be worth sweeping: it loses, it repeats itself, and the
/// spread of its delays is wide enough that messages overtake one another.
fn unreliable() -> Link {
    Link::new(Duration::from_millis(50)..=Duration::from_secs(3))
        .with_loss(odds(1, 4))
        .with_duplication(odds(1, 8))
}

/// Asks its follower until it gets an answer back or runs out of tries.
///
/// Every wait here is bounded by a deadline and the loop by a count, which is what makes the sweep
/// possible: no seed can leave this task waiting for something that is never coming.
async fn asker<C, N>(clock: &C, node: &N, follower: NodeId, label: usize, history: &Recorder)
where
    C: Clock,
    N: Network<Message = u64>,
{
    for attempt in 1..=ATTEMPTS {
        node.send(follower, u64::from(attempt));
        match clock.timeout(PATIENCE, node.recv()).await {
            Ok(answer) => {
                history.record(
                    clock,
                    format!(
                        "asker {label} heard {} back on try {attempt}",
                        answer.message()
                    ),
                );
                return;
            }
            Err(_) => history.record(
                clock,
                format!("asker {label} heard nothing on try {attempt}"),
            ),
        }
    }
    history.record(clock, format!("asker {label} gave up"));
}

/// Answers whatever arrives, and stops once the wire has been quiet long enough.
async fn follower<C, R, N>(clock: &C, rng: &mut R, node: &N, label: usize, history: &Recorder)
where
    C: Clock,
    R: Rng,
    N: Network<Message = u64>,
{
    while let Ok(asked) = clock.timeout(QUIET, node.recv()).await {
        let question = *asked.message();
        history.record(clock, format!("follower {label} was asked {question}"));
        clock.sleep(rng.duration_in(THINKING)).await;
        node.send(asked.sender(), question);
    }
}

/// Runs the whole cluster under `seed` and returns the history it wrote.
fn run(seed: u64) -> Recording {
    let mut executor = Executor::new();
    let recorder = Recorder::new();
    let mut seeds = SeededRng::from_seed(seed);

    // The wire and every task draw from generators of their own, all seeded from the run's, so what
    // one of them does never depends on how often another drew.
    let network: VirtualNetwork<u64, SeededRng> =
        VirtualNetwork::new(executor.handle(), SeededRng::from_seed(seeds.next_u64()))
            .with_default_link(unreliable());

    let mut askers = Vec::with_capacity(PAIRS);
    let mut followers = Vec::with_capacity(PAIRS);
    for _ in 0..PAIRS {
        askers.push(network.add_node());
        followers.push(network.add_node());
    }
    let peers: Vec<NodeId> = followers.iter().map(Network::id).collect();

    // Trouble on top of an already-unreliable wire: the first pair's way out is dead for the first
    // few seconds, and the second pair's loses most of what it carries for the whole run.
    network.set_faults(FaultSchedule::new(vec![
        Fault::Partition {
            from: askers[0].id(),
            to: peers[0],
            during: span(0, 6 * SECOND),
        },
        Fault::Loss {
            from: askers[1].id(),
            to: peers[1],
            during: Window::forever_from(VirtualTime::ZERO),
            odds: odds(3, 4),
        },
    ]));

    for (label, node) in followers.into_iter().enumerate() {
        let clock = executor.handle();
        let history = recorder.clone();
        let mut rng = SeededRng::from_seed(seeds.next_u64());
        executor.spawn(async move {
            follower(&clock, &mut rng, &node, label, &history).await;
        });
    }

    // The askers are spawned from inside the run rather than before it, and waited on in the order
    // they were started however they finish.
    let clock = executor.handle();
    let history = recorder.clone();
    executor.spawn(async move {
        let mut waiting = Vec::with_capacity(PAIRS);
        for (label, node) in askers.into_iter().enumerate() {
            let task_clock = clock.clone();
            let task_history = history.clone();
            let peer = peers[label];
            waiting.push(clock.spawn(async move {
                asker(&task_clock, &node, peer, label, &task_history).await;
                label
            }));
        }
        for joined in waiting {
            let label = joined.await;
            history.record(&clock, format!("asker {label} is done"));
        }
    });

    executor
        .run()
        .unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"));
    let entries = recorder
        .finish()
        .unwrap_or_else(|e| panic!("seed {seed} recorded something unreadable: {e}"));
    Recording::new(seed, entries)
}

#[test]
fn a_run_of_the_whole_engine_is_a_function_of_its_seed() {
    for seed in SEEDS {
        assert_eq!(
            run(seed).to_string(),
            run(seed).to_string(),
            "seed {seed} must write the same history every time"
        );
    }
}

#[test]
fn a_recorded_run_of_the_whole_engine_replays_unchanged() {
    // Recorded from an actual run. Unlike the case above this one outlives the process that wrote
    // it, which is what makes the debug-and-release half of the claim real: `make local-validation`
    // runs this file in both profiles and both compare against this same text. It fails if the
    // engine ever schedules this cluster differently — a tie broken the other way, a draw taken in
    // a different order, a wait that stopped leaving nothing behind.
    //
    // It is worth reading as well as asserting. Asker 0 hears nothing until the partition over its
    // way out closes at six seconds. Asker 2 has its answer at 6.2s but is not reported done until
    // twelve, because the coordinator waits on the three of them in the order it started them.
    let recorded = "\
chronoloop history seed 20260918
4.000000000s asker 0 heard nothing on try 1
4.000000000s asker 1 heard nothing on try 1
4.000000000s asker 2 heard nothing on try 1
4.526320909s follower 2 was asked 2
6.263818457s asker 2 heard 2 back on try 2
8.000000000s asker 0 heard nothing on try 2
8.000000000s asker 1 heard nothing on try 2
9.718495989s follower 0 was asked 3
10.213431924s asker 0 heard 3 back on try 3
10.213431924s asker 0 is done
12.000000000s asker 1 heard nothing on try 3
12.000000000s asker 1 gave up
12.000000000s asker 1 is done
12.000000000s asker 2 is done
";

    assert_eq!(run(20_260_918).to_string(), recorded);
    assert_eq!(
        recorded.parse::<Recording>(),
        Ok(run(20_260_918)),
        "the recorded form reads back to the run that wrote it"
    );
}

#[test]
fn different_seeds_explore_different_runs() {
    struct Case {
        name: &'static str,
        left: u64,
        right: u64,
    }
    let cases = [
        Case {
            name: "the first two seeds",
            left: 0,
            right: 1,
        },
        Case {
            name: "adjacent seeds further along",
            left: 42,
            right: 43,
        },
        Case {
            name: "either end of the range",
            left: 7,
            right: u64::MAX,
        },
    ];

    // Without this, a change that quietly stopped the seed reaching behaviour — a generator built
    // from a constant, the delays replaced by their midpoints — would leave every other case in
    // this file passing, since a run that ignores its seed is still perfectly reproducible.
    for case in cases {
        assert_ne!(
            body(&run(case.left)),
            body(&run(case.right)),
            "{}",
            case.name
        );
    }
}

#[test]
fn no_two_of_the_cheap_seeds_write_the_same_history() {
    // The cheap half of the sweep, so CI has a version of the assertion too. Collisions are not
    // impossible — the gated sweep below pins exactly where they are and why — but none of these
    // eight seeds is one.
    let distinct: BTreeSet<String> = SEEDS.iter().map(|&seed| body(&run(seed))).collect();
    assert_eq!(distinct.len(), SEEDS.len());
}

#[test]
#[ignore = "sweeps thousands of seeds; run it with `make local-validation`"]
fn a_long_sweep_finds_no_seed_that_is_not_a_function_of_itself() {
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    for seed in 0..SWEEP {
        let recording = run(seed);
        assert_eq!(
            recording.to_string(),
            run(seed).to_string(),
            "seed {seed} wrote two different histories in one process"
        );
        *seen.entry(body(&recording)).or_default() += 1;
    }

    // Measured, not guessed: 19,972 of the 20,000 seeds write a history no other seed writes. All
    // of these are fixed numbers for a fixed system, so they either always hold or always fail —
    // deterministic rather than flaky. The count is what collapses if entropy ever stops reaching
    // behaviour: with the engine cut off from its seed it goes to 1.
    assert_eq!(seen.len(), 19_972, "distinct histories across the sweep");

    // The twenty-nine seeds that do collide all write the *same* history, and it is worth knowing
    // which: the run where nothing ever reached a follower. Every instant in it comes from the
    // four-second deadline rather than from a draw, so it is the one history a seed cannot change.
    let shared: Vec<(&String, usize)> = seen
        .iter()
        .map(|(text, &count)| (text, count))
        .filter(|&(_, count)| count > 1)
        .collect();
    assert_eq!(
        shared.len(),
        1,
        "only one history is reachable from more than one seed"
    );
    let (text, seeds) = shared[0];
    assert_eq!(seeds, 29, "how many seeds reach it");
    assert!(
        !text.contains("follower"),
        "and it is the run in which nothing got through: {text}"
    );
}
