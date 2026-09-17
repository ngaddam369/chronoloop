//! Replay: a run is a function of its seed, and of nothing else.

use core::time::Duration;

use chronoloop::executor::Executor;
use chronoloop::rng::SeededRng;

mod common;

use common::{Journal, finish};

const CONTROLLERS: u64 = 3;
const RECONCILES: u64 = 4;
/// The seed the recorded history below was taken from.
const RECORDED_SEED: u64 = 20_260_917;

/// Runs a small fleet whose reconcile intervals are drawn from `seed`, and nothing else.
fn run(seed: u64) -> Vec<(u64, String)> {
    let mut executor = Executor::new();
    let journal = Journal::new();
    let mut seeds = SeededRng::from_seed(seed);

    for controller in 1..=CONTROLLERS {
        let handle = executor.handle();
        let controller_journal = journal.clone();
        // Each controller draws from its own generator, seeded from the run's. Sharing one
        // generator would make a controller's intervals depend on how often its neighbours drew.
        let mut rng = SeededRng::from_seed(seeds.next_u64());
        executor.spawn(async move {
            for reconcile in 1..=RECONCILES {
                let interval = rng.duration_in(Duration::from_secs(1)..=Duration::from_secs(60));
                handle.sleep(interval).await;
                controller_journal.record(
                    &handle,
                    format!("controller {controller} reconciled {reconcile}"),
                );
            }
        });
    }

    finish(&mut executor);
    journal.entries()
}

#[test]
fn the_same_seed_replays_the_same_history() {
    // Two runs of one seed in one process can only differ if something outside the seed reached
    // behaviour — a static, an address, an iteration order. That is what this is here to catch.
    for seed in [0, 1, 7, u64::MAX] {
        assert_eq!(run(seed), run(seed), "seed {seed}");
    }
}

#[test]
fn different_seeds_write_different_histories() {
    struct Case {
        name: &'static str,
        left: u64,
        right: u64,
    }
    let cases = [
        Case {
            name: "adjacent seeds",
            left: 0,
            right: 1,
        },
        Case {
            name: "small seeds",
            left: 7,
            right: 8,
        },
        Case {
            name: "either end of the range",
            left: 42,
            right: u64::MAX,
        },
    ];

    // Without this, a change that quietly stopped the seed reaching behaviour at all — intervals
    // replaced by constants, a generator built from a fixed value — would leave every other test
    // in this file passing.
    for case in cases {
        assert_ne!(run(case.left), run(case.right), "{}", case.name);
    }
}

#[test]
fn a_recorded_history_replays_unchanged() {
    // Recorded from an actual run of RECORDED_SEED. Unlike the two tests above, this one survives
    // the process it was written in: it fails if the engine ever schedules this fleet differently.
    // These instants also depend on the draws behind the intervals, which are pinned beside the
    // generator itself — so a change there fails there first, and a failure here means the engine.
    let want: Vec<(u64, String)> = [
        (17_406_116_577, "controller 3 reconciled 1"),
        (40_347_322_415, "controller 1 reconciled 1"),
        (53_516_597_934, "controller 2 reconciled 1"),
        (57_185_825_859, "controller 2 reconciled 2"),
        (58_216_500_548, "controller 3 reconciled 2"),
        (66_918_492_476, "controller 1 reconciled 2"),
        (75_620_381_691, "controller 1 reconciled 3"),
        (82_609_732_243, "controller 3 reconciled 3"),
        (88_246_539_970, "controller 2 reconciled 3"),
        (90_126_775_954, "controller 2 reconciled 4"),
        (94_891_080_590, "controller 1 reconciled 4"),
        (138_944_060_141, "controller 3 reconciled 4"),
    ]
    .iter()
    .map(|(at, entry)| (*at, (*entry).to_owned()))
    .collect();

    assert_eq!(run(RECORDED_SEED), want);
}
