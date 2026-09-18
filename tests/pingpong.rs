//! The end-to-end slice: a system, a history, and a seed that always writes the same one.

use chronoloop::history::Recording;
use chronoloop::systems::pingpong;

/// The seed the recorded history below was taken from.
const RECORDED_SEED: u64 = 20_260_917;

/// Runs the exchange, failing the test rather than returning an error no case here expects.
fn history(seed: u64) -> Recording {
    pingpong::run(seed).unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"))
}

#[test]
fn the_same_seed_writes_a_byte_identical_history() {
    // The acceptance test for the whole slice, asserted on the written form rather than on the
    // values behind it: what a person keeps in a file is the text, so the text is what has to
    // match. Two runs in one process can only differ if something outside the seed reached
    // behaviour — a static, an address, an iteration order.
    for seed in [0, 1, 7, u64::MAX] {
        assert_eq!(
            history(seed).to_string(),
            history(seed).to_string(),
            "seed {seed}"
        );
    }
}

#[test]
fn a_recorded_history_replays_unchanged() {
    // Recorded from an actual run of RECORDED_SEED. Unlike the case above, this one survives the
    // process it was written in: it fails if the engine ever schedules this exchange differently,
    // or if the written form ever stops being what `replay` can read.
    let recorded = "\
chronoloop history seed 20260917
1.780333547s ping sent
1.802570620s ping received
2.359432095s pong sent
2.389402930s pong received
";

    assert_eq!(history(RECORDED_SEED).to_string(), recorded);
    assert_eq!(
        recorded.parse::<Recording>(),
        Ok(history(RECORDED_SEED)),
        "the recorded form reads back to the run that wrote it"
    );
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

    // Without this, a change that quietly stopped the seed reaching behaviour at all — the delays
    // replaced by constants, a generator built from a fixed value — would leave every other case
    // in this file passing.
    for case in cases {
        assert_ne!(
            history(case.left).to_string(),
            history(case.right).to_string(),
            "{}",
            case.name
        );
    }
}
