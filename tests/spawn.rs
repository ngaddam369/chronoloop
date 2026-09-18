//! Fanning work out to spawned tasks and waiting for it to come back.
//!
//! The shape a reconcile takes when it has several things to do at once: start one task per piece
//! of work, then collect the answers. The order the work *finishes* in is the wire's and the seed's
//! to decide; the order it is *collected* in is the supervisor's, and the two are not the same.

use core::ops::RangeInclusive;
use core::time::Duration;

use chronoloop::clock::Clock;
use chronoloop::executor::Executor;
use chronoloop::history::{Entry, Recorder};
use chronoloop::rng::{Rng, SeededRng};

mod common;

use common::{entry, finish};

/// How many workers the supervisor fans out to.
const WORKERS: usize = 4;

/// How long a piece of work takes.
const WORK: RangeInclusive<Duration> = Duration::from_secs(1)..=Duration::from_secs(60);

/// Spawns one worker per piece of work and waits on each in the order it spawned them.
///
/// Returns what the run recorded and the instant it ended at.
fn fan_out(seed: u64) -> (Vec<Entry>, u64) {
    let mut executor = Executor::new();
    let recorder = Recorder::new();
    let mut seeds = SeededRng::from_seed(seed);
    // Each worker draws from its own generator, seeded from the run's, so how long one of them
    // takes does not depend on how many drew before it.
    let worker_seeds: Vec<u64> = (0..WORKERS).map(|_| seeds.next_u64()).collect();

    let clock = executor.handle();
    let history = recorder.clone();
    executor.spawn(async move {
        let mut workers = Vec::with_capacity(WORKERS);
        for (nth, worker_seed) in worker_seeds.into_iter().enumerate() {
            let worker_clock = clock.clone();
            let worker_history = history.clone();
            workers.push(clock.spawn(async move {
                let mut rng = SeededRng::from_seed(worker_seed);
                worker_clock.sleep(rng.duration_in(WORK)).await;
                worker_history.record(&worker_clock, format!("worker {nth} finished"));
                nth
            }));
        }
        for worker in workers {
            let nth = worker.await;
            history.record(&clock, format!("worker {nth} came back"));
        }
    });

    let ended_at = finish(&mut executor);
    (
        recorder.finish().expect("every message is one line"),
        ended_at,
    )
}

/// The worker numbers of every entry whose message ends in `what`, in the order they were recorded.
fn order(log: &[Entry], what: &str) -> Vec<String> {
    log.iter()
        .map(Entry::message)
        .filter(|message| message.ends_with(what))
        .map(|message| message.trim_end_matches(what).trim_end().to_owned())
        .collect()
}

#[test]
fn a_fan_out_is_a_function_of_its_seed() {
    struct Case {
        name: &'static str,
        left: u64,
        right: u64,
    }
    let cases = [
        Case {
            name: "adjacent seeds",
            left: 1,
            right: 2,
        },
        Case {
            name: "either end of the range",
            left: 7,
            right: u64::MAX,
        },
    ];

    for case in cases {
        assert_eq!(
            fan_out(case.left).0,
            fan_out(case.left).0,
            "{}: one seed, one history",
            case.name
        );
        assert_ne!(
            fan_out(case.left).0,
            fan_out(case.right).0,
            "{}: different seeds must send the workers off for different lengths of time",
            case.name
        );
    }
}

#[test]
fn workers_are_collected_in_the_order_they_were_spawned_whatever_order_they_finish_in() {
    let (log, _) = fan_out(1);

    let spawned: Vec<String> = (0..WORKERS).map(|nth| format!("worker {nth}")).collect();
    assert_eq!(
        order(&log, "came back"),
        spawned,
        "the supervisor waits on them in the order it started them"
    );
    // Seed 1 is a seed on which they do not finish in that order. Without this, a fan-out where
    // every worker happened to take the same time would pass the case above and prove nothing.
    assert_ne!(
        order(&log, "finished"),
        spawned,
        "and they finished in an order of the seed's choosing"
    );
}

#[test]
fn the_run_ends_when_the_last_worker_does() {
    let (log, ended_at) = fan_out(1);
    assert_eq!(log.len(), 2 * WORKERS, "each worker finished and came back");

    let latest = log
        .iter()
        .map(|entry| entry.at().as_nanos())
        .max()
        .expect("the fan-out recorded something");
    // Every worker was waited on, so nothing outlives the collecting: the run ends with the slowest
    // of them rather than carrying on to a deadline nobody is holding.
    assert_eq!(ended_at, latest);
}

#[test]
fn a_fan_out_writes_the_history_its_joins_imply() {
    // Recorded from an actual run of seed 1. Worker 2 is the line worth reading: it finished at
    // 26.3 seconds but was not collected until 27.7, because the supervisor was still waiting on
    // worker 1. A join hands the answer over when it is asked for, not the moment it is ready —
    // and a worker that is ready first does not overtake one spawned before it.
    let (log, _) = fan_out(1);

    assert_eq!(
        log,
        vec![
            entry(25_266_372_253, "worker 0 finished"),
            entry(25_266_372_253, "worker 0 came back"),
            entry(26_311_153_826, "worker 2 finished"),
            entry(27_760_525_681, "worker 1 finished"),
            entry(27_760_525_681, "worker 1 came back"),
            entry(27_760_525_681, "worker 2 came back"),
            entry(59_098_088_829, "worker 3 finished"),
            entry(59_098_088_829, "worker 3 came back"),
        ]
    );
}
