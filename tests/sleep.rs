//! A retrying worker and a watchdog, written as ordinary async code over the virtual clock.

use core::future::Future;
use core::task::Poll;
use core::time::Duration;

use chronoloop::executor::Executor;

mod common;

use common::{Journal, finish};

const SECOND: u64 = 1_000_000_000;
const BACKOFF_SECS: [u64; 3] = [1, 2, 4];
const WATCHDOG_SECS: u64 = 5;
const DEADLINE_SECS: u64 = 30;

/// Runs the worker and the watchdog together and returns the history they wrote.
fn run() -> Vec<(u64, String)> {
    let mut executor = Executor::new();
    let journal = Journal::new();

    let worker = executor.handle();
    let worker_journal = journal.clone();
    executor.spawn(async move {
        for (attempt, backoff) in BACKOFF_SECS.iter().enumerate() {
            worker.sleep(Duration::from_secs(*backoff)).await;
            worker_journal.record(&worker, format!("attempt {attempt} failed"));
        }
        worker_journal.record(&worker, "worker gave up".to_owned());
    });

    spawn_watchdog(&mut executor, &journal);

    finish(&mut executor);
    journal.entries()
}

/// Runs the same pair, but under an overall deadline the worker abandons once it is done.
fn run_under_deadline() -> (Vec<(u64, String)>, u64) {
    let mut executor = Executor::new();
    let journal = Journal::new();

    let worker = executor.handle();
    let worker_journal = journal.clone();
    executor.spawn(async move {
        let mut deadline = Box::pin(worker.sleep(Duration::from_secs(DEADLINE_SECS)));
        // Arm the deadline without waiting on it, the way a race between work and timeout does.
        let armed = core::future::poll_fn(|cx| Poll::Ready(deadline.as_mut().poll(cx))).await;
        assert!(
            armed.is_pending(),
            "the deadline must not have passed already"
        );

        for (attempt, backoff) in BACKOFF_SECS.iter().enumerate() {
            worker.sleep(Duration::from_secs(*backoff)).await;
            worker_journal.record(&worker, format!("attempt {attempt} failed"));
        }
        worker_journal.record(&worker, "worker succeeded".to_owned());
        // The work is finished, so the deadline is of no further interest.
        drop(deadline);
    });

    spawn_watchdog(&mut executor, &journal);

    let ended_at = finish(&mut executor);
    (journal.entries(), ended_at)
}

/// Spawns the watchdog that fires once, part way through the worker's retries.
fn spawn_watchdog(executor: &mut Executor, journal: &Journal) {
    let watchdog = executor.handle();
    let watchdog_journal = journal.clone();
    executor.spawn(async move {
        watchdog.sleep(Duration::from_secs(WATCHDOG_SECS)).await;
        watchdog_journal.record(&watchdog, "watchdog fired".to_owned());
    });
}

#[test]
fn a_run_ends_with_its_work_not_with_the_longest_deadline_armed() {
    let (log, ended_at) = run_under_deadline();

    let want: Vec<(u64, String)> = vec![
        (SECOND, "attempt 0 failed".to_owned()),
        (3 * SECOND, "attempt 1 failed".to_owned()),
        (5 * SECOND, "watchdog fired".to_owned()),
        (7 * SECOND, "attempt 2 failed".to_owned()),
        (7 * SECOND, "worker succeeded".to_owned()),
    ];

    assert_eq!(log, want);
    // The abandoned deadline sat thirty seconds out. The run is over at seven, where the work is.
    assert_eq!(ended_at, 7 * SECOND);
}

#[test]
fn retry_backoff_and_watchdog_interleave_deterministically() {
    let want: Vec<(u64, String)> = vec![
        (SECOND, "attempt 0 failed".to_owned()),
        (3 * SECOND, "attempt 1 failed".to_owned()),
        (5 * SECOND, "watchdog fired".to_owned()),
        (7 * SECOND, "attempt 2 failed".to_owned()),
        (7 * SECOND, "worker gave up".to_owned()),
    ];

    assert_eq!(run(), want);
    assert_eq!(run(), want, "the same run must replay identically");
}
