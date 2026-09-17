//! A retrying worker and a watchdog, written as ordinary async code over the virtual clock.

use core::cell::RefCell;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;
use std::rc::Rc;

use chronoloop::executor::{Executor, Handle, Sleep};
use chronoloop::history::{Entry, Recorder};

mod common;

use common::{entry, finish};

const SECOND: u64 = 1_000_000_000;
const BACKOFF_SECS: [u64; 3] = [1, 2, 4];
const WATCHDOG_SECS: u64 = 5;
const DEADLINE_SECS: u64 = 30;
/// How long the worker keeps going after abandoning its deadline — long enough to pass it.
const SETTLE_SECS: u64 = 60;

/// A wait that records the instant of every poll it passes through.
///
/// Boxing the inner wait keeps the wrapper `Unpin` without a hand-written projection.
struct WatchedSleep {
    sleep: Pin<Box<Sleep>>,
    handle: Handle,
    polls: Rc<RefCell<Vec<u64>>>,
}

impl Future for WatchedSleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let now = self.handle.now().as_nanos();
        self.polls.borrow_mut().push(now);
        self.sleep.as_mut().poll(cx)
    }
}

/// Runs the worker and the watchdog together and returns the history they wrote.
fn run() -> Vec<Entry> {
    let mut executor = Executor::new();
    let recorder = Recorder::new();

    let worker = executor.handle();
    let worker_history = recorder.clone();
    executor.spawn(async move {
        for (attempt, backoff) in BACKOFF_SECS.iter().enumerate() {
            worker.sleep(Duration::from_secs(*backoff)).await;
            worker_history.record(&worker, format!("attempt {attempt} failed"));
        }
        worker_history.record(&worker, "worker gave up");
    });

    spawn_watchdog(&mut executor, &recorder);

    finish(&mut executor);
    recorder.entries()
}

/// Runs the same pair, but under an overall deadline the worker abandons once it is done.
fn run_under_deadline() -> (Vec<Entry>, u64) {
    let mut executor = Executor::new();
    let recorder = Recorder::new();

    let worker = executor.handle();
    let worker_history = recorder.clone();
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
            worker_history.record(&worker, format!("attempt {attempt} failed"));
        }
        worker_history.record(&worker, "worker succeeded");
        // The work is finished, so the deadline is of no further interest.
        drop(deadline);
    });

    spawn_watchdog(&mut executor, &recorder);

    let ended_at = finish(&mut executor);
    (recorder.entries(), ended_at)
}

/// Runs the same pair, but the worker carries on past the instant its abandoned deadline sat at.
///
/// Returns the history, and the instant of every poll of the wait that outlives the deadline.
fn run_past_an_abandoned_deadline() -> (Vec<Entry>, Vec<u64>) {
    let mut executor = Executor::new();
    let recorder = Recorder::new();
    let polls: Rc<RefCell<Vec<u64>>> = Rc::default();

    let worker = executor.handle();
    let worker_history = recorder.clone();
    let worker_polls = Rc::clone(&polls);
    executor.spawn(async move {
        let mut deadline = Box::pin(worker.sleep(Duration::from_secs(DEADLINE_SECS)));
        let armed = core::future::poll_fn(|cx| Poll::Ready(deadline.as_mut().poll(cx))).await;
        assert!(
            armed.is_pending(),
            "the deadline must not have passed already"
        );

        for (attempt, backoff) in BACKOFF_SECS.iter().enumerate() {
            worker.sleep(Duration::from_secs(*backoff)).await;
            worker_history.record(&worker, format!("attempt {attempt} failed"));
        }
        worker_history.record(&worker, "worker succeeded");
        drop(deadline);

        // Settling takes the worker past the instant the deadline was armed for, which is the one
        // place a timer that outlived the wait which armed it can show itself.
        WatchedSleep {
            sleep: Box::pin(worker.sleep(Duration::from_secs(SETTLE_SECS))),
            handle: worker.clone(),
            polls: worker_polls,
        }
        .await;
        worker_history.record(&worker, "worker settled");
    });

    spawn_watchdog(&mut executor, &recorder);

    finish(&mut executor);
    let polls = polls.borrow().clone();
    (recorder.entries(), polls)
}

/// Spawns the watchdog that fires once, part way through the worker's retries.
fn spawn_watchdog(executor: &mut Executor, recorder: &Recorder) {
    let watchdog = executor.handle();
    let watchdog_history = recorder.clone();
    executor.spawn(async move {
        watchdog.sleep(Duration::from_secs(WATCHDOG_SECS)).await;
        watchdog_history.record(&watchdog, "watchdog fired");
    });
}

#[test]
fn a_run_ends_with_its_work_not_with_the_longest_deadline_armed() {
    let (log, ended_at) = run_under_deadline();

    let want: Vec<Entry> = vec![
        entry(SECOND, "attempt 0 failed"),
        entry(3 * SECOND, "attempt 1 failed"),
        entry(5 * SECOND, "watchdog fired"),
        entry(7 * SECOND, "attempt 2 failed"),
        entry(7 * SECOND, "worker succeeded"),
    ];

    assert_eq!(log, want);
    // The abandoned deadline sat thirty seconds out. The run is over at seven, where the work is.
    assert_eq!(ended_at, 7 * SECOND);
}

#[test]
fn a_deadline_abandoned_mid_run_never_wakes_the_task_that_walked_away() {
    let (log, polls) = run_past_an_abandoned_deadline();

    let settled_at = (BACKOFF_SECS.iter().sum::<u64>() + SETTLE_SECS) * SECOND;
    let want: Vec<Entry> = vec![
        entry(SECOND, "attempt 0 failed"),
        entry(3 * SECOND, "attempt 1 failed"),
        entry(5 * SECOND, "watchdog fired"),
        entry(7 * SECOND, "attempt 2 failed"),
        entry(7 * SECOND, "worker succeeded"),
        entry(settled_at, "worker settled"),
    ];

    assert_eq!(log, want);
    // Polled once where it is armed and once where it comes due. A deadline whose timer outlived
    // it would wake the worker at thirty seconds as well, and the settle — the only wait it has
    // outstanding by then — would be polled there too, while the history above stayed as it is.
    assert_eq!(polls, [7 * SECOND, settled_at]);
}

#[test]
fn retry_backoff_and_watchdog_interleave_deterministically() {
    let want: Vec<Entry> = vec![
        entry(SECOND, "attempt 0 failed"),
        entry(3 * SECOND, "attempt 1 failed"),
        entry(5 * SECOND, "watchdog fired"),
        entry(7 * SECOND, "attempt 2 failed"),
        entry(7 * SECOND, "worker gave up"),
    ];

    assert_eq!(run(), want);
    assert_eq!(run(), want, "the same run must replay identically");
}
