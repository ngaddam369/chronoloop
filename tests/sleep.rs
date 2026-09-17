//! A retrying worker and a watchdog, written as ordinary async code over the virtual clock.

use core::cell::RefCell;
use core::time::Duration;
use std::rc::Rc;

use chronoloop::executor::Executor;

const SECOND: u64 = 1_000_000_000;
const BACKOFF_SECS: [u64; 3] = [1, 2, 4];
const WATCHDOG_SECS: u64 = 5;

type Log = Rc<RefCell<Vec<(u64, String)>>>;

/// Runs the worker and the watchdog together and returns the log they wrote.
fn run() -> Vec<(u64, String)> {
    let mut executor = Executor::new();
    let log: Log = Log::default();

    let worker = executor.handle();
    let worker_log = Rc::clone(&log);
    executor.spawn(async move {
        for (attempt, backoff) in BACKOFF_SECS.iter().enumerate() {
            worker.sleep(Duration::from_secs(*backoff)).await;
            worker_log
                .borrow_mut()
                .push((worker.now().as_nanos(), format!("attempt {attempt} failed")));
        }
        worker_log
            .borrow_mut()
            .push((worker.now().as_nanos(), "worker gave up".to_owned()));
    });

    let watchdog = executor.handle();
    let watchdog_log = Rc::clone(&log);
    executor.spawn(async move {
        watchdog.sleep(Duration::from_secs(WATCHDOG_SECS)).await;
        watchdog_log
            .borrow_mut()
            .push((watchdog.now().as_nanos(), "watchdog fired".to_owned()));
    });

    executor
        .run()
        .unwrap_or_else(|e| panic!("run did not finish: {e}"));
    log.borrow().clone()
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
