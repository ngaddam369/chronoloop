//! A year of virtual time, traversed by two long-lived loops the way a real operator runs.

use core::cell::RefCell;
use core::time::Duration;
use std::rc::Rc;

use chronoloop::executor::Executor;

const SECOND: u64 = 1_000_000_000;
const HOUR: u64 = 3600 * SECOND;
const DAY: u64 = 24 * HOUR;
const YEAR_DAYS: u64 = 365;
const RENEWALS: u64 = YEAR_DAYS * 24;

type Log = Rc<RefCell<Vec<(u64, String)>>>;

/// Runs a lease renewer and a watchdog for a simulated year, returning the log and the final instant.
fn run() -> (Vec<(u64, String)>, u64) {
    let mut executor = Executor::new();
    let log: Log = Log::default();

    let renewer = executor.handle();
    let renewer_log = Rc::clone(&log);
    executor.spawn(async move {
        for renewal in 1..=RENEWALS {
            renewer.sleep(Duration::from_nanos(HOUR)).await;
            renewer_log
                .borrow_mut()
                .push((renewer.now().as_nanos(), format!("renewed lease {renewal}")));
        }
    });

    let watchdog = executor.handle();
    let watchdog_log = Rc::clone(&log);
    executor.spawn(async move {
        for sweep in 1..=YEAR_DAYS {
            watchdog.sleep(Duration::from_nanos(DAY)).await;
            watchdog_log
                .borrow_mut()
                .push((watchdog.now().as_nanos(), format!("swept day {sweep}")));
        }
    });

    executor
        .run()
        .unwrap_or_else(|e| panic!("run did not finish: {e}"));
    let ended_at = executor.handle().now().as_nanos();
    let entries = log.borrow().clone();
    (entries, ended_at)
}

#[test]
fn a_year_of_virtual_time_costs_only_its_scheduled_events() {
    let (log, ended_at) = run();

    // One entry per scheduled wake-up and not one more, however far apart the wake-ups are.
    assert_eq!(log.len() as u64, RENEWALS + YEAR_DAYS);
    assert_eq!(ended_at, YEAR_DAYS * DAY);

    assert_eq!(
        log.first(),
        Some(&(HOUR, "renewed lease 1".to_owned())),
        "the first entry is the first renewal"
    );
    assert_eq!(
        log.last(),
        Some(&(YEAR_DAYS * DAY, format!("renewed lease {RENEWALS}"))),
        "the last entry is the final renewal"
    );
}

#[test]
fn entries_run_forwards_with_ties_broken_by_the_order_the_timers_were_armed() {
    let (log, _) = run();

    for pair in log.windows(2) {
        let [(earlier, before), (later, after)] = pair else {
            unreachable!("windows(2) always yields pairs")
        };
        assert!(
            earlier <= later,
            "log went backwards from {before} at {earlier} to {after} at {later}"
        );
    }

    // Both loops are due at the end of the first day. The tie goes to whichever timer was armed
    // first, not to whichever task was spawned first: the watchdog asked to be woken at instant 0,
    // the renewer only at the twenty-third hour, so the sweep holds the lower sequence number.
    let midnight: Vec<&(u64, String)> = log.iter().filter(|(at, _)| *at == DAY).collect();
    assert_eq!(
        midnight,
        [
            &(DAY, "swept day 1".to_owned()),
            &(DAY, "renewed lease 24".to_owned()),
        ]
    );
}

#[test]
fn a_year_of_virtual_time_replays_identically() {
    assert_eq!(run(), run());
}
