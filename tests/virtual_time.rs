//! A year of virtual time, traversed by two long-lived loops the way a real operator runs.

use core::cell::Cell;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;
use std::rc::Rc;

use chronoloop::executor::{Executor, Sleep};

mod common;

use common::{Journal, finish};

const SECOND: u64 = 1_000_000_000;
const HOUR: u64 = 3600 * SECOND;
const DAY: u64 = 24 * HOUR;
const YEAR_DAYS: u64 = 365;
const RENEWALS: u64 = YEAR_DAYS * 24;

/// A wait that counts the polls it passes through, so the cost of a run can be observed.
struct CountedSleep {
    sleep: Pin<Box<Sleep>>,
    polls: Rc<Cell<u64>>,
}

impl Future for CountedSleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.polls.set(self.polls.get() + 1);
        self.sleep.as_mut().poll(cx)
    }
}

/// What a year of the lease renewer and the watchdog came to.
struct Year {
    log: Vec<(u64, String)>,
    ended_at: u64,
    polls: u64,
}

/// Runs a lease renewer and a watchdog for a simulated year.
fn run() -> Year {
    let mut executor = Executor::new();
    let journal = Journal::new();
    let polls: Rc<Cell<u64>> = Rc::default();

    let renewer = executor.handle();
    let renewer_journal = journal.clone();
    let renewer_polls = Rc::clone(&polls);
    executor.spawn(async move {
        for renewal in 1..=RENEWALS {
            CountedSleep {
                sleep: Box::pin(renewer.sleep(Duration::from_nanos(HOUR))),
                polls: Rc::clone(&renewer_polls),
            }
            .await;
            renewer_journal.record(&renewer, format!("renewed lease {renewal}"));
        }
    });

    let watchdog = executor.handle();
    let watchdog_journal = journal.clone();
    let watchdog_polls = Rc::clone(&polls);
    executor.spawn(async move {
        for sweep in 1..=YEAR_DAYS {
            CountedSleep {
                sleep: Box::pin(watchdog.sleep(Duration::from_nanos(DAY))),
                polls: Rc::clone(&watchdog_polls),
            }
            .await;
            watchdog_journal.record(&watchdog, format!("swept day {sweep}"));
        }
    });

    let ended_at = finish(&mut executor);

    Year {
        log: journal.entries(),
        ended_at,
        polls: polls.get(),
    }
}

/// The history the two schedules imply, worked out from the schedules rather than from the engine.
///
/// The renewer wakes on every hour and the watchdog at the end of every day. Where they share an
/// instant the sweep comes first, because its timer for that midnight was armed twenty-three hours
/// before the renewal's was.
fn implied_history() -> Vec<(u64, String)> {
    let mut want = Vec::new();
    for hour in 1..=RENEWALS {
        if hour % 24 == 0 {
            want.push((hour * HOUR, format!("swept day {}", hour / 24)));
        }
        want.push((hour * HOUR, format!("renewed lease {hour}")));
    }
    want
}

#[test]
fn a_year_writes_the_history_its_schedules_imply() {
    let year = run();

    assert_eq!(year.log, implied_history());
    assert_eq!(year.ended_at, YEAR_DAYS * DAY);
}

#[test]
fn a_year_of_virtual_time_costs_two_polls_per_scheduled_wake() {
    let year = run();

    // Pending once when the wait is armed, ready once when its instant arrives — and nothing for
    // the hours, days or months in between. A timer that fired twice, or any other wake-up nothing
    // in the run asked for, would show up here and nowhere else: the history would be unchanged.
    // Re-arming is a different matter and is not visible from here — it moves queue traffic around
    // without adding a poll, so it is caught where the queue itself is under test.
    assert_eq!(year.polls, 2 * (RENEWALS + YEAR_DAYS));
}

#[test]
fn ties_at_a_day_boundary_go_to_the_timer_that_was_armed_first() {
    let year = run();

    // The tie goes to whichever timer was armed first, not to whichever task was spawned first:
    // the watchdog asked for midnight a whole day ahead, the renewer only at the twenty-third
    // hour, so the sweep holds the lower sequence number — on every boundary, not just the first.
    for day in [1, 2, 180, YEAR_DAYS] {
        let at_midnight: Vec<&(u64, String)> =
            year.log.iter().filter(|(at, _)| *at == day * DAY).collect();
        assert_eq!(
            at_midnight,
            [
                &(day * DAY, format!("swept day {day}")),
                &(day * DAY, format!("renewed lease {}", day * 24)),
            ],
            "day {day}"
        );
    }
}
