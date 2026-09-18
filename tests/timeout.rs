//! Deadlines around work: the move a control loop makes on every pass.
//!
//! A reconciler arms a deadline, does something that may not come back, and calls the deadline off
//! when the work lands first. Nearly every deadline it arms is one it never lets fire, so what
//! matters as much as the result is that the loser of the race leaves nothing behind — the instant
//! a run ends at is the instant its work ended at, and an abandoned wait that still fired would put
//! an instant into the recorded history that nothing in the simulation caused.

use core::cell::RefCell;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::rc::Rc;

use chronoloop::clock::{Clock, Elapsed};
use chronoloop::executor::Executor;
use chronoloop::history::{Entry, Recorder};

mod common;

use common::{entry, finish};

const SECOND: u64 = 1_000_000_000;
const DAY: u64 = 24 * 3600 * SECOND;

/// How long a reconcile is given before its deadline fires.
const DEADLINE: Duration = Duration::from_secs(10);
/// How long a reconcile that finishes in time takes.
const WORK: Duration = Duration::from_secs(2);
/// How many passes the loop makes.
const PASSES: u64 = 3;

/// What a reconcile pass wrote down, and the instant the run ended at.
fn reconcile_loop(work: Duration) -> (Vec<Entry>, u64) {
    let mut executor = Executor::new();
    let recorder = Recorder::new();
    let clock = executor.handle();
    let history = recorder.clone();

    executor.spawn(async move {
        for pass in 1..=PASSES {
            let reconciling = clock.clone();
            let outcome = clock
                .timeout(DEADLINE, async move { reconciling.sleep(work).await })
                .await;
            let said = match outcome {
                Ok(()) => format!("pass {pass} reconciled"),
                Err(elapsed) => format!("pass {pass} gave up: {elapsed}"),
            };
            history.record(&clock, said);
        }
    });

    let ended_at = finish(&mut executor);
    (
        recorder.finish().expect("every message is one line"),
        ended_at,
    )
}

#[test]
fn a_reconcile_that_beats_its_deadline_calls_the_deadline_off() {
    let (log, ended_at) = reconcile_loop(WORK);

    assert_eq!(
        log,
        vec![
            entry(2 * SECOND, "pass 1 reconciled"),
            entry(4 * SECOND, "pass 2 reconciled"),
            entry(6 * SECOND, "pass 3 reconciled"),
        ]
    );
    // Three deadlines armed and three never let fire: were even one of them left in the queue, the
    // run would carry on to the ten-second mark after its last pass.
    assert_eq!(ended_at, 6 * SECOND);
}

#[test]
fn work_that_overruns_its_deadline_is_abandoned_where_the_deadline_fell() {
    let (log, ended_at) = reconcile_loop(Duration::from_nanos(DAY));

    assert_eq!(
        log,
        vec![
            entry(
                10 * SECOND,
                "pass 1 gave up: the deadline passed before the work finished"
            ),
            entry(
                20 * SECOND,
                "pass 2 gave up: the deadline passed before the work finished"
            ),
            entry(
                30 * SECOND,
                "pass 3 gave up: the deadline passed before the work finished"
            ),
        ]
    );
    // The abandoned work wanted a day each time. The run ends at thirty seconds, so none of those
    // three waits survived being given up on.
    assert_eq!(ended_at, 30 * SECOND);
}

/// What a deadline inside a deadline produces: the inner verdict, unless the outer one fell first.
type Nested = Result<Result<(), Elapsed>, Elapsed>;

#[test]
fn the_shorter_of_two_nested_deadlines_is_the_one_that_fires() {
    struct Case {
        name: &'static str,
        outer: u64,
        inner: u64,
        want: Nested,
        want_ended_at: u64,
    }
    let cases = [
        Case {
            name: "the inner deadline is the tighter one",
            outer: 60,
            inner: 5,
            want: Ok(Err(Elapsed)),
            want_ended_at: 5 * SECOND,
        },
        Case {
            name: "the outer deadline is the tighter one",
            outer: 5,
            inner: 60,
            want: Err(Elapsed),
            want_ended_at: 5 * SECOND,
        },
    ];

    for case in cases {
        let mut executor = Executor::new();
        let clock = executor.handle();
        let outcome: Rc<RefCell<Option<Nested>>> = Rc::default();
        let slot = Rc::clone(&outcome);

        executor.spawn(async move {
            let working = clock.clone();
            let inner = clock.timeout(Duration::from_secs(case.inner), async move {
                working.sleep(Duration::from_secs(30)).await;
            });
            let result = clock.timeout(Duration::from_secs(case.outer), inner).await;
            *slot.borrow_mut() = Some(result);
        });

        let ended_at = finish(&mut executor);
        assert_eq!(
            outcome.borrow_mut().take(),
            Some(case.want),
            "{}",
            case.name
        );
        // Whichever deadline lost, both it and the half-hour of work it wrapped are gone.
        assert_eq!(ended_at, case.want_ended_at, "{}", case.name);
    }
}

/// A handover between two tasks: work that finishes when another task says so.
#[derive(Debug, Default)]
struct Handover {
    fired: bool,
    waker: Option<Waker>,
}

/// Both ends of the handover. Cloning shares one, rather than copying it.
#[derive(Clone, Debug, Default)]
struct Signal(Rc<RefCell<Handover>>);

impl Signal {
    /// Finishes the work, waking whoever is waiting on it.
    ///
    /// The one wait this fixture creates is alive whenever this is called, so nothing here has to
    /// take a waker back the way the engine's own waits do.
    fn fire(&self) {
        // The borrow ends with the statement, so the waking happens outside it.
        let waiting = {
            let mut handover = self.0.borrow_mut();
            handover.fired = true;
            handover.waker.take()
        };
        if let Some(waker) = waiting {
            waker.wake();
        }
    }

    /// Returns a future that completes once the work has been signalled.
    fn wait(&self) -> Wait {
        Wait(self.clone())
    }
}

/// A wait for a signal, created by [`Signal::wait`].
struct Wait(Signal);

impl Future for Wait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut handover = self.0.0.borrow_mut();
        if handover.fired {
            return Poll::Ready(());
        }
        handover.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[test]
fn work_woken_by_another_task_finishes_inside_its_deadline() {
    // The deadline is a timer, but the work is not: it completes because another task said so.
    // A timeout that only ever re-checked its work when its own timer fired would miss this and
    // report a deadline that the work had already beaten.
    let mut executor = Executor::new();
    let recorder = Recorder::new();
    let signal = Signal::default();

    let clock = executor.handle();
    let history = recorder.clone();
    let waiting = signal.clone();
    executor.spawn(async move {
        let outcome = clock.timeout(DEADLINE, waiting.wait()).await;
        let said = match outcome {
            Ok(()) => "the call came back",
            Err(_) => "the call timed out",
        };
        history.record(&clock, said);
    });

    let clock = executor.handle();
    executor.spawn(async move {
        clock.sleep(Duration::from_secs(5)).await;
        signal.fire();
    });

    let ended_at = finish(&mut executor);
    assert_eq!(
        recorder.finish().expect("every message is one line"),
        vec![entry(5 * SECOND, "the call came back")]
    );
    assert_eq!(ended_at, 5 * SECOND);
}
