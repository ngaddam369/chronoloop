//! The virtual clock driven through its public API as a simulation would drive it.

use core::cell::RefCell;
use core::time::Duration;
use std::rc::Rc;

use chronoloop::clock::{Clock, ClockError, VirtualClock, VirtualTime};
use chronoloop::executor::Executor;

#[test]
fn clock_never_moves_backwards_across_a_sequence_of_advances() {
    // Targets in the order a simulation might request them: forward steps, repeats of the
    // current instant (several events sharing one instant), and stale targets from the past.
    let targets = [5, 5, 12, 3, 12, 40, 0, 39, 41, 41];
    let mut clock = VirtualClock::new();
    let mut previous = clock.now();

    for nanos in targets {
        let target = VirtualTime::from_nanos(nanos);
        let result = clock.advance_to(target);

        if target < previous {
            assert_eq!(
                result,
                Err(ClockError::Backwards {
                    now: previous,
                    target,
                }),
                "advance to {target} from {previous}"
            );
            assert_eq!(
                clock.now(),
                previous,
                "rejected advance to {target} moved the clock"
            );
        } else {
            assert_eq!(result, Ok(()), "advance to {target} from {previous}");
            assert_eq!(clock.now(), target, "advance to {target} from {previous}");
        }

        assert!(
            clock.now() >= previous,
            "clock moved backwards to {}",
            clock.now()
        );
        previous = clock.now();
    }

    assert_eq!(clock.now(), VirtualTime::from_nanos(41));
}

/// A retry loop written against the capability alone.
///
/// It can ask the simulation what time it is and ask it to wait, and it has no way to reach a real
/// clock — which is the whole reason the capability is a trait rather than a convention. Returns
/// the instant each attempt was made at.
async fn retry_with_backoff<C: Clock>(clock: &C, backoffs: &[u64]) -> Vec<u64> {
    let mut attempts = vec![clock.now().as_nanos()];
    for &seconds in backoffs {
        clock.sleep(Duration::from_secs(seconds)).await;
        attempts.push(clock.now().as_nanos());
    }
    attempts
}

#[test]
fn a_system_written_against_the_clock_capability_runs_on_the_engine() {
    const SECOND: u64 = 1_000_000_000;

    let mut executor = Executor::new();
    let attempts: Rc<RefCell<Vec<u64>>> = Rc::default();
    let handle = executor.handle();
    let recorded = Rc::clone(&attempts);
    executor.spawn(async move {
        *recorded.borrow_mut() = retry_with_backoff(&handle, &[1, 2, 4]).await;
    });

    executor.run().expect("the run finishes");
    assert_eq!(*attempts.borrow(), [0, SECOND, 3 * SECOND, 7 * SECOND]);
}

#[test]
fn a_deadline_kept_in_virtual_time_is_met_exactly_however_the_wait_is_split() {
    // The arithmetic a control loop does on every pass: fix a deadline once, do some work, and wait
    // out whatever is left of it. Reached through the executor's clock rather than by constructing
    // instants, so the methods are held to what a running system sees.
    let mut executor = Executor::new();
    let seen: Rc<RefCell<Vec<(VirtualTime, Duration)>>> = Rc::default();
    let handle = executor.handle();
    let recorded = Rc::clone(&seen);
    executor.spawn(async move {
        let deadline = handle.now().saturating_add(Duration::from_secs(5));
        handle.sleep(Duration::from_secs(2)).await;
        let left = deadline.saturating_duration_since(handle.now());
        handle.sleep(left).await;
        recorded.borrow_mut().push((handle.now(), left));
        // Once it has passed, nothing is left, rather than a wait that wraps around.
        handle.sleep(Duration::from_secs(1)).await;
        recorded.borrow_mut().push((
            handle.now(),
            deadline.saturating_duration_since(handle.now()),
        ));
    });

    executor.run().expect("the run finishes");
    assert_eq!(
        *seen.borrow(),
        [
            (
                VirtualTime::from_nanos(5_000_000_000),
                Duration::from_secs(3)
            ),
            (VirtualTime::from_nanos(6_000_000_000), Duration::ZERO),
        ]
    );
}

#[test]
fn a_sleep_past_the_end_of_virtual_time_ends_there() {
    let mut executor = Executor::new();
    let handle = executor.handle();
    executor.spawn(async move {
        handle.sleep(Duration::MAX).await;
    });
    executor.run().expect("the run finishes");
    assert_eq!(executor.handle().now(), VirtualTime::MAX);
}
