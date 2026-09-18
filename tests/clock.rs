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
