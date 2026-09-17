//! The virtual clock driven through its public API as a simulation would drive it.

use chronoloop::clock::{ClockError, VirtualClock, VirtualTime};

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
