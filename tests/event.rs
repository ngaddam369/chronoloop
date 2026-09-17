//! The event queue driving the virtual clock, the way the executor's loop does.

use chronoloop::clock::{VirtualClock, VirtualTime};
use chronoloop::event::EventQueue;

const PUSHES: [(u64, &str); 8] = [
    (40, "reconcile"),
    (10, "watch fires"),
    (40, "status update"),
    (0, "start"),
    (25, "api reply"),
    (10, "retry timer"),
    (0, "spawn standby"),
    (25, "api reply duplicate"),
];

fn run() -> Vec<(u64, &'static str)> {
    let mut queue = EventQueue::new();
    for (at, event) in PUSHES {
        queue.push(VirtualTime::from_nanos(at), event);
    }

    let mut clock = VirtualClock::new();
    let mut log = Vec::new();
    while let Some(scheduled) = queue.pop() {
        clock
            .advance_to(scheduled.at)
            .unwrap_or_else(|e| panic!("queue drove the clock backwards: {e}"));
        log.push((clock.now().as_nanos(), scheduled.event));
    }
    log
}

#[test]
fn clock_driven_by_queue_advances_monotonically_in_event_order() {
    let log = run();

    assert_eq!(
        log,
        [
            (0, "start"),
            (0, "spawn standby"),
            (10, "watch fires"),
            (10, "retry timer"),
            (25, "api reply"),
            (25, "api reply duplicate"),
            (40, "reconcile"),
            (40, "status update"),
        ]
    );
    assert_eq!(run(), log, "the same pushes must replay the same log");
}
