//! The queue of future events, ordered so that replay is deterministic.

use core::fmt;
use std::collections::BTreeMap;

use crate::clock::VirtualTime;

/// An event taken off an [`EventQueue`], together with the instant it was scheduled for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scheduled<E> {
    /// The virtual instant the event was scheduled for.
    pub at: VirtualTime,
    /// The event itself.
    pub event: E,
}

/// Identifies one scheduled event, so that whoever scheduled it can take it back.
///
/// Returned by [`EventQueue::push`] and accepted by [`EventQueue::cancel`]. Sequence numbers only
/// ever go up, so an identifier whose event has already fired or been cancelled can never name a
/// later one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId {
    at: VirtualTime,
    seq: u64,
}

/// Future events, popped earliest first.
///
/// Events scheduled for the same virtual instant pop in the order they were pushed. Every push is
/// stamped with a monotonic sequence number and the queue is keyed by `(instant, sequence)`, so
/// ties never depend on the queue's internal layout and the same pushes always pop in the same
/// order. That key is unique, which is also what lets a single event be cancelled by name.
pub struct EventQueue<E> {
    pending: BTreeMap<EventId, E>,
    next_seq: u64,
}

impl<E> Default for EventQueue<E> {
    fn default() -> Self {
        Self {
            pending: BTreeMap::new(),
            next_seq: 0,
        }
    }
}

impl<E: fmt::Debug> fmt::Debug for EventQueue<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventQueue")
            .field("pending", &self.pending.len())
            .field("next_seq", &self.next_seq)
            .finish()
    }
}

impl<E> EventQueue<E> {
    /// Creates an empty queue.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of events waiting to fire.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Returns `true` if no event is waiting to fire.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Schedules `event` for the virtual instant `at`, returning the identifier that cancels it.
    ///
    /// # Panics
    ///
    /// Panics if the queue has already accepted `u64::MAX` pushes, which cannot happen in practice.
    pub fn push(&mut self, at: VirtualTime, event: E) -> EventId {
        let seq = self.next_seq;
        // Unreachable: exhausting a u64 at a billion pushes per second takes over 580 years.
        self.next_seq = seq
            .checked_add(1)
            .expect("event sequence number exhausted after u64::MAX pushes");
        let id = EventId { at, seq };
        self.pending.insert(id, event);
        id
    }

    /// Removes and returns the earliest event, or `None` if the queue is empty.
    ///
    /// Events scheduled for the same instant are returned in the order they were pushed.
    pub fn pop(&mut self) -> Option<Scheduled<E>> {
        self.pending
            .pop_first()
            .map(|(EventId { at, .. }, event)| Scheduled { at, event })
    }

    /// Takes the event named by `id` back out of the queue, returning it.
    ///
    /// Returns `None` if that event has already fired or already been cancelled, so cancelling a
    /// wait that has since completed costs nothing and means nothing.
    pub fn cancel(&mut self, id: EventId) -> Option<E> {
        self.pending.remove(&id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(nanos: u64) -> VirtualTime {
        VirtualTime::from_nanos(nanos)
    }

    fn drain<E>(queue: &mut EventQueue<E>) -> Vec<(u64, E)> {
        core::iter::from_fn(|| queue.pop())
            .map(|s| (s.at.as_nanos(), s.event))
            .collect()
    }

    #[test]
    fn pop_returns_events_in_time_then_insertion_order() {
        struct Case {
            name: &'static str,
            pushes: &'static [(u64, &'static str)],
            want: &'static [(u64, &'static str)],
        }
        const MAX: u64 = u64::MAX;
        let cases = [
            Case {
                name: "empty queue",
                pushes: &[],
                want: &[],
            },
            Case {
                name: "distinct times pushed out of order",
                pushes: &[(30, "c"), (10, "a"), (20, "b")],
                want: &[(10, "a"), (20, "b"), (30, "c")],
            },
            Case {
                name: "all at one instant keep push order",
                pushes: &[(5, "first"), (5, "second"), (5, "third"), (5, "fourth")],
                want: &[(5, "first"), (5, "second"), (5, "third"), (5, "fourth")],
            },
            Case {
                name: "mixed times with ties at several instants",
                pushes: &[
                    (20, "b1"),
                    (10, "a1"),
                    (20, "b2"),
                    (30, "c1"),
                    (10, "a2"),
                    (20, "b3"),
                ],
                want: &[
                    (10, "a1"),
                    (10, "a2"),
                    (20, "b1"),
                    (20, "b2"),
                    (20, "b3"),
                    (30, "c1"),
                ],
            },
            Case {
                name: "tie at zero",
                pushes: &[(0, "x"), (0, "y")],
                want: &[(0, "x"), (0, "y")],
            },
            Case {
                name: "tie at the maximum instant",
                pushes: &[(MAX, "x"), (0, "early"), (MAX, "y")],
                want: &[(0, "early"), (MAX, "x"), (MAX, "y")],
            },
        ];
        for case in cases {
            let mut queue = EventQueue::new();
            for &(at, label) in case.pushes {
                queue.push(t(at), label);
            }
            assert_eq!(drain(&mut queue), case.want, "{}", case.name);
        }
    }

    #[test]
    fn push_after_pop_still_orders_by_time_then_seq() {
        let mut queue = EventQueue::new();
        queue.push(t(10), "a");
        queue.push(t(20), "b");
        queue.push(t(30), "c");

        assert_eq!(queue.pop().map(|s| s.event), Some("a"));

        queue.push(t(15), "earlier than the rest");
        queue.push(t(20), "tied with b, pushed later");

        assert_eq!(
            drain(&mut queue),
            [
                (15, "earlier than the rest"),
                (20, "b"),
                (20, "tied with b, pushed later"),
                (30, "c"),
            ]
        );
    }

    #[test]
    fn cancel_removes_only_the_named_event() {
        struct Case {
            name: &'static str,
            pushes: &'static [(u64, &'static str)],
            cancel: &'static [usize],
            want: &'static [(u64, &'static str)],
        }
        const MAX: u64 = u64::MAX;
        let cases = [
            Case {
                name: "nothing cancelled",
                pushes: &[(10, "a"), (20, "b")],
                cancel: &[],
                want: &[(10, "a"), (20, "b")],
            },
            Case {
                name: "the earliest event",
                pushes: &[(10, "a"), (20, "b"), (30, "c")],
                cancel: &[0],
                want: &[(20, "b"), (30, "c")],
            },
            Case {
                name: "the latest event",
                pushes: &[(10, "a"), (20, "b"), (30, "c")],
                cancel: &[2],
                want: &[(10, "a"), (20, "b")],
            },
            Case {
                name: "one of three tied at an instant",
                pushes: &[(5, "first"), (5, "second"), (5, "third")],
                cancel: &[1],
                want: &[(5, "first"), (5, "third")],
            },
            Case {
                name: "a tie at the maximum instant",
                pushes: &[(MAX, "x"), (0, "early"), (MAX, "y")],
                cancel: &[0],
                want: &[(0, "early"), (MAX, "y")],
            },
            Case {
                name: "every event",
                pushes: &[(10, "a"), (20, "b")],
                cancel: &[0, 1],
                want: &[],
            },
        ];
        for case in cases {
            let mut queue = EventQueue::new();
            let ids: Vec<EventId> = case
                .pushes
                .iter()
                .map(|&(at, label)| queue.push(t(at), label))
                .collect();

            for &index in case.cancel {
                assert_eq!(
                    queue.cancel(ids[index]),
                    Some(case.pushes[index].1),
                    "{}: cancelling a pending event returns it",
                    case.name
                );
            }

            assert_eq!(queue.len(), case.want.len(), "{}", case.name);
            assert_eq!(drain(&mut queue), case.want, "{}", case.name);
        }
    }

    #[test]
    fn cancelling_an_event_that_is_no_longer_pending_does_nothing() {
        struct Case {
            name: &'static str,
            pops: usize,
            cancels_before: usize,
        }
        let cases = [
            Case {
                name: "already popped",
                pops: 1,
                cancels_before: 0,
            },
            Case {
                name: "already cancelled",
                pops: 0,
                cancels_before: 1,
            },
        ];
        for case in cases {
            let mut queue = EventQueue::new();
            let first = queue.push(t(10), "a");
            queue.push(t(20), "b");

            for _ in 0..case.pops {
                assert_eq!(queue.pop().map(|s| s.event), Some("a"), "{}", case.name);
            }
            for _ in 0..case.cancels_before {
                assert_eq!(queue.cancel(first), Some("a"), "{}", case.name);
            }

            assert_eq!(queue.cancel(first), None, "{}", case.name);
            assert_eq!(drain(&mut queue), [(20, "b")], "{}", case.name);
        }
    }

    #[test]
    fn a_cancelled_identifier_never_names_a_later_event() {
        // Sequence numbers only ever go up, so a stale identifier cannot collide with a live event.
        let mut queue = EventQueue::new();
        let first = queue.push(t(10), "a");

        assert_eq!(queue.cancel(first), Some("a"));
        queue.push(t(10), "pushed after the cancel");

        assert_eq!(queue.cancel(first), None);
        assert_eq!(drain(&mut queue), [(10, "pushed after the cancel")]);
    }

    #[test]
    fn len_and_is_empty_follow_the_pending_events() {
        let mut queue = EventQueue::new();
        assert_eq!(queue.len(), 0);
        assert!(queue.is_empty());

        let first = queue.push(t(10), "a");
        queue.push(t(20), "b");
        assert_eq!(queue.len(), 2);
        assert!(!queue.is_empty());

        assert_eq!(queue.cancel(first), Some("a"));
        assert_eq!(queue.len(), 1);

        assert_eq!(queue.pop().map(|s| s.event), Some("b"));
        assert_eq!(queue.len(), 0);
        assert!(queue.is_empty());
    }

    #[test]
    fn payload_needs_no_ordering() {
        // Neither Ord nor PartialEq: ordering must never consult the payload.
        struct Opaque(f64);

        let mut queue = EventQueue::new();
        queue.push(t(2), Opaque(f64::NAN));
        queue.push(t(1), Opaque(1.0));
        queue.push(t(2), Opaque(2.0));

        let popped: Vec<(u64, f64)> = drain(&mut queue)
            .into_iter()
            .map(|(at, Opaque(v))| (at, v))
            .collect();

        assert_eq!(popped.len(), 3);
        assert_eq!((popped[0].0, popped[0].1), (1, 1.0));
        assert_eq!(popped[1].0, 2);
        assert!(
            popped[1].1.is_nan(),
            "tied NaN payload pushed first pops first"
        );
        assert_eq!((popped[2].0, popped[2].1), (2, 2.0));
    }
}
