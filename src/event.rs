//! The queue of future events, ordered so that replay is deterministic.

use core::cmp::Ordering;
use core::fmt;
use std::collections::BinaryHeap;

use crate::clock::VirtualTime;

/// An event taken off an [`EventQueue`], together with the instant it was scheduled for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scheduled<E> {
    /// The virtual instant the event was scheduled for.
    pub at: VirtualTime,
    /// The event itself.
    pub event: E,
}

/// A heap entry. Ordered by `(at, seq)` only, reversed so the max-heap pops the earliest entry.
struct Entry<E> {
    at: VirtualTime,
    seq: u64,
    event: E,
}

impl<E> Entry<E> {
    fn key(&self) -> (VirtualTime, u64) {
        (self.at, self.seq)
    }
}

impl<E> PartialEq for Entry<E> {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl<E> Eq for Entry<E> {}

impl<E> PartialOrd for Entry<E> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<E> Ord for Entry<E> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.key().cmp(&self.key())
    }
}

/// Future events, popped earliest first.
///
/// Events scheduled for the same virtual instant pop in the order they were pushed. Every push is
/// stamped with a monotonic sequence number, so ties never depend on the heap's internal layout
/// and the same pushes always pop in the same order.
pub struct EventQueue<E> {
    heap: BinaryHeap<Entry<E>>,
    next_seq: u64,
}

impl<E> Default for EventQueue<E> {
    fn default() -> Self {
        Self {
            heap: BinaryHeap::new(),
            next_seq: 0,
        }
    }
}

impl<E: fmt::Debug> fmt::Debug for EventQueue<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventQueue")
            .field("pending", &self.heap.len())
            .field("next_seq", &self.next_seq)
            .finish()
    }
}

impl<E> EventQueue<E> {
    /// Creates an empty queue.
    pub fn new() -> Self {
        Self::default()
    }

    /// Schedules `event` for the virtual instant `at`.
    ///
    /// # Panics
    ///
    /// Panics if the queue has already accepted `u64::MAX` pushes, which cannot happen in practice.
    pub fn push(&mut self, at: VirtualTime, event: E) {
        let seq = self.next_seq;
        // Unreachable: exhausting a u64 at a billion pushes per second takes over 580 years.
        self.next_seq = seq
            .checked_add(1)
            .expect("event sequence number exhausted after u64::MAX pushes");
        self.heap.push(Entry { at, seq, event });
    }

    /// Removes and returns the earliest event, or `None` if the queue is empty.
    ///
    /// Events scheduled for the same instant are returned in the order they were pushed.
    pub fn pop(&mut self) -> Option<Scheduled<E>> {
        self.heap
            .pop()
            .map(|Entry { at, event, .. }| Scheduled { at, event })
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
