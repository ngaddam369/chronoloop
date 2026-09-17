//! Virtual time: the only notion of time a simulation has.

use core::fmt;
use core::time::Duration;

/// An instant in simulated time, measured in nanoseconds since the start of the simulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct VirtualTime(u64);

impl VirtualTime {
    /// The start of the simulation.
    pub const ZERO: Self = Self(0);

    /// Creates an instant `nanos` nanoseconds after the start of the simulation.
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// Returns the number of nanoseconds since the start of the simulation.
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Returns the instant `duration` after `self`, or `None` if it does not fit in virtual time.
    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        let nanos = u64::try_from(duration.as_nanos()).ok()?;
        self.0.checked_add(nanos).map(Self)
    }
}

impl fmt::Display for VirtualTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const NANOS_PER_SEC: u64 = 1_000_000_000;
        write!(
            f,
            "{}.{:09}s",
            self.0 / NANOS_PER_SEC,
            self.0 % NANOS_PER_SEC
        )
    }
}

/// Errors returned by [`VirtualClock`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClockError {
    /// An advance targeted an instant earlier than the current one.
    Backwards {
        /// The clock's current instant.
        now: VirtualTime,
        /// The earlier instant the advance targeted.
        target: VirtualTime,
    },
}

impl fmt::Display for ClockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backwards { now, target } => {
                write!(
                    f,
                    "cannot move the virtual clock backwards from {now} to {target}"
                )
            }
        }
    }
}

impl std::error::Error for ClockError {}

/// A simulation's clock. It starts at [`VirtualTime::ZERO`] and only ever moves forward.
#[derive(Debug, Clone, Default)]
pub struct VirtualClock {
    now: VirtualTime,
}

impl VirtualClock {
    /// Creates a clock at the start of the simulation.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the current instant.
    pub fn now(&self) -> VirtualTime {
        self.now
    }

    /// Moves the clock to `target`.
    ///
    /// Advancing to the current instant succeeds, since several events may share one instant.
    /// Advancing to an earlier instant fails with [`ClockError::Backwards`] and leaves the clock
    /// where it was: a clock that moved backwards would reorder a replayed history.
    pub fn advance_to(&mut self, target: VirtualTime) -> Result<(), ClockError> {
        if target < self.now {
            return Err(ClockError::Backwards {
                now: self.now,
                target,
            });
        }
        self.now = target;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_clock_starts_at_zero() {
        assert_eq!(VirtualClock::new().now(), VirtualTime::ZERO);
    }

    #[test]
    fn advance_to_accepts_forward_and_same_instant_and_rejects_backwards() {
        struct Case {
            name: &'static str,
            start: u64,
            target: u64,
            want: Result<(), ClockError>,
            want_now: u64,
        }
        let backwards = |now, target| {
            Err(ClockError::Backwards {
                now: VirtualTime::from_nanos(now),
                target: VirtualTime::from_nanos(target),
            })
        };
        let cases = [
            Case {
                name: "forward",
                start: 10,
                target: 25,
                want: Ok(()),
                want_now: 25,
            },
            Case {
                name: "same instant",
                start: 10,
                target: 10,
                want: Ok(()),
                want_now: 10,
            },
            Case {
                name: "one nanosecond backwards",
                start: 10,
                target: 9,
                want: backwards(10, 9),
                want_now: 10,
            },
            Case {
                name: "back to zero",
                start: 10,
                target: 0,
                want: backwards(10, 0),
                want_now: 10,
            },
        ];
        for case in cases {
            let mut clock = VirtualClock::new();
            clock
                .advance_to(VirtualTime::from_nanos(case.start))
                .unwrap_or_else(|e| panic!("{}: setup advance failed: {e}", case.name));
            assert_eq!(
                clock.advance_to(VirtualTime::from_nanos(case.target)),
                case.want,
                "{}",
                case.name
            );
            assert_eq!(clock.now().as_nanos(), case.want_now, "{}", case.name);
        }
    }

    #[test]
    fn checked_add_returns_none_on_overflow() {
        struct Case {
            name: &'static str,
            start: u64,
            duration: Duration,
            want: Option<u64>,
        }
        let cases = [
            Case {
                name: "zero plus one second",
                start: 0,
                duration: Duration::from_secs(1),
                want: Some(1_000_000_000),
            },
            Case {
                name: "zero duration",
                start: 42,
                duration: Duration::ZERO,
                want: Some(42),
            },
            Case {
                name: "exactly reaches the maximum",
                start: u64::MAX - 1,
                duration: Duration::from_nanos(1),
                want: Some(u64::MAX),
            },
            Case {
                name: "one past the maximum",
                start: u64::MAX,
                duration: Duration::from_nanos(1),
                want: None,
            },
            Case {
                name: "duration wider than u64 nanoseconds",
                start: 0,
                duration: Duration::from_secs(u64::MAX),
                want: None,
            },
        ];
        for case in cases {
            assert_eq!(
                VirtualTime::from_nanos(case.start)
                    .checked_add(case.duration)
                    .map(VirtualTime::as_nanos),
                case.want,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn display_formats_seconds_with_nanosecond_precision() {
        struct Case {
            name: &'static str,
            nanos: u64,
            want: &'static str,
        }
        let cases = [
            Case {
                name: "zero",
                nanos: 0,
                want: "0.000000000s",
            },
            Case {
                name: "one nanosecond",
                nanos: 1,
                want: "0.000000001s",
            },
            Case {
                name: "one and a half seconds",
                nanos: 1_500_000_000,
                want: "1.500000000s",
            },
        ];
        for case in cases {
            assert_eq!(
                VirtualTime::from_nanos(case.nanos).to_string(),
                case.want,
                "{}",
                case.name
            );
        }
    }
}
