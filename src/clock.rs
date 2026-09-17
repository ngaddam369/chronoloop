//! Virtual time: the only notion of time a simulation has.

use core::fmt;
use core::str::FromStr;
use core::time::Duration;

/// Nanoseconds in a second, the scale [`VirtualTime`] is both written and read at.
const NANOS_PER_SEC: u64 = 1_000_000_000;

/// The number of fractional digits [`VirtualTime`] is written with.
const FRACTION_DIGITS: usize = 9;

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
        write!(
            f,
            "{}.{:0width$}s",
            self.0 / NANOS_PER_SEC,
            self.0 % NANOS_PER_SEC,
            width = FRACTION_DIGITS
        )
    }
}

/// Errors returned when reading a [`VirtualTime`] back from the form [`Display`] writes.
///
/// The error names what is wrong with the text but not where the text came from: a caller reading a
/// recorded history knows the line number and adds it.
///
/// [`Display`]: fmt::Display
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseVirtualTimeError {
    /// The text is not decimal seconds followed by `s`.
    Malformed,
    /// The fraction is not exactly nine digits, so the text does not name a whole nanosecond.
    FractionWidth {
        /// How many fractional digits the text carried.
        digits: usize,
    },
    /// The instant named is later than virtual time reaches.
    Overflow,
}

impl fmt::Display for ParseVirtualTimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => write!(
                f,
                "expected an instant written as seconds and {FRACTION_DIGITS} digits of \
                 nanoseconds, such as 1.500000000s"
            ),
            Self::FractionWidth { digits } => write!(
                f,
                "expected {FRACTION_DIGITS} digits of nanoseconds, found {digits}"
            ),
            Self::Overflow => write!(f, "instant is later than virtual time reaches"),
        }
    }
}

impl std::error::Error for ParseVirtualTimeError {}

impl FromStr for VirtualTime {
    type Err = ParseVirtualTimeError;

    /// Reads back exactly what [`Display`] writes: `<seconds>.<nine digits>s`.
    ///
    /// Only digits are accepted either side of the point, so a sign is rejected rather than quietly
    /// taken for its absolute value.
    ///
    /// [`Display`]: fmt::Display
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let digits = text
            .strip_suffix('s')
            .ok_or(ParseVirtualTimeError::Malformed)?;
        let (seconds, fraction) = digits
            .split_once('.')
            .ok_or(ParseVirtualTimeError::Malformed)?;
        if fraction.len() != FRACTION_DIGITS {
            return Err(ParseVirtualTimeError::FractionWidth {
                digits: fraction.len(),
            });
        }
        let seconds = decimal(seconds)?;
        let nanos = decimal(fraction)?;
        seconds
            .checked_mul(NANOS_PER_SEC)
            .and_then(|whole| whole.checked_add(nanos))
            .map(Self)
            .ok_or(ParseVirtualTimeError::Overflow)
    }
}

/// Reads a run of ASCII digits, rejecting a sign or any other character outright.
fn decimal(text: &str) -> Result<u64, ParseVirtualTimeError> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ParseVirtualTimeError::Malformed);
    }
    text.parse().map_err(|_| ParseVirtualTimeError::Overflow)
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
    fn parsing_inverts_display() {
        struct Case {
            name: &'static str,
            nanos: u64,
        }
        let cases = [
            Case {
                name: "the start of the simulation",
                nanos: 0,
            },
            Case {
                name: "one nanosecond",
                nanos: 1,
            },
            Case {
                name: "one second",
                nanos: 1_000_000_000,
            },
            Case {
                name: "one hour",
                nanos: 3_600_000_000_000,
            },
            Case {
                name: "the end of virtual time",
                nanos: u64::MAX,
            },
        ];
        for case in cases {
            let time = VirtualTime::from_nanos(case.nanos);
            assert_eq!(time.to_string().parse(), Ok(time), "{}", case.name);
        }
    }

    #[test]
    fn parsing_rejects_anything_display_would_not_have_written() {
        struct Case {
            name: &'static str,
            text: &'static str,
            want: ParseVirtualTimeError,
        }
        let cases = [
            Case {
                name: "empty",
                text: "",
                want: ParseVirtualTimeError::Malformed,
            },
            Case {
                name: "no trailing unit",
                text: "1.000000000",
                want: ParseVirtualTimeError::Malformed,
            },
            Case {
                name: "no fraction",
                text: "1s",
                want: ParseVirtualTimeError::Malformed,
            },
            Case {
                name: "no seconds",
                text: ".000000000s",
                want: ParseVirtualTimeError::Malformed,
            },
            Case {
                name: "eight fractional digits",
                text: "1.00000000s",
                want: ParseVirtualTimeError::FractionWidth { digits: 8 },
            },
            Case {
                name: "ten fractional digits",
                text: "1.0000000000s",
                want: ParseVirtualTimeError::FractionWidth { digits: 10 },
            },
            Case {
                name: "a signed fraction",
                text: "1.+00000001s",
                want: ParseVirtualTimeError::Malformed,
            },
            Case {
                name: "a negative instant",
                text: "-1.000000000s",
                want: ParseVirtualTimeError::Malformed,
            },
            Case {
                name: "seconds that are not a number",
                text: "later.000000000s",
                want: ParseVirtualTimeError::Malformed,
            },
            Case {
                name: "one nanosecond past the end of virtual time",
                text: "18446744073.709551616s",
                want: ParseVirtualTimeError::Overflow,
            },
            Case {
                name: "more seconds than virtual time holds",
                text: "99999999999.000000000s",
                want: ParseVirtualTimeError::Overflow,
            },
            Case {
                name: "more seconds than a u64 holds",
                text: "99999999999999999999999.000000000s",
                want: ParseVirtualTimeError::Overflow,
            },
        ];
        for case in cases {
            assert_eq!(
                case.text.parse::<VirtualTime>(),
                Err(case.want),
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
