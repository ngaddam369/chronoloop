//! Virtual time: the only notion of time a simulation has.

use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::str::FromStr;
use core::task::{Context, Poll};
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

/// What a system may ask the simulation about time.
///
/// A system under test names this capability rather than reaching for a clock of its own: there is
/// no way to get the machine's time through it, so a run of that system cannot depend on anything
/// but its own schedule. Everything a simulation offers is here — the instant it has reached, a
/// wait until a later one, and a deadline around work that may not come back.
pub trait Clock {
    /// The wait this clock hands back.
    type Sleep: Future<Output = ()> + Unpin;

    /// Returns the current instant.
    fn now(&self) -> VirtualTime;

    /// Returns a future that completes once the simulation reaches `deadline`.
    ///
    /// A deadline the simulation has already passed completes on the first poll.
    fn sleep_until(&self, deadline: VirtualTime) -> Self::Sleep;

    /// Returns a future that completes once `duration` of simulated time has gone by.
    ///
    /// Waiting costs no real time: the clock jumps to the deadline once nothing else can run. A
    /// duration reaching beyond the end of virtual time is capped there.
    ///
    /// # Examples
    ///
    /// ```
    /// use core::time::Duration;
    /// use chronoloop::clock::Clock;
    /// use chronoloop::executor::{Executor, ExecutorError};
    ///
    /// let mut executor = Executor::new();
    /// let handle = executor.handle();
    /// executor.spawn(async move {
    ///     handle.sleep(Duration::from_secs(3600)).await;
    /// });
    /// executor.run()?;
    ///
    /// assert_eq!(executor.handle().now().to_string(), "3600.000000000s");
    /// # Ok::<(), ExecutorError>(())
    /// ```
    fn sleep(&self, duration: Duration) -> Self::Sleep {
        let deadline = self
            .now()
            .checked_add(duration)
            .unwrap_or(VirtualTime::from_nanos(u64::MAX));
        self.sleep_until(deadline)
    }

    /// Returns a future that runs `future`, giving up on it once `duration` has gone by.
    ///
    /// This is the move a control loop makes on every pass: arm a deadline, do something that may
    /// not come back, and call the deadline off when the work lands first. Both halves are dropped
    /// as soon as one of them wins, so the loser takes its own wait back out of the queue.
    ///
    /// # Examples
    ///
    /// ```
    /// use core::time::Duration;
    /// use chronoloop::clock::Clock;
    /// use chronoloop::executor::{Executor, ExecutorError};
    ///
    /// let mut executor = Executor::new();
    /// let handle = executor.handle();
    /// let working = handle.clone();
    /// executor.spawn(async move {
    ///     let outcome = handle
    ///         .timeout(Duration::from_secs(10), async move {
    ///             working.sleep(Duration::from_secs(60)).await;
    ///         })
    ///         .await;
    ///     assert!(outcome.is_err());
    /// });
    /// executor.run()?;
    ///
    /// // The deadline fell at ten seconds, and the minute of work it gave up on left nothing
    /// // behind that could carry the clock any further.
    /// assert_eq!(executor.handle().now().to_string(), "10.000000000s");
    /// # Ok::<(), ExecutorError>(())
    /// ```
    fn timeout<F: Future>(&self, duration: Duration, future: F) -> Timeout<Self::Sleep, F>
    where
        Self: Sized,
    {
        Timeout {
            deadline: self.sleep(duration),
            work: Box::pin(future),
        }
    }
}

/// Returned by [`Clock::timeout`] when the deadline arrived before the work finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Elapsed;

impl fmt::Display for Elapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the deadline passed before the work finished")
    }
}

impl std::error::Error for Elapsed {}

/// A deadline around a future, created by [`Clock::timeout`].
///
/// The work is polled first and the deadline second. Work that becomes ready at the very instant
/// its deadline fires has therefore finished in time — a tie settled by a fixed rule rather than by
/// which of the two the simulation happened to wake first, which is what makes the answer the same
/// on every replay.
///
/// Dropping this drops both halves, so whichever of them lost the race takes its own wait back out
/// of the queue. An abandoned deadline cannot go on to move the clock to an instant nothing in the
/// simulation is waiting for.
///
/// The work is held behind a `Box`, allocated once when the timeout is created and never again.
/// Reaching a pinned field through a pinned struct cannot be written without `unsafe`, which this
/// crate forbids outright; one allocation per deadline is the price of that, and it is paid where a
/// deadline is armed rather than on every poll.
pub struct Timeout<S, F> {
    deadline: S,
    work: Pin<Box<F>>,
}

impl<S, F> fmt::Debug for Timeout<S, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Timeout").finish_non_exhaustive()
    }
}

impl<S: Future<Output = ()> + Unpin, F: Future> Future for Timeout<S, F> {
    type Output = Result<F::Output, Elapsed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Poll::Ready(output) = this.work.as_mut().poll(cx) {
            return Poll::Ready(Ok(output));
        }
        Pin::new(&mut this.deadline).poll(cx).map(|()| Err(Elapsed))
    }
}

#[cfg(test)]
mod tests {
    use core::cell::RefCell;
    use std::rc::Rc;

    use super::*;
    use crate::executor::{Executor, Handle};

    const DAY: u64 = 24 * 3600 * NANOS_PER_SEC;

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

    /// Runs `body`, built around a handle to the simulation, as that simulation's only task.
    ///
    /// Returns what the task produced together with the instant the run ended at. The second is
    /// what shows up a timer nobody called off: a run ends where its work ended, so a wait left
    /// armed after the thing waiting for it walked away carries the clock somewhere the work never
    /// reached.
    fn simulate<T, F>(body: impl FnOnce(Handle) -> F) -> (T, u64)
    where
        T: 'static,
        F: Future<Output = T> + 'static,
    {
        let mut executor = Executor::new();
        let outcome: Rc<RefCell<Option<T>>> = Rc::default();
        let slot = Rc::clone(&outcome);
        let future = body(executor.handle());
        executor.spawn(async move {
            *slot.borrow_mut() = Some(future.await);
        });
        executor
            .run()
            .unwrap_or_else(|e| panic!("run did not finish: {e}"));
        let ended_at = executor.handle().now().as_nanos();
        let produced = outcome
            .borrow_mut()
            .take()
            .expect("the only task of the run finished");
        (produced, ended_at)
    }

    /// The work a timeout is put around: a wait that ends at an instant, or one that never does.
    fn work(handle: &Handle, done_at: Option<u64>) -> Pin<Box<dyn Future<Output = ()>>> {
        match done_at {
            Some(at) => Box::pin(handle.sleep_until(VirtualTime::from_nanos(at))),
            None => Box::pin(core::future::pending()),
        }
    }

    #[test]
    fn a_timeout_resolves_to_whichever_of_the_work_and_the_deadline_comes_first() {
        struct Case {
            name: &'static str,
            /// The instant the work finishes at, or `None` if it never finishes.
            work_done_at: Option<u64>,
            /// How long the work is given.
            limit: u64,
            want: Result<(), Elapsed>,
            /// Where the run ends, which is where the surviving wait ended. Whichever of the two
            /// lost the race is dropped, so its timer must leave no instant behind it.
            want_ended_at: u64,
        }
        let cases = [
            Case {
                name: "the work finishes first",
                work_done_at: Some(10),
                limit: DAY,
                want: Ok(()),
                want_ended_at: 10,
            },
            Case {
                name: "the deadline comes first",
                work_done_at: Some(2 * DAY),
                limit: 50,
                want: Err(Elapsed),
                want_ended_at: 50,
            },
            Case {
                name: "the work finishes at the very instant the deadline fires",
                work_done_at: Some(50),
                limit: 50,
                want: Ok(()),
                want_ended_at: 50,
            },
            Case {
                name: "work that never finishes",
                work_done_at: None,
                limit: 50,
                want: Err(Elapsed),
                want_ended_at: 50,
            },
            Case {
                name: "no time at all, and work that wanted none",
                work_done_at: Some(0),
                limit: 0,
                want: Ok(()),
                want_ended_at: 0,
            },
            Case {
                name: "no time at all, and work that wanted some",
                work_done_at: Some(10),
                limit: 0,
                want: Err(Elapsed),
                want_ended_at: 0,
            },
        ];

        for case in cases {
            let (outcome, ended_at) = simulate(|handle| {
                let waiting = work(&handle, case.work_done_at);
                handle.timeout(Duration::from_nanos(case.limit), waiting)
            });
            assert_eq!(outcome, case.want, "{}", case.name);
            assert_eq!(
                ended_at, case.want_ended_at,
                "{}: the run ends where its work ended",
                case.name
            );
        }
    }

    #[test]
    fn a_timeout_hands_back_what_the_work_produced() {
        // The work's own output travels through, so a timeout wraps a value-producing call rather
        // than only a wait.
        let (outcome, ended_at) = simulate(|handle| {
            let clock = handle.clone();
            handle.timeout(Duration::from_nanos(DAY), async move {
                clock.sleep(Duration::from_nanos(10)).await;
                "reconciled"
            })
        });
        assert_eq!(outcome, Ok("reconciled"));
        assert_eq!(ended_at, 10);
    }

    #[test]
    fn a_deadline_that_expired_says_so() {
        assert_eq!(
            Elapsed.to_string(),
            "the deadline passed before the work finished"
        );
    }
}
