//! The faults injected into a run, as data rather than as behaviour.
//!
//! A fault is not something that happens to a simulation; it is something a simulation was *told*
//! about before it began. A [`FaultSchedule`] is an ordered list of them, written to a file and read
//! back, so the conditions a run met can be kept, compared, cut down and run again.
//!
//! That is what makes it the thing to reduce when a run fails. A failure found among thousands of
//! seeds is only useful if it can be shrunk to the few faults that actually caused it, and faults
//! expressed as method calls on a network cannot be dropped one at a time and tried again.
//!
//! **A schedule adds nothing to a run.** The wire asks it what is true at the instant it is sending,
//! rather than a task waking up to make it true — a task that slept until an outage began would move
//! the clock to that instant even when nothing in the simulation cared, writing an instant into the
//! recorded history that nothing had caused.

use core::fmt;
use core::str::FromStr;

use crate::clock::{ParseVirtualTimeError, VirtualTime};
use crate::net::{NodeId, Odds, OddsError};

/// The first line of a written schedule.
///
/// Reachable across the crate because a schedule is written inside other forms — a repro
/// nests one whole — and a header spelled a second time is a second thing to keep in step.
pub(crate) const HEADER: &str = "chronoloop faults";

/// How an end that never comes is written.
const FOREVER: &str = "forever";

/// Returned when a window could not mean anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WindowError {
    /// The window closes before it opens.
    Backwards {
        /// When it was to open.
        start: VirtualTime,
        /// The earlier instant it was to close at.
        end: VirtualTime,
    },
}

impl fmt::Display for WindowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backwards { start, end } => {
                write!(f, "a window opening at {start} cannot close at {end}")
            }
        }
    }
}

impl std::error::Error for WindowError {}

/// The span of simulated time a fault is in force for.
///
/// Half open: it holds the instant it opens at and not the one it closes at, so two windows laid end
/// to end neither overlap nor leave a gap between them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    start: VirtualTime,
    end: VirtualTime,
}

impl Window {
    /// Creates a window open from `start` until `end`.
    ///
    /// # Errors
    ///
    /// Returns [`WindowError`] if `end` is earlier than `start`. A window that opens and closes at
    /// the same instant holds nothing, which is meaningful; one that closes first is a mistake.
    pub fn new(start: VirtualTime, end: VirtualTime) -> Result<Self, WindowError> {
        if end < start {
            return Err(WindowError::Backwards { start, end });
        }
        Ok(Self { start, end })
    }

    /// Creates a window open from `start` that never closes.
    pub fn forever_from(start: VirtualTime) -> Self {
        Self {
            start,
            end: VirtualTime::from_nanos(u64::MAX),
        }
    }

    /// Returns the instant the window opens at.
    pub fn start(&self) -> VirtualTime {
        self.start
    }

    /// Returns the instant the window closes at, which it does not itself hold.
    pub fn end(&self) -> VirtualTime {
        self.end
    }

    /// Returns this window with its ends moved inward to `start` and `end`.
    ///
    /// Crate-private on purpose: reducing a failing run narrows a window towards the span the failure
    /// actually needs, and that is the only caller.
    ///
    /// Infallible by construction, which is why it exists rather than the caller reaching for
    /// [`Window::new`]: each end is clamped into what this window already holds, and the end is
    /// clamped to be no earlier than the start that came out of the first clamp, so a narrowing
    /// cannot produce the one window `new` refuses. A reduction then never has to handle a failure it
    /// could not have caused.
    pub(crate) fn narrowed_to(self, start: VirtualTime, end: VirtualTime) -> Self {
        let start = start.clamp(self.start, self.end);
        Self {
            start,
            end: end.clamp(start, self.end),
        }
    }

    /// Returns `true` if `at` falls inside the window.
    pub fn holds(&self, at: VirtualTime) -> bool {
        // The end of virtual time is the only instant a window that never closes has to hold, and
        // it is the one instant the half-open rule would leave out.
        at >= self.start && (at < self.end || self.end == VirtualTime::from_nanos(u64::MAX))
    }
}

impl fmt::Display for Window {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "from {} until ", self.start)?;
        if self.end == VirtualTime::from_nanos(u64::MAX) {
            write!(f, "{FOREVER}")
        } else {
            write!(f, "{}", self.end)
        }
    }
}

/// Something injected into a run, in force for a span of its simulated time.
///
/// Every fault names one direction of one link, because that is what a link is: a failure that cuts
/// only the way out, and leaves a node hearing answers it can no longer ask for, is both real and
/// the kind a system most easily assumes away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Fault {
    /// The direction carries nothing at all while the window lasts.
    Partition {
        /// The node that sends.
        from: NodeId,
        /// The node that would receive.
        to: NodeId,
        /// How long it lasts.
        during: Window,
    },
    /// The direction drops what it carries at the given odds while the window lasts, in place of
    /// whatever the link itself would have done.
    Loss {
        /// The node that sends.
        from: NodeId,
        /// The node that would receive.
        to: NodeId,
        /// How long it lasts.
        during: Window,
        /// How likely a message is to be dropped.
        odds: Odds,
    },
}

impl Fault {
    /// Returns `true` if this fault is in force on this direction at this instant.
    fn applies(&self, from: NodeId, to: NodeId, at: VirtualTime) -> bool {
        let (sender, receiver, during) = match self {
            Self::Partition {
                from, to, during, ..
            }
            | Self::Loss {
                from, to, during, ..
            } => (*from, *to, during),
        };
        sender == from && receiver == to && during.holds(at)
    }
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Partition { from, to, during } => {
                write!(f, "partition on {from} -> {to} {during}")
            }
            Self::Loss {
                from,
                to,
                during,
                odds,
            } => write!(f, "loss {odds} on {from} -> {to} {during}"),
        }
    }
}

/// Errors returned when reading a [`Fault`] back from the form [`Display`] writes.
///
/// The error names what is wrong with the text but not where the text came from: a caller reading a
/// schedule knows the line number and adds it.
///
/// [`Display`]: fmt::Display
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseFaultError {
    /// The line is not a fault written the way one is written.
    Malformed,
    /// An instant in the line could not be read.
    Time(ParseVirtualTimeError),
    /// The odds could not mean anything.
    Odds(OddsError),
    /// The window could not mean anything.
    Window(WindowError),
}

impl fmt::Display for ParseFaultError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => write!(
                f,
                "expected a fault written as \"partition on node <n> -> node <n> from <instant> \
                 until <instant or {FOREVER}>\", or the same beginning \"loss <n> in <n> on\""
            ),
            Self::Time(error) => write!(f, "{error}"),
            Self::Odds(error) => write!(f, "{error}"),
            Self::Window(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ParseFaultError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Malformed => None,
            Self::Time(error) => Some(error),
            Self::Odds(error) => Some(error),
            Self::Window(error) => Some(error),
        }
    }
}

impl From<ParseVirtualTimeError> for ParseFaultError {
    fn from(error: ParseVirtualTimeError) -> Self {
        Self::Time(error)
    }
}

impl From<OddsError> for ParseFaultError {
    fn from(error: OddsError) -> Self {
        Self::Odds(error)
    }
}

impl From<WindowError> for ParseFaultError {
    fn from(error: WindowError) -> Self {
        Self::Window(error)
    }
}

/// Reads the `node <n>` a link's ends are written as.
fn node(text: &str) -> Result<NodeId, ParseFaultError> {
    let index = text
        .strip_prefix("node ")
        .ok_or(ParseFaultError::Malformed)?;
    if index.is_empty() || !index.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ParseFaultError::Malformed);
    }
    index
        .parse()
        .map(NodeId::from_index)
        .map_err(|_| ParseFaultError::Malformed)
}

/// Reads the `from <instant> until <instant>` a fault's window is written as.
fn window(text: &str) -> Result<Window, ParseFaultError> {
    let span = text
        .strip_prefix("from ")
        .ok_or(ParseFaultError::Malformed)?;
    let (start, end) = span
        .split_once(" until ")
        .ok_or(ParseFaultError::Malformed)?;
    let start = start.parse()?;
    if end == FOREVER {
        return Ok(Window::forever_from(start));
    }
    Ok(Window::new(start, end.parse()?)?)
}

/// Reads the `<n> in <n>` odds are written as.
fn odds(text: &str) -> Result<Odds, ParseFaultError> {
    let (numerator, denominator) = text.split_once(" in ").ok_or(ParseFaultError::Malformed)?;
    let numerator = numerator.parse().map_err(|_| ParseFaultError::Malformed)?;
    let denominator = denominator
        .parse()
        .map_err(|_| ParseFaultError::Malformed)?;
    Ok(Odds::new(numerator, denominator)?)
}

impl FromStr for Fault {
    type Err = ParseFaultError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (kind, rest) = text.split_once(" on ").ok_or(ParseFaultError::Malformed)?;
        let (link, span) = rest
            .split_once(" from ")
            .ok_or(ParseFaultError::Malformed)?;
        let (from, to) = link.split_once(" -> ").ok_or(ParseFaultError::Malformed)?;
        let from = node(from)?;
        let to = node(to)?;
        let during = window(&format!("from {span}"))?;

        if kind == "partition" {
            return Ok(Self::Partition { from, to, during });
        }
        let odds = odds(
            kind.strip_prefix("loss ")
                .ok_or(ParseFaultError::Malformed)?,
        )?;
        Ok(Self::Loss {
            from,
            to,
            during,
            odds,
        })
    }
}

/// The faults a run is to meet, in the order they were put there.
///
/// A list rather than a set: reducing a failing run to its cause works by dropping faults one at a
/// time and trying again, so the order has to be the one it was given. Where two faults are in force
/// on the same direction at the same instant, **the first of them is the one that applies**, so
/// dropping one has a predictable effect on the next.
///
/// Written and read back through [`Display`] and [`FromStr`], which is the form it takes in a file:
///
/// ```
/// use chronoloop::clock::VirtualTime;
/// use chronoloop::fault::{Fault, FaultSchedule, Window};
///
/// let schedule = FaultSchedule::default();
/// assert_eq!(schedule.to_string(), "chronoloop faults\n");
/// assert_eq!("chronoloop faults\n".parse(), Ok(schedule));
/// ```
///
/// [`Display`]: fmt::Display
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FaultSchedule {
    faults: Vec<Fault>,
}

impl FaultSchedule {
    /// Creates a schedule of `faults`, in the order given.
    pub fn new(faults: Vec<Fault>) -> Self {
        Self { faults }
    }

    /// Returns the faults, in the order they were given.
    pub fn faults(&self) -> &[Fault] {
        &self.faults
    }

    /// Returns how many faults the schedule holds, which is what a shrunk one is measured against.
    pub fn len(&self) -> usize {
        self.faults.len()
    }

    /// Returns `true` if the run meets no injected faults at all.
    pub fn is_empty(&self) -> bool {
        self.faults.is_empty()
    }

    /// Returns the first fault in force on this direction at this instant, if any.
    ///
    /// A straight walk of the list. A run carries a handful of faults against many thousands of
    /// messages, so there is nothing here worth indexing, and an index nobody measured would be the
    /// wrong kind of cleverness.
    fn first_applying(&self, from: NodeId, to: NodeId, at: VirtualTime) -> Option<&Fault> {
        self.faults.iter().find(|fault| fault.applies(from, to, at))
    }

    /// Returns `true` if this direction carries nothing at this instant.
    pub(crate) fn partitioned(&self, from: NodeId, to: NodeId, at: VirtualTime) -> bool {
        matches!(
            self.first_applying(from, to, at),
            Some(Fault::Partition { .. })
        )
    }

    /// Returns the odds this direction drops a message at this instant, if a fault says so.
    pub(crate) fn loss(&self, from: NodeId, to: NodeId, at: VirtualTime) -> Option<Odds> {
        match self.first_applying(from, to, at) {
            Some(Fault::Loss { odds, .. }) => Some(*odds),
            _ => None,
        }
    }
}

impl fmt::Display for FaultSchedule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{HEADER}")?;
        for fault in &self.faults {
            writeln!(f, "{fault}")?;
        }
        Ok(())
    }
}

/// Errors returned when reading a [`FaultSchedule`] back from the form [`Display`] writes.
///
/// [`Display`]: fmt::Display
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseScheduleError {
    /// The text is empty, so it carries no header.
    MissingHeader,
    /// The first line is not a schedule's header.
    BadHeader,
    /// A fault could not be read.
    BadFault {
        /// Which line it was, counting the header as line 1.
        line: usize,
        /// What was wrong with it.
        source: ParseFaultError,
    },
}

impl fmt::Display for ParseScheduleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingHeader => write!(f, "a schedule is empty and carries no header"),
            Self::BadHeader => write!(f, "expected a first line reading \"{HEADER}\""),
            Self::BadFault { line, source } => write!(f, "line {line}: {source}"),
        }
    }
}

impl std::error::Error for ParseScheduleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MissingHeader | Self::BadHeader => None,
            Self::BadFault { source, .. } => Some(source),
        }
    }
}

impl FromStr for FaultSchedule {
    type Err = ParseScheduleError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut lines = text.lines();
        let header = lines.next().ok_or(ParseScheduleError::MissingHeader)?;
        if header != HEADER {
            return Err(ParseScheduleError::BadHeader);
        }
        let faults = lines
            .enumerate()
            .map(|(offset, line)| {
                line.parse().map_err(|source| ParseScheduleError::BadFault {
                    // The header is line 1 and the first fault is line 2.
                    line: offset + 2,
                    source,
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(Self::new(faults))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: u64 = 1_000_000_000;

    fn t(nanos: u64) -> VirtualTime {
        VirtualTime::from_nanos(nanos)
    }

    fn node(index: u64) -> NodeId {
        NodeId::from_index(index)
    }

    /// A window from `start` to `end`, failing the test rather than returning an error.
    fn window(start: u64, end: u64) -> Window {
        Window::new(t(start), t(end)).unwrap_or_else(|e| panic!("a test window is sound: {e}"))
    }

    /// Odds of `numerator` in `denominator`, failing the test rather than returning an error.
    fn odds(numerator: u32, denominator: u32) -> Odds {
        Odds::new(numerator, denominator).unwrap_or_else(|e| panic!("test odds are sound: {e}"))
    }

    fn partition(from: u64, to: u64, during: Window) -> Fault {
        Fault::Partition {
            from: node(from),
            to: node(to),
            during,
        }
    }

    fn loss(from: u64, to: u64, during: Window, odds: Odds) -> Fault {
        Fault::Loss {
            from: node(from),
            to: node(to),
            during,
            odds,
        }
    }

    #[test]
    fn a_schedule_writes_the_form_it_documents() {
        let schedule = FaultSchedule::new(vec![
            partition(0, 1, window(5 * SECOND, 20 * SECOND)),
            loss(1, 0, Window::forever_from(VirtualTime::ZERO), odds(1, 2)),
        ]);

        assert_eq!(
            schedule.to_string(),
            "chronoloop faults\n\
             partition on node 0 -> node 1 from 5.000000000s until 20.000000000s\n\
             loss 1 in 2 on node 1 -> node 0 from 0.000000000s until forever\n"
        );
    }

    #[test]
    fn writing_a_schedule_and_reading_it_back_gives_the_same_schedule() {
        struct Case {
            name: &'static str,
            schedule: FaultSchedule,
        }
        let cases = [
            Case {
                name: "no faults at all",
                schedule: FaultSchedule::default(),
            },
            Case {
                name: "one outage that ends",
                schedule: FaultSchedule::new(vec![partition(0, 1, window(SECOND, 2 * SECOND))]),
            },
            Case {
                name: "one outage that never ends",
                schedule: FaultSchedule::new(vec![partition(3, 7, Window::forever_from(t(42)))]),
            },
            Case {
                name: "a link that drops half of what it carries",
                schedule: FaultSchedule::new(vec![loss(2, 0, window(0, u64::MAX), odds(1, 2))]),
            },
            Case {
                name: "several faults together",
                schedule: FaultSchedule::new(vec![
                    partition(0, 1, window(SECOND, 2 * SECOND)),
                    loss(1, 0, Window::forever_from(t(0)), odds(3, 4)),
                    partition(1, 0, window(0, 1)),
                ]),
            },
        ];

        for case in cases {
            assert_eq!(
                case.schedule.to_string().parse(),
                Ok(case.schedule.clone()),
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn reading_a_schedule_rejects_text_it_could_not_have_written() {
        struct Case {
            name: &'static str,
            text: &'static str,
            want: ParseScheduleError,
        }
        let malformed = |line| ParseScheduleError::BadFault {
            line,
            source: ParseFaultError::Malformed,
        };
        let cases = [
            Case {
                name: "empty",
                text: "",
                want: ParseScheduleError::MissingHeader,
            },
            Case {
                name: "some other tool's output",
                text: "faults:\n",
                want: ParseScheduleError::BadHeader,
            },
            Case {
                name: "a header with something after it",
                text: "chronoloop faults for seed 7\n",
                want: ParseScheduleError::BadHeader,
            },
            Case {
                name: "a kind that is not a fault",
                text: "chronoloop faults\nreorder on node 0 -> node 1 from 0.000000000s until forever\n",
                want: malformed(2),
            },
            Case {
                name: "no window",
                text: "chronoloop faults\npartition on node 0 -> node 1\n",
                want: malformed(2),
            },
            Case {
                name: "only one node",
                text: "chronoloop faults\npartition on node 0 from 0.000000000s until forever\n",
                want: malformed(2),
            },
            Case {
                name: "a node that is not numbered",
                text: "chronoloop faults\npartition on node one -> node 1 from 0.000000000s until forever\n",
                want: malformed(2),
            },
            Case {
                name: "a blank line",
                text: "chronoloop faults\n\n",
                want: malformed(2),
            },
            Case {
                name: "an instant that is not one",
                text: "chronoloop faults\npartition on node 0 -> node 1 from 5s until forever\n",
                want: ParseScheduleError::BadFault {
                    line: 2,
                    source: ParseFaultError::Time(ParseVirtualTimeError::Malformed),
                },
            },
            Case {
                name: "a window that runs backwards",
                text: "chronoloop faults\npartition on node 0 -> node 1 from 9.000000000s until 1.000000000s\n",
                want: ParseScheduleError::BadFault {
                    line: 2,
                    source: ParseFaultError::Window(WindowError::Backwards {
                        start: t(9 * SECOND),
                        end: t(SECOND),
                    }),
                },
            },
            Case {
                name: "odds that ask for more than always",
                text: "chronoloop faults\nloss 3 in 2 on node 0 -> node 1 from 0.000000000s until forever\n",
                want: ParseScheduleError::BadFault {
                    line: 2,
                    source: ParseFaultError::Odds(OddsError::TooLikely {
                        numerator: 3,
                        denominator: 2,
                    }),
                },
            },
            Case {
                name: "a second fault that is wrong",
                text: "chronoloop faults\npartition on node 0 -> node 1 from 0.000000000s until forever\nnonsense\n",
                want: malformed(3),
            },
        ];

        for case in cases {
            assert_eq!(
                case.text.parse::<FaultSchedule>(),
                Err(case.want),
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn a_window_holds_its_start_and_not_its_end() {
        struct Case {
            name: &'static str,
            at: u64,
            want: bool,
        }
        let cases = [
            Case {
                name: "the instant before it opens",
                at: 4,
                want: false,
            },
            Case {
                name: "the instant it opens",
                at: 5,
                want: true,
            },
            Case {
                name: "the middle",
                at: 12,
                want: true,
            },
            Case {
                name: "the instant before it closes",
                at: 19,
                want: true,
            },
            Case {
                name: "the instant it closes",
                at: 20,
                want: false,
            },
            Case {
                name: "after it has closed",
                at: 21,
                want: false,
            },
        ];

        // Half open, so two windows laid end to end neither overlap nor leave a gap between them.
        let during = window(5, 20);
        for case in cases {
            assert_eq!(during.holds(t(case.at)), case.want, "{}", case.name);
        }
    }

    #[test]
    fn a_window_that_never_ends_holds_the_end_of_virtual_time() {
        let forever = Window::forever_from(VirtualTime::ZERO);
        assert!(forever.holds(VirtualTime::ZERO));
        assert!(forever.holds(t(u64::MAX)));
    }

    #[test]
    fn a_narrowing_only_ever_moves_a_window_inward() {
        // What makes narrowing infallible, and so what keeps a reduction from having to handle a
        // failure it could not have caused: each end is clamped into what the window already holds,
        // and the end is clamped to be no earlier than the start that came out of the first clamp.
        //
        // Reducing a run bisects inside the window it is narrowing, so it never asks for any of the
        // cases below. They are pinned because the clamp is the whole reason the signature does not
        // return a `Result`, and without them nothing at all holds it.
        struct Case {
            name: &'static str,
            during: Window,
            start: u64,
            end: u64,
            want: Window,
        }
        let cases = [
            Case {
                name: "inside, which is what a reduction asks for",
                during: window(5, 20),
                start: 7,
                end: 12,
                want: window(7, 12),
            },
            Case {
                name: "an end asked to move outward stays where it is",
                during: window(5, 20),
                start: 0,
                end: 99,
                want: window(5, 20),
            },
            Case {
                name: "one end outward and the other in",
                during: window(5, 20),
                start: 0,
                end: 12,
                want: window(5, 12),
            },
            Case {
                name: "backwards, so the start it settled on wins",
                during: window(5, 20),
                start: 12,
                end: 7,
                want: window(12, 12),
            },
            Case {
                name: "a start past the end of the window",
                during: window(5, 20),
                start: 99,
                end: 99,
                want: window(20, 20),
            },
            Case {
                name: "a window that never closes, narrowed to an instant",
                during: Window::forever_from(t(5)),
                start: 5,
                end: 6,
                want: window(5, 6),
            },
        ];

        for case in cases {
            assert_eq!(
                case.during.narrowed_to(t(case.start), t(case.end)),
                case.want,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn a_window_cannot_close_before_it_opens() {
        assert_eq!(
            Window::new(t(9), t(1)),
            Err(WindowError::Backwards {
                start: t(9),
                end: t(1),
            })
        );
        assert!(
            Window::new(t(9), t(9)).is_ok(),
            "a window that holds nothing is still a window"
        );
    }

    #[test]
    fn a_fault_applies_to_one_direction_and_says_nothing_about_the_other() {
        let schedule = FaultSchedule::new(vec![partition(0, 1, window(5, 20))]);

        assert!(schedule.partitioned(node(0), node(1), t(10)));
        assert!(!schedule.partitioned(node(1), node(0), t(10)));
        assert!(!schedule.partitioned(node(0), node(2), t(10)));
    }

    #[test]
    fn a_partition_is_not_a_loss_and_a_loss_is_not_a_partition() {
        let schedule = FaultSchedule::new(vec![
            partition(0, 1, window(0, 100)),
            loss(2, 3, window(0, 100), odds(1, 2)),
        ]);

        assert!(schedule.partitioned(node(0), node(1), t(10)));
        assert_eq!(schedule.loss(node(0), node(1), t(10)), None);
        assert!(!schedule.partitioned(node(2), node(3), t(10)));
        assert_eq!(schedule.loss(node(2), node(3), t(10)), Some(odds(1, 2)));
    }

    #[test]
    fn the_first_of_two_overlapping_faults_is_the_one_that_applies() {
        // Dropping a fault has to have a predictable effect on the next, which is the whole point
        // of a schedule being an ordered list rather than a set.
        let schedule = FaultSchedule::new(vec![
            loss(0, 1, window(0, 100), odds(1, 4)),
            loss(0, 1, window(0, 100), odds(3, 4)),
        ]);

        assert_eq!(schedule.loss(node(0), node(1), t(10)), Some(odds(1, 4)));
    }

    #[test]
    fn an_empty_schedule_faults_nothing() {
        let schedule = FaultSchedule::default();

        assert!(schedule.is_empty());
        assert_eq!(schedule.len(), 0);
        assert!(!schedule.partitioned(node(0), node(1), t(10)));
        assert_eq!(schedule.loss(node(0), node(1), t(10)), None);
    }
}
