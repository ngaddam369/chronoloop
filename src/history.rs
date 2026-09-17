//! What a run wrote down, and the form it keeps when it outlives the process that wrote it.
//!
//! A [`Recorder`] is what the tasks of a run write into; a [`Recording`] is what comes out, and it
//! names the seed it came from. Because the seed travels with the entries, a recording read back
//! from a file is enough on its own to run the same simulation again and check the result against
//! it — a comparison whose two sides did not come from one process.

use core::cell::RefCell;
use core::fmt;
use core::str::FromStr;
use std::rc::Rc;

use crate::clock::{ParseVirtualTimeError, VirtualTime};
use crate::executor::Handle;

/// The first line of a written recording, up to the seed itself.
const HEADER: &str = "chronoloop history seed ";

/// One thing that happened in a run, stamped with the virtual instant it happened at.
///
/// A message is a single line: it carries no newline, since a written entry occupies one line and a
/// newline inside the message would be read back as two entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The virtual instant the entry was recorded at.
    pub at: VirtualTime,
    /// What happened.
    pub message: String,
}

impl Entry {
    /// Creates an entry recorded at `at`.
    pub fn new(at: VirtualTime, message: impl Into<String>) -> Self {
        Self {
            at,
            message: message.into(),
        }
    }
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.at, self.message)
    }
}

/// Errors returned when reading an [`Entry`] back from the form [`Display`] writes.
///
/// [`Display`]: fmt::Display
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseEntryError {
    /// The line is not an instant followed by a message.
    Malformed,
    /// The instant could not be read.
    Time(ParseVirtualTimeError),
}

impl fmt::Display for ParseEntryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => write!(f, "expected an instant followed by a message"),
            Self::Time(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ParseEntryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Malformed => None,
            Self::Time(error) => Some(error),
        }
    }
}

impl From<ParseVirtualTimeError> for ParseEntryError {
    fn from(error: ParseVirtualTimeError) -> Self {
        Self::Time(error)
    }
}

impl FromStr for Entry {
    type Err = ParseEntryError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (at, message) = text.split_once(' ').ok_or(ParseEntryError::Malformed)?;
        Ok(Self::new(at.parse()?, message))
    }
}

/// The history a run is writing, shared by every task taking part in it.
///
/// Cloning shares one history rather than copying it, so a task can be handed its own recorder and
/// the entries still land in one place, in the order the run produced them.
#[derive(Debug, Clone, Default)]
pub struct Recorder(Rc<RefCell<Vec<Entry>>>);

impl Recorder {
    /// Creates an empty history.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `message` at the instant `handle` has reached.
    ///
    /// The instant is taken from the virtual clock rather than supplied, so an entry cannot be
    /// stamped with an instant the run was never at.
    pub fn record(&self, handle: &Handle, message: impl Into<String>) {
        self.0.borrow_mut().push(Entry::new(handle.now(), message));
    }

    /// Returns everything recorded so far, in the order it was recorded.
    pub fn entries(&self) -> Vec<Entry> {
        self.0.borrow().clone()
    }
}

/// A run's history together with the seed that produced it.
///
/// Written and read back through [`Display`] and [`FromStr`], which is the form a recording takes
/// when it is kept in a file:
///
/// ```
/// use chronoloop::clock::VirtualTime;
/// use chronoloop::history::{Entry, Recording};
///
/// let recording = Recording::new(7, vec![Entry::new(VirtualTime::ZERO, "ping sent")]);
/// let written = recording.to_string();
/// assert_eq!(written, "chronoloop history seed 7\n0.000000000s ping sent\n");
/// assert_eq!(written.parse(), Ok(recording));
/// ```
///
/// [`Display`]: fmt::Display
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recording {
    seed: u64,
    entries: Vec<Entry>,
}

impl Recording {
    /// Creates a recording of `entries`, produced by `seed`.
    pub fn new(seed: u64, entries: Vec<Entry>) -> Self {
        Self { seed, entries }
    }

    /// Returns the seed the run was given, which is all that is needed to run it again.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the entries the run recorded, in order.
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }
}

impl fmt::Display for Recording {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{HEADER}{}", self.seed)?;
        for entry in &self.entries {
            writeln!(f, "{entry}")?;
        }
        Ok(())
    }
}

/// Errors returned when reading a [`Recording`] back from the form [`Display`] writes.
///
/// [`Display`]: fmt::Display
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseRecordingError {
    /// The text is empty, so it carries no header and names no seed.
    MissingHeader,
    /// The first line does not name the seed the way a recording's header does.
    BadHeader,
    /// An entry could not be read.
    BadEntry {
        /// Which line it was, counting the header as line 1.
        line: usize,
        /// What was wrong with it.
        source: ParseEntryError,
    },
}

impl fmt::Display for ParseRecordingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingHeader => write!(f, "a recording is empty and names no seed"),
            Self::BadHeader => write!(f, "expected a first line reading \"{HEADER}<seed>\""),
            Self::BadEntry { line, source } => write!(f, "line {line}: {source}"),
        }
    }
}

impl std::error::Error for ParseRecordingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MissingHeader | Self::BadHeader => None,
            Self::BadEntry { source, .. } => Some(source),
        }
    }
}

impl FromStr for Recording {
    type Err = ParseRecordingError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut lines = text.lines();
        let header = lines.next().ok_or(ParseRecordingError::MissingHeader)?;
        let seed = header
            .strip_prefix(HEADER)
            .and_then(|seed| seed.parse().ok())
            .ok_or(ParseRecordingError::BadHeader)?;
        let entries = lines
            .enumerate()
            .map(|(offset, line)| {
                line.parse()
                    .map_err(|source| ParseRecordingError::BadEntry {
                        // The header is line 1 and the first entry is line 2.
                        line: offset + 2,
                        source,
                    })
            })
            .collect::<Result<_, _>>()?;
        Ok(Self::new(seed, entries))
    }
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use super::*;
    use crate::executor::Executor;

    /// A recording with a couple of entries, used wherever the contents do not matter.
    fn sample() -> Recording {
        Recording::new(
            7,
            vec![
                Entry::new(VirtualTime::from_nanos(0), "ping sent"),
                Entry::new(VirtualTime::from_nanos(1_500_000_000), "pong received"),
            ],
        )
    }

    #[test]
    fn an_entry_round_trips_through_its_written_form() {
        struct Case {
            name: &'static str,
            at: u64,
            message: &'static str,
        }
        let cases = [
            Case {
                name: "the start of the simulation",
                at: 0,
                message: "ping sent",
            },
            Case {
                name: "a message of one word",
                at: 1,
                message: "started",
            },
            Case {
                name: "a message carrying spaces of its own",
                at: 1_500_000_000,
                message: "controller 3 reconciled 12 times",
            },
            Case {
                name: "the end of virtual time",
                at: u64::MAX,
                message: "gave up",
            },
        ];
        for case in cases {
            let entry = Entry::new(VirtualTime::from_nanos(case.at), case.message);
            assert_eq!(entry.to_string().parse(), Ok(entry), "{}", case.name);
        }
    }

    #[test]
    fn an_entry_is_written_as_its_instant_then_its_message() {
        let entry = Entry::new(VirtualTime::from_nanos(1_500_000_000), "ping sent");
        assert_eq!(entry.to_string(), "1.500000000s ping sent");
    }

    #[test]
    fn reading_an_entry_rejects_a_line_it_could_not_have_written() {
        struct Case {
            name: &'static str,
            text: &'static str,
            want: ParseEntryError,
        }
        let cases = [
            Case {
                name: "empty",
                text: "",
                want: ParseEntryError::Malformed,
            },
            Case {
                name: "an instant and nothing else",
                text: "1.500000000s",
                want: ParseEntryError::Malformed,
            },
            Case {
                name: "a single word",
                text: "started",
                want: ParseEntryError::Malformed,
            },
            Case {
                name: "a message whose first word is not an instant",
                text: "ping sent",
                want: ParseEntryError::Time(ParseVirtualTimeError::Malformed),
            },
            Case {
                name: "an instant past the end of virtual time",
                text: "99999999999.000000000s ping sent",
                want: ParseEntryError::Time(ParseVirtualTimeError::Overflow),
            },
        ];
        for case in cases {
            assert_eq!(case.text.parse::<Entry>(), Err(case.want), "{}", case.name);
        }
    }

    #[test]
    fn a_recording_round_trips_through_its_written_form() {
        struct Case {
            name: &'static str,
            recording: Recording,
        }
        let cases = [
            Case {
                name: "a run that recorded nothing",
                recording: Recording::new(0, Vec::new()),
            },
            Case {
                name: "a run that recorded two entries",
                recording: sample(),
            },
            Case {
                name: "the largest seed",
                recording: Recording::new(u64::MAX, vec![Entry::new(VirtualTime::ZERO, "done")]),
            },
        ];
        for case in cases {
            assert_eq!(
                case.recording.to_string().parse(),
                Ok(case.recording.clone()),
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn a_recording_names_its_seed_on_its_first_line() {
        // The seed is what makes the file replayable on its own, so its position is part of the
        // format rather than an implementation detail.
        let written = sample().to_string();
        let mut lines = written.lines();
        assert_eq!(lines.next(), Some("chronoloop history seed 7"));
        assert_eq!(lines.next(), Some("0.000000000s ping sent"));
        assert_eq!(lines.next(), Some("1.500000000s pong received"));
        assert_eq!(lines.next(), None);
        assert!(written.ends_with('\n'), "every line is terminated");
    }

    #[test]
    fn reading_a_recording_rejects_text_it_could_not_have_written() {
        struct Case {
            name: &'static str,
            text: &'static str,
            want: ParseRecordingError,
        }
        let cases = [
            Case {
                name: "empty",
                text: "",
                want: ParseRecordingError::MissingHeader,
            },
            Case {
                name: "a header from somewhere else",
                text: "some other tool\n",
                want: ParseRecordingError::BadHeader,
            },
            Case {
                name: "a header with no seed",
                text: "chronoloop history seed\n",
                want: ParseRecordingError::BadHeader,
            },
            Case {
                name: "a seed that is not a number",
                text: "chronoloop history seed lucky\n",
                want: ParseRecordingError::BadHeader,
            },
            Case {
                name: "entries but no header",
                text: "0.000000000s ping sent\n",
                want: ParseRecordingError::BadHeader,
            },
            Case {
                name: "an entry that is not one",
                text: "chronoloop history seed 7\nstarted\n",
                want: ParseRecordingError::BadEntry {
                    line: 2,
                    source: ParseEntryError::Malformed,
                },
            },
            Case {
                name: "a bad entry after a good one",
                text: "chronoloop history seed 7\n0.000000000s ping sent\nlater pong sent\n",
                want: ParseRecordingError::BadEntry {
                    line: 3,
                    source: ParseEntryError::Time(ParseVirtualTimeError::Malformed),
                },
            },
            Case {
                name: "a blank line among the entries",
                text: "chronoloop history seed 7\n\n0.000000000s ping sent\n",
                want: ParseRecordingError::BadEntry {
                    line: 2,
                    source: ParseEntryError::Malformed,
                },
            },
        ];
        for case in cases {
            assert_eq!(
                case.text.parse::<Recording>(),
                Err(case.want),
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn clones_of_a_recorder_append_to_one_history() {
        // Every task in a run holds its own clone, and the entries have to land in one place in the
        // order the run produced them — not in one buffer per task.
        let mut executor = Executor::new();
        let recorder = Recorder::new();

        for task in 1..=2_u64 {
            let handle = executor.handle();
            let recorder = recorder.clone();
            executor.spawn(async move {
                handle.sleep(Duration::from_secs(task)).await;
                recorder.record(&handle, format!("task {task} ran"));
            });
        }
        executor.run().expect("the run finishes");

        assert_eq!(
            recorder.entries(),
            vec![
                Entry::new(VirtualTime::from_nanos(1_000_000_000), "task 1 ran"),
                Entry::new(VirtualTime::from_nanos(2_000_000_000), "task 2 ran"),
            ]
        );
    }

    #[test]
    fn an_entry_is_stamped_with_the_instant_the_run_has_reached() {
        // The instant comes from the virtual clock and can come from nowhere else, which is why
        // recording takes a handle rather than an instant a caller chose.
        let mut executor = Executor::new();
        let recorder = Recorder::new();
        let handle = executor.handle();
        let task_recorder = recorder.clone();
        executor.spawn(async move {
            task_recorder.record(&handle, "before");
            handle.sleep(Duration::from_secs(3600)).await;
            task_recorder.record(&handle, "after");
        });
        executor.run().expect("the run finishes");

        let instants: Vec<u64> = recorder
            .entries()
            .iter()
            .map(|entry| entry.at.as_nanos())
            .collect();
        assert_eq!(instants, vec![0, 3_600_000_000_000]);
    }
}
