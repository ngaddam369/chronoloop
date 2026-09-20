//! What a run passed through, in the order it passed through it.
//!
//! [`crate::world`] gives a state a name and [`crate::store`] keeps it cheaply; neither says *when*
//! a run was in it. A store is a bag of nodes with no order, and it dies with the process that built
//! it. A [`crate::history::Recording`] is ordered and outlives its process, but it holds what
//! happened and never what the world became.
//!
//! A [`Trace`] is both: for every step, the event and the name of the state the run was in after it,
//! written to a file and read back. That is what a run has to leave behind before it can be scrubbed
//! through, compared step against step, or handed to someone else as an artifact.
//!
//! # What a trace names, and what it does not hold
//!
//! The states themselves are not in it. A trace holds their **names**, which are addresses into a
//! [`crate::store::StateStore`] or into a re-run of what the trace's header and fork line name. So a trace
//! read back out of a file can say what changed and when, and needs the store beside it to hand back
//! a world. That is what the form is: an index, not a copy.
//!
//! # The written form
//!
//! ```text
//! chronoloop trace seed 7
//! step 0 0.000000000s 3f2a…c19 opener sent ping
//! step 1 0.412881003s 8b74…a02 answerer replied
//! ```
//!
//! Read left to right: `step `, the step's number, the instant, the state's 64-digit name, and the
//! message. The message is the only field whose length is not fixed and it comes last, which is why
//! the hash sits between the instant and the message rather than at the end of the line — the same
//! discipline the state encoding in [`crate::world`] follows.
//!
//! A run that changed seed partway through says so on a line of its own under the header, and a run
//! that did not writes nothing there:
//!
//! ```text
//! chronoloop trace seed 7
//! forked at 0.412881003s to seed 99
//! step 0 0.000000000s 3f2a…c19 opener sent ping
//! ```
//!
//! The header still names the seed the run started from, because up to that instant that is the run
//! this was. What the fork adds is the rest of what it takes to produce these steps again — without
//! it the file would name a run it is not, which is the one thing a header exists to prevent.
//!
//! **A step's number is written even though the line's position already gives it.** It is a checked
//! redundancy rather than a second source of truth: reading a trace refuses one whose numbers do not
//! run from zero, so a file that lost a line in the middle is turned away rather than quietly
//! renumbering every step after the gap. A trace is addressed by step — a step is what a person asks
//! to be shown and what a report quotes — so the number belongs in the file a person reads.
//!
//! **Every record is one event and the state after it.** There is no record for the world as it was
//! before anything happened, and so no second shape in the format: a system that wants the state it
//! started from in the trace records an event saying so, which is then a step like any other.

use core::fmt;
use core::str::FromStr;

use crate::clock::{ParseVirtualTimeError, VirtualTime};
use crate::fork::Fork;
use crate::history::Entry;
use crate::world::{ParseStateHashError, StateHash};

/// The first line of a written trace, up to the seed itself.
const HEADER: &str = "chronoloop trace seed ";

/// What the line naming a fork opens with, before the instant the run changed seed at.
const FORKED_AT: &str = "forked at ";

/// What separates a fork's instant from the seed the run went on with.
const TO_SEED: &str = " to seed ";

/// What every record of a written trace opens with, before the step's own number.
const STEP: &str = "step ";

/// One step of a run: something that happened, and the state the run was in once it had.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    event: Entry,
    state: StateHash,
}

impl Step {
    /// Creates a step in which `event` happened and left the run in the state called `state`.
    pub fn new(event: Entry, state: StateHash) -> Self {
        Self { event, state }
    }

    /// Returns what happened, and the instant it happened at.
    pub fn event(&self) -> &Entry {
        &self.event
    }

    /// Returns the name of the state the run was in after the event.
    pub fn state(&self) -> StateHash {
        self.state
    }
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.event.at(),
            self.state,
            self.event.message()
        )
    }
}

/// Errors returned when reading a [`Step`] back from the form [`Display`] writes.
///
/// The error names what is wrong with the text but not where the text came from: a caller reading a
/// trace knows the line number and adds it.
///
/// [`Display`]: fmt::Display
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseStepError {
    /// The text is not an instant, a state's name and a message.
    Malformed,
    /// The instant could not be read.
    Time(ParseVirtualTimeError),
    /// The state's name could not be read.
    Hash(ParseStateHashError),
}

impl fmt::Display for ParseStepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => write!(
                f,
                "expected an instant, a state hash and a message, such as \
                 \"1.500000000s <64 hexadecimal digits> ping sent\""
            ),
            Self::Time(error) => write!(f, "{error}"),
            Self::Hash(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ParseStepError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Malformed => None,
            Self::Time(error) => Some(error),
            Self::Hash(error) => Some(error),
        }
    }
}

impl From<ParseVirtualTimeError> for ParseStepError {
    fn from(error: ParseVirtualTimeError) -> Self {
        Self::Time(error)
    }
}

impl From<ParseStateHashError> for ParseStepError {
    fn from(error: ParseStateHashError) -> Self {
        Self::Hash(error)
    }
}

impl FromStr for Step {
    type Err = ParseStepError;

    /// Reads a record from the left, so a line wrong in two places is reported at the earlier one.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (at, rest) = text.split_once(' ').ok_or(ParseStepError::Malformed)?;
        let at: VirtualTime = at.parse()?;
        let (state, message) = rest.split_once(' ').ok_or(ParseStepError::Malformed)?;
        let state: StateHash = state.parse()?;
        // A line of a written trace carries no line ending of its own, so reading one back never
        // lands here. Text handed straight to `parse` can, and several lines of it are not the one
        // step this reads.
        let event = Entry::new(at, message).map_err(|_| ParseStepError::Malformed)?;
        Ok(Self::new(event, state))
    }
}

/// Reads a count the one way a written trace writes one.
///
/// Digits and nothing else, so a sign is refused rather than quietly taken for what it precedes: the
/// text a trace is read from has to be text a trace would have written.
fn count<T: FromStr>(text: &str) -> Option<T> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// What a run passed through, together with the seed that produced it.
///
/// Written and read back through [`Display`] and [`FromStr`], which is the form a trace takes when
/// it is kept in a file:
///
/// ```
/// use chronoloop::clock::VirtualTime;
/// use chronoloop::history::Entry;
/// use chronoloop::trace::{Step, Trace};
/// use chronoloop::world::{Snapshot, World};
///
/// let started = Entry::new(VirtualTime::ZERO, "run started")?;
/// let trace = Trace::new(7, vec![Step::new(started, World::new().state_hash())]);
///
/// let written = trace.to_string();
/// assert_eq!(written.lines().next(), Some("chronoloop trace seed 7"));
/// assert_eq!(written.parse(), Ok(trace));
/// # Ok::<(), chronoloop::history::MultilineMessageError>(())
/// ```
///
/// [`Display`]: fmt::Display
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trace {
    seed: u64,
    fork: Option<Fork>,
    steps: Vec<Step>,
}

impl Trace {
    /// Creates a trace of `steps`, produced by `seed`, in the order the run took them.
    pub fn new(seed: u64, steps: Vec<Step>) -> Self {
        Self {
            seed,
            fork: None,
            steps,
        }
    }

    /// Creates a trace of a run of `seed` that drew from `fork`'s seed after `fork`'s instant.
    ///
    /// The header still names the seed the run started from, because that is the run this one was
    /// until the fork; what the fork adds is the rest of what it takes to produce these steps
    /// again. A trace that left it out would name a run it is not.
    pub fn forked(seed: u64, fork: Fork, steps: Vec<Step>) -> Self {
        Self {
            seed,
            fork: Some(fork),
            steps,
        }
    }

    /// Returns the seed the run was given, which is what it started out drawing from.
    ///
    /// For a run that never forked that is the whole of what produces it again. For one that did,
    /// [`Self::fork`] is the rest.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns where the run stopped drawing from its own seed, or `None` if it never did.
    pub fn fork(&self) -> Option<Fork> {
        self.fork
    }

    /// Returns the steps the run took, in order. The first of them is step zero.
    pub fn steps(&self) -> &[Step] {
        &self.steps
    }

    /// Returns the step numbered `step`, or `None` if the run never reached it.
    ///
    /// A trace is addressed by step — that is what the number written on every record is for — and
    /// this is the way in. What the step names is a state, which a [`crate::store::StateStore`]
    /// hands back and a comparison tells from another.
    pub fn at(&self, step: usize) -> Option<&Step> {
        self.steps.get(step)
    }
}

impl fmt::Display for Trace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{HEADER}{}", self.seed)?;
        if let Some(fork) = self.fork {
            writeln!(f, "{FORKED_AT}{}{TO_SEED}{}", fork.at(), fork.seed())?;
        }
        for (number, step) in self.steps.iter().enumerate() {
            writeln!(f, "{STEP}{number} {step}")?;
        }
        Ok(())
    }
}

/// Errors returned when reading a [`Trace`] back from the form [`Display`] writes.
///
/// [`Display`]: fmt::Display
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseTraceError {
    /// The text is empty, so it carries no header and names no seed.
    MissingHeader,
    /// The first line does not name the seed the way a trace's header does.
    BadHeader,
    /// The line after the header opens like a fork but does not name one.
    BadFork,
    /// A line does not open with the number a step is written with.
    Unnumbered {
        /// Which line it was, counting the header as line 1.
        line: usize,
    },
    /// A step is numbered, but not with the number it would have been written with.
    OutOfSequence {
        /// Which line it was, counting the header as line 1.
        line: usize,
        /// The step the trace had reached.
        expected: usize,
        /// The step the line claims to be.
        found: usize,
    },
    /// A step could not be read.
    BadStep {
        /// Which line it was, counting the header as line 1.
        line: usize,
        /// What was wrong with it.
        source: ParseStepError,
    },
}

impl fmt::Display for ParseTraceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingHeader => write!(f, "a trace is empty and names no seed"),
            Self::BadHeader => write!(f, "expected a first line reading \"{HEADER}<seed>\""),
            Self::BadFork => write!(
                f,
                "line 2: expected \"{FORKED_AT}<instant>{TO_SEED}<seed>\", such as \
                 \"{FORKED_AT}1.500000000s{TO_SEED}99\""
            ),
            Self::Unnumbered { line } => {
                write!(f, "line {line}: expected a step numbered \"{STEP}<n>\"")
            }
            Self::OutOfSequence {
                line,
                expected,
                found,
            } => write!(
                f,
                "line {line}: expected step {expected} and found step {found}"
            ),
            Self::BadStep { line, source } => write!(f, "line {line}: {source}"),
        }
    }
}

impl std::error::Error for ParseTraceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MissingHeader
            | Self::BadHeader
            | Self::BadFork
            | Self::Unnumbered { .. }
            | Self::OutOfSequence { .. } => None,
            Self::BadStep { source, .. } => Some(source),
        }
    }
}

impl FromStr for Trace {
    type Err = ParseTraceError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut lines = text.lines();
        let header = lines.next().ok_or(ParseTraceError::MissingHeader)?;
        let seed = header
            .strip_prefix(HEADER)
            .and_then(count)
            .ok_or(ParseTraceError::BadHeader)?;

        // A fork is written on its own line straight after the header, and only a run that forked
        // has one. Whether it is there settles which line the first step falls on.
        let records: Vec<&str> = lines.collect();
        let (fork, records) = match records.split_first() {
            Some((first, rest)) if first.starts_with(FORKED_AT) => (
                Some(read_fork(first).ok_or(ParseTraceError::BadFork)?),
                rest,
            ),
            _ => (None, records.as_slice()),
        };
        let first_step = if fork.is_some() { 3 } else { 2 };

        let mut steps = Vec::new();
        for (offset, text) in records.iter().enumerate() {
            let line = offset + first_step;
            let (number, record) = text
                .strip_prefix(STEP)
                .and_then(|numbered| numbered.split_once(' '))
                .ok_or(ParseTraceError::Unnumbered { line })?;
            let number: usize = count(number).ok_or(ParseTraceError::Unnumbered { line })?;
            // The numbering is settled before the record is read, so a step in the wrong place is
            // reported as that rather than as whatever its contents turn out to be.
            if number != offset {
                return Err(ParseTraceError::OutOfSequence {
                    line,
                    expected: offset,
                    found: number,
                });
            }
            steps.push(
                record
                    .parse()
                    .map_err(|source| ParseTraceError::BadStep { line, source })?,
            );
        }
        Ok(Self { seed, fork, steps })
    }
}

/// Reads the line a forked trace names its fork on, or nothing if that is not what it is.
fn read_fork(text: &str) -> Option<Fork> {
    let (at, seed) = text.strip_prefix(FORKED_AT)?.split_once(TO_SEED)?;
    Some(Fork::new(at.parse().ok()?, count(seed)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::world::{Name, Resource, Snapshot, Value, World};

    const SECOND: u64 = 1_000_000_000;

    /// An entry, in the shorthand these tests write their expectations in.
    fn entry(at: u64, message: &str) -> Entry {
        Entry::new(VirtualTime::from_nanos(at), message)
            .unwrap_or_else(|e| panic!("a test expectation is one line: {e}"))
    }

    /// A name, failing the test rather than returning an error no case expects.
    fn name(text: &str) -> Name {
        Name::new(text).unwrap_or_else(|e| panic!("a test name is a name: {e}"))
    }

    /// A world holding a database of `replicas` replicas, so the cases have a state to name.
    fn world(replicas: u64) -> World {
        World::new().with_resource(
            name("database"),
            Resource::new().with_field(name("replicas"), Value::Count(replicas)),
        )
    }

    /// A trace of two steps, used wherever the contents do not matter.
    fn sample() -> Trace {
        Trace::new(
            7,
            vec![
                Step::new(entry(0, "provisioning began"), world(1).state_hash()),
                Step::new(
                    entry(3 * SECOND, "the standby caught up"),
                    world(2).state_hash(),
                ),
            ],
        )
    }

    #[test]
    fn a_forked_trace_says_where_it_stopped_being_the_run_it_names() {
        // A trace's header is what a later run is built from, so a run that changed seed partway
        // through has to say so or the file claims to be a run it is not. Pinned as a literal, for
        // the reason the case below is.
        let trace = Trace::forked(
            7,
            Fork::new(VirtualTime::from_nanos(3 * SECOND), 99),
            sample().steps().to_vec(),
        );
        assert_eq!(
            trace.to_string(),
            "chronoloop trace seed 7\n\
             forked at 3.000000000s to seed 99\n\
             step 0 0.000000000s \
             a06ce3f2f48a440ef815cf23196dd9db4915cd40a6ec60cc40a5d333d0d0e1a2 \
             provisioning began\n\
             step 1 3.000000000s \
             079b29cd856e4a9dca5a9ce45edf71f91270b3cef2d383c22dd17da037b5c402 \
             the standby caught up\n"
        );
        assert_eq!(trace.to_string().parse(), Ok(trace));
    }

    #[test]
    fn a_trace_that_names_no_fork_is_the_run_its_seed_produces() {
        assert_eq!(sample().fork(), None);
        assert_eq!(
            sample()
                .to_string()
                .lines()
                .nth(1)
                .map(|line| line.split(' ').next().unwrap_or_default()),
            Some("step"),
            "an unforked trace writes no line between the header and its first step"
        );
    }

    #[test]
    fn a_trace_is_addressed_by_step() {
        // A trace is an index, and this is the way in: a step number is what a person asks to be
        // shown and what a report quotes, so it is what the trace answers to.
        let trace = sample();
        assert_eq!(
            trace.at(0).map(Step::state),
            Some(world(1).state_hash()),
            "the first step is step zero"
        );
        assert_eq!(
            trace.at(1).map(|step| step.event().message()),
            Some("the standby caught up"),
            "and the last of two is step one"
        );
        assert_eq!(
            trace.at(2),
            None,
            "a run of two steps was never in a third state"
        );
    }

    #[test]
    fn a_trace_writes_the_form_it_documents() {
        // Pinned as a literal, recorded from an actual run rather than rebuilt here from the parts
        // — a line assembled out of the same calls the code makes would agree with it whatever the
        // layout was. What this case holds to account is the layout: the header, the step's number,
        // and the instant, the hash and the message in that order. The hashes themselves are held
        // to account by the byte-for-byte oracle beside the encoding in `world.rs`.
        assert_eq!(
            sample().to_string(),
            "chronoloop trace seed 7\n\
             step 0 0.000000000s \
             a06ce3f2f48a440ef815cf23196dd9db4915cd40a6ec60cc40a5d333d0d0e1a2 \
             provisioning began\n\
             step 1 3.000000000s \
             079b29cd856e4a9dca5a9ce45edf71f91270b3cef2d383c22dd17da037b5c402 \
             the standby caught up\n"
        );
    }

    #[test]
    fn the_steps_of_a_trace_are_numbered_from_zero_in_order() {
        // The write side of the rule reading enforces: a trace's steps are numbered from zero and
        // every one of them is the next.
        let steps = (0..4)
            .map(|replicas| {
                Step::new(
                    entry(replicas * SECOND, "scaled"),
                    world(replicas).state_hash(),
                )
            })
            .collect();
        let written = Trace::new(7, steps).to_string();

        let numbers: Vec<String> = written
            .lines()
            .skip(1)
            .map(|line| line.split(' ').take(2).collect::<Vec<_>>().join(" "))
            .collect();
        assert_eq!(numbers, ["step 0", "step 1", "step 2", "step 3"]);
    }

    #[test]
    fn a_trace_round_trips_through_its_written_form() {
        struct Case {
            name: &'static str,
            trace: Trace,
        }
        let cases = [
            Case {
                name: "a run that took no steps",
                trace: Trace::new(0, Vec::new()),
            },
            Case {
                name: "a run of two steps",
                trace: sample(),
            },
            Case {
                name: "the largest seed",
                trace: Trace::new(
                    u64::MAX,
                    vec![Step::new(entry(0, "done"), World::new().state_hash())],
                ),
            },
            Case {
                name: "a step whose message is empty",
                trace: Trace::new(7, vec![Step::new(entry(0, ""), world(1).state_hash())]),
            },
            Case {
                name: "a step at the end of virtual time",
                trace: Trace::new(
                    7,
                    vec![Step::new(entry(u64::MAX, "gave up"), world(1).state_hash())],
                ),
            },
        ];
        for case in cases {
            assert_eq!(
                case.trace.to_string().parse(),
                Ok(case.trace.clone()),
                "{}",
                case.name
            );
        }
    }

    /// One piece of text a trace has to refuse, and what it should say about it.
    struct Refusal {
        name: &'static str,
        text: String,
        want: ParseTraceError,
    }

    /// Asserts each case's text is refused, with the error the case says it should be refused with.
    fn refused(cases: impl IntoIterator<Item = Refusal>) {
        for case in cases {
            assert_eq!(case.text.parse::<Trace>(), Err(case.want), "{}", case.name);
        }
    }

    /// A header naming seed 7, which the records below hang under.
    const HEAD: &str = "chronoloop trace seed 7";

    /// A record a trace would have written for the step numbered `number`.
    fn record(number: usize) -> String {
        format!(
            "step {number} 0.000000000s {} provisioning began",
            world(1).state_hash()
        )
    }

    #[test]
    fn reading_a_trace_rejects_a_header_it_could_not_have_written() {
        // The seed is what makes the file worth keeping, so the header is settled before anything
        // under it is read: a trace whose steps are fine but whose seed is not is not a trace.
        refused([
            Refusal {
                name: "empty",
                text: String::new(),
                want: ParseTraceError::MissingHeader,
            },
            Refusal {
                name: "a header from somewhere else",
                text: "some other tool\n".to_string(),
                want: ParseTraceError::BadHeader,
            },
            Refusal {
                name: "a header with no seed",
                text: "chronoloop trace seed\n".to_string(),
                want: ParseTraceError::BadHeader,
            },
            Refusal {
                name: "a recorded history rather than a trace",
                text: "chronoloop history seed 7\n".to_string(),
                want: ParseTraceError::BadHeader,
            },
            Refusal {
                name: "a seed that is not a number",
                text: "chronoloop trace seed lucky\n".to_string(),
                want: ParseTraceError::BadHeader,
            },
            Refusal {
                name: "a seed wearing a sign",
                text: "chronoloop trace seed +7\n".to_string(),
                want: ParseTraceError::BadHeader,
            },
            Refusal {
                name: "steps but no header",
                text: format!("{}\n", record(0)),
                want: ParseTraceError::BadHeader,
            },
        ]);
    }

    #[test]
    fn reading_a_trace_rejects_a_fork_line_it_could_not_have_written() {
        // A line that opens like a fork is read as one rather than falling through to be read as a
        // step: what is wrong with it is that it does not name a fork, and saying it is an
        // unnumbered step would send a reader looking in the wrong place.
        refused([
            Refusal {
                name: "an instant and no seed",
                text: format!("{HEAD}\nforked at 1.500000000s\n"),
                want: ParseTraceError::BadFork,
            },
            Refusal {
                name: "a seed and no instant",
                text: format!("{HEAD}\nforked at  to seed 99\n"),
                want: ParseTraceError::BadFork,
            },
            Refusal {
                name: "an instant that is not one",
                text: format!("{HEAD}\nforked at halfway to seed 99\n"),
                want: ParseTraceError::BadFork,
            },
            Refusal {
                name: "a seed that is not a number",
                text: format!("{HEAD}\nforked at 1.500000000s to seed lucky\n"),
                want: ParseTraceError::BadFork,
            },
            Refusal {
                name: "a seed wearing a sign",
                text: format!("{HEAD}\nforked at 1.500000000s to seed +99\n"),
                want: ParseTraceError::BadFork,
            },
        ]);
    }

    #[test]
    fn a_fork_line_moves_the_steps_under_it_down_a_line() {
        // The number a step is refused at is the number of the line it is on, which a fork line
        // above it changes. Reported anywhere else, a person opening the file looks at the step
        // before the one that is wrong.
        let text = format!(
            "{HEAD}\nforked at 1.500000000s to seed 99\nstep 1 0.000000000s {} began\n",
            world(1).state_hash()
        );
        assert_eq!(
            text.parse::<Trace>(),
            Err(ParseTraceError::OutOfSequence {
                line: 3,
                expected: 0,
                found: 1,
            })
        );
    }

    #[test]
    fn reading_a_trace_rejects_steps_that_are_not_numbered_the_way_it_numbers_them() {
        // This is what writing the number down buys. A file that lost a line in the middle, or had
        // one added to it, is refused — rather than read back as a trace whose steps from the gap
        // onwards are quietly not the steps the run took.
        let unnumbered = |line| ParseTraceError::Unnumbered { line };
        refused([
            Refusal {
                name: "a step that is not numbered",
                text: format!("{HEAD}\n0.000000000s {} began\n", world(1).state_hash()),
                want: unnumbered(2),
            },
            Refusal {
                name: "a step numbered in words",
                text: format!("{HEAD}\nstep one 0.000000000s began\n"),
                want: unnumbered(2),
            },
            Refusal {
                name: "a step number wearing a sign",
                text: format!(
                    "{HEAD}\nstep +0 0.000000000s {} began\n",
                    world(1).state_hash()
                ),
                want: unnumbered(2),
            },
            Refusal {
                name: "a number and nothing after it",
                text: format!("{HEAD}\nstep 0\n"),
                want: unnumbered(2),
            },
            Refusal {
                name: "a blank line among the steps",
                text: format!("{HEAD}\n\n{}\n", record(0)),
                want: unnumbered(2),
            },
            Refusal {
                name: "a trace that does not start at zero",
                text: format!("{HEAD}\n{}\n", record(1)),
                want: ParseTraceError::OutOfSequence {
                    line: 2,
                    expected: 0,
                    found: 1,
                },
            },
            Refusal {
                name: "a step missing from the middle",
                text: format!("{HEAD}\n{}\n{}\n", record(0), record(2)),
                want: ParseTraceError::OutOfSequence {
                    line: 3,
                    expected: 1,
                    found: 2,
                },
            },
            Refusal {
                name: "the same step twice",
                text: format!("{HEAD}\n{}\n{}\n", record(0), record(0)),
                want: ParseTraceError::OutOfSequence {
                    line: 3,
                    expected: 1,
                    found: 0,
                },
            },
        ]);
    }

    #[test]
    fn reading_a_trace_rejects_a_step_whose_contents_it_could_not_have_written() {
        let hash = world(1).state_hash().to_string();
        refused([
            Refusal {
                name: "an instant that is not one",
                text: format!("{HEAD}\nstep 0 5s {hash} began\n"),
                want: ParseTraceError::BadStep {
                    line: 2,
                    source: ParseStepError::Time(ParseVirtualTimeError::Malformed),
                },
            },
            Refusal {
                name: "a message where the state's name should be",
                text: format!("{HEAD}\nstep 0 0.000000000s provisioning began\n"),
                want: ParseTraceError::BadStep {
                    line: 2,
                    source: ParseStepError::Hash(ParseStateHashError::BadLength { found: 12 }),
                },
            },
            Refusal {
                name: "a state's name a digit short",
                text: format!("{HEAD}\nstep 0 0.000000000s {} began\n", &hash[1..]),
                want: ParseTraceError::BadStep {
                    line: 2,
                    source: ParseStepError::Hash(ParseStateHashError::BadLength { found: 63 }),
                },
            },
            Refusal {
                name: "a state's name in uppercase",
                text: format!(
                    "{HEAD}\nstep 0 0.000000000s {} began\n",
                    hash.to_uppercase()
                ),
                want: ParseTraceError::BadStep {
                    line: 2,
                    source: ParseStepError::Hash(ParseStateHashError::BadDigit),
                },
            },
            Refusal {
                name: "a step with no message at all",
                text: format!("{HEAD}\nstep 0 0.000000000s {hash}\n"),
                want: ParseTraceError::BadStep {
                    line: 2,
                    source: ParseStepError::Malformed,
                },
            },
            Refusal {
                name: "a second step that is wrong",
                text: format!("{HEAD}\n{}\nstep 1 nonsense at all\n", record(0)),
                want: ParseTraceError::BadStep {
                    line: 3,
                    source: ParseStepError::Time(ParseVirtualTimeError::Malformed),
                },
            },
        ]);
    }

    #[test]
    fn a_step_out_of_place_is_reported_as_that_rather_than_as_its_contents() {
        // A line can be both misnumbered and unreadable, and the numbering is what is wrong with the
        // file: renumbering it would leave a trace whose steps are not the steps the run took.
        let text = "chronoloop trace seed 7\nstep 4 nonsense\n";
        assert_eq!(
            text.parse::<Trace>(),
            Err(ParseTraceError::OutOfSequence {
                line: 2,
                expected: 0,
                found: 4,
            })
        );
    }

    #[test]
    fn a_trace_hands_back_what_went_in() {
        let trace = sample();
        assert_eq!(trace.seed(), 7);
        assert_eq!(trace.steps().len(), 2);
        assert_eq!(trace.steps()[0].event(), &entry(0, "provisioning began"));
        assert_eq!(trace.steps()[1].state(), world(2).state_hash());
    }
}
