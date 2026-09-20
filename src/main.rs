//! chronoloop's command line: run a simulation, and go back through what it left behind.
//!
//! There are two things a run leaves behind, and a family of commands for each. A recorded history
//! is what *happened*: `run --seed <n>` writes one and `replay <path>` reads one back, runs the
//! seed it names again and compares. A trace is what happened **and what the world became**:
//! `trace --seed <n>` writes one, and `inspect`, `diff` and `fork` read one back.
//!
//! In both families the recorded side and the re-run side are separate processes reading a file. A
//! check whose two sides both came from one run would only be comparing a pure function with
//! itself.
//!
//! # A trace is read by running again what it names
//!
//! A trace holds the *names* of the states a run passed through rather than the states themselves,
//! so there is nothing to show until a store is back. Reading one runs again what the file's header
//! and fork line name, refuses to go on if what comes out is not what is written down, and hands
//! back the states that run kept. Everything `inspect`, `diff` and `fork` then show comes out of a
//! run that still produces the file on the page.

use core::fmt;
use std::fs;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use chronoloop::diff::{MissingState, diff, list};
use chronoloop::fork::{Fork, UnreachedStep, fork};
use chronoloop::history::{Entry, ParseRecordingError, Recording};
use chronoloop::store::StateStore;
use chronoloop::systems::{RunError, pingpong, ring};
use chronoloop::trace::{ParseTraceError, Step, Trace};

/// Deterministic simulation of infrastructure control loops.
#[derive(Debug, Parser)]
#[command(name = "chronoloop", version)]
struct Cli {
    /// What to do.
    #[command(subcommand)]
    command: Command,
}

/// The things chronoloop can be asked to do.
#[derive(Debug, Subcommand)]
enum Command {
    /// Run a simulation and write the history it produced.
    Run {
        /// The seed every choice in the run is drawn from.
        #[arg(long)]
        seed: u64,
    },
    /// Run again the seed a recorded history names, and check the result still matches it.
    Replay {
        /// A history written earlier by `run`.
        path: PathBuf,
    },
    /// Run a simulation and write what it passed through, state by state.
    Trace {
        /// The seed every choice in the run is drawn from.
        #[arg(long)]
        seed: u64,
    },
    /// Show one step of a recorded trace, and the world the run was in after it.
    Inspect {
        /// A trace written earlier by `trace` or by `fork`.
        path: PathBuf,
        /// Which step to show, counting from zero.
        #[arg(long)]
        step: usize,
    },
    /// Say what changed between the states two of a recorded trace's steps left the run in.
    Diff {
        /// A trace written earlier by `trace` or by `fork`.
        path: PathBuf,
        /// The step to compare from.
        before: usize,
        /// The step to compare to.
        after: usize,
    },
    /// Run a recorded trace again, drawing from another seed after one of its steps.
    Fork {
        /// A trace written earlier by `trace`.
        path: PathBuf,
        /// The step the run stops being the run the trace records, counting from zero.
        #[arg(long)]
        at: usize,
        /// The seed everything after that step is drawn from.
        #[arg(long)]
        seed: u64,
    },
}

/// What kind of written run a divergence is being reported against.
///
/// Finding the first difference is the same work for a history and for a trace; what differs is
/// what the file is called, what it calls the things in it, and which line the first of them is on.
/// Those three go here rather than into two copies of the search.
#[derive(Debug, Clone, Copy)]
struct Written {
    /// The line the first record falls on, counting the header as line 1.
    first_line: usize,
    /// What the file calls the things it holds, in the plural.
    records: &'static str,
    /// What the file itself is called.
    file: &'static str,
}

impl Written {
    /// A recorded history: a header, and an entry per line under it.
    const RECORDING: Self = Self {
        first_line: 2,
        records: "entries",
        file: "recording",
    };

    /// The trace `trace` was read from, whose fork line — if it has one — moves its steps down.
    fn trace(trace: &Trace) -> Self {
        Self {
            first_line: if trace.fork().is_some() { 3 } else { 2 },
            records: "steps",
            file: "trace",
        }
    }
}

/// The first place a replayed run stopped matching the one recorded.
#[derive(Debug)]
struct Divergence<T> {
    /// What kind of file it was recorded in, which is how it is described.
    written: Written,
    /// Where the two stopped agreeing.
    at: Parting<T>,
}

/// How two written runs stopped agreeing.
#[derive(Debug)]
enum Parting<T> {
    /// A record differs from the one recorded in its place.
    Record {
        /// Its position in the run, counting from zero.
        index: usize,
        /// What the recording holds there.
        recorded: T,
        /// What the replay produced instead.
        replayed: T,
    },
    /// The two agree as far as the shorter one goes, then one of them stops.
    Length {
        /// How many records the recording holds.
        recorded: usize,
        /// How many the replay produced.
        replayed: usize,
    },
}

impl<T: fmt::Display> fmt::Display for Divergence<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.at {
            Parting::Record {
                index,
                recorded,
                replayed,
            } => {
                let line = index + self.written.first_line;
                write!(
                    f,
                    "the replay diverged at line {line}\n  recorded: {recorded}\n  replayed: {replayed}"
                )
            }
            Parting::Length { recorded, replayed } => write!(
                f,
                "the replay recorded {replayed} {} where the {} holds {recorded}",
                self.written.records, self.written.file
            ),
        }
    }
}

/// What went wrong, in the terms of the person who typed the command.
#[derive(Debug)]
enum CliError {
    /// The run produced no history.
    Run(RunError),
    /// The recording could not be read.
    Read {
        /// The path that was asked for.
        path: PathBuf,
        /// Why reading it failed.
        source: io::Error,
    },
    /// The file is not a recording.
    Parse(ParseRecordingError),
    /// The file is not a trace.
    ParseTrace(ParseTraceError),
    /// The output could not be got out.
    Write(io::Error),
    /// The replay produced a different history from the one recorded.
    ///
    /// Boxed, with the one below: a divergence carries both sides of the record it is about, and
    /// two steps of a trace are the widest thing this error has to hold by some way. Every command
    /// returns this type, so the size of the rarest thing that can go wrong is paid on the way out
    /// of all of them.
    Diverged(Box<Divergence<Entry>>),
    /// Running again what the trace names no longer produces the trace.
    TraceDiverged(Box<Divergence<Step>>),
    /// A step was asked for that the run never reached.
    Unreached(UnreachedStep),
    /// A state was asked for that the run's store never saw.
    Missing(MissingState),
    /// A trace that already draws from a second seed was asked for a third.
    AlreadyForked(Fork),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Run(error) => write!(f, "{error}"),
            Self::Read { path, source } => write!(f, "cannot read {}: {source}", path.display()),
            Self::Parse(error) => write!(f, "{error}"),
            Self::ParseTrace(error) => write!(f, "{error}"),
            Self::Write(source) => write!(f, "cannot write to standard output: {source}"),
            Self::Diverged(divergence) => write!(f, "{divergence}"),
            Self::TraceDiverged(divergence) => write!(f, "{divergence}"),
            Self::Unreached(error) => write!(f, "{error}"),
            Self::Missing(error) => write!(f, "{error}"),
            // A run draws from one seed until one instant and from another after it, so there is
            // nowhere for a second fork to go. Written in the words the file's own fork line uses,
            // so that a reader sees the line being talked about.
            Self::AlreadyForked(fork) => write!(
                f,
                "the trace forked at {} to seed {} already, and a run carries one fork",
                fork.at(),
                fork.seed()
            ),
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Run(error) => Some(error),
            Self::Read { source, .. } | Self::Write(source) => Some(source),
            Self::Parse(error) => Some(error),
            Self::ParseTrace(error) => Some(error),
            Self::Unreached(error) => Some(error),
            Self::Missing(error) => Some(error),
            Self::Diverged(_) | Self::TraceDiverged(_) | Self::AlreadyForked(_) => None,
        }
    }
}

impl From<RunError> for CliError {
    fn from(error: RunError) -> Self {
        Self::Run(error)
    }
}

impl From<ParseRecordingError> for CliError {
    fn from(error: ParseRecordingError) -> Self {
        Self::Parse(error)
    }
}

impl From<ParseTraceError> for CliError {
    fn from(error: ParseTraceError) -> Self {
        Self::ParseTrace(error)
    }
}

impl From<UnreachedStep> for CliError {
    fn from(error: UnreachedStep) -> Self {
        Self::Unreached(error)
    }
}

impl From<MissingState> for CliError {
    fn from(error: MissingState) -> Self {
        Self::Missing(error)
    }
}

/// Returns the first place `replayed` stops matching `recorded`, or `None` if it never does.
///
/// Records are compared before lengths, so a run that both differs partway through and ends early
/// is reported at the difference rather than at the end.
fn first_divergence<T: Clone + PartialEq>(
    written: Written,
    recorded: &[T],
    replayed: &[T],
) -> Option<Divergence<T>> {
    for (index, (left, right)) in recorded.iter().zip(replayed).enumerate() {
        if left != right {
            return Some(Divergence {
                written,
                at: Parting::Record {
                    index,
                    recorded: left.clone(),
                    replayed: right.clone(),
                },
            });
        }
    }
    if recorded.len() == replayed.len() {
        return None;
    }
    Some(Divergence {
        written,
        at: Parting::Length {
            recorded: recorded.len(),
            replayed: replayed.len(),
        },
    })
}

/// Returns the text of the file at `path`, or says which path could not be read.
fn read(path: &Path) -> Result<String, CliError> {
    fs::read_to_string(path).map_err(|source| CliError::Read {
        path: path.to_path_buf(),
        source,
    })
}

/// Runs again the seed the recording at `path` names, and checks the result still matches it.
fn replay(path: &Path) -> Result<String, CliError> {
    let recorded: Recording = read(path)?.parse()?;
    let replayed = pingpong::run(recorded.seed())?;
    if let Some(divergence) =
        first_divergence(Written::RECORDING, recorded.entries(), replayed.entries())
    {
        return Err(CliError::Diverged(Box::new(divergence)));
    }
    Ok(format!(
        "seed {}: {} entries replayed identically\n",
        recorded.seed(),
        recorded.entries().len()
    ))
}

/// A recorded trace, and the states of a run that still produces it.
///
/// The three commands that read a trace all ask this the same three questions, and all of them
/// need the run behind the file to have been checked first, which is what building one does.
struct Loaded {
    trace: Trace,
    store: StateStore,
}

impl Loaded {
    /// Reads the trace `text` holds and runs again what it names, checking it still produces it.
    ///
    /// The states come from the run rather than from the file, because a trace names states
    /// without holding them. That is also why the check is not optional: a store rebuilt from a
    /// run that no longer produces the trace would answer every question about a run nobody
    /// recorded.
    fn parse(text: &str) -> Result<Self, CliError> {
        let trace: Trace = text.parse()?;
        let (produced, store) = match trace.fork() {
            Some(fork) => ring::forked(trace.seed(), fork)?,
            None => ring::run(trace.seed())?,
        };
        if let Some(divergence) =
            first_divergence(Written::trace(&trace), trace.steps(), produced.steps())
        {
            return Err(CliError::TraceDiverged(Box::new(divergence)));
        }
        Ok(Self { trace, store })
    }

    /// Reads the trace at `path`, checking it the same way.
    fn read(path: &Path) -> Result<Self, CliError> {
        Self::parse(&read(path)?)
    }

    /// Returns the step numbered `step`, or says how far the run actually got.
    fn at(&self, step: usize) -> Result<&Step, CliError> {
        self.trace
            .at(step)
            .ok_or(CliError::Unreached(UnreachedStep {
                step,
                steps: self.trace.steps().len(),
            }))
    }

    /// Shows the step numbered `step`, and the world it left the run in.
    fn show(&self, step: usize) -> Result<String, CliError> {
        let reached = self.at(step)?;
        let state = self.store.get(reached.state()).ok_or(MissingState {
            hash: reached.state(),
        })?;
        // The step is written the way the file writes it, number and all, so what is shown and the
        // line the reader has open in front of them are the same text.
        Ok(format!("step {step} {reached}\n{}", list(&state)))
    }

    /// Says what changed between the states two of the run's steps left it in.
    fn compare(&self, before: usize, after: usize) -> Result<String, CliError> {
        let (from, to) = (self.at(before)?, self.at(after)?);
        let changes = diff(&self.store, from.state(), to.state())?;
        let mut report = format!("step {before} -> step {after}\n");
        // A comparison of two states that are one state writes nothing, and silence is not an
        // answer to a question somebody typed.
        if changes.is_empty() {
            report.push_str("nothing changed\n");
        } else {
            report.push_str(&changes.to_string());
        }
        Ok(report)
    }

    /// Runs the trace again, drawing from `seed` after its step numbered `step`.
    fn forked(&self, step: usize, seed: u64) -> Result<String, CliError> {
        // A run draws from its own seed until one instant and from one other after it. A second
        // fork would claim the first fork's steps as a prefix it does not have, so it is refused
        // rather than approximated.
        if let Some(already) = self.trace.fork() {
            return Err(CliError::AlreadyForked(already));
        }
        let (forked, _) = ring::forked(self.trace.seed(), fork(&self.trace, step, seed)?)?;
        Ok(forked.to_string())
    }
}

/// Carries out `command`, returning what it should print.
fn execute(command: &Command) -> Result<String, CliError> {
    match command {
        Command::Run { seed } => Ok(pingpong::run(*seed)?.to_string()),
        Command::Replay { path } => replay(path),
        Command::Trace { seed } => {
            let (trace, _) = ring::run(*seed)?;
            Ok(trace.to_string())
        }
        Command::Inspect { path, step } => Loaded::read(path)?.show(*step),
        Command::Diff {
            path,
            before,
            after,
        } => Loaded::read(path)?.compare(*before, *after),
        Command::Fork { path, at, seed } => Loaded::read(path)?.forked(*at, *seed),
    }
}

/// Writes `output` to `out`, flushing it before returning.
///
/// # Errors
///
/// Returns [`CliError::Write`] if the output could not be got out — except to a reader that is no
/// longer there, which is not a failure of the command. Rust leaves `SIGPIPE` ignored, so writing
/// down a pipe whose far end has closed surfaces here as an error rather than as a signal, and
/// printing through `print!` turns that error into a panic from the one program whose job is to
/// report a divergence and nothing else.
fn emit(output: &str, out: &mut impl Write) -> Result<(), CliError> {
    let written = out.write_all(output.as_bytes()).and_then(|()| out.flush());
    match written {
        Err(error) if error.kind() != io::ErrorKind::BrokenPipe => Err(CliError::Write(error)),
        _ => Ok(()),
    }
}

fn main() -> ExitCode {
    let carried_out =
        execute(&Cli::parse().command).and_then(|output| emit(&output, &mut io::stdout().lock()));
    match carried_out {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("chronoloop: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use chronoloop::clock::VirtualTime;
    use chronoloop::world::Name;

    use super::*;

    /// The recording `run --seed 7` produces, as a value to compare against.
    fn recording(seed: u64) -> Recording {
        execute(&Command::Run { seed })
            .unwrap_or_else(|e| panic!("seed {seed} did not run: {e}"))
            .parse()
            .unwrap_or_else(|e| panic!("seed {seed} printed something unreadable: {e}"))
    }

    /// An entry, in the shorthand the divergence cases below are written in.
    fn entry(at: u64, message: &str) -> Entry {
        Entry::new(VirtualTime::from_nanos(at), message)
            .unwrap_or_else(|e| panic!("a test expectation is one line: {e}"))
    }

    #[test]
    fn the_root_accepts_every_subcommand() {
        struct Case {
            name: &'static str,
            argv: &'static [&'static str],
            /// What the parsed command carries, as it describes itself.
            want: &'static str,
        }
        let cases = [
            Case {
                name: "a run of the smallest seed",
                argv: &["chronoloop", "run", "--seed", "0"],
                want: "Run { seed: 0 }",
            },
            Case {
                name: "a run of the largest seed",
                argv: &["chronoloop", "run", "--seed", "18446744073709551615"],
                want: "Run { seed: 18446744073709551615 }",
            },
            Case {
                name: "a replay of a relative path",
                argv: &["chronoloop", "replay", "run.history"],
                want: "Replay { path: \"run.history\" }",
            },
            Case {
                name: "a replay of an absolute path",
                argv: &["chronoloop", "replay", "/tmp/run.history"],
                want: "Replay { path: \"/tmp/run.history\" }",
            },
            Case {
                name: "a trace of a run",
                argv: &["chronoloop", "trace", "--seed", "20260919"],
                want: "Trace { seed: 20260919 }",
            },
            Case {
                name: "an inspection of a step",
                argv: &["chronoloop", "inspect", "run.trace", "--step", "7"],
                want: "Inspect { path: \"run.trace\", step: 7 }",
            },
            Case {
                name: "an inspection of the first step",
                argv: &["chronoloop", "inspect", "run.trace", "--step", "0"],
                want: "Inspect { path: \"run.trace\", step: 0 }",
            },
            Case {
                name: "a comparison of two steps",
                argv: &["chronoloop", "diff", "run.trace", "7", "8"],
                want: "Diff { path: \"run.trace\", before: 7, after: 8 }",
            },
            Case {
                name: "a comparison that reads backwards",
                argv: &["chronoloop", "diff", "run.trace", "8", "7"],
                want: "Diff { path: \"run.trace\", before: 8, after: 7 }",
            },
            Case {
                name: "a fork of a step",
                argv: &[
                    "chronoloop",
                    "fork",
                    "run.trace",
                    "--at",
                    "7",
                    "--seed",
                    "99",
                ],
                want: "Fork { path: \"run.trace\", at: 7, seed: 99 }",
            },
        ];
        for case in cases {
            let cli =
                Cli::try_parse_from(case.argv).unwrap_or_else(|e| panic!("{}: {e}", case.name));
            assert_eq!(format!("{:?}", cli.command), case.want, "{}", case.name);
        }
    }

    #[test]
    fn the_root_rejects_an_invocation_it_could_not_carry_out() {
        struct Case {
            name: &'static str,
            argv: &'static [&'static str],
        }
        let cases = [
            Case {
                name: "no subcommand",
                argv: &["chronoloop"],
            },
            Case {
                name: "a subcommand that does not exist",
                argv: &["chronoloop", "shrink"],
            },
            Case {
                name: "a run with no seed",
                argv: &["chronoloop", "run"],
            },
            Case {
                name: "a seed that is not a number",
                argv: &["chronoloop", "run", "--seed", "lucky"],
            },
            Case {
                name: "a negative seed",
                argv: &["chronoloop", "run", "--seed", "-1"],
            },
            Case {
                name: "a seed past the end of the range",
                argv: &["chronoloop", "run", "--seed", "18446744073709551616"],
            },
            Case {
                name: "a replay with no path",
                argv: &["chronoloop", "replay"],
            },
            Case {
                name: "a trace with no seed",
                argv: &["chronoloop", "trace"],
            },
            Case {
                name: "an inspection with no step",
                argv: &["chronoloop", "inspect", "run.trace"],
            },
            Case {
                name: "an inspection with no trace to inspect",
                argv: &["chronoloop", "inspect", "--step", "7"],
            },
            Case {
                name: "a step that is not a number",
                argv: &["chronoloop", "inspect", "run.trace", "--step", "last"],
            },
            Case {
                name: "a step before the first one",
                argv: &["chronoloop", "inspect", "run.trace", "--step", "-1"],
            },
            Case {
                name: "a comparison with only one step",
                argv: &["chronoloop", "diff", "run.trace", "7"],
            },
            Case {
                name: "a comparison with a third step",
                argv: &["chronoloop", "diff", "run.trace", "7", "8", "9"],
            },
            Case {
                name: "a fork with no seed to fork to",
                argv: &["chronoloop", "fork", "run.trace", "--at", "7"],
            },
            Case {
                name: "a fork with no step to fork at",
                argv: &["chronoloop", "fork", "run.trace", "--seed", "99"],
            },
        ];
        for case in cases {
            assert!(Cli::try_parse_from(case.argv).is_err(), "{}", case.name);
        }
    }

    /// A writer that fails every write and every flush the same way.
    struct Failing(io::ErrorKind);

    impl Write for Failing {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            let _ = buffer;
            Err(io::Error::from(self.0))
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::from(self.0))
        }
    }

    #[test]
    fn the_output_is_written_exactly_as_it_was_produced() {
        // What `run` writes is what `replay` reads, so nothing may be added or dropped on the way
        // out — not a trailing newline, not a lost final flush.
        let recording = execute(&Command::Run { seed: 7 }).unwrap_or_else(|e| panic!("{e}"));
        let mut written = Vec::new();
        emit(&recording, &mut written).expect("a vector takes every write");
        assert_eq!(
            String::from_utf8(written).expect("the output is text"),
            recording
        );
    }

    #[test]
    fn a_reader_that_stopped_reading_is_not_a_failure_of_the_command() {
        struct Case {
            name: &'static str,
            kind: io::ErrorKind,
            /// Whether the command can be called done despite the write failing.
            done: bool,
        }
        // Rust ignores SIGPIPE, so a reader that has gone away arrives here as an error rather
        // than as a signal. A history nobody is left to read is still a history the run produced
        // correctly; anything else that stopped the output getting out is a failure to report.
        let cases = [
            Case {
                name: "a reader that stopped reading",
                kind: io::ErrorKind::BrokenPipe,
                done: true,
            },
            Case {
                name: "a device with no room left",
                kind: io::ErrorKind::StorageFull,
                done: false,
            },
            Case {
                name: "a handle that is not open for writing",
                kind: io::ErrorKind::PermissionDenied,
                done: false,
            },
        ];
        for case in cases {
            let emitted = emit("chronoloop history seed 7\n", &mut Failing(case.kind));
            assert_eq!(emitted.is_ok(), case.done, "{}", case.name);
        }
    }

    #[test]
    fn a_run_prints_a_recording_that_reads_back() {
        // What `run` writes to stdout is what `replay` reads, so the two have to agree on the form
        // without anything in between reformatting it.
        let recording = recording(7);
        assert_eq!(recording.seed(), 7);
        assert_eq!(recording.entries().len(), 4);
    }

    #[test]
    fn a_run_of_one_seed_prints_the_same_thing_twice() {
        for seed in [0, 7, u64::MAX] {
            for command in [Command::Run { seed }, Command::Trace { seed }] {
                let first = execute(&command).unwrap_or_else(|e| panic!("{e}"));
                let second = execute(&command).unwrap_or_else(|e| panic!("{e}"));
                assert_eq!(first, second, "{command:?}");
            }
        }
    }

    #[test]
    fn a_history_that_matches_has_no_divergence() {
        let recording = recording(7);
        assert!(
            first_divergence(Written::RECORDING, recording.entries(), recording.entries())
                .is_none()
        );
    }

    #[test]
    fn a_divergence_names_the_first_place_the_histories_differ() {
        // A person reading the report has the file open, so what it says is checked in full, line
        // number and both entries. The header takes the recording's first line, which is why the
        // first entry is reported as line 2.
        struct Case {
            name: &'static str,
            recorded: Vec<Entry>,
            replayed: Vec<Entry>,
            /// What the divergence reports, which is what a person reading it is given.
            want: Option<&'static str>,
        }
        let recorded = || {
            vec![
                entry(1, "ping sent"),
                entry(1, "ping received"),
                entry(2, "pong sent"),
            ]
        };
        let cases = [
            Case {
                name: "identical histories",
                recorded: recorded(),
                replayed: recorded(),
                want: None,
            },
            Case {
                name: "both empty",
                recorded: Vec::new(),
                replayed: Vec::new(),
                want: None,
            },
            Case {
                name: "a different message in the first entry",
                recorded: recorded(),
                replayed: vec![
                    entry(1, "ping dropped"),
                    entry(1, "ping received"),
                    entry(2, "pong sent"),
                ],
                want: Some(
                    "the replay diverged at line 2\n  \
                     recorded: 0.000000001s ping sent\n  \
                     replayed: 0.000000001s ping dropped",
                ),
            },
            Case {
                name: "a different instant partway through",
                recorded: recorded(),
                replayed: vec![
                    entry(1, "ping sent"),
                    entry(1, "ping received"),
                    entry(3, "pong sent"),
                ],
                want: Some(
                    "the replay diverged at line 4\n  \
                     recorded: 0.000000002s pong sent\n  \
                     replayed: 0.000000003s pong sent",
                ),
            },
            Case {
                name: "the replay stopped early",
                recorded: recorded(),
                replayed: vec![entry(1, "ping sent"), entry(1, "ping received")],
                want: Some("the replay recorded 2 entries where the recording holds 3"),
            },
            Case {
                name: "the replay ran on",
                recorded: recorded(),
                replayed: vec![
                    entry(1, "ping sent"),
                    entry(1, "ping received"),
                    entry(2, "pong sent"),
                    entry(2, "pong received"),
                ],
                want: Some("the replay recorded 4 entries where the recording holds 3"),
            },
            Case {
                name: "an entry differs before the lengths do",
                recorded: recorded(),
                replayed: vec![entry(1, "ping sent"), entry(9, "ping received")],
                want: Some(
                    "the replay diverged at line 3\n  \
                     recorded: 0.000000001s ping received\n  \
                     replayed: 0.000000009s ping received",
                ),
            },
        ];
        for case in cases {
            let reported = first_divergence(Written::RECORDING, &case.recorded, &case.replayed);
            assert_eq!(
                reported.map(|divergence| divergence.to_string()).as_deref(),
                case.want,
                "{}",
                case.name
            );
        }
    }
    /// The seed the trace cases run, since none of them is about a particular one.
    const SEED: u64 = 20_260_919;

    /// The seed a fork sends a run off to.
    const OTHER: u64 = 99;

    /// How many steps that run takes, which is what a step is asked for out of.
    const STEPS: usize = 20;

    /// What `trace --seed` writes for `seed`.
    fn written(seed: u64) -> String {
        execute(&Command::Trace { seed }).unwrap_or_else(|e| panic!("seed {seed} did not run: {e}"))
    }

    /// A trace read back the way every command that reads one reads it.
    fn loaded(seed: u64) -> Loaded {
        Loaded::parse(&written(seed))
            .unwrap_or_else(|e| panic!("what `trace` writes is what a trace reads: {e}"))
    }

    /// The paths a shown world names, which is what a reader of one is given.
    fn paths(world: &str) -> Vec<&str> {
        world
            .lines()
            .map(|line| line.split(' ').next().unwrap_or_default())
            .collect()
    }

    #[test]
    fn a_trace_names_its_seed_and_every_step_the_run_took() {
        let trace: Trace = written(SEED)
            .parse()
            .unwrap_or_else(|e| panic!("a written trace reads back: {e}"));

        assert_eq!(trace.seed(), SEED);
        assert_eq!(trace.steps().len(), STEPS);
        assert_eq!(
            trace.fork(),
            None,
            "a run that was asked for no fork took none"
        );
    }

    #[test]
    fn showing_a_step_shows_the_record_the_file_holds_and_the_world_behind_it() {
        // The two halves of what `inspect` is for. The first line is the file's own record, so a
        // reader can find it in the file they have open; the rest is the world, which the file
        // names and does not hold.
        let loaded = loaded(SEED);
        let shown = loaded.show(0).unwrap_or_else(|e| panic!("{e}"));
        let (record, world) = shown
            .split_once('\n')
            .unwrap_or_else(|| panic!("a step, and then the world it left"));

        assert_eq!(
            Some(record),
            written(SEED).lines().nth(1),
            "the record shown is the record written, character for character"
        );

        // The ring's first step is one node writing down the four things a node writes; by the end
        // every one of the four has.
        assert_eq!(paths(world).len(), 4);
        for path in paths(world) {
            assert_eq!(
                path.split(Name::SEPARATOR).count(),
                2,
                "a world is written out as the fields of its resources: {path}"
            );
        }
        let last = loaded
            .show(STEPS - 1)
            .unwrap_or_else(|e| panic!("a run of {STEPS} steps has a last one: {e}"));
        assert_eq!(
            last.lines().count(),
            1 + 16,
            "a record and four nodes of four"
        );
    }

    #[test]
    fn comparing_two_steps_says_what_moved_between_them() {
        let loaded = loaded(SEED);
        let report = loaded
            .compare(0, 1)
            .unwrap_or_else(|e| panic!("both steps are the run's: {e}"));
        let mut lines = report.lines();

        assert_eq!(
            lines.next(),
            Some("step 0 -> step 1"),
            "the report says which two states are being held against each other"
        );
        let changes: Vec<&str> = lines.collect();
        assert!(!changes.is_empty(), "two steps of a ring are two states");
        for change in changes {
            assert!(
                change.starts_with("+ ") || change.starts_with("- ") || change.starts_with("~ "),
                "every line under the header is a change: {change}"
            );
        }
    }

    #[test]
    fn comparing_a_step_with_itself_says_so_rather_than_saying_nothing() {
        // A comparison of one state with itself writes no lines at all, and a command that answered
        // a question with silence would read as one that had failed.
        let loaded = loaded(SEED);
        assert_eq!(
            loaded
                .compare(7, 7)
                .unwrap_or_else(|e| panic!("step 7 is the run's: {e}")),
            "step 7 -> step 7\nnothing changed\n"
        );
    }

    #[test]
    fn a_step_the_run_never_reached_is_refused_and_says_how_far_it_got() {
        let loaded = loaded(SEED);
        let asked = [
            ("showing it", loaded.show(STEPS)),
            ("comparing it", loaded.compare(0, STEPS)),
            ("forking at it", loaded.forked(STEPS, OTHER)),
        ];
        for (name, outcome) in asked {
            let refused = outcome.err().unwrap_or_else(|| {
                panic!("{name}: a run of {STEPS} steps never reached step {STEPS}")
            });
            assert_eq!(
                refused.to_string(),
                format!("the run never reached step {STEPS}; it took {STEPS} steps"),
                "{name}"
            );
        }
    }

    #[test]
    fn a_trace_that_the_run_no_longer_produces_is_refused_at_the_line_that_differs() {
        // What the re-run buys. A file saying the run passed through something it did not is turned
        // away, rather than being shown back as though the states it names were the run's.
        let tampered = written(SEED).replacen("heard from", "heard nothing from", 1);
        let refused = Loaded::parse(&tampered)
            .err()
            .unwrap_or_else(|| panic!("a trace nothing produces is not a trace of a run"));

        let said = refused.to_string();
        assert!(
            said.starts_with("the replay diverged at line 2"),
            "the first step of an unforked trace is on line 2: {said}"
        );
        assert!(
            said.contains("heard nothing from"),
            "the complaint quotes what the file holds: {said}"
        );
    }

    #[test]
    fn a_fork_line_moves_the_line_a_divergence_is_reported_at() {
        // A forked trace writes its fork under the header, so its first step is on line 3. Reported
        // anywhere else, a person opening the file looks at the line above the one that is wrong.
        let forked = loaded(SEED)
            .forked(7, OTHER)
            .unwrap_or_else(|e| panic!("step 7 is the run's: {e}"));
        let tampered = forked.replacen("heard from", "heard nothing from", 1);
        let refused = Loaded::parse(&tampered)
            .err()
            .unwrap_or_else(|| panic!("a trace nothing produces is not a trace of a run"));

        assert!(
            refused
                .to_string()
                .starts_with("the replay diverged at line 3"),
            "{refused}"
        );
    }

    #[test]
    fn a_trace_that_stops_early_is_reported_in_the_words_a_trace_is_read_in() {
        // The other half of the divergence, and the half where the wording of the two kinds of file
        // parts company: a recording holds entries and a trace holds steps.
        let loaded = loaded(SEED);
        let steps = loaded.trace.steps();
        let short = first_divergence(
            Written::trace(&loaded.trace),
            steps,
            &steps[..steps.len() - 1],
        );

        assert_eq!(
            short.map(|divergence| divergence.to_string()).as_deref(),
            Some("the replay recorded 19 steps where the trace holds 20")
        );
    }

    #[test]
    fn a_fork_is_a_trace_of_the_run_it_forked_and_says_where_it_parted() {
        let forked = loaded(SEED)
            .forked(7, OTHER)
            .unwrap_or_else(|e| panic!("step 7 is the run's: {e}"));
        let trace: Trace = forked
            .parse()
            .unwrap_or_else(|e| panic!("a written trace reads back: {e}"));

        assert_eq!(
            trace.seed(),
            SEED,
            "the header still names the run it was until the fork"
        );
        assert_eq!(
            trace.fork().map(|fork| fork.seed()),
            Some(OTHER),
            "and the fork line names the rest of what produces it again"
        );
        assert_eq!(
            trace.fork().map(|fork| fork.at()),
            loaded(SEED).at(7).ok().map(|step| step.event().at()),
            "the fork is at the instant the step forked at happened"
        );
    }

    #[test]
    fn a_trace_that_has_already_forked_is_refused_a_second_fork() {
        // A run draws from its own seed up to one instant and from one other after it, so a second
        // fork has nowhere to go: its prefix would be the first fork's steps, which the engine
        // cannot produce again. Reading the forked trace back also puts the `forked` half of the
        // re-run under test, since that is what produces it.
        let forked = loaded(SEED)
            .forked(7, OTHER)
            .unwrap_or_else(|e| panic!("step 7 is the run's: {e}"));
        let again = Loaded::parse(&forked)
            .unwrap_or_else(|e| panic!("a fork's own trace reads back and runs again: {e}"));

        let refused = again
            .forked(3, 5)
            .err()
            .unwrap_or_else(|| panic!("a run carries one fork"));

        assert!(
            refused.to_string().ends_with("and a run carries one fork"),
            "{refused}"
        );
        assert!(
            refused.to_string().contains("to seed 99"),
            "the complaint names the fork the file already carries: {refused}"
        );
    }
}
