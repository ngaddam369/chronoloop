//! chronoloop's command line: run a simulation, or check that a recording of one still replays.
//!
//! `run --seed <n>` writes the history a run produced, seed and all. `replay <path>` reads one of
//! those back, runs the seed it names again, and compares. The two halves are deliberately separate
//! processes reading a file: a check whose recorded and replayed sides both came from one run would
//! only be comparing a pure function with itself.

use core::fmt;
use std::fs;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use chronoloop::history::{Entry, ParseRecordingError, Recording};
use chronoloop::systems::{RunError, pingpong};

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
}

/// The first place a replayed history stopped matching the one recorded.
#[derive(Debug)]
enum Divergence {
    /// An entry differs from the one recorded in its place.
    Entry {
        /// Its position in the history, counting from zero.
        index: usize,
        /// What the recording holds there.
        recorded: Entry,
        /// What the replay produced instead.
        replayed: Entry,
    },
    /// The histories agree as far as the shorter one goes, then one of them stops.
    Length {
        /// How many entries the recording holds.
        recorded: usize,
        /// How many entries the replay produced.
        replayed: usize,
    },
}

impl fmt::Display for Divergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Entry {
                index,
                recorded,
                replayed,
            } => {
                // The header takes the recording's first line, so the first entry is on line 2.
                let line = index + 2;
                write!(
                    f,
                    "the replay diverged at line {line}\n  recorded: {recorded}\n  replayed: {replayed}"
                )
            }
            Self::Length { recorded, replayed } => write!(
                f,
                "the replay recorded {replayed} entries where the recording holds {recorded}"
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
    /// The output could not be got out.
    Write(io::Error),
    /// The replay produced a different history from the one recorded.
    Diverged(Divergence),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Run(error) => write!(f, "{error}"),
            Self::Read { path, source } => write!(f, "cannot read {}: {source}", path.display()),
            Self::Parse(error) => write!(f, "{error}"),
            Self::Write(source) => write!(f, "cannot write to standard output: {source}"),
            Self::Diverged(divergence) => write!(f, "{divergence}"),
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Run(error) => Some(error),
            Self::Read { source, .. } | Self::Write(source) => Some(source),
            Self::Parse(error) => Some(error),
            Self::Diverged(_) => None,
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

/// Returns the first place `replayed` stops matching `recorded`, or `None` if it never does.
///
/// Entries are compared before lengths, so a history that both differs partway through and ends
/// early is reported at the difference rather than at the end.
fn first_divergence(recorded: &[Entry], replayed: &[Entry]) -> Option<Divergence> {
    for (index, (left, right)) in recorded.iter().zip(replayed).enumerate() {
        if left != right {
            return Some(Divergence::Entry {
                index,
                recorded: left.clone(),
                replayed: right.clone(),
            });
        }
    }
    if recorded.len() == replayed.len() {
        return None;
    }
    Some(Divergence::Length {
        recorded: recorded.len(),
        replayed: replayed.len(),
    })
}

/// Runs again the seed the recording at `path` names, and checks the result still matches it.
fn replay(path: &Path) -> Result<String, CliError> {
    let text = fs::read_to_string(path).map_err(|source| CliError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let recorded: Recording = text.parse()?;
    let replayed = pingpong::run(recorded.seed())?;
    if let Some(divergence) = first_divergence(recorded.entries(), replayed.entries()) {
        return Err(CliError::Diverged(divergence));
    }
    Ok(format!(
        "seed {}: {} entries replayed identically\n",
        recorded.seed(),
        recorded.entries().len()
    ))
}

/// Carries out `command`, returning what it should print.
fn execute(command: &Command) -> Result<String, CliError> {
    match command {
        Command::Run { seed } => Ok(pingpong::run(*seed)?.to_string()),
        Command::Replay { path } => replay(path),
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
    fn the_root_accepts_both_subcommands() {
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
            let first = execute(&Command::Run { seed }).unwrap_or_else(|e| panic!("{e}"));
            let second = execute(&Command::Run { seed }).unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(first, second, "seed {seed}");
        }
    }

    #[test]
    fn a_history_that_matches_has_no_divergence() {
        let recording = recording(7);
        assert!(first_divergence(recording.entries(), recording.entries()).is_none());
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
            let reported = first_divergence(&case.recorded, &case.replayed);
            assert_eq!(
                reported.map(|divergence| divergence.to_string()).as_deref(),
                case.want,
                "{}",
                case.name
            );
        }
    }
}
