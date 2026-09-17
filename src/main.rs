//! chronoloop's command line: run a simulation, or check that a recording of one still replays.
//!
//! `run --seed <n>` writes the history a run produced, seed and all. `replay <path>` reads one of
//! those back, runs the seed it names again, and compares. The two halves are deliberately separate
//! processes reading a file: a check whose recorded and replayed sides both came from one run would
//! only be comparing a pure function with itself.

use core::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use chronoloop::executor::ExecutorError;
use chronoloop::history::{Entry, ParseRecordingError, Recording};
use chronoloop::systems::pingpong;

/// Deterministic simulation of infrastructure control loops.
#[derive(Debug, Parser)]
#[command(name = "chronoloop", version)]
struct Cli {
    /// What to do.
    #[command(subcommand)]
    command: Command,
}

/// The things chronoloop can be asked to do.
#[derive(Debug, PartialEq, Eq, Subcommand)]
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
#[derive(Debug, PartialEq, Eq)]
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
    /// The simulation could not finish.
    Engine(ExecutorError),
    /// The recording could not be read.
    Read {
        /// The path that was asked for.
        path: PathBuf,
        /// Why reading it failed.
        source: io::Error,
    },
    /// The file is not a recording.
    Parse(ParseRecordingError),
    /// The replay produced a different history from the one recorded.
    Diverged(Divergence),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Engine(error) => write!(f, "{error}"),
            Self::Read { path, source } => write!(f, "cannot read {}: {source}", path.display()),
            Self::Parse(error) => write!(f, "{error}"),
            Self::Diverged(divergence) => write!(f, "{divergence}"),
        }
    }
}

impl std::error::Error for CliError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Engine(error) => Some(error),
            Self::Read { source, .. } => Some(source),
            Self::Parse(error) => Some(error),
            Self::Diverged(_) => None,
        }
    }
}

impl From<ExecutorError> for CliError {
    fn from(error: ExecutorError) -> Self {
        Self::Engine(error)
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

fn main() -> ExitCode {
    match execute(&Cli::parse().command) {
        Ok(output) => {
            print!("{output}");
            ExitCode::SUCCESS
        }
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
    }

    #[test]
    fn the_root_accepts_both_subcommands() {
        struct Case {
            name: &'static str,
            argv: &'static [&'static str],
            want: Command,
        }
        let cases = [
            Case {
                name: "a run of the smallest seed",
                argv: &["chronoloop", "run", "--seed", "0"],
                want: Command::Run { seed: 0 },
            },
            Case {
                name: "a run of the largest seed",
                argv: &["chronoloop", "run", "--seed", "18446744073709551615"],
                want: Command::Run { seed: u64::MAX },
            },
            Case {
                name: "a replay of a relative path",
                argv: &["chronoloop", "replay", "run.history"],
                want: Command::Replay {
                    path: PathBuf::from("run.history"),
                },
            },
            Case {
                name: "a replay of an absolute path",
                argv: &["chronoloop", "replay", "/tmp/run.history"],
                want: Command::Replay {
                    path: PathBuf::from("/tmp/run.history"),
                },
            },
        ];
        for case in cases {
            let cli =
                Cli::try_parse_from(case.argv).unwrap_or_else(|e| panic!("{}: {e}", case.name));
            assert_eq!(cli.command, case.want, "{}", case.name);
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
        assert_eq!(
            first_divergence(recording.entries(), recording.entries()),
            None
        );
    }

    #[test]
    fn a_divergence_names_the_first_place_the_histories_differ() {
        struct Case {
            name: &'static str,
            recorded: Vec<Entry>,
            replayed: Vec<Entry>,
            want: Option<Divergence>,
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
                want: Some(Divergence::Entry {
                    index: 0,
                    recorded: entry(1, "ping sent"),
                    replayed: entry(1, "ping dropped"),
                }),
            },
            Case {
                name: "a different instant partway through",
                recorded: recorded(),
                replayed: vec![
                    entry(1, "ping sent"),
                    entry(1, "ping received"),
                    entry(3, "pong sent"),
                ],
                want: Some(Divergence::Entry {
                    index: 2,
                    recorded: entry(2, "pong sent"),
                    replayed: entry(3, "pong sent"),
                }),
            },
            Case {
                name: "the replay stopped early",
                recorded: recorded(),
                replayed: vec![entry(1, "ping sent"), entry(1, "ping received")],
                want: Some(Divergence::Length {
                    recorded: 3,
                    replayed: 2,
                }),
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
                want: Some(Divergence::Length {
                    recorded: 3,
                    replayed: 4,
                }),
            },
            Case {
                name: "an entry differs before the lengths do",
                recorded: recorded(),
                replayed: vec![entry(1, "ping sent"), entry(9, "ping received")],
                want: Some(Divergence::Entry {
                    index: 1,
                    recorded: entry(1, "ping received"),
                    replayed: entry(9, "ping received"),
                }),
            },
        ];
        for case in cases {
            assert_eq!(
                first_divergence(&case.recorded, &case.replayed),
                case.want,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn a_divergence_reports_the_line_the_recording_holds_it_on() {
        // A person reading the message has the file open, and the header takes the first line.
        let divergence = Divergence::Entry {
            index: 0,
            recorded: entry(1, "ping sent"),
            replayed: entry(2, "ping sent"),
        };
        assert!(
            divergence.to_string().contains("line 2"),
            "the first entry is on line 2: {divergence}"
        );
    }
}
