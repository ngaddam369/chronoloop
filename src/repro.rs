//! A failing run, small enough to keep in a file.
//!
//! [`crate::shrink`] cuts a failing run's schedule down to the faults the failure could not do
//! without, and hands back a [`Reduction`] that dies with the process that made it: it carries no
//! seed — the seed is closed over by the predicate — and it has no written form of its own. This
//! module is that written form, and it puts the seed back.
//!
//! A [`Repro`] is the three things a later run needs and nothing more: the **seed**, the **faults**
//! it is put through, and the **failure to expect** out the other end. A sweep finds a seed, a
//! reduction cuts its schedule down, and what is left is a few hundred bytes that can be committed
//! as a fixture and handed to a run months later.
//!
//! # It composes the forms that already exist
//!
//! ```text
//! chronoloop repro seed 20260921
//! failed at step 14: round 3 lost quorum
//! chronoloop faults
//! partition on node 0 -> node 1 from 15.000000000s until 15.000000001s
//! ```
//!
//! The second line is an [`Outcome`] in its own written form. Everything from the third line on is a
//! [`FaultSchedule`] in its own written form, **header included** — and it is read back by handing
//! that text to the schedule's own reader in one piece. There is therefore no second implementation
//! of the fault reader here, and the tail of a repro file is a schedule file.
//!
//! The faults are the one part with no bound on its length, so they come last and the file reads
//! from the top down — the discipline [`crate::world`]'s encoding and [`crate::trace`]'s step line
//! already follow.
//!
//! # Every line it names is a line of this file
//!
//! A nested form counts its own lines: [`ParseScheduleError::BadFault`] calls the schedule's header
//! line 1, which in a repro is line 3. The line is shifted before it is reported, and the schedule's
//! two header failures become [`ParseReproError::BadSchedule`], which names the third line outright
//! — in a repro the text is not empty and the schedule's header is not the first line, so the
//! schedule's own wording would be wrong in both directions. Reported as the schedule counts them, a
//! person opening the file would be looking two lines above the one that is wrong.
//!
//! # A repro expects a failure
//!
//! [`Outcome::reproduces`] is `false` whenever either side held up, so a repro naming a run that
//! passed is one nothing could ever honour. [`Repro::new`] refuses it, the way
//! [`Reason::new`][crate::outcome::Reason::new] refuses an empty reason: settled where the value is
//! made rather than wherever it is later checked.
//!
//! [`Reduction`]: crate::shrink::Reduction

use core::fmt;
use core::str::FromStr;

use crate::fault::{self, FaultSchedule, ParseFaultError, ParseScheduleError};
use crate::outcome::{Outcome, ParseOutcomeError};

/// The first line of a written repro.
const HEADER: &str = "chronoloop repro seed ";

/// The line a repro's faults begin on, counting its header as line 1.
const SCHEDULE_AT: usize = 3;

/// Reads a seed the one way a written repro writes one.
///
/// Digits and nothing else, so a sign is refused rather than quietly taken for what it precedes.
/// [`crate::trace`] reads its seed under the same rule; [`crate::history`] does not, and a new
/// reader should not take up an old reader's defect.
fn count(text: &str) -> Option<u64> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// Takes the first line off `text`, and hands back what follows it.
///
/// [`None`] means the text has run out, which is what tells a line that is missing from a line that
/// is empty.
fn line(text: &str) -> Option<(&str, &str)> {
    if text.is_empty() {
        return None;
    }
    Some(text.split_once('\n').unwrap_or((text, "")))
}

/// Returned when a repro could not be made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReproError {
    /// The run held up, so there is no failure to reproduce.
    HeldUp,
}

impl fmt::Display for ReproError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HeldUp => write!(
                f,
                "a repro names the failure to expect, and this run held up"
            ),
        }
    }
}

impl std::error::Error for ReproError {}

/// Whether `expected` is a failure a later run could be held to.
fn reproducible(expected: &Outcome) -> Result<(), ReproError> {
    match expected {
        Outcome::Pass => Err(ReproError::HeldUp),
        Outcome::Fail { .. } => Ok(()),
    }
}

/// A failing run, small enough to keep in a file.
///
/// ```
/// use chronoloop::fault::FaultSchedule;
/// use chronoloop::outcome::{Outcome, Reason};
/// use chronoloop::repro::Repro;
///
/// let faults: FaultSchedule =
///     "chronoloop faults\n\
///      partition on node 0 -> node 1 from 15.000000000s until 15.000000001s\n"
///         .parse()?;
/// let repro = Repro::new(
///     20_260_921,
///     faults,
///     Outcome::Fail { reason: Reason::new("round 3 lost quorum")?, step: 14 },
/// )?;
///
/// assert_eq!(
///     repro.to_string(),
///     "chronoloop repro seed 20260921\n\
///      failed at step 14: round 3 lost quorum\n\
///      chronoloop faults\n\
///      partition on node 0 -> node 1 from 15.000000000s until 15.000000001s\n"
/// );
/// assert_eq!(repro.to_string().parse(), Ok(repro));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Repro {
    seed: u64,
    faults: FaultSchedule,
    expected: Outcome,
}

impl Repro {
    /// Creates a repro of the run of `seed` under `faults`, expecting `expected`.
    ///
    /// # Errors
    ///
    /// Returns [`ReproError`] if `expected` is a run that held up: a repro exists to be reproduced,
    /// and [`Outcome::reproduces`] answers `false` for a pass on either side, so such a repro is one
    /// no later run could ever be said to honour.
    pub fn new(seed: u64, faults: FaultSchedule, expected: Outcome) -> Result<Self, ReproError> {
        reproducible(&expected)?;
        Ok(Self {
            seed,
            faults,
            expected,
        })
    }

    /// Returns the seed the run is of.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the faults the run is put through.
    pub fn faults(&self) -> &FaultSchedule {
        &self.faults
    }

    /// Returns the failure a later run is expected to produce.
    ///
    /// This is what [`Outcome::reproduces`] takes, which is how the run is held to it.
    pub fn expected(&self) -> &Outcome {
        &self.expected
    }
}

impl fmt::Display for Repro {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{HEADER}{}", self.seed)?;
        writeln!(f, "{}", self.expected)?;
        // A schedule writes its own header and ends every line it writes.
        write!(f, "{}", self.faults)
    }
}

/// Returned when a repro could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseReproError {
    /// The text is empty, so it carries no header.
    MissingHeader,
    /// The first line is not a repro's header.
    BadHeader,
    /// The second line is not a failure to expect.
    BadFailure {
        /// What was wrong with it.
        source: ParseOutcomeError,
    },
    /// The second line names a run the repro cannot be made of.
    Refused {
        /// Why it was turned away.
        source: ReproError,
    },
    /// The third line does not open the faults.
    BadSchedule,
    /// A fault could not be read.
    BadFault {
        /// Which line of the repro it was, counting the header as line 1.
        line: usize,
        /// What was wrong with it.
        source: ParseFaultError,
    },
}

impl fmt::Display for ParseReproError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingHeader => write!(f, "a repro is empty and names no seed"),
            Self::BadHeader => write!(f, "expected a first line reading \"{HEADER}<seed>\""),
            Self::BadFailure { source } => write!(f, "line 2: {source}"),
            Self::Refused { source } => write!(f, "line 2: {source}"),
            Self::BadSchedule => {
                write!(f, "expected a third line reading \"{}\"", fault::HEADER)
            }
            Self::BadFault { line, source } => write!(f, "line {line}: {source}"),
        }
    }
}

impl std::error::Error for ParseReproError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MissingHeader | Self::BadHeader | Self::BadSchedule => None,
            Self::BadFailure { source } => Some(source),
            Self::Refused { source } => Some(source),
            Self::BadFault { source, .. } => Some(source),
        }
    }
}

/// A schedule's complaint, told in the lines of the repro it was nested in.
fn nested(error: ParseScheduleError) -> ParseReproError {
    match error {
        // Neither is what went wrong here: a repro holding this text is not empty, and the line the
        // schedule calls its first is this file's third.
        ParseScheduleError::MissingHeader | ParseScheduleError::BadHeader => {
            ParseReproError::BadSchedule
        }
        ParseScheduleError::BadFault { line, source } => ParseReproError::BadFault {
            line: line + (SCHEDULE_AT - 1),
            source,
        },
    }
}

impl FromStr for Repro {
    type Err = ParseReproError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (header, rest) = line(text).ok_or(ParseReproError::MissingHeader)?;
        let seed = header
            .strip_prefix(HEADER)
            .and_then(count)
            .ok_or(ParseReproError::BadHeader)?;
        let (failure, faults) = line(rest).ok_or(ParseReproError::BadFailure {
            source: ParseOutcomeError::Malformed,
        })?;
        let expected: Outcome = failure
            .parse()
            .map_err(|source| ParseReproError::BadFailure { source })?;
        // `Self::new` settles this below. It is asked here as well so that a repro naming a run
        // that held up is reported at the line that says so, rather than after the faults beneath
        // it have been read and complained about first.
        reproducible(&expected).map_err(|source| ParseReproError::Refused { source })?;
        let faults: FaultSchedule = faults.parse().map_err(nested)?;
        Self::new(seed, faults, expected).map_err(|source| ParseReproError::Refused { source })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::outcome::{ParseOutcomeError, Reason};

    /// The repro the recorded cases are written around.
    const REPRO: &str = "chronoloop repro seed 20260921\n\
                         failed at step 14: round 3 lost quorum\n\
                         chronoloop faults\n\
                         partition on node 0 -> node 1 from 15.000000000s until 15.000000001s\n\
                         partition on node 0 -> node 2 from 15.000000000s until 15.000000001s\n";

    /// A schedule read back from the text it is written in, failing the test rather than returning.
    fn schedule(text: &str) -> FaultSchedule {
        text.parse()
            .unwrap_or_else(|e| panic!("a test schedule is a schedule: {e}"))
    }

    /// A failure of `text` at `step`, failing the test rather than returning an error.
    fn failed(text: &str, step: usize) -> Outcome {
        Outcome::Fail {
            reason: Reason::new(text).unwrap_or_else(|e| panic!("a test reason is a reason: {e}")),
            step,
        }
    }

    /// A repro, failing the test rather than returning an error no case expects.
    fn repro(seed: u64, faults: FaultSchedule, expected: Outcome) -> Repro {
        Repro::new(seed, faults, expected)
            .unwrap_or_else(|e| panic!("a test repro is a repro: {e}"))
    }

    /// The repro `REPRO` is the written form of, built rather than read.
    fn recorded() -> Repro {
        repro(
            20_260_921,
            schedule(
                "chronoloop faults\n\
                 partition on node 0 -> node 1 from 15.000000000s until 15.000000001s\n\
                 partition on node 0 -> node 2 from 15.000000000s until 15.000000001s\n",
            ),
            failed("round 3 lost quorum", 14),
        )
    }

    #[test]
    fn a_repro_writes_the_form_it_documents() {
        // Pinned as a literal: the seed, the failure to expect, and then a fault schedule entire,
        // its own header included — so everything from the third line on is a schedule file.
        assert_eq!(recorded().to_string(), REPRO);
    }

    #[test]
    fn writing_a_repro_and_reading_it_back_gives_the_same_repro() {
        let written = recorded().to_string();
        assert_eq!(written.parse(), Ok(recorded()));
        assert_eq!(
            REPRO.parse(),
            Ok(recorded()),
            "and the pinned text is that text"
        );
    }

    #[test]
    fn a_repro_of_a_run_that_held_up_is_refused() {
        // A repro exists to be reproduced, and `Outcome::reproduces` is false whenever either side
        // held up — so a repro expecting a pass is one nothing could ever honour. Settled where it
        // is made, not where it is checked.
        assert_eq!(
            Repro::new(7, FaultSchedule::default(), Outcome::Pass),
            Err(ReproError::HeldUp)
        );
        assert_eq!(
            "chronoloop repro seed 7\npassed\nchronoloop faults\n".parse::<Repro>(),
            Err(ParseReproError::Refused {
                source: ReproError::HeldUp
            }),
            "and reading one runs the same rule rather than a second copy of it"
        );
    }

    #[test]
    fn a_repro_that_needed_no_faults_is_still_a_repro() {
        // The seed alone carries the failure. Three lines, and the third opens a schedule holding
        // nothing — which is what keeps the tail of the file a schedule whatever it says.
        let bare = repro(
            7,
            FaultSchedule::default(),
            failed("round 1 lost quorum", 0),
        );
        assert_eq!(
            bare.to_string(),
            "chronoloop repro seed 7\n\
             failed at step 0: round 1 lost quorum\n\
             chronoloop faults\n"
        );
        assert_eq!(bare.to_string().parse(), Ok(bare));
    }

    #[test]
    fn a_repro_carries_the_three_things_a_run_needs() {
        let held = recorded();
        assert_eq!(held.seed(), 20_260_921);
        assert_eq!(held.faults().len(), 2);
        assert_eq!(held.expected(), &failed("round 3 lost quorum", 14));
    }

    #[test]
    fn reading_a_repro_rejects_text_it_could_not_have_written() {
        // Strict enough to accept only what `Display` writes, and every line it names is a line of
        // the repro file rather than of the schedule nested inside it.
        struct Case {
            name: &'static str,
            text: &'static str,
            want: ParseReproError,
        }
        let cases = [
            Case {
                name: "nothing at all",
                text: "",
                want: ParseReproError::MissingHeader,
            },
            Case {
                name: "a header naming no seed",
                text: "chronoloop repro\nfailed at step 0: gone\nchronoloop faults\n",
                want: ParseReproError::BadHeader,
            },
            Case {
                name: "a signed seed, which a written repro never carries",
                text: "chronoloop repro seed +7\nfailed at step 0: gone\nchronoloop faults\n",
                want: ParseReproError::BadHeader,
            },
            Case {
                name: "a seed that is not there",
                text: "chronoloop repro seed \nfailed at step 0: gone\nchronoloop faults\n",
                want: ParseReproError::BadHeader,
            },
            Case {
                name: "another of the crate's files",
                text: "chronoloop trace seed 7\nfailed at step 0: gone\nchronoloop faults\n",
                want: ParseReproError::BadHeader,
            },
            Case {
                name: "a header and nothing after it",
                text: "chronoloop repro seed 7\n",
                want: ParseReproError::BadFailure {
                    source: ParseOutcomeError::Malformed,
                },
            },
            Case {
                name: "a second line that is not a failure",
                text: "chronoloop repro seed 7\nbroke somehow\nchronoloop faults\n",
                want: ParseReproError::BadFailure {
                    source: ParseOutcomeError::Malformed,
                },
            },
            Case {
                name: "a failure to expect and no faults under it",
                text: "chronoloop repro seed 7\nfailed at step 0: gone\n",
                want: ParseReproError::BadSchedule,
            },
            Case {
                name: "a third line that does not open a schedule",
                text: "chronoloop repro seed 7\nfailed at step 0: gone\nchronoloop trace seed 7\n",
                want: ParseReproError::BadSchedule,
            },
            Case {
                name: "a fault on the first line a fault can fall on",
                text: "chronoloop repro seed 7\nfailed at step 0: gone\nchronoloop faults\nnot a fault\n",
                want: ParseReproError::BadFault {
                    line: 4,
                    source: ParseFaultError::Malformed,
                },
            },
            Case {
                name: "a fault one line further down, reported one line further down",
                text: "chronoloop repro seed 7\nfailed at step 0: gone\nchronoloop faults\n\
                       partition on node 0 -> node 1 from 0.000000000s until forever\nnot a fault\n",
                want: ParseReproError::BadFault {
                    line: 5,
                    source: ParseFaultError::Malformed,
                },
            },
            Case {
                name: "a run that held up, reported above the faults rather than after them",
                text: "chronoloop repro seed 7\npassed\nchronoloop faults\nnot a fault\n",
                want: ParseReproError::Refused {
                    source: ReproError::HeldUp,
                },
            },
        ];
        for case in cases {
            assert_eq!(case.text.parse::<Repro>(), Err(case.want), "{}", case.name);
        }
    }

    #[test]
    fn a_repro_that_could_not_be_read_says_where_to_look() {
        assert_eq!(
            ReproError::HeldUp.to_string(),
            "a repro names the failure to expect, and this run held up"
        );
        assert_eq!(
            ParseReproError::MissingHeader.to_string(),
            "a repro is empty and names no seed"
        );
        assert_eq!(
            ParseReproError::BadHeader.to_string(),
            "expected a first line reading \"chronoloop repro seed <seed>\""
        );
        assert_eq!(
            ParseReproError::BadFailure {
                source: ParseOutcomeError::Malformed
            }
            .to_string(),
            "line 2: expected \"passed\", or a failure written as \"failed at step <n>: <reason>\""
        );
        assert_eq!(
            ParseReproError::Refused {
                source: ReproError::HeldUp
            }
            .to_string(),
            "line 2: a repro names the failure to expect, and this run held up"
        );
        assert_eq!(
            ParseReproError::BadSchedule.to_string(),
            "expected a third line reading \"chronoloop faults\""
        );
        assert_eq!(
            ParseReproError::BadFault {
                line: 5,
                source: ParseFaultError::Malformed
            }
            .to_string()
            .split_once(':')
            .map(|(where_, _)| where_.to_string()),
            Some("line 5".to_string())
        );
    }
}
