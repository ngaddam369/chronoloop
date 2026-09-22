//! How a run went.
//!
//! Everything before this module describes what a run *did*: [`crate::history`] holds what
//! happened, [`crate::trace`] puts the states it passed through in order, and [`crate::diff`] tells
//! two of them apart. None of it says whether the run was all right.
//!
//! An [`Outcome`] is that verdict, and it exists because reducing a failing run to its cause needs
//! something to test against. Shrinking is a loop — drop a fault, run again, ask whether the same
//! failure came back — so "the same failure" has to be a question this type can answer before the
//! loop can be written at all.
//!
//! # A failure points into the trace it is a failure of
//!
//! [`Outcome::Fail`] carries the step the run failed at, which is an index into the [`crate::trace::Trace`]
//! the same run produced. That is what makes a verdict worth reading: `inspect <path> --step <n>`
//! is the next thing a person types, and the comparison with the step before it is the one after
//! that. A system therefore has to derive its outcome from the same observations its steps are
//! derived from, so that the step a failure names is one the trace has.
//!
//! # Why the step is not part of what makes two failures the same
//!
//! Reducing a schedule changes a run. Faults a failure never needed still cost the run draws and
//! still move messages about, so the step a failure surfaces at moves with them while the failure
//! itself stands. Two outcomes are therefore the same failure exactly when their **reasons** agree,
//! and the step is carried along as where to look rather than as part of the identity.
//!
//! Comparing steps as well would be the stricter guard against shrinking wandering off to a
//! neighbouring bug, and it would reject nearly every sound reduction on the way — a schedule that
//! barely shrinks is not a repro anyone can read. What keeps the reduction honest instead is the
//! reason being written to name *what* broke rather than the incidental numbers around it.
//!
//! # Written, and read back
//!
//! An outcome is one line, and that line reads back into the outcome it was written from. The
//! reader sits here beside the writer for the reason the state encoding lives in [`crate::world`]
//! and is never spelled a second time: a form written in two places is two things to keep in step.
//!
//! It is earned by something taking an outcome as **input**. A [`crate::repro`] names the failure a
//! later run is expected to produce, and that run is checked against what the file says — so the
//! line has to survive the trip. [`crate::diff`]'s report has no reader for exactly the opposite
//! reason: nothing reads a comparison back.

use core::fmt;
use core::str::FromStr;

/// How a run that held up is written.
const PASSED: &str = "passed";

/// How a failure opens.
const FAILED_AT: &str = "failed at step ";

/// What stands between a failure's step and its reason.
const BECAUSE: &str = ": ";

/// Reads a step the one way a written outcome writes one.
///
/// Digits and nothing else, so a sign is refused rather than quietly taken for what it precedes:
/// the text an outcome is read from has to be text an outcome would have written. [`crate::trace`]
/// reads its step numbers under the same rule.
fn step(text: &str) -> Option<usize> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// Returned when a reason could not be used as one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReasonError {
    /// The reason is empty, so the failure says nothing about itself.
    Empty,
    /// The reason carries a line ending.
    NotOneLine {
        /// The reason that was refused.
        reason: String,
    },
}

impl fmt::Display for ReasonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "a reason cannot be empty"),
            // Written in its escaped form, so a line ending shows up as one.
            Self::NotOneLine { reason } => write!(f, "a reason must be a single line: {reason:?}"),
        }
    }
}

impl std::error::Error for ReasonError {}

/// What a run failed for: one line, saying what broke.
///
/// A reason is what one failure is told from another, so both of its rules are settled here rather
/// than wherever one is printed. An empty reason identifies nothing. A reason holding a line ending
/// breaks the line it is reported on, the same way a two-line message would break a recorded
/// history and a name holding a separator would break the path around it.
///
/// It is written to name **what** broke rather than the numbers that happened to be around when it
/// did. That is what makes it survive a reduction that leaves the failure standing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reason(String);

impl Reason {
    /// Creates a reason.
    ///
    /// # Errors
    ///
    /// Returns [`ReasonError`] if `reason` is empty or carries a line ending.
    pub fn new(reason: impl Into<String>) -> Result<Self, ReasonError> {
        let reason = reason.into();
        if reason.is_empty() {
            return Err(ReasonError::Empty);
        }
        if reason.contains(['\n', '\r']) {
            return Err(ReasonError::NotOneLine { reason });
        }
        Ok(Self(reason))
    }

    /// Returns the reason as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// How a run went.
///
/// Two variants and no more: a run either held up or it did not. There is nothing to add later, so
/// a caller may match on it exhaustively and needs no method here to ask which it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The run held up.
    Pass,
    /// The run broke.
    Fail {
        /// What broke.
        reason: Reason,
        /// The step of the run's trace it broke at.
        step: usize,
    },
}

impl Outcome {
    /// Returns `true` if this outcome is the failure `original` was.
    ///
    /// This is what a reduction is checked against: a candidate schedule is kept only when the run
    /// it produces still fails the way the run being reduced did. A [`Outcome::Pass`] on either
    /// side is `false` — a run that held up reproduces nothing, and a run that never failed is not
    /// a failure to reproduce.
    pub fn reproduces(&self, original: &Self) -> bool {
        match (self, original) {
            (Self::Fail { reason, .. }, Self::Fail { reason: was, .. }) => reason == was,
            _ => false,
        }
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pass => write!(f, "{PASSED}"),
            Self::Fail { reason, step } => write!(f, "{FAILED_AT}{step}{BECAUSE}{reason}"),
        }
    }
}

/// How a seed is named where a failing run is reported.
const SEED: &str = "seed ";

/// A seed, and the failure the run under it produced.
///
/// More than one command has to say *which* run broke and how, and a form spelled in two places is
/// two things to keep in step — the argument that already keeps the state encoding in
/// [`crate::world`] and the fault header in [`crate::fault`] to one copy each. The failure itself is
/// written by [`Outcome`], so this adds the seed in front of it and nothing else.
///
/// A run that held up cannot be made into one of these. That is settled at construction rather than
/// wherever the value is printed, the way [`Reason::new`] settles what a reason may be: a report of
/// a failure that is not a failure names nothing a reader can go and look at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Broke {
    seed: u64,
    failure: Outcome,
}

impl Broke {
    /// Returns how the run under `seed` broke, or [`None`] if it held up.
    ///
    /// ```
    /// use chronoloop::outcome::{Broke, Outcome, Reason};
    ///
    /// let failure = Outcome::Fail { reason: Reason::new("round 3 lost quorum")?, step: 14 };
    ///
    /// assert_eq!(
    ///     Broke::new(20_260_921, failure).map(|broke| broke.to_string()).as_deref(),
    ///     Some("seed 20260921: failed at step 14: round 3 lost quorum"),
    /// );
    /// assert_eq!(Broke::new(7, Outcome::Pass), None);
    /// # Ok::<(), chronoloop::outcome::ReasonError>(())
    /// ```
    pub fn new(seed: u64, outcome: Outcome) -> Option<Self> {
        match outcome {
            Outcome::Pass => None,
            failure @ Outcome::Fail { .. } => Some(Self { seed, failure }),
        }
    }

    /// Returns the seed the run that broke was drawn from.
    ///
    /// This is what a reduction is asked for next: the seed goes back in at `shrink --seed`, which
    /// is the whole reason a sweep names the seeds it does rather than counting them.
    pub fn seed(&self) -> u64 {
        self.seed
    }
}

impl fmt::Display for Broke {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{SEED}{}{BECAUSE}{}", self.seed, self.failure)
    }
}

/// Returned when an outcome could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseOutcomeError {
    /// The text is not an outcome this form would have written.
    Malformed,
}

impl fmt::Display for ParseOutcomeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => write!(
                f,
                "expected \"{PASSED}\", or a failure written as \"{FAILED_AT}<n>{BECAUSE}<reason>\""
            ),
        }
    }
}

impl std::error::Error for ParseOutcomeError {}

impl FromStr for Outcome {
    type Err = ParseOutcomeError;

    /// Reads back what [`fmt::Display`] wrote, and nothing else.
    ///
    /// A reason that is empty or spans lines collapses to [`ParseOutcomeError::Malformed`] rather
    /// than carrying a [`ReasonError`] out, the way [`crate::history::Entry`] treats a message it
    /// cannot take: such text is simply not something this form ever wrote, and collapsing it keeps
    /// the error `Copy`.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text == PASSED {
            return Ok(Self::Pass);
        }
        let failure = text
            .strip_prefix(FAILED_AT)
            .ok_or(ParseOutcomeError::Malformed)?;
        // The first separator, since the step before it is digits alone — which is what lets a
        // reason carry one of its own.
        let (at, reason) = failure
            .split_once(BECAUSE)
            .ok_or(ParseOutcomeError::Malformed)?;
        let at = step(at).ok_or(ParseOutcomeError::Malformed)?;
        let reason = Reason::new(reason).map_err(|_| ParseOutcomeError::Malformed)?;
        Ok(Self::Fail { reason, step: at })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A reason, failing the test rather than returning an error no case expects.
    fn reason(text: &str) -> Reason {
        Reason::new(text).unwrap_or_else(|e| panic!("a test reason is a reason: {e}"))
    }

    /// A failure of `text` at `step`, in the shorthand the cases below are written in.
    fn failed(text: &str, step: usize) -> Outcome {
        Outcome::Fail {
            reason: reason(text),
            step,
        }
    }

    #[test]
    fn a_seed_that_broke_is_written_as_the_seed_and_what_its_run_did() {
        // One form for "which run, and how it went", so that every command reporting a failing seed
        // says it the same way and there is one place to keep in step. A run that held up did not
        // break, and is refused where the value is made rather than written out as a failure that
        // is not one — the rule `Repro::new` already follows for a repro naming a run that passed.
        struct Case {
            name: &'static str,
            seed: u64,
            outcome: Outcome,
            /// What a person is shown, or nothing at all because there is no failure to show.
            want: Option<&'static str>,
        }
        let cases = [
            Case {
                name: "a failure, named by its seed and written in the outcome's own form",
                seed: 20_260_921,
                outcome: failed("round 3 lost quorum", 14),
                want: Some("seed 20260921: failed at step 14: round 3 lost quorum"),
            },
            Case {
                name: "the smallest seed",
                seed: 0,
                outcome: failed("round 1 lost quorum", 2),
                want: Some("seed 0: failed at step 2: round 1 lost quorum"),
            },
            Case {
                name: "the largest seed",
                seed: u64::MAX,
                outcome: failed("round 5 lost quorum", 0),
                want: Some("seed 18446744073709551615: failed at step 0: round 5 lost quorum"),
            },
            Case {
                name: "a run that held up, which is not a run that broke",
                seed: 7,
                outcome: Outcome::Pass,
                want: None,
            },
        ];
        for case in cases {
            let broke = Broke::new(case.seed, case.outcome);
            assert_eq!(
                broke.as_ref().map(ToString::to_string).as_deref(),
                case.want,
                "{}",
                case.name
            );
            assert_eq!(
                broke.map(|broke| broke.seed()),
                case.want.map(|_| case.seed),
                "{}: and it still knows which run it was",
                case.name
            );
        }
    }

    #[test]
    fn a_reason_that_could_not_identify_a_failure_is_refused() {
        // Both rules are settled here rather than wherever a reason is printed, which is what
        // `Name::new` and `Entry::new` already do for the same argument.
        struct Case {
            name: &'static str,
            text: &'static str,
            want: Result<&'static str, ReasonError>,
        }
        let not_one_line = |text: &str| ReasonError::NotOneLine {
            reason: text.to_string(),
        };
        let cases = [
            Case {
                name: "a plain line",
                text: "round 3 lost quorum",
                want: Ok("round 3 lost quorum"),
            },
            Case {
                name: "a reason that says nothing",
                text: "",
                want: Err(ReasonError::Empty),
            },
            Case {
                name: "two reasons on two lines",
                text: "round 3 lost quorum\nround 4 lost quorum",
                want: Err(not_one_line("round 3 lost quorum\nround 4 lost quorum")),
            },
            Case {
                name: "a line ending at the end",
                text: "round 3 lost quorum\n",
                want: Err(not_one_line("round 3 lost quorum\n")),
            },
            Case {
                name: "a carriage return, which reading a line back would drop",
                text: "round 3 lost quorum\r",
                want: Err(not_one_line("round 3 lost quorum\r")),
            },
            Case {
                name: "a line ending and nothing else",
                text: "\n",
                want: Err(not_one_line("\n")),
            },
        ];
        for case in cases {
            assert_eq!(
                Reason::new(case.text).map(|reason| reason.as_str().to_string()),
                case.want.map(ToString::to_string),
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn two_failures_are_the_same_failure_when_their_reasons_agree() {
        // The premise the whole reduction rests on. The step is deliberately different on the two
        // sides of every case that should hold, since a reduction moves it — an assertion whose two
        // sides were one value would say nothing about which fields are being compared.
        struct Case {
            name: &'static str,
            candidate: Outcome,
            original: Outcome,
            want: bool,
        }
        let cases = [
            Case {
                name: "the same failure, surfacing at another step",
                candidate: failed("round 3 lost quorum", 11),
                original: failed("round 3 lost quorum", 4),
                want: true,
            },
            Case {
                name: "a failure of another round at the same step",
                candidate: failed("round 1 lost quorum", 4),
                original: failed("round 3 lost quorum", 4),
                want: false,
            },
            Case {
                name: "a reason that differs only in its incidentals",
                candidate: failed("round 3 lost quorum with 2 of 5", 4),
                original: failed("round 3 lost quorum", 4),
                want: false,
            },
            Case {
                name: "a run that held up reproduces nothing",
                candidate: Outcome::Pass,
                original: failed("round 3 lost quorum", 4),
                want: false,
            },
            Case {
                name: "a run that never failed is not a failure to reproduce",
                candidate: failed("round 3 lost quorum", 4),
                original: Outcome::Pass,
                want: false,
            },
            Case {
                name: "neither of them failed",
                candidate: Outcome::Pass,
                original: Outcome::Pass,
                want: false,
            },
        ];
        for case in cases {
            assert_eq!(
                case.candidate.reproduces(&case.original),
                case.want,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn an_outcome_says_how_the_run_went_on_one_line() {
        // Pinned as literals: this is what a person is shown, and the step is in it because
        // `inspect --step` is the next thing they type.
        struct Case {
            name: &'static str,
            outcome: Outcome,
            want: &'static str,
        }
        let cases = [
            Case {
                name: "a run that held up",
                outcome: Outcome::Pass,
                want: "passed",
            },
            Case {
                name: "a run that broke",
                outcome: failed("round 3 lost quorum", 11),
                want: "failed at step 11: round 3 lost quorum",
            },
            Case {
                name: "a run that broke at its very first step",
                outcome: failed("round 1 lost quorum", 0),
                want: "failed at step 0: round 1 lost quorum",
            },
        ];
        for case in cases {
            assert_eq!(case.outcome.to_string(), case.want, "{}", case.name);
        }
    }

    #[test]
    fn a_refused_reason_says_which_rule_it_broke() {
        assert_eq!(ReasonError::Empty.to_string(), "a reason cannot be empty");
        assert_eq!(
            ReasonError::NotOneLine {
                reason: "lost\nquorum".to_string(),
            }
            .to_string(),
            "a reason must be a single line: \"lost\\nquorum\"",
            "the reason is escaped, so a line ending shows up as one"
        );
    }

    #[test]
    fn an_outcome_read_back_is_the_outcome_that_was_written() {
        // The failure line of a repro file is this form, so what `Display` writes has to come back
        // as the value it was written from.
        struct Case {
            name: &'static str,
            outcome: Outcome,
        }
        let cases = [
            Case {
                name: "a run that held up",
                outcome: Outcome::Pass,
            },
            Case {
                name: "a run that broke",
                outcome: failed("round 3 lost quorum", 11),
            },
            Case {
                name: "a run that broke at its very first step",
                outcome: failed("round 1 lost quorum", 0),
            },
            Case {
                name: "a reason carrying the separator the step is cut off at",
                outcome: failed("round 3: lost quorum, 2 of 5", 11),
            },
            Case {
                name: "a step no trace could hold, which is still a step",
                outcome: failed("round 3 lost quorum", usize::MAX),
            },
        ];
        for case in cases {
            assert_eq!(
                case.outcome.to_string().parse(),
                Ok(case.outcome.clone()),
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn reading_an_outcome_rejects_text_it_could_not_have_written() {
        // Strict enough to accept only what `Display` writes: a repro's failure line is what one
        // failure is told from another, so text that is nearly a failure is not one.
        struct Case {
            name: &'static str,
            text: &'static str,
        }
        let cases = [
            Case {
                name: "nothing at all",
                text: "",
            },
            Case {
                name: "a verdict in another case",
                text: "Passed",
            },
            Case {
                name: "a verdict with something after it",
                text: "passed ",
            },
            Case {
                name: "a failure that says nothing about where",
                text: "failed",
            },
            Case {
                name: "a step that is not a number",
                text: "failed at step one: round 3 lost quorum",
            },
            Case {
                name: "a signed step, which a written outcome never carries",
                text: "failed at step +1: round 3 lost quorum",
            },
            Case {
                name: "a step below the first one",
                text: "failed at step -1: round 3 lost quorum",
            },
            Case {
                name: "no step at all",
                text: "failed at step : round 3 lost quorum",
            },
            Case {
                name: "a step past every step there could be",
                text: "failed at step 99999999999999999999999999: round 3 lost quorum",
            },
            Case {
                name: "a step with no reason after it",
                text: "failed at step 11:",
            },
            Case {
                name: "a reason that says nothing",
                text: "failed at step 11: ",
            },
            Case {
                name: "two lines, which one reason is not",
                text: "failed at step 11: round 3 lost quorum\nround 4 lost quorum",
            },
        ];
        for case in cases {
            assert_eq!(
                case.text.parse::<Outcome>(),
                Err(ParseOutcomeError::Malformed),
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn an_outcome_that_could_not_be_read_says_what_was_expected() {
        assert_eq!(
            ParseOutcomeError::Malformed.to_string(),
            "expected \"passed\", or a failure written as \"failed at step <n>: <reason>\""
        );
    }
}
