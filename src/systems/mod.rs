//! The systems chronoloop runs under simulation.
//!
//! A system is ordinary asynchronous code that takes its time and its randomness from the
//! simulation rather than from the machine, so a run of it is a pure function of its seed.

use core::fmt;
use core::str::FromStr;

use crate::executor::ExecutorError;
use crate::fault::FaultSchedule;
use crate::history::MultilineMessageError;
use crate::invariant::CheckError;
use crate::outcome::{Outcome, ReasonError};
use crate::systems::reconciler::PlacementError;
use crate::world::NameError;

pub mod pingpong;
pub mod quorum;
pub mod reconciler;
pub mod replog;
pub mod ring;

/// A system a schedule of faults can be thrown at, named the way a written repro names it.
///
/// A repro is a seed, a schedule and the failure to expect, and none of those three says which code
/// they are about: a coordinator's repro run against the reconciler fails to reproduce, and says so
/// in words that blame the run rather than the reader. So the file names its system, and this is the
/// name. Each is written as the module it lives in, so a person reading the header knows which file
/// to open.
///
/// Only the systems that take faults are here. The exchange and the ring take none — a schedule has
/// nothing to say to either — so a repro could never be of them.
///
/// ```
/// use chronoloop::systems::System;
///
/// assert_eq!(System::Reconciler.to_string(), "reconciler");
/// assert_eq!("quorum".parse(), Ok(System::Quorum));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum System {
    /// The coordinator asking five replicas for a quorum, in [`quorum`].
    Quorum,
    /// The control loop keeping databases placed across regions, in [`reconciler`].
    Reconciler,
}

impl System {
    /// Runs this system's seed `seed` under `faults`, and says how it went.
    ///
    /// # Errors
    ///
    /// Returns [`RunError`] if the run could not finish.
    pub fn run(self, seed: u64, faults: &FaultSchedule) -> Result<Outcome, RunError> {
        let (_, _, outcome) = match self {
            Self::Quorum => quorum::run(seed, faults)?,
            Self::Reconciler => reconciler::run(seed, faults)?,
        };
        Ok(outcome)
    }
}

impl fmt::Display for System {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Quorum => write!(f, "quorum"),
            Self::Reconciler => write!(f, "reconciler"),
        }
    }
}

/// Returned when text does not name a system faults can be thrown at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownSystem;

impl fmt::Display for UnknownSystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "expected \"quorum\" or \"reconciler\"")
    }
}

impl std::error::Error for UnknownSystem {}

impl FromStr for System {
    type Err = UnknownSystem;

    /// Reads only what [`Display`](fmt::Display) writes, in the case it writes it.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "quorum" => Ok(Self::Quorum),
            "reconciler" => Ok(Self::Reconciler),
            _ => Err(UnknownSystem),
        }
    }
}

/// Why a system's run produced no history.
#[derive(Debug)]
#[non_exhaustive]
pub enum RunError {
    /// The simulation could not finish.
    Engine(ExecutorError),
    /// The run tried to record something that could not be read back.
    Message(MultilineMessageError),
    /// The run tried to call a part of its world something that is not a name.
    Name(NameError),
    /// The run broke, and could not say why in a form a failure is identified by.
    Reason(ReasonError),
    /// The run asked for a database to be placed somewhere no database can be.
    Placement(PlacementError),
    /// The run could not be judged from what it recorded.
    Check(CheckError),
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Engine(error) => write!(f, "{error}"),
            Self::Message(error) => write!(f, "{error}"),
            Self::Name(error) => write!(f, "{error}"),
            Self::Reason(error) => write!(f, "{error}"),
            Self::Placement(error) => write!(f, "{error}"),
            Self::Check(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for RunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Engine(error) => Some(error),
            Self::Message(error) => Some(error),
            Self::Name(error) => Some(error),
            Self::Reason(error) => Some(error),
            Self::Placement(error) => Some(error),
            Self::Check(error) => Some(error),
        }
    }
}

impl From<CheckError> for RunError {
    fn from(error: CheckError) -> Self {
        Self::Check(error)
    }
}

impl From<ExecutorError> for RunError {
    fn from(error: ExecutorError) -> Self {
        Self::Engine(error)
    }
}

impl From<MultilineMessageError> for RunError {
    fn from(error: MultilineMessageError) -> Self {
        Self::Message(error)
    }
}

impl From<NameError> for RunError {
    fn from(error: NameError) -> Self {
        Self::Name(error)
    }
}

impl From<ReasonError> for RunError {
    fn from(error: ReasonError) -> Self {
        Self::Reason(error)
    }
}

impl From<PlacementError> for RunError {
    fn from(error: PlacementError) -> Self {
        Self::Placement(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_system_reads_back_as_the_name_it_writes() {
        for system in [System::Quorum, System::Reconciler] {
            assert_eq!(system.to_string().parse(), Ok(system), "{system}");
        }
    }

    #[test]
    fn a_name_no_system_writes_is_refused() {
        for text in ["", "pingpong", "ring", "Quorum", "quorum ", "coordinator"] {
            assert_eq!(text.parse::<System>(), Err(UnknownSystem), "{text:?}");
        }
    }

    #[test]
    fn a_system_runs_the_code_it_names() {
        // One schedule, cutting node 0 off from node 1 for the whole run, does different things to
        // the two: the coordinator still hears from four of its five replicas, while the controller
        // is the one node 0 and never reaches its server. So which verdict comes back says which
        // system ran, and a dispatch sending both names to one system turns one of these red.
        let cut: FaultSchedule = "chronoloop faults\n\
                                  partition on node 0 -> node 1 from 0.000000000s until forever\n"
            .parse()
            .unwrap_or_else(|e| panic!("a test schedule is a schedule: {e}"));
        let ran = |system: System| {
            system
                .run(20_261_005, &cut)
                .unwrap_or_else(|e| panic!("{system} finishes: {e}"))
                .to_string()
        };
        assert_eq!(ran(System::Quorum), "passed");
        assert!(
            ran(System::Reconciler).ends_with(": did not converge"),
            "{}",
            ran(System::Reconciler)
        );
    }
}
