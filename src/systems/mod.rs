//! The systems chronoloop runs under simulation.
//!
//! A system is ordinary asynchronous code that takes its time and its randomness from the
//! simulation rather than from the machine, so a run of it is a pure function of its seed.

use core::fmt;

use crate::executor::ExecutorError;
use crate::history::MultilineMessageError;
use crate::world::NameError;

pub mod pingpong;
pub mod ring;

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
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Engine(error) => write!(f, "{error}"),
            Self::Message(error) => write!(f, "{error}"),
            Self::Name(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for RunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Engine(error) => Some(error),
            Self::Message(error) => Some(error),
            Self::Name(error) => Some(error),
        }
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
