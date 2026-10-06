//! A sweep over seeds `0..count`, in the shape a case compares against.
//!
//! `tests/headline.rs` and `tests/replog.rs` each sweep a system and assert what came back: how
//! many seeds ran, and which broke. They reached that through one harness written out twice — the
//! counts, the workers, and the failures turned into the text a case prints — which is two places
//! for an edit meant for both to land in one of them. `tests/sweep.rs` does not use it, since the
//! worker count and the survey itself are what that file is about.
//!
//! Reached by `#[path]` rather than from `common/mod.rs`, for the reason `common/timeline.rs`
//! gives: a helper is compiled into every crate that declares it, and dead code there fails the
//! build.

use core::fmt::Display;
use core::num::{NonZeroU64, NonZeroUsize};

use chronoloop::outcome::Outcome;
use chronoloop::sweep::sweep;

/// How many workers a survey runs on, which moves wall-clock time and nothing else.
const JOBS: usize = 4;

/// Runs `run` on every seed in `0..count` and returns how many seeds it ran and each failure, as
/// the survey writes it.
///
/// A seed that cannot finish is a broken test rather than an expected outcome, so this panics with
/// the reason rather than handing back a `Result` every caller would have to unwrap.
pub fn survey<E, F>(count: u64, run: F) -> (u64, Vec<String>)
where
    F: Fn(u64) -> Result<Outcome, E> + Sync,
    E: Send + Display,
{
    let seeds =
        NonZeroU64::new(count).unwrap_or_else(|| panic!("a survey covers at least one seed"));
    let jobs = NonZeroUsize::new(JOBS).unwrap_or_else(|| panic!("{JOBS} is not zero"));
    let survey = sweep(seeds, jobs, run).unwrap_or_else(|e| panic!("every seed finishes: {e}"));
    (
        survey.swept(),
        survey.broke().iter().map(ToString::to_string).collect(),
    )
}
