//! What every integration test needs: a way to finish a run, and a way to spell what it recorded.

use chronoloop::clock::VirtualTime;
use chronoloop::executor::Executor;
use chronoloop::history::Entry;

/// An entry of a history, in the shorthand these tests write their expectations in.
pub fn entry(at: u64, message: impl Into<String>) -> Entry {
    Entry::new(VirtualTime::from_nanos(at), message)
        .unwrap_or_else(|e| panic!("a test expectation is one line: {e}"))
}

/// Runs `executor` to completion and returns the instant the run ended at.
///
/// A run that cannot finish is a broken test rather than an expected outcome, so this panics with
/// the reason rather than handing back a `Result` every caller would have to unwrap.
pub fn finish(executor: &mut Executor) -> u64 {
    executor
        .run()
        .unwrap_or_else(|e| panic!("run did not finish: {e}"));
    executor.handle().now().as_nanos()
}
