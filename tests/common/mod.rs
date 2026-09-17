//! What every integration test needs: somewhere to write a history, and a way to finish a run.

use core::cell::RefCell;
use std::rc::Rc;

use chronoloop::executor::{Executor, Handle};

/// The history a run wrote, each entry stamped with the virtual instant it happened at.
///
/// Cloning shares one journal rather than copying it, so every task in a run can hold one and the
/// entries all land in the same place, in the order the run produced them.
#[derive(Clone)]
pub struct Journal(Rc<RefCell<Vec<(u64, String)>>>);

impl Journal {
    /// Creates an empty journal.
    pub fn new() -> Self {
        Self(Rc::new(RefCell::new(Vec::new())))
    }

    /// Records `entry` at the instant `handle` has reached.
    pub fn record(&self, handle: &Handle, entry: String) {
        self.0.borrow_mut().push((handle.now().as_nanos(), entry));
    }

    /// Returns everything recorded so far, in order.
    pub fn entries(&self) -> Vec<(u64, String)> {
        self.0.borrow().clone()
    }
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
