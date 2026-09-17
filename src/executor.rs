//! The single-threaded poll loop that runs tasks over virtual time.

use core::cell::RefCell;
use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Waker};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Wake;

use crate::clock::{ClockError, VirtualClock, VirtualTime};
use crate::event::EventQueue;

/// Identifies a task, assigned in spawn order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(u64);

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "task {}", self.0)
    }
}

/// The clock and the event queue, shared between the executor and every [`Handle`].
///
/// Borrows of this cell are always short-lived and are never held across a poll of a future, so a
/// conflicting borrow cannot arise.
#[derive(Debug, Default)]
struct Shared {
    clock: VirtualClock,
    queue: EventQueue<Waker>,
}

/// A future's view of the simulation it is running inside.
#[derive(Clone)]
pub struct Handle {
    shared: Rc<RefCell<Shared>>,
}

impl Handle {
    /// Returns the current virtual instant.
    pub fn now(&self) -> VirtualTime {
        self.shared.borrow().clock.now()
    }

    /// Schedules `waker` to be woken once the simulation reaches the instant `at`.
    pub fn schedule_wake(&self, at: VirtualTime, waker: Waker) {
        self.shared.borrow_mut().queue.push(at, waker);
    }
}

impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handle")
            .field("now", &self.now())
            .finish_non_exhaustive()
    }
}

/// The tasks waiting to be polled.
///
/// [`Wake`] requires `Send + Sync`, so this lives behind an `Arc<Mutex<..>>` even though a
/// simulation only ever touches it from one thread. The ordered set both de-duplicates repeated
/// wake-ups and fixes the order tasks are polled in.
#[derive(Debug, Default)]
struct ReadyQueue(Mutex<BTreeSet<TaskId>>);

impl ReadyQueue {
    fn mark(&self, id: TaskId) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id);
    }

    fn take_lowest(&self) -> Option<TaskId> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_first()
    }
}

/// The waker handed to one task. Waking it marks that task ready.
struct TaskWaker {
    id: TaskId,
    ready: Arc<ReadyQueue>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.ready.mark(self.id);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.mark(self.id);
    }
}

/// A spawned task: the future itself and the waker that marks it ready.
struct Task {
    future: Pin<Box<dyn Future<Output = ()>>>,
    waker: Waker,
}

/// Errors that end a run.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExecutorError {
    /// An event was scheduled for an instant the simulation had already passed.
    Clock(ClockError),
    /// The run ran out of events while tasks were still waiting, so nothing can make progress.
    Stalled {
        /// The tasks that never finished, in task order.
        pending: Vec<TaskId>,
    },
}

impl fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Clock(error) => write!(f, "{error}"),
            Self::Stalled { pending } => {
                write!(f, "simulation stalled with no events left; still waiting:")?;
                for id in pending {
                    write!(f, " {id}")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for ExecutorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Clock(error) => Some(error),
            Self::Stalled { .. } => None,
        }
    }
}

impl From<ClockError> for ExecutorError {
    fn from(error: ClockError) -> Self {
        Self::Clock(error)
    }
}

/// Runs tasks over virtual time, one thread and one task at a time.
///
/// The loop polls every ready task until none can make further progress, then takes the earliest
/// event off the queue, moves the clock to that instant and wakes whoever was waiting for it.
/// Tasks ready at the same moment are always polled in task order, so a run's interleaving comes
/// from the schedule alone and repeats exactly on the next run.
#[derive(Default)]
pub struct Executor {
    shared: Rc<RefCell<Shared>>,
    tasks: BTreeMap<TaskId, Task>,
    ready: Arc<ReadyQueue>,
    next_id: u64,
}

impl fmt::Debug for Executor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Executor")
            .field("now", &self.shared.borrow().clock.now())
            .field("unfinished", &self.tasks.len())
            .finish_non_exhaustive()
    }
}

impl Executor {
    /// Creates an executor with no tasks, its clock at the start of the simulation.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a handle to this executor's clock and event queue.
    pub fn handle(&self) -> Handle {
        Handle {
            shared: Rc::clone(&self.shared),
        }
    }

    /// Spawns `future` as a task and returns its identifier.
    ///
    /// # Panics
    ///
    /// Panics after `u64::MAX` spawns, which cannot happen in practice.
    pub fn spawn(&mut self, future: impl Future<Output = ()> + 'static) -> TaskId {
        let id = TaskId(self.next_id);
        // Unreachable: a simulation cannot spawn u64::MAX tasks.
        self.next_id = self
            .next_id
            .checked_add(1)
            .expect("task identifiers exhausted after u64::MAX spawns");
        let waker = Waker::from(Arc::new(TaskWaker {
            id,
            ready: Arc::clone(&self.ready),
        }));
        self.tasks.insert(
            id,
            Task {
                future: Box::pin(future),
                waker,
            },
        );
        self.ready.mark(id);
        id
    }

    /// Runs until every task has finished.
    ///
    /// Returns [`ExecutorError::Stalled`] if the events run out while tasks are still waiting, and
    /// [`ExecutorError::Clock`] if an event was scheduled for an instant already passed.
    pub fn run(&mut self) -> Result<(), ExecutorError> {
        loop {
            self.poll_ready();

            // The borrow ends with the statement: nothing may hold it across a poll.
            let next = self.shared.borrow_mut().queue.pop();
            let Some(scheduled) = next else {
                if self.tasks.is_empty() {
                    return Ok(());
                }
                return Err(ExecutorError::Stalled {
                    pending: self.tasks.keys().copied().collect(),
                });
            };

            self.shared.borrow_mut().clock.advance_to(scheduled.at)?;
            scheduled.event.wake();
        }
    }

    /// Polls ready tasks until none is left, including tasks woken during this pass.
    fn poll_ready(&mut self) {
        while let Some(id) = self.ready.take_lowest() {
            // A task that has already finished may still have live wakers.
            let Some(task) = self.tasks.get_mut(&id) else {
                continue;
            };
            let Task { future, waker } = task;
            let mut cx = Context::from_waker(waker);
            if future.as_mut().poll(&mut cx).is_ready() {
                self.tasks.remove(&id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::task::Poll;

    type Log = Rc<RefCell<Vec<(u64, String)>>>;
    type WakerSlot = Rc<RefCell<Option<Waker>>>;

    fn t(nanos: u64) -> VirtualTime {
        VirtualTime::from_nanos(nanos)
    }

    /// Waits for each deadline in turn, logging every time it wakes, then finishes.
    struct WakeAt {
        handle: Handle,
        label: &'static str,
        deadlines: Vec<VirtualTime>,
        next: usize,
        log: Log,
    }

    impl WakeAt {
        fn spawn(executor: &mut Executor, label: &'static str, deadlines: &[u64], log: &Log) {
            let future = Self {
                handle: executor.handle(),
                label,
                deadlines: deadlines.iter().copied().map(t).collect(),
                next: 0,
                log: Rc::clone(log),
            };
            executor.spawn(future);
        }
    }

    impl Future for WakeAt {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            loop {
                let now = self.handle.now();
                let Some(&deadline) = self.deadlines.get(self.next) else {
                    self.log
                        .borrow_mut()
                        .push((now.as_nanos(), format!("{} done", self.label)));
                    return Poll::Ready(());
                };
                if now >= deadline {
                    self.log
                        .borrow_mut()
                        .push((now.as_nanos(), format!("{} wake", self.label)));
                    self.next += 1;
                } else {
                    self.handle.schedule_wake(deadline, cx.waker().clone());
                    return Poll::Pending;
                }
            }
        }
    }

    /// Parks forever, publishing its waker so another task can wake it.
    struct Park {
        label: &'static str,
        slot: WakerSlot,
        log: Log,
        handle: Handle,
        parked: bool,
    }

    impl Future for Park {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let now = self.handle.now().as_nanos();
            if self.parked {
                self.log
                    .borrow_mut()
                    .push((now, format!("{} woken", self.label)));
                return Poll::Ready(());
            }
            self.parked = true;
            *self.slot.borrow_mut() = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    /// Waits until `at`, then wakes the parked tasks in the order it was given them.
    struct WakeOthers {
        handle: Handle,
        at: VirtualTime,
        slots: Vec<WakerSlot>,
        armed: bool,
    }

    impl Future for WakeOthers {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if !self.armed {
                self.armed = true;
                let at = self.at;
                self.handle.schedule_wake(at, cx.waker().clone());
                return Poll::Pending;
            }
            for slot in &self.slots {
                if let Some(waker) = slot.borrow_mut().take() {
                    waker.wake();
                }
            }
            Poll::Ready(())
        }
    }

    #[test]
    fn spawn_polls_every_task_once_in_spawn_order() {
        let log: Log = Log::default();
        let mut executor = Executor::new();
        for label in ["first", "second", "third"] {
            WakeAt::spawn(&mut executor, label, &[], &log);
        }

        assert_eq!(executor.run(), Ok(()));
        assert_eq!(
            *log.borrow(),
            [
                (0, "first done".to_owned()),
                (0, "second done".to_owned()),
                (0, "third done".to_owned()),
            ]
        );
    }

    #[test]
    fn tasks_ready_at_the_same_instant_poll_in_task_id_order() {
        let log: Log = Log::default();
        let mut executor = Executor::new();
        let slots: Vec<WakerSlot> = (0..3).map(|_| WakerSlot::default()).collect();

        for (label, slot) in ["a", "b", "c"].iter().zip(&slots) {
            let future = Park {
                label,
                slot: Rc::clone(slot),
                log: Rc::clone(&log),
                handle: executor.handle(),
                parked: false,
            };
            executor.spawn(future);
        }
        // Woken in the reverse of spawn order: the poll order must not follow the wake order.
        let waker_order: Vec<WakerSlot> = slots.iter().rev().map(Rc::clone).collect();
        let future = WakeOthers {
            handle: executor.handle(),
            at: t(10),
            slots: waker_order,
            armed: false,
        };
        executor.spawn(future);

        assert_eq!(executor.run(), Ok(()));
        assert_eq!(
            *log.borrow(),
            [
                (10, "a woken".to_owned()),
                (10, "b woken".to_owned()),
                (10, "c woken".to_owned()),
            ]
        );
    }

    #[test]
    fn run_advances_the_clock_to_each_scheduled_wakeup() {
        struct Case {
            name: &'static str,
            tasks: &'static [(&'static str, &'static [u64])],
            want: &'static [(u64, &'static str)],
        }
        let cases = [
            Case {
                name: "one task waiting several times",
                tasks: &[("a", &[10, 25, 100])],
                want: &[
                    (10, "a wake"),
                    (25, "a wake"),
                    (100, "a wake"),
                    (100, "a done"),
                ],
            },
            Case {
                name: "two tasks interleaving",
                tasks: &[("a", &[10, 30]), ("b", &[20, 40])],
                want: &[
                    (10, "a wake"),
                    (20, "b wake"),
                    (30, "a wake"),
                    (30, "a done"),
                    (40, "b wake"),
                    (40, "b done"),
                ],
            },
            Case {
                name: "a wait that is already over",
                tasks: &[("a", &[0])],
                want: &[(0, "a wake"), (0, "a done")],
            },
            Case {
                name: "two tasks due at the same instant",
                tasks: &[("a", &[10]), ("b", &[10])],
                want: &[
                    (10, "a wake"),
                    (10, "a done"),
                    (10, "b wake"),
                    (10, "b done"),
                ],
            },
        ];

        for case in cases {
            let log: Log = Log::default();
            let mut executor = Executor::new();
            for (label, deadlines) in case.tasks {
                WakeAt::spawn(&mut executor, label, deadlines, &log);
            }

            assert_eq!(executor.run(), Ok(()), "{}", case.name);
            let want: Vec<(u64, String)> = case
                .want
                .iter()
                .map(|(at, entry)| (*at, (*entry).to_owned()))
                .collect();
            assert_eq!(*log.borrow(), want, "{}", case.name);
        }
    }

    #[test]
    fn stalled_run_reports_every_unfinished_task() {
        let log: Log = Log::default();
        let mut executor = Executor::new();

        let first = executor.spawn(Park {
            label: "parked",
            slot: WakerSlot::default(),
            log: Rc::clone(&log),
            handle: executor.handle(),
            parked: false,
        });
        WakeAt::spawn(&mut executor, "finishes", &[], &log);
        let third = executor.spawn(Park {
            label: "also parked",
            slot: WakerSlot::default(),
            log: Rc::clone(&log),
            handle: executor.handle(),
            parked: false,
        });

        assert_eq!(
            executor.run(),
            Err(ExecutorError::Stalled {
                pending: vec![first, third],
            })
        );
        assert_eq!(*log.borrow(), [(0, "finishes done".to_owned())]);
    }

    #[test]
    fn event_scheduled_in_the_past_surfaces_the_clock_error() {
        /// Waits until instant 100, then asks to be woken at instant 50.
        struct LateSchedule {
            handle: Handle,
            armed: bool,
        }

        impl Future for LateSchedule {
            type Output = ();

            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
                let at = if self.armed { t(50) } else { t(100) };
                self.armed = true;
                self.handle.schedule_wake(at, cx.waker().clone());
                Poll::Pending
            }
        }

        let mut executor = Executor::new();
        let future = LateSchedule {
            handle: executor.handle(),
            armed: false,
        };
        executor.spawn(future);

        assert_eq!(
            executor.run(),
            Err(ExecutorError::Clock(ClockError::Backwards {
                now: t(100),
                target: t(50),
            }))
        );
    }

    #[test]
    fn finished_task_is_not_polled_again() {
        /// Arranges a wake-up for later, then finishes straight away.
        struct FinishEarly {
            handle: Handle,
            log: Log,
        }

        impl Future for FinishEarly {
            type Output = ();

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
                let now = self.handle.now().as_nanos();
                self.log.borrow_mut().push((now, "polled".to_owned()));
                self.handle.schedule_wake(t(10), cx.waker().clone());
                Poll::Ready(())
            }
        }

        let log: Log = Log::default();
        let mut executor = Executor::new();
        let future = FinishEarly {
            handle: executor.handle(),
            log: Rc::clone(&log),
        };
        executor.spawn(future);

        assert_eq!(executor.run(), Ok(()));
        assert_eq!(*log.borrow(), [(0, "polled".to_owned())]);
    }
}
