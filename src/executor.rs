//! The single-threaded poll loop that runs tasks over virtual time.

use core::cell::RefCell;
use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Wake;

use crate::clock::{Clock, ClockError, VirtualClock, VirtualTime};
use crate::event::{EventId, EventQueue};

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
    /// Schedules `waker` to be woken once the simulation reaches the instant `at`.
    ///
    /// Returns the identifier that [`Handle::cancel_wake`] takes to call the wake-up off again.
    pub fn schedule_wake(&self, at: VirtualTime, waker: Waker) -> EventId {
        self.shared.borrow_mut().queue.push(at, waker)
    }

    /// Calls off the wake-up named by `id`, dropping the waker it was holding.
    ///
    /// A wake-up that has already been delivered, or already been called off, is left alone: a
    /// wait that completed before it was abandoned costs nothing to abandon.
    pub fn cancel_wake(&self, id: EventId) {
        // The borrow ends with the statement, so the waker is dropped outside it.
        let cancelled = self.shared.borrow_mut().queue.cancel(id);
        drop(cancelled);
    }
}

impl Clock for Handle {
    type Sleep = Sleep;

    fn now(&self) -> VirtualTime {
        self.shared.borrow().clock.now()
    }

    fn sleep_until(&self, deadline: VirtualTime) -> Sleep {
        Sleep {
            handle: self.clone(),
            deadline,
            registered: None,
        }
    }
}

/// A wait on the virtual clock, created by [`Clock::sleep`] or [`Clock::sleep_until`].
///
/// Dropping a wait takes its timer back out of the queue. An abandoned wait therefore leaves no
/// trace in the run: it cannot wake the task that walked away from it, and it cannot move the
/// clock to an instant nothing in the simulation is waiting for.
pub struct Sleep {
    handle: Handle,
    deadline: VirtualTime,
    registered: Option<(EventId, Waker)>,
}

impl Sleep {
    /// Takes this wait's timer back out of the queue, if it still holds one.
    fn disarm(&mut self) {
        if let Some((id, _)) = self.registered.take() {
            self.handle.cancel_wake(id);
        }
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        self.disarm();
    }
}

impl fmt::Debug for Sleep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sleep")
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.handle.now() >= self.deadline {
            self.disarm();
            return Poll::Ready(());
        }
        // Re-arm only when the stored waker would not wake this task, so being polled again
        // before the deadline leaves one timer in the queue rather than two.
        let armed = self
            .registered
            .as_ref()
            .is_some_and(|(_, waker)| waker.will_wake(cx.waker()));
        if !armed {
            // A waker that would not wake this task is of no use, and neither is the timer holding
            // it: call that one off rather than leaving it to fire beside its replacement.
            self.disarm();
            let waker = cx.waker().clone();
            let id = self.handle.schedule_wake(self.deadline, waker.clone());
            self.registered = Some((id, waker));
        }
        Poll::Pending
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

    /// Runs until every task has finished, and stops the clock where the last one left it.
    ///
    /// Events still queued when the final task finishes are discarded rather than run out: with no
    /// task left to observe them, delivering them would only carry the clock past the end of the
    /// work. The instant the run ends at is therefore the instant the work ended at.
    ///
    /// Returns [`ExecutorError::Stalled`] if the events run out while tasks are still waiting, and
    /// [`ExecutorError::Clock`] if an event was scheduled for an instant already passed.
    pub fn run(&mut self) -> Result<(), ExecutorError> {
        loop {
            self.poll_ready();
            if self.tasks.is_empty() {
                return Ok(());
            }

            // The borrow ends with the statement: nothing may hold it across a poll.
            let next = self.shared.borrow_mut().queue.pop();
            let Some(scheduled) = next else {
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
    use core::time::Duration;

    use super::*;

    const HOUR: u64 = 3600 * 1_000_000_000;
    const DAY: u64 = 24 * HOUR;

    type Log = Rc<RefCell<Vec<(u64, String)>>>;
    type WakerSlot = Rc<RefCell<Option<Waker>>>;

    fn t(nanos: u64) -> VirtualTime {
        VirtualTime::from_nanos(nanos)
    }

    /// Spawns a task that waits until each instant in turn, logging every wake-up.
    fn spawn_sleeper(
        executor: &mut Executor,
        label: &'static str,
        deadlines: &'static [u64],
        log: &Log,
    ) {
        let handle = executor.handle();
        let log = Rc::clone(log);
        executor.spawn(async move {
            for &deadline in deadlines {
                handle.sleep_until(t(deadline)).await;
                log.borrow_mut()
                    .push((handle.now().as_nanos(), format!("{label} wake")));
            }
            log.borrow_mut()
                .push((handle.now().as_nanos(), format!("{label} done")));
        });
    }

    /// Spawns a task that sleeps for each duration in turn, logging every wake-up.
    fn spawn_napper(executor: &mut Executor, label: &'static str, naps: &'static [u64], log: &Log) {
        let handle = executor.handle();
        let log = Rc::clone(log);
        executor.spawn(async move {
            for &nap in naps {
                handle.sleep(Duration::from_nanos(nap)).await;
                log.borrow_mut()
                    .push((handle.now().as_nanos(), format!("{label} wake")));
            }
            log.borrow_mut()
                .push((handle.now().as_nanos(), format!("{label} done")));
        });
    }

    fn entries(want: &[(u64, &str)]) -> Vec<(u64, String)> {
        want.iter()
            .map(|(at, entry)| (*at, (*entry).to_owned()))
            .collect()
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

    /// Wraps a [`Sleep`], recording the instant of every poll it passes through.
    ///
    /// Boxing the inner future keeps the wrapper `Unpin` without a hand-written projection.
    struct CountedSleep {
        sleep: Pin<Box<Sleep>>,
        handle: Handle,
        polls: Rc<RefCell<Vec<u64>>>,
    }

    impl Future for CountedSleep {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let now = self.handle.now().as_nanos();
            self.polls.borrow_mut().push(now);
            self.sleep.as_mut().poll(cx)
        }
    }

    /// Wraps a [`Sleep`], handing it a freshly built waker on every poll.
    ///
    /// The waker forwards to the one the executor supplied, so waking it still wakes the task, but
    /// [`Waker::will_wake`] answers `false` every time — the case a combinator that polls its
    /// children through their own wakers produces.
    struct FreshWakerEachPoll {
        sleep: Pin<Box<Sleep>>,
    }

    struct Forward(Mutex<Option<Waker>>);

    impl Wake for Forward {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            let waker = self.0.lock().unwrap_or_else(PoisonError::into_inner).take();
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }

    impl Future for FreshWakerEachPoll {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let forwarding = Waker::from(Arc::new(Forward(Mutex::new(Some(cx.waker().clone())))));
            let mut forwarded = Context::from_waker(&forwarding);
            self.sleep.as_mut().poll(&mut forwarded)
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
            spawn_sleeper(&mut executor, label, &[], &log);
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
                spawn_sleeper(&mut executor, label, deadlines, &log);
            }

            assert_eq!(executor.run(), Ok(()), "{}", case.name);
            assert_eq!(*log.borrow(), entries(case.want), "{}", case.name);
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
        spawn_sleeper(&mut executor, "finishes", &[], &log);
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
        // The run ended with the last task, so the wake-up it left behind never moved the clock.
        assert_eq!(executor.handle().now(), VirtualTime::ZERO);
    }

    #[test]
    fn a_wait_abandoned_before_its_deadline_never_fires() {
        let polls: Rc<RefCell<Vec<u64>>> = Rc::default();
        let mut executor = Executor::new();
        let handle = executor.handle();
        let task_polls = Rc::clone(&polls);
        executor.spawn(async move {
            let mut abandoned = Box::pin(handle.sleep(Duration::from_nanos(DAY)));
            // Arm the wait, then walk away from it — a reconcile whose work finished before its
            // own timeout did.
            let armed = core::future::poll_fn(|cx| Poll::Ready(abandoned.as_mut().poll(cx))).await;
            assert!(armed.is_pending(), "the wait must still have been pending");
            drop(abandoned);

            CountedSleep {
                sleep: Box::pin(handle.sleep(Duration::from_nanos(2 * DAY))),
                handle: handle.clone(),
                polls: task_polls,
            }
            .await;
        });

        assert_eq!(executor.run(), Ok(()));
        // Without the abandoned timer there is no stop at one day: the task waits out its own two.
        assert_eq!(*polls.borrow(), [0, 2 * DAY]);
        assert_eq!(executor.handle().now().as_nanos(), 2 * DAY);
    }

    #[test]
    fn a_run_ends_at_the_instant_its_last_task_finished() {
        let mut executor = Executor::new();
        let handle = executor.handle();
        executor.spawn(async move {
            let mut abandoned = Box::pin(handle.sleep(Duration::from_nanos(DAY)));
            let armed = core::future::poll_fn(|cx| Poll::Ready(abandoned.as_mut().poll(cx))).await;
            assert!(armed.is_pending(), "the wait must still have been pending");
            drop(abandoned);
            handle.sleep(Duration::from_nanos(10)).await;
        });

        assert_eq!(executor.run(), Ok(()));
        assert_eq!(executor.handle().now().as_nanos(), 10);
    }

    #[test]
    fn a_wait_polled_with_a_new_waker_arms_only_one_timer() {
        const DEADLINE: u64 = 50;

        let polls: Rc<RefCell<Vec<u64>>> = Rc::default();
        let mut executor = Executor::new();
        let handle = executor.handle();
        let task_polls = Rc::clone(&polls);
        executor.spawn(async move {
            let waker = core::future::poll_fn(|cx| Poll::Ready(cx.waker().clone())).await;
            // Wake the task while the wait is outstanding, so the wait is polled a second time and
            // sees a different waker than the one it registered.
            waker.wake();
            FreshWakerEachPoll {
                sleep: Box::pin(handle.sleep_until(t(DEADLINE))),
            }
            .await;

            // A second wait, whose polls count the wake-ups the first one left behind.
            CountedSleep {
                sleep: Box::pin(handle.sleep_until(t(DEADLINE + 10))),
                handle: handle.clone(),
                polls: task_polls,
            }
            .await;
        });

        assert_eq!(executor.run(), Ok(()));
        // A second timer left over from the re-arm would wake the task again at the deadline, and
        // the wait that follows would be polled twice there.
        assert_eq!(*polls.borrow(), [DEADLINE, DEADLINE + 10]);
    }

    #[test]
    fn sleep_resolves_when_the_clock_reaches_the_deadline() {
        struct Case {
            name: &'static str,
            tasks: &'static [(&'static str, &'static [u64])],
            want: &'static [(u64, &'static str)],
        }
        let cases = [
            Case {
                name: "one task sleeping several times",
                tasks: &[("a", &[10, 15, 75])],
                want: &[
                    (10, "a wake"),
                    (25, "a wake"),
                    (100, "a wake"),
                    (100, "a done"),
                ],
            },
            Case {
                name: "two tasks with different naps interleave",
                tasks: &[("a", &[10, 20]), ("b", &[20, 20])],
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
                name: "equal naps wake in spawn order",
                tasks: &[("a", &[10]), ("b", &[10])],
                want: &[
                    (10, "a wake"),
                    (10, "a done"),
                    (10, "b wake"),
                    (10, "b done"),
                ],
            },
            Case {
                name: "a nap of no length",
                tasks: &[("a", &[0])],
                want: &[(0, "a wake"), (0, "a done")],
            },
        ];

        for case in cases {
            let log: Log = Log::default();
            let mut executor = Executor::new();
            for (label, naps) in case.tasks {
                spawn_napper(&mut executor, label, naps, &log);
            }

            assert_eq!(executor.run(), Ok(()), "{}", case.name);
            assert_eq!(*log.borrow(), entries(case.want), "{}", case.name);
        }
    }

    #[test]
    fn sleep_until_an_instant_already_passed_completes_immediately() {
        let log: Log = Log::default();
        let mut executor = Executor::new();
        spawn_sleeper(&mut executor, "a", &[100, 50], &log);

        assert_eq!(executor.run(), Ok(()));
        assert_eq!(
            *log.borrow(),
            entries(&[(100, "a wake"), (100, "a wake"), (100, "a done")])
        );
    }

    #[test]
    fn sleep_saturates_at_the_end_of_virtual_time() {
        let log: Log = Log::default();
        let mut executor = Executor::new();
        let handle = executor.handle();
        let task_log = Rc::clone(&log);
        executor.spawn(async move {
            handle.sleep(Duration::from_nanos(1)).await;
            handle.sleep(Duration::MAX).await;
            task_log
                .borrow_mut()
                .push((handle.now().as_nanos(), "woke at the end".to_owned()));
        });

        assert_eq!(executor.run(), Ok(()));
        assert_eq!(*log.borrow(), entries(&[(u64::MAX, "woke at the end")]));
    }

    #[test]
    fn spurious_wake_does_not_resolve_a_pending_sleep() {
        let log: Log = Log::default();
        let mut executor = Executor::new();
        let handle = executor.handle();
        let task_log = Rc::clone(&log);
        executor.spawn(async move {
            let waker = core::future::poll_fn(|cx| Poll::Ready(cx.waker().clone())).await;
            let sleep = handle.sleep(Duration::from_nanos(50));
            // Wake the task while the sleep is outstanding: it must be polled again and stay pending.
            waker.wake();
            sleep.await;
            task_log
                .borrow_mut()
                .push((handle.now().as_nanos(), "slept".to_owned()));
        });

        assert_eq!(executor.run(), Ok(()));
        assert_eq!(*log.borrow(), entries(&[(50, "slept")]));
    }

    #[test]
    fn a_sleep_costs_the_same_polls_however_long_the_span() {
        struct Case {
            name: &'static str,
            deadline: u64,
        }
        // The spans differ by nineteen orders of magnitude; the cost must not notice.
        let cases = [
            Case {
                name: "one nanosecond",
                deadline: 1,
            },
            Case {
                name: "one hour",
                deadline: HOUR,
            },
            Case {
                name: "one year",
                deadline: 365 * 24 * HOUR,
            },
            Case {
                name: "the end of virtual time",
                deadline: u64::MAX,
            },
        ];

        for case in cases {
            let polls: Rc<RefCell<Vec<u64>>> = Rc::default();
            let mut executor = Executor::new();
            let handle = executor.handle();
            let future = CountedSleep {
                sleep: Box::pin(handle.sleep_until(t(case.deadline))),
                handle,
                polls: Rc::clone(&polls),
            };
            executor.spawn(future);

            assert_eq!(executor.run(), Ok(()), "{}", case.name);
            // Polled once at the start and once at the deadline: the clock got there in one move.
            assert_eq!(*polls.borrow(), [0, case.deadline], "{}", case.name);
            assert_eq!(
                executor.handle().now().as_nanos(),
                case.deadline,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn run_cost_tracks_event_count_not_elapsed_virtual_time() {
        const SLEEPS: u64 = 10_000;

        let polls: Rc<RefCell<Vec<u64>>> = Rc::default();
        let wakes: Rc<RefCell<Vec<u64>>> = Rc::default();
        let mut executor = Executor::new();
        let handle = executor.handle();
        let task_polls = Rc::clone(&polls);
        let task_wakes = Rc::clone(&wakes);
        executor.spawn(async move {
            for _ in 0..SLEEPS {
                CountedSleep {
                    sleep: Box::pin(handle.sleep(Duration::from_nanos(HOUR))),
                    handle: handle.clone(),
                    polls: Rc::clone(&task_polls),
                }
                .await;
                task_wakes.borrow_mut().push(handle.now().as_nanos());
            }
        });

        assert_eq!(executor.run(), Ok(()));

        let want_wakes: Vec<u64> = (1..=SLEEPS).map(|nth| nth * HOUR).collect();
        assert_eq!(*wakes.borrow(), want_wakes);
        // Two polls per sleep — pending, then ready — and not one more for the year in between.
        assert_eq!(polls.borrow().len(), 2 * want_wakes.len());
        assert_eq!(executor.handle().now().as_nanos(), SLEEPS * HOUR);
    }
}
