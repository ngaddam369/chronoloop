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

/// The clock, the event queue and the tasks waiting to be taken on, shared between the executor and
/// every [`Handle`].
///
/// Borrows of this cell are always short-lived and are never held across a poll of a future, so a
/// conflicting borrow cannot arise.
#[derive(Default)]
struct Shared {
    clock: VirtualClock,
    queue: EventQueue<Waker>,
    /// Tasks spawned but not yet taken on by the loop, in the order they were spawned.
    spawned: Vec<(TaskId, Task)>,
    next_id: u64,
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("clock", &self.clock)
            .field("queue", &self.queue)
            .field("spawned", &self.spawned.len())
            .field("next_id", &self.next_id)
            .finish()
    }
}

/// A future's view of the simulation it is running inside.
#[derive(Clone)]
pub struct Handle {
    shared: Rc<RefCell<Shared>>,
    ready: Arc<ReadyQueue>,
}

impl Handle {
    /// Spawns `future` as a task of the simulation this handle belongs to.
    ///
    /// A task spawned while the simulation is running is taken on before the next task is polled,
    /// so it gets its first poll in the same pass rather than waiting for the clock to move. Its
    /// identifier is the next one the counter has, whoever asked for it and whenever they asked.
    ///
    /// Dropping the returned handle detaches the task: it says only that nobody is waiting for the
    /// answer, and the work still happens.
    ///
    /// # Panics
    ///
    /// Panics after `u64::MAX` spawns, which cannot happen in practice.
    pub fn spawn<T: 'static>(&self, future: impl Future<Output = T> + 'static) -> JoinHandle<T> {
        let join = Rc::new(RefCell::new(Join::default()));
        let finishing = Rc::clone(&join);
        let finishes = async move {
            let output = future.await;
            // The borrow ends with the statement, so the waking happens outside it. Waking marks
            // the joiner ready rather than polling it, so this task finishes first.
            let waiting = {
                let mut join = finishing.borrow_mut();
                join.output = Some(output);
                join.finished = true;
                join.waiting.take()
            };
            if let Some(waker) = waiting {
                waker.wake();
            }
        };

        let mut shared = self.shared.borrow_mut();
        let id = TaskId(shared.next_id);
        // Unreachable: a simulation cannot spawn u64::MAX tasks.
        shared.next_id = shared
            .next_id
            .checked_add(1)
            .expect("task identifiers exhausted after u64::MAX spawns");
        let waker = Waker::from(Arc::new(TaskWaker {
            id,
            ready: Arc::clone(&self.ready),
        }));
        shared.spawned.push((
            id,
            Task {
                future: Box::pin(finishes),
                waker,
            },
        ));
        drop(shared);

        self.ready.mark(id);
        JoinHandle { id, join }
    }

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

/// What a spawned task leaves behind for whoever joins it.
#[derive(Debug)]
struct Join<T> {
    /// What the task produced, from the moment it finished until the joiner takes it.
    output: Option<T>,
    /// Whether the task has finished, which stays true after the output has been taken.
    finished: bool,
    /// The waker of whoever is waiting, if anyone is.
    waiting: Option<Waker>,
}

impl<T> Default for Join<T> {
    fn default() -> Self {
        Self {
            output: None,
            finished: false,
            waiting: None,
        }
    }
}

/// A wait for a spawned task to finish, returned by [`Handle::spawn`] and [`Executor::spawn`].
///
/// Awaiting it gives back whatever the task produced. Dropping it instead detaches the task, which
/// runs on exactly as it would have: a handle nobody holds means nobody is waiting for the answer,
/// not that the answer is no longer wanted. Either way the wait takes back the waker it registered,
/// so an abandoned join cannot mark a task ready for something nothing is waiting for.
///
/// One waker is enough here, which it was not for a mailbox: a handle cannot be cloned, so there is
/// exactly one joiner and it is the one the finishing task wakes.
pub struct JoinHandle<T> {
    id: TaskId,
    join: Rc<RefCell<Join<T>>>,
}

impl<T> JoinHandle<T> {
    /// Returns the identifier of the task being waited on.
    pub fn id(&self) -> TaskId {
        self.id
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        // The borrow ends with the statement, so the waker is dropped outside it.
        let registered = self.join.borrow_mut().waiting.take();
        drop(registered);
    }
}

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    /// # Panics
    ///
    /// Panics if polled again after it has already given back the task's output, which a future
    /// must never be.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let this = self.get_mut();
        // One borrow, ending with the statement.
        let taken = {
            let mut join = this.join.borrow_mut();
            let taken = join.output.take();
            if taken.is_none() {
                assert!(
                    !join.finished,
                    "a join was polled again after it gave back its task's output"
                );
                // Register only when the waker held would not wake whoever is polling now, so
                // being polled twice before the task finishes leaves one waker rather than two.
                if !join
                    .waiting
                    .as_ref()
                    .is_some_and(|waker| waker.will_wake(cx.waker()))
                {
                    join.waiting = Some(cx.waker().clone());
                }
            }
            taken
        };
        match taken {
            Some(output) => Poll::Ready(output),
            None => Poll::Pending,
        }
    }
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

    /// Returns a handle to this executor's clock, event queue and spawning.
    pub fn handle(&self) -> Handle {
        Handle {
            shared: Rc::clone(&self.shared),
            ready: Arc::clone(&self.ready),
        }
    }

    /// Spawns `future` as a task, returning the wait for what it produces.
    ///
    /// The same spawning a running task does through [`Handle::spawn`], from outside the run.
    ///
    /// # Panics
    ///
    /// Panics after `u64::MAX` spawns, which cannot happen in practice.
    pub fn spawn<T: 'static>(
        &mut self,
        future: impl Future<Output = T> + 'static,
    ) -> JoinHandle<T> {
        self.handle().spawn(future)
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

    /// Takes on every task spawned since this was last called.
    ///
    /// Spawning only files a task; the loop is what adopts it. Doing that here, rather than once
    /// between passes, is what lets a task spawned during a poll run in the same pass as its
    /// parent — and what keeps it from being mistaken below for a task that has already finished.
    fn adopt_spawned(&mut self) {
        // The borrow ends with the statement, so nothing is held while the tasks are moved over.
        let spawned = core::mem::take(&mut self.shared.borrow_mut().spawned);
        for (id, task) in spawned {
            self.tasks.insert(id, task);
        }
    }

    /// Polls ready tasks until none is left, including tasks woken or spawned during this pass.
    fn poll_ready(&mut self) {
        loop {
            self.adopt_spawned();
            let Some(id) = self.ready.take_lowest() else {
                return;
            };
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

        let first = executor
            .spawn(Park {
                label: "parked",
                slot: WakerSlot::default(),
                log: Rc::clone(&log),
                handle: executor.handle(),
                parked: false,
            })
            .id();
        spawn_sleeper(&mut executor, "finishes", &[], &log);
        let third = executor
            .spawn(Park {
                label: "also parked",
                slot: WakerSlot::default(),
                log: Rc::clone(&log),
                handle: executor.handle(),
                parked: false,
            })
            .id();

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

    #[test]
    fn task_ids_are_handed_out_in_spawn_order_including_from_inside_a_run() {
        let log: Log = Log::default();
        let mut executor = Executor::new();
        let handle = executor.handle();
        let task_log = Rc::clone(&log);

        let first = executor.spawn(async move {
            // Spawned while the run is under way. Its identifier is the next one the counter has,
            // never one that depends on where its future happens to sit in memory.
            let child = handle.spawn(async {});
            task_log
                .borrow_mut()
                .push((0, format!("child is {}", child.id())));
            child.await;
        });
        let second = executor.spawn(async {});

        assert_eq!(executor.run(), Ok(()));
        assert_eq!((first.id(), second.id()), (TaskId(0), TaskId(1)));
        assert_eq!(*log.borrow(), entries(&[(0, "child is task 2")]));
    }

    #[test]
    fn a_task_spawned_during_a_run_is_polled_in_the_same_pass() {
        // The loop takes on what has been spawned before it polls the next task, so a child runs
        // out its first poll beside its parent rather than waiting for the clock to move. A loop
        // that only took on new tasks between passes would leave the child sitting until something
        // else woke the run — and one that never took them on at all would drop it silently.
        let log: Log = Log::default();
        let mut executor = Executor::new();
        let handle = executor.handle();
        let task_log = Rc::clone(&log);

        executor.spawn(async move {
            let child_log = Rc::clone(&task_log);
            let child_clock = handle.clone();
            handle.spawn(async move {
                child_log
                    .borrow_mut()
                    .push((child_clock.now().as_nanos(), "child ran".to_owned()));
            });
            task_log
                .borrow_mut()
                .push((handle.now().as_nanos(), "parent spawned".to_owned()));
            handle.sleep(Duration::from_nanos(10)).await;
            task_log
                .borrow_mut()
                .push((handle.now().as_nanos(), "parent woke".to_owned()));
        });

        assert_eq!(executor.run(), Ok(()));
        assert_eq!(
            *log.borrow(),
            entries(&[(0, "parent spawned"), (0, "child ran"), (10, "parent woke")])
        );
    }

    #[test]
    fn joining_a_task_yields_what_it_produced() {
        struct Case {
            name: &'static str,
            wait: u64,
        }
        let cases = [
            Case {
                name: "a task that finishes at once",
                wait: 0,
            },
            Case {
                name: "a task that finishes after a wait",
                wait: HOUR,
            },
        ];

        for case in cases {
            let mut executor = Executor::new();
            let handle = executor.handle();
            let outcome: Rc<RefCell<Option<u64>>> = Rc::default();
            let slot = Rc::clone(&outcome);

            let worker_clock = handle.clone();
            let worker = executor.spawn(async move {
                worker_clock.sleep(Duration::from_nanos(case.wait)).await;
                worker_clock.now().as_nanos()
            });
            executor.spawn(async move {
                *slot.borrow_mut() = Some(worker.await);
            });

            assert_eq!(executor.run(), Ok(()), "{}", case.name);
            assert_eq!(*outcome.borrow(), Some(case.wait), "{}", case.name);
        }
    }

    #[test]
    fn a_join_lands_at_the_instant_its_task_finished() {
        // The quicker task was spawned second and is waited on first, and its join lands when it
        // finishes rather than when the other one does.
        let log: Log = Log::default();
        let mut executor = Executor::new();
        let handle = executor.handle();
        let task_log = Rc::clone(&log);

        let slow_clock = handle.clone();
        let slow = executor.spawn(async move {
            slow_clock.sleep(Duration::from_nanos(2 * HOUR)).await;
            "slow"
        });
        let quick_clock = handle.clone();
        let quick = executor.spawn(async move {
            quick_clock.sleep(Duration::from_nanos(HOUR)).await;
            "quick"
        });

        executor.spawn(async move {
            for worker in [quick, slow] {
                let who = worker.await;
                task_log
                    .borrow_mut()
                    .push((handle.now().as_nanos(), format!("{who} joined")));
            }
        });

        assert_eq!(executor.run(), Ok(()));
        assert_eq!(
            *log.borrow(),
            entries(&[(HOUR, "quick joined"), (2 * HOUR, "slow joined")])
        );
    }

    #[test]
    fn joining_a_task_that_has_already_finished_completes_at_once() {
        // Waiting on the slow one first means the quick one is long done by the time it is asked
        // about. Its join completes on the first poll, at the instant the wait before it ended.
        let log: Log = Log::default();
        let mut executor = Executor::new();
        let handle = executor.handle();
        let task_log = Rc::clone(&log);

        let slow_clock = handle.clone();
        let slow = executor.spawn(async move {
            slow_clock.sleep(Duration::from_nanos(2 * HOUR)).await;
            "slow"
        });
        let quick_clock = handle.clone();
        let quick = executor.spawn(async move {
            quick_clock.sleep(Duration::from_nanos(HOUR)).await;
            "quick"
        });

        executor.spawn(async move {
            for worker in [slow, quick] {
                let who = worker.await;
                task_log
                    .borrow_mut()
                    .push((handle.now().as_nanos(), format!("{who} joined")));
            }
        });

        assert_eq!(executor.run(), Ok(()));
        assert_eq!(
            *log.borrow(),
            entries(&[(2 * HOUR, "slow joined"), (2 * HOUR, "quick joined")])
        );
        // Nothing was left to wait for after the second join, so the run ends where the first did.
        assert_eq!(executor.handle().now().as_nanos(), 2 * HOUR);
    }

    #[test]
    fn a_dropped_join_handle_leaves_its_task_running() {
        // Walking away from the handle says only that nobody is waiting. The work still happens.
        let log: Log = Log::default();
        let mut executor = Executor::new();
        let handle = executor.handle();
        let task_log = Rc::clone(&log);

        let worker = executor.spawn(async move {
            handle.sleep(Duration::from_nanos(HOUR)).await;
            task_log
                .borrow_mut()
                .push((handle.now().as_nanos(), "worked anyway".to_owned()));
        });
        drop(worker);

        assert_eq!(executor.run(), Ok(()));
        assert_eq!(*log.borrow(), entries(&[(HOUR, "worked anyway")]));
        assert_eq!(executor.handle().now().as_nanos(), HOUR);
    }

    #[test]
    fn a_join_abandoned_before_its_task_finished_wakes_nobody() {
        // The same rule as an abandoned timer, in the joiner's terms: a waker left behind in the
        // task being waited on would mark a task ready for an answer nothing is waiting for. The
        // polls of the wait that follows are where that shows up.
        let polls: Rc<RefCell<Vec<u64>>> = Rc::default();
        let mut executor = Executor::new();
        let handle = executor.handle();
        let task_polls = Rc::clone(&polls);

        let worker_clock = handle.clone();
        let worker = executor.spawn(async move {
            worker_clock.sleep(Duration::from_nanos(HOUR)).await;
        });
        executor.spawn(async move {
            let mut joining = Box::pin(worker);
            let armed = core::future::poll_fn(|cx| Poll::Ready(joining.as_mut().poll(cx))).await;
            assert!(armed.is_pending(), "the task has not finished yet");
            drop(joining);

            CountedSleep {
                sleep: Box::pin(handle.sleep_until(t(2 * HOUR))),
                handle: handle.clone(),
                polls: task_polls,
            }
            .await;
        });

        assert_eq!(executor.run(), Ok(()));
        // A waker left behind would wake this task when the worker finished, and the wait it is
        // sitting in would be polled a third time, at the hour mark.
        assert_eq!(*polls.borrow(), [0, 2 * HOUR]);
    }

    #[test]
    fn a_join_on_a_task_that_never_finishes_stalls_naming_both() {
        let log: Log = Log::default();
        let mut executor = Executor::new();

        let parked = executor.spawn(Park {
            label: "parked",
            slot: WakerSlot::default(),
            log: Rc::clone(&log),
            handle: executor.handle(),
            parked: false,
        });
        let waited_on = parked.id();
        let joiner = executor.spawn(async move {
            parked.await;
        });

        assert_eq!(
            executor.run(),
            Err(ExecutorError::Stalled {
                pending: vec![waited_on, joiner.id()],
            })
        );
    }
}
