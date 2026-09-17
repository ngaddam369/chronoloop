//! Two tasks exchanging one message over virtual time.
//!
//! The smallest system that is still a whole simulation: one task waits, sends a ping and waits for
//! the reply; the other waits for the ping, waits again, and sends a pong back. Every delay is drawn
//! from the run's seed, so the four entries the exchange records are a function of that seed and of
//! nothing else.
//!
//! It is also the first thing here that wakes a task for a reason other than a timer. The receiving
//! task waits on the message *arriving* rather than on an instant both sides agreed in advance,
//! which is the shape a simulated network takes.

use core::cell::RefCell;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::rc::Rc;

use crate::executor::{Executor, ExecutorError};
use crate::history::{Recorder, Recording};
use crate::rng::SeededRng;

/// The shortest a message may take to go out.
const MIN_DELAY: Duration = Duration::from_millis(1);

/// The longest a message may take to go out.
const MAX_DELAY: Duration = Duration::from_secs(2);

/// A one-shot handover between two tasks: a message, and the waker of whoever is waiting for it.
#[derive(Debug, Default)]
struct Mailbox {
    message: Option<&'static str>,
    waker: Option<Waker>,
}

/// Both ends of a handover. Cloning shares one mailbox rather than copying it.
#[derive(Clone, Debug, Default)]
struct Channel(Rc<RefCell<Mailbox>>);

impl Channel {
    /// Creates an empty channel.
    fn new() -> Self {
        Self::default()
    }

    /// Leaves `message` for the receiver, waking it if it is already waiting.
    fn send(&self, message: &'static str) {
        // The borrow ends with the statement, so the waker is woken outside it. Waking marks a task
        // ready rather than polling it, so the sender runs on to whatever it does next first.
        let waiting = {
            let mut mailbox = self.0.borrow_mut();
            mailbox.message = Some(message);
            mailbox.waker.take()
        };
        if let Some(waker) = waiting {
            waker.wake();
        }
    }

    /// Returns a future that completes once a message has been left.
    fn recv(&self) -> Recv {
        Recv {
            channel: self.clone(),
            registered: None,
        }
    }
}

/// A wait for a message, created by [`Channel::recv`].
///
/// Dropping the wait takes its waker back out of the mailbox, so an abandoned wait cannot leave
/// behind a waker that marks a task ready for a message nothing is waiting for.
struct Recv {
    channel: Channel,
    registered: Option<Waker>,
}

impl Recv {
    /// Takes this wait's waker back out of the mailbox, if the mailbox still holds it.
    fn disarm(&mut self) {
        let Some(waker) = self.registered.take() else {
            return;
        };
        // The borrow ends with the statement, so the stale waker is dropped outside it.
        let stale = {
            let mut mailbox = self.channel.0.borrow_mut();
            if mailbox
                .waker
                .as_ref()
                .is_some_and(|left| left.will_wake(&waker))
            {
                mailbox.waker.take()
            } else {
                None
            }
        };
        drop(stale);
    }
}

impl Drop for Recv {
    fn drop(&mut self) {
        self.disarm();
    }
}

impl Future for Recv {
    type Output = &'static str;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // The borrow ends with the statement: nothing may hold it across what follows.
        let delivered = self.channel.0.borrow_mut().message.take();
        if let Some(message) = delivered {
            self.disarm();
            return Poll::Ready(message);
        }
        // Re-register only when the stored waker would not wake this task, so being polled twice
        // before the message arrives leaves one waker behind rather than replacing a good one.
        let armed = self
            .registered
            .as_ref()
            .is_some_and(|waker| waker.will_wake(cx.waker()));
        if !armed {
            self.disarm();
            let waker = cx.waker().clone();
            self.channel.0.borrow_mut().waker = Some(waker.clone());
            self.registered = Some(waker);
        }
        Poll::Pending
    }
}

/// Runs the exchange under `seed` and returns the history it wrote.
///
/// The same seed always writes the same history, which is what makes a recording of one run enough
/// to check a later run of the engine against:
///
/// ```
/// let recording = chronoloop::systems::pingpong::run(7)?;
///
/// assert_eq!(recording.seed(), 7);
/// assert_eq!(recording.entries().len(), 4);
/// assert_eq!(recording, chronoloop::systems::pingpong::run(7)?);
/// # Ok::<(), chronoloop::executor::ExecutorError>(())
/// ```
pub fn run(seed: u64) -> Result<Recording, ExecutorError> {
    let mut executor = Executor::new();
    let recorder = Recorder::new();
    let mut seeds = SeededRng::from_seed(seed);
    let request = Channel::new();
    let reply = Channel::new();

    // Each task draws from its own generator, seeded from the run's. Sharing one generator would
    // make a task's delays depend on how often the other task drew.
    let handle = executor.handle();
    let history = recorder.clone();
    let mut rng = SeededRng::from_seed(seeds.next_u64());
    let outgoing = request.clone();
    let awaited = reply.clone();
    executor.spawn(async move {
        handle.sleep(rng.duration_in(MIN_DELAY..=MAX_DELAY)).await;
        outgoing.send("ping");
        history.record(&handle, "ping sent");
        let answer = awaited.recv().await;
        history.record(&handle, format!("{answer} received"));
    });

    let handle = executor.handle();
    let history = recorder.clone();
    let mut rng = SeededRng::from_seed(seeds.next_u64());
    executor.spawn(async move {
        let message = request.recv().await;
        history.record(&handle, format!("{message} received"));
        handle.sleep(rng.duration_in(MIN_DELAY..=MAX_DELAY)).await;
        reply.send("pong");
        history.record(&handle, "pong sent");
    });

    executor.run()?;
    Ok(Recording::new(seed, recorder.entries()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs the exchange, failing the test rather than returning an error no case expects.
    fn history(seed: u64) -> Recording {
        run(seed).unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"))
    }

    /// The messages a recording holds, in order.
    fn messages(recording: &Recording) -> Vec<&str> {
        recording
            .entries()
            .iter()
            .map(|entry| entry.message.as_str())
            .collect()
    }

    #[test]
    fn the_exchange_records_one_round_trip_in_causal_order() {
        for seed in [0, 1, 7, u64::MAX] {
            assert_eq!(
                messages(&history(seed)),
                vec!["ping sent", "ping received", "pong sent", "pong received"],
                "seed {seed}"
            );
        }
    }

    #[test]
    fn the_recording_names_the_seed_that_produced_it() {
        for seed in [0, 1, 7, u64::MAX] {
            assert_eq!(history(seed).seed(), seed);
        }
    }

    #[test]
    fn a_message_takes_time_to_arrive() {
        // Both halves of the exchange wait on the virtual clock, so the reply lands strictly later
        // than the message that asked for it. A system whose whole history sat at one instant would
        // not be exercising the clock at all.
        for seed in [0, 1, 7, u64::MAX] {
            let recording = history(seed);
            let instants: Vec<u64> = recording
                .entries()
                .iter()
                .map(|entry| entry.at.as_nanos())
                .collect();
            assert!(
                instants[0] > 0,
                "seed {seed}: the ping waited before it went"
            );
            assert_eq!(
                instants[0], instants[1],
                "seed {seed}: the ping arrives at once"
            );
            assert!(
                instants[2] > instants[1],
                "seed {seed}: the pong waited before it went"
            );
            assert_eq!(
                instants[2], instants[3],
                "seed {seed}: the pong arrives at once"
            );
        }
    }

    #[test]
    fn different_seeds_produce_different_histories() {
        struct Case {
            name: &'static str,
            left: u64,
            right: u64,
        }
        let cases = [
            Case {
                name: "adjacent seeds",
                left: 0,
                right: 1,
            },
            Case {
                name: "small seeds",
                left: 7,
                right: 8,
            },
            Case {
                name: "either end of the range",
                left: 42,
                right: u64::MAX,
            },
        ];

        // Without this, a change that stopped the seed reaching behaviour — the delays replaced by
        // constants, a generator built from a fixed value — would leave every other case passing.
        for case in cases {
            assert_ne!(
                history(case.left).entries(),
                history(case.right).entries(),
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn a_wait_for_a_message_registers_a_waker_and_takes_it_back() {
        struct Case {
            name: &'static str,
            /// Whether a message is left before the wait is abandoned.
            deliver: bool,
        }
        let cases = [
            Case {
                name: "abandoned before the message arrives",
                deliver: false,
            },
            Case {
                name: "abandoned after the message arrives",
                deliver: true,
            },
        ];

        // A waker left behind in the mailbox would mark a task ready for a message nothing is
        // waiting for — the same family of defect as a timer that outlives the wait which armed it.
        for case in cases {
            let channel = Channel::new();
            let mut wait = channel.recv();
            let mut cx = Context::from_waker(Waker::noop());
            assert!(
                Pin::new(&mut wait).poll(&mut cx).is_pending(),
                "{}: nothing has been sent yet",
                case.name
            );
            assert!(
                channel.0.borrow().waker.is_some(),
                "{}: the wait registered",
                case.name
            );

            if case.deliver {
                channel.send("ping");
                assert!(
                    Pin::new(&mut wait).poll(&mut cx).is_ready(),
                    "{}: the message was delivered",
                    case.name
                );
            }
            drop(wait);
            assert!(
                channel.0.borrow().waker.is_none(),
                "{}: the wait took its waker back",
                case.name
            );
        }
    }
}
