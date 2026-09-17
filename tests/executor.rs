//! Two tasks taking turns over the virtual clock, the shape a simulated system takes.

use core::cell::RefCell;
use std::rc::Rc;

use chronoloop::clock::VirtualTime;
use chronoloop::executor::Executor;
use chronoloop::history::{Entry, Recorder};

mod common;

use common::{entry, finish};

const SECOND: u64 = 1_000_000_000;
const HOUR: u64 = 3600 * SECOND;
const ROUNDS: u32 = 3;

/// Runs the exchange and returns the history plus the instant the run ended at.
fn exchange() -> (Vec<Entry>, u64) {
    let mut executor = Executor::new();
    let recorder = Recorder::new();
    let mailbox: Rc<RefCell<Option<u32>>> = Rc::default();

    let sender = executor.handle();
    let sender_history = recorder.clone();
    let sender_mailbox = Rc::clone(&mailbox);
    executor.spawn(async move {
        for round in 0..ROUNDS {
            sender
                .sleep_until(VirtualTime::from_nanos(u64::from(round) * HOUR + SECOND))
                .await;
            *sender_mailbox.borrow_mut() = Some(round);
            sender_history.record(&sender, format!("sent {round}"));
        }
    });

    let receiver = executor.handle();
    let receiver_history = recorder.clone();
    let receiver_mailbox = Rc::clone(&mailbox);
    executor.spawn(async move {
        for round in 0..ROUNDS {
            receiver
                .sleep_until(VirtualTime::from_nanos(
                    u64::from(round) * HOUR + 2 * SECOND,
                ))
                .await;
            let entry = match receiver_mailbox.borrow_mut().take() {
                Some(message) => format!("received {message}"),
                None => "received nothing".to_owned(),
            };
            receiver_history.record(&receiver, entry);
        }
    });

    let ended_at = finish(&mut executor);
    (recorder.entries(), ended_at)
}

#[test]
fn two_tasks_exchange_messages_over_hours_of_virtual_time() {
    let (log, ended_at) = exchange();

    let want: Vec<Entry> = vec![
        entry(SECOND, "sent 0"),
        entry(2 * SECOND, "received 0"),
        entry(HOUR + SECOND, "sent 1"),
        entry(HOUR + 2 * SECOND, "received 1"),
        entry(2 * HOUR + SECOND, "sent 2"),
        entry(2 * HOUR + 2 * SECOND, "received 2"),
    ];
    assert_eq!(log, want);
    assert_eq!(ended_at, 2 * HOUR + 2 * SECOND);
}
