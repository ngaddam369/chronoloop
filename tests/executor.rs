//! Two tasks taking turns over the virtual clock, the shape a simulated system takes.

use core::cell::RefCell;
use std::rc::Rc;

use chronoloop::clock::VirtualTime;
use chronoloop::executor::Executor;

const SECOND: u64 = 1_000_000_000;
const HOUR: u64 = 3600 * SECOND;
const ROUNDS: u32 = 3;

type Log = Rc<RefCell<Vec<(u64, String)>>>;

/// Runs the exchange and returns the log plus the instant the run ended at.
fn exchange() -> (Vec<(u64, String)>, u64) {
    let mut executor = Executor::new();
    let log: Log = Log::default();
    let mailbox: Rc<RefCell<Option<u32>>> = Rc::default();

    let sender = executor.handle();
    let sender_log = Rc::clone(&log);
    let sender_mailbox = Rc::clone(&mailbox);
    executor.spawn(async move {
        for round in 0..ROUNDS {
            sender
                .sleep_until(VirtualTime::from_nanos(u64::from(round) * HOUR + SECOND))
                .await;
            *sender_mailbox.borrow_mut() = Some(round);
            sender_log
                .borrow_mut()
                .push((sender.now().as_nanos(), format!("sent {round}")));
        }
    });

    let receiver = executor.handle();
    let receiver_log = Rc::clone(&log);
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
            receiver_log
                .borrow_mut()
                .push((receiver.now().as_nanos(), entry));
        }
    });

    executor
        .run()
        .unwrap_or_else(|e| panic!("run did not finish: {e}"));
    let ended_at = executor.handle().now().as_nanos();
    let entries = log.borrow().clone();
    (entries, ended_at)
}

#[test]
fn two_tasks_exchange_messages_over_hours_of_virtual_time() {
    let (log, ended_at) = exchange();

    let want: Vec<(u64, String)> = vec![
        (SECOND, "sent 0".to_owned()),
        (2 * SECOND, "received 0".to_owned()),
        (HOUR + SECOND, "sent 1".to_owned()),
        (HOUR + 2 * SECOND, "received 1".to_owned()),
        (2 * HOUR + SECOND, "sent 2".to_owned()),
        (2 * HOUR + 2 * SECOND, "received 2".to_owned()),
    ];
    assert_eq!(log, want);
    assert_eq!(ended_at, 2 * HOUR + 2 * SECOND);
}

#[test]
fn the_same_run_replays_identically() {
    assert_eq!(exchange(), exchange());
}
