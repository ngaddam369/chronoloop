//! A schedule of faults as a thing that survives its run.
//!
//! This is what a repro artifact will be made of: the conditions a failing run met, small enough to
//! keep in a file and exact enough that reading it back puts a later run through the same trouble.
//! So the check that matters here is not that faults work — the network's own tests cover that — but
//! that a schedule written down and read back is still the same schedule, judged by what a run does
//! under it rather than by the value comparing equal to itself.

use core::time::Duration;

use chronoloop::clock::{Clock, VirtualTime};
use chronoloop::executor::Executor;
use chronoloop::fault::{Fault, FaultSchedule, Window};
use chronoloop::history::{Entry, Recorder};
use chronoloop::net::{Network, NodeId, Odds, VirtualNetwork};
use chronoloop::rng::{Rng, SeededRng};

const SECOND: u64 = 1_000_000_000;

/// How many beats the sender sends.
const BEATS: u64 = 5;

/// How long it leaves between them.
const BEAT: Duration = Duration::from_secs(1);

/// How long the receiver waits for a beat before deciding there are no more coming.
const PATIENCE: Duration = Duration::from_secs(5);

/// The seed every run here uses. Nothing in these tests is about which seed it is.
const SEED: u64 = 1;

/// Runs a sender beating at one node and a receiver recording what arrives at the other, under
/// whatever faults `trouble` asks for once the two have addresses to be named by.
///
/// Returns what the receiver recorded and the schedule the run was given.
fn beats_under(
    trouble: impl FnOnce(NodeId, NodeId) -> FaultSchedule,
) -> (Vec<Entry>, FaultSchedule) {
    let mut executor = Executor::new();
    let recorder = Recorder::new();
    let mut seeds = SeededRng::from_seed(SEED);

    let network: VirtualNetwork<u64, SeededRng> =
        VirtualNetwork::new(executor.handle(), SeededRng::from_seed(seeds.next_u64()));
    let sender = network.add_node();
    let receiver = network.add_node();
    let schedule = trouble(sender.id(), receiver.id());
    network.set_faults(schedule.clone());

    let clock = executor.handle();
    let to = receiver.id();
    executor.spawn(async move {
        for beat in 1..=BEATS {
            sender.send(to, beat);
            clock.sleep(BEAT).await;
        }
    });

    let clock = executor.handle();
    let history = recorder.clone();
    executor.spawn(async move {
        while let Ok(delivery) = clock.timeout(PATIENCE, receiver.recv()).await {
            history.record(&clock, format!("beat {} arrived", delivery.message()));
        }
    });

    executor.run().expect("the run finishes");
    (
        recorder.finish().expect("every message is one line"),
        schedule,
    )
}

/// No faults at all.
fn untroubled(_: NodeId, _: NodeId) -> FaultSchedule {
    FaultSchedule::default()
}

/// An outage covering the third and fourth beats, which go out at two and three seconds.
fn outage(from: NodeId, to: NodeId) -> FaultSchedule {
    let during = Window::new(
        VirtualTime::from_nanos(3 * SECOND / 2),
        VirtualTime::from_nanos(7 * SECOND / 2),
    )
    .expect("an outage that ends after it began");
    FaultSchedule::new(vec![Fault::Partition { from, to, during }])
}

/// Which beats a run recorded, in the order they arrived.
fn arrived(log: &[Entry]) -> Vec<&str> {
    log.iter().map(Entry::message).collect()
}

#[test]
fn a_partition_eats_the_beats_it_covers_and_no_others() {
    let (untroubled_log, _) = beats_under(untroubled);
    let (outage_log, _) = beats_under(outage);

    assert_eq!(
        arrived(&untroubled_log),
        [
            "beat 1 arrived",
            "beat 2 arrived",
            "beat 3 arrived",
            "beat 4 arrived",
            "beat 5 arrived",
        ],
        "with nothing in the way every beat lands"
    );
    // The outage opens at a second and a half and closes at three and a half, so it swallows the
    // beats that went out at two and three seconds, and neither the ones before nor the one after.
    assert_eq!(
        arrived(&outage_log),
        ["beat 1 arrived", "beat 2 arrived", "beat 5 arrived"]
    );
}

#[test]
fn a_schedule_read_back_from_its_written_form_puts_a_run_through_the_same_trouble() {
    // The text is pinned rather than round-tripped through the value that produced it: a schedule
    // that only ever agreed with itself in one process would prove nothing about what a file holds.
    let recorded = "\
chronoloop faults
partition on node 0 -> node 1 from 1.500000000s until 3.500000000s
";

    let (written_run, schedule) = beats_under(outage);
    assert_eq!(schedule.to_string(), recorded);

    let read_back: FaultSchedule = recorded.parse().expect("the recorded form is a schedule");
    assert_eq!(read_back, schedule);

    let (read_back_run, _) = beats_under(|_, _| read_back.clone());
    assert_eq!(
        read_back_run, written_run,
        "the schedule that came back from the text is the schedule that wrote it"
    );
}

#[test]
fn a_fault_that_loses_nothing_leaves_the_run_exactly_as_it_was() {
    // A fault in force still draws against its odds, exactly as the link would have. If it drew
    // only when it had something to decide, a run carrying a harmless fault would consume a
    // different amount of the seed from one carrying none, and every later message would land
    // somewhere else for reasons that had nothing to do with the fault.
    let (untroubled_log, _) = beats_under(untroubled);
    let (harmless_log, _) = beats_under(|from, to| {
        FaultSchedule::new(vec![Fault::Loss {
            from,
            to,
            during: Window::forever_from(VirtualTime::ZERO),
            odds: Odds::never(),
        }])
    });

    assert_eq!(harmless_log, untroubled_log);
}

#[test]
fn dropping_the_only_fault_leaves_the_run_with_nothing_to_meet() {
    // The step a shrinker takes over and over: take a fault out and see whether what happened still
    // happens. With the one fault gone the run has to be the untroubled one again.
    let (_, schedule) = beats_under(outage);
    assert_eq!(schedule.len(), 1);

    let shrunk = FaultSchedule::new(schedule.faults()[1..].to_vec());
    assert!(shrunk.is_empty());

    let (shrunk_log, _) = beats_under(|_, _| shrunk.clone());
    let (untroubled_log, _) = beats_under(untroubled);
    assert_eq!(shrunk_log, untroubled_log);
}
