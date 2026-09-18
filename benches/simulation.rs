//! How fast a fleet of controllers reconciles its way through simulated time.
//!
//! Every controller here does what a provisioning operator does: wake on a resync interval, arm a
//! deadline for the reconcile it is about to attempt, do some work, and call the deadline off when
//! the work lands first. Nearly every reconcile therefore arms a timer it never lets fire, which is
//! the arm-and-cancel mix the event queue has to be good at — a workload of pure sleeps would not
//! touch it.
//!
//! This is the one place in the crate allowed to read a wall clock: it measures the engine from
//! outside rather than taking part in a simulation. Nothing here is asserted and nothing here runs
//! in CI. Run it with `make bench`.

use core::cell::Cell;
use core::future::Future;
use core::task::Poll;
use core::time::Duration;
use std::rc::Rc;
use std::time::Instant;

use chronoloop::clock::Clock;
use chronoloop::executor::{Executor, Handle};
use chronoloop::rng::{Rng, SeededRng};

/// The seed every figure this bench prints is reproducible from.
const SEED: u64 = 20_260_917;
/// Fleet sizes to measure, in controllers.
const FLEETS: [u64; 3] = [100, 1_000, 10_000];
/// Timed runs per fleet size. The fastest is reported: the slower ones differ by what else the
/// machine was doing, which is not a property of the engine.
const REPEATS: usize = 3;

const SIMULATED_DAYS: u64 = 7;
const NANOS_PER_SEC: u64 = 1_000_000_000;
const HORIZON_NANOS: u64 = SIMULATED_DAYS * 24 * 3600 * NANOS_PER_SEC;

/// How often a controller resyncs, before jitter.
const RESYNC: Duration = Duration::from_secs(600);
/// Spread over the resync interval, so the fleet does not move in lockstep.
const JITTER_MILLIS: u64 = 30_000;
/// How long a reconcile is given before its deadline fires.
const DEADLINE: Duration = Duration::from_secs(10);
/// How long a reconcile usually takes.
const WORK: core::ops::RangeInclusive<Duration> = Duration::from_millis(1)..=Duration::from_secs(2);
/// One reconcile in this many overruns its deadline instead.
const OVERRUN_ODDS: u32 = 50;

/// What one fleet run did, counted from inside the simulation.
#[derive(Default)]
struct Counters {
    reconciles: Cell<u64>,
    overruns: Cell<u64>,
    timers: Cell<u64>,
}

impl Counters {
    fn bump(counter: &Cell<u64>, by: u64) {
        counter.set(counter.get().saturating_add(by));
    }
}

/// One controller: resync, arm a deadline, reconcile, call the deadline off.
async fn controller(handle: Handle, mut rng: SeededRng, counters: Rc<Counters>) {
    while handle.now().as_nanos() < HORIZON_NANOS {
        let jitter = Duration::from_millis(rng.range(0..=JITTER_MILLIS));
        handle.sleep(RESYNC + jitter).await;

        let mut deadline = Box::pin(handle.sleep(DEADLINE));
        // Arm the deadline without waiting on it, the way a race between work and timeout does.
        let armed = core::future::poll_fn(|cx| Poll::Ready(deadline.as_mut().poll(cx))).await;
        assert!(
            armed.is_pending(),
            "the deadline cannot have passed already"
        );

        let work = if rng.chance(1, OVERRUN_ODDS) {
            rng.duration_in(DEADLINE..=(DEADLINE * 2))
        } else {
            rng.duration_in(WORK)
        };
        let mut working = Box::pin(handle.sleep(work));
        Counters::bump(&counters.timers, 3);

        if work < DEADLINE {
            // The usual path: the work lands and the deadline is called off unfired.
            working.as_mut().await;
            drop(deadline);
            Counters::bump(&counters.reconciles, 1);
        } else {
            // The reconcile overran, so the deadline fires and the work is abandoned instead.
            deadline.as_mut().await;
            drop(working);
            Counters::bump(&counters.overruns, 1);
        }
    }
}

/// What one fleet size cost to simulate.
struct Sample {
    fleet: u64,
    counters: Rc<Counters>,
    elapsed: Duration,
    ended_at: u64,
}

/// Runs `fleet` controllers to the horizon and times the run itself, not the setting up.
fn run_fleet(fleet: u64) -> Sample {
    let mut executor = Executor::new();
    let counters = Rc::new(Counters::default());
    let mut seeds = SeededRng::from_seed(SEED);

    for _ in 0..fleet {
        let handle = executor.handle();
        let rng = SeededRng::from_seed(seeds.next_u64());
        executor.spawn(controller(handle, rng, Rc::clone(&counters)));
    }

    let started = Instant::now();
    executor
        .run()
        .unwrap_or_else(|e| panic!("the fleet did not finish: {e}"));
    let elapsed = started.elapsed();

    Sample {
        fleet,
        counters,
        elapsed,
        ended_at: executor.handle().now().as_nanos(),
    }
}

/// Divides in fixed point, so no floating-point cast sits between a measurement and its report.
fn per_second(amount: u64, elapsed: Duration) -> u128 {
    let micros = elapsed.as_micros().max(1);
    u128::from(amount) * 1_000_000 / micros
}

fn report(sample: &Sample) {
    let Sample {
        fleet,
        counters,
        elapsed,
        ended_at,
    } = sample;
    let reconciles = counters.reconciles.get();
    let timers = counters.timers.get();
    let millis = elapsed.as_millis().max(1);
    let simulated_days = u128::from(ended_at / NANOS_PER_SEC / 86_400).max(1);

    println!(
        "{fleet:>8}  {reconciles:>12}  {timers:>12}  {overruns:>9}  {millis:>9}  \
         {timers_per_sec:>13}  {per_day:>11}",
        overruns = counters.overruns.get(),
        timers_per_sec = per_second(timers, *elapsed),
        per_day = millis / simulated_days,
    );
}

fn main() {
    println!(
        "chronoloop — {SIMULATED_DAYS} simulated days per fleet, seed {SEED}, \
         fastest of {REPEATS} runs"
    );
    println!(
        "{:>8}  {:>12}  {:>12}  {:>9}  {:>9}  {:>13}  {:>11}",
        "fleet", "reconciles", "timers", "overruns", "wall ms", "timers/sec", "ms/sim day"
    );

    for fleet in FLEETS {
        let mut fastest = run_fleet(fleet);
        for _ in 1..REPEATS {
            let sample = run_fleet(fleet);
            if sample.elapsed < fastest.elapsed {
                fastest = sample;
            }
        }
        assert!(
            fastest.ended_at >= HORIZON_NANOS,
            "every controller must reconcile up to the horizon"
        );
        report(&fastest);
    }
}
