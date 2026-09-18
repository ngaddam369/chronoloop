//! Two tasks exchanging one message over virtual time.
//!
//! The smallest system that is still a whole simulation: one task waits, sends a ping and waits for
//! the reply; the other waits for the ping, waits again, and sends a pong back. Every delay is drawn
//! from the run's seed, so the four entries the exchange records are a function of that seed and of
//! nothing else.
//!
//! The two nodes talk over the simulated network, on an ordinary link that loses nothing — with no
//! retry between them, a lost ping would leave both halves waiting for something that is never
//! coming. What a lossy wire does to a system that does retry belongs with the systems built to
//! survive it.
//!
//! It is also the first thing here that wakes a task for a reason other than a timer: the answering
//! node waits on the message *arriving* rather than on an instant both sides agreed in advance.

use core::time::Duration;

use crate::clock::Clock;
use crate::executor::Executor;
use crate::history::{Recorder, Recording};
use crate::net::{Network, NodeId, VirtualNetwork};
use crate::rng::{Rng, SeededRng};
use crate::systems::RunError;

/// The shortest a node thinks before it says anything.
const MIN_DELAY: Duration = Duration::from_millis(1);

/// The longest a node thinks before it says anything.
const MAX_DELAY: Duration = Duration::from_secs(2);

/// Opens the exchange: wait, send, then wait for the answer.
///
/// Written against the capabilities alone — it can ask the simulation for the time and for a delay,
/// and it has no way to reach a real clock or the machine's entropy.
async fn opening<C, R, N>(clock: &C, rng: &mut R, history: &Recorder, node: &N, peer: NodeId)
where
    C: Clock,
    R: Rng,
    N: Network<Message = &'static str>,
{
    clock.sleep(rng.duration_in(MIN_DELAY..=MAX_DELAY)).await;
    node.send(peer, "ping");
    history.record(clock, "ping sent");
    let answer = node.recv().await;
    history.record(clock, format!("{} received", answer.message()));
}

/// Answers the exchange: wait for the message, wait again, then reply to whoever asked.
async fn answering<C, R, N>(clock: &C, rng: &mut R, history: &Recorder, node: &N)
where
    C: Clock,
    R: Rng,
    N: Network<Message = &'static str>,
{
    let asked = node.recv().await;
    history.record(clock, format!("{} received", asked.message()));
    clock.sleep(rng.duration_in(MIN_DELAY..=MAX_DELAY)).await;
    node.send(asked.sender(), "pong");
    history.record(clock, "pong sent");
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
/// # Ok::<(), chronoloop::systems::RunError>(())
/// ```
pub fn run(seed: u64) -> Result<Recording, RunError> {
    let mut executor = Executor::new();
    let recorder = Recorder::new();
    let mut seeds = SeededRng::from_seed(seed);

    // The wire and each task draw from generators of their own, all seeded from the run's. Sharing
    // one would make a node's delays depend on how often its neighbour — or the network — drew.
    let network: VirtualNetwork<&'static str, SeededRng> =
        VirtualNetwork::new(executor.handle(), SeededRng::from_seed(seeds.next_u64()));
    let opener = network.add_node();
    let answerer = network.add_node();
    let peer = answerer.id();

    let clock = executor.handle();
    let history = recorder.clone();
    let mut rng = SeededRng::from_seed(seeds.next_u64());
    executor.spawn(async move {
        opening(&clock, &mut rng, &history, &opener, peer).await;
    });

    let clock = executor.handle();
    let history = recorder.clone();
    let mut rng = SeededRng::from_seed(seeds.next_u64());
    executor.spawn(async move {
        answering(&clock, &mut rng, &history, &answerer).await;
    });

    executor.run()?;
    Ok(Recording::new(seed, recorder.finish()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::Entry;

    /// Runs the exchange, failing the test rather than returning an error no case expects.
    fn history(seed: u64) -> Recording {
        run(seed).unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"))
    }

    /// The messages a recording holds, in order.
    fn messages(recording: &Recording) -> Vec<&str> {
        recording.entries().iter().map(Entry::message).collect()
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
        // Every step of the exchange takes time: each node thinks before it speaks, and each
        // message then spends a while on the wire. A history whose four entries sat at one instant
        // would be exercising neither the clock nor the network.
        for seed in [0, 1, 7, u64::MAX] {
            let recording = history(seed);
            let instants: Vec<u64> = recording
                .entries()
                .iter()
                .map(|entry| entry.at().as_nanos())
                .collect();
            assert!(
                instants[0] > 0,
                "seed {seed}: the ping waited before it went"
            );
            assert!(
                instants[1] > instants[0],
                "seed {seed}: the ping took time on the wire"
            );
            assert!(
                instants[2] > instants[1],
                "seed {seed}: the pong waited before it went"
            );
            assert!(
                instants[3] > instants[2],
                "seed {seed}: the pong took time on the wire too"
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
}
