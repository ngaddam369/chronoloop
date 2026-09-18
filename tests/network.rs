//! Systems talking over the simulated wire, written against the capabilities and nothing else.
//!
//! None of the code under test here names an executor, a clock or a generator: it asks for a
//! `Clock` and a `Network`, which is the whole point of those being traits. What the wire then does
//! to it — the order answers come back in, whether a request survives at all — is the simulation's
//! to decide and the seed's to fix.

use core::ops::RangeInclusive;
use core::time::Duration;

use chronoloop::clock::Clock;
use chronoloop::executor::Executor;
use chronoloop::history::{Entry, Recorder};
use chronoloop::net::{Link, Network, NodeId, Odds, VirtualNetwork};
use chronoloop::rng::{Rng, SeededRng};

const SECOND: u64 = 1_000_000_000;

/// How many followers the coordinator asks.
const FOLLOWERS: usize = 2;

/// How long a node takes to think about a request before answering it.
const THINKING: RangeInclusive<Duration> = Duration::from_millis(50)..=Duration::from_millis(500);

/// A wire slow and uneven enough that the order answers come back in is worth asking about.
fn wide_link() -> Link {
    Link::new(Duration::from_millis(10)..=Duration::from_secs(2))
}

/// Asks every peer once, then records each answer in the order it lands.
async fn coordinator<C, N>(clock: &C, node: &N, peers: &[NodeId], history: &Recorder)
where
    C: Clock,
    N: Network<Message = &'static str>,
{
    for &peer in peers {
        node.send(peer, "check");
    }
    for _ in peers {
        let answer = node.recv().await;
        history.record(
            clock,
            format!("{} said {}", answer.sender(), answer.message()),
        );
    }
}

/// Waits to be asked, thinks about it, and answers whoever asked.
async fn follower<C, R, N>(clock: &C, rng: &mut R, node: &N)
where
    C: Clock,
    R: Rng,
    N: Network<Message = &'static str>,
{
    let asked = node.recv().await;
    clock.sleep(rng.duration_in(THINKING)).await;
    node.send(asked.sender(), "ok");
}

/// Runs one coordinator against two followers over a wide link, and returns what it recorded.
fn round_of_checks(seed: u64) -> Vec<Entry> {
    let mut executor = Executor::new();
    let recorder = Recorder::new();
    let mut seeds = SeededRng::from_seed(seed);

    let network: VirtualNetwork<&'static str, SeededRng> =
        VirtualNetwork::new(executor.handle(), SeededRng::from_seed(seeds.next_u64()))
            .with_default_link(wide_link());

    let lead = network.add_node();
    let mut peers = Vec::with_capacity(FOLLOWERS);
    for _ in 0..FOLLOWERS {
        let node = network.add_node();
        peers.push(node.id());
        let clock = executor.handle();
        let mut rng = SeededRng::from_seed(seeds.next_u64());
        executor.spawn(async move {
            follower(&clock, &mut rng, &node).await;
        });
    }

    let clock = executor.handle();
    let history = recorder.clone();
    executor.spawn(async move {
        coordinator(&clock, &lead, &peers, &history).await;
    });

    executor
        .run()
        .unwrap_or_else(|e| panic!("seed {seed}: run did not finish: {e}"));
    recorder.finish().expect("every message is one line")
}

#[test]
fn a_round_of_checks_is_a_function_of_its_seed() {
    struct Case {
        name: &'static str,
        left: u64,
        right: u64,
    }
    let cases = [
        Case {
            name: "adjacent seeds",
            left: 1,
            right: 2,
        },
        Case {
            name: "either end of the range",
            left: 7,
            right: u64::MAX,
        },
    ];

    for case in cases {
        assert_eq!(
            round_of_checks(case.left),
            round_of_checks(case.left),
            "{}: one seed, one history",
            case.name
        );
        // Without this, a wire that had quietly stopped drawing at all — every delay the same, every
        // answer back in the order it was asked for — would leave the case above passing.
        assert_ne!(
            round_of_checks(case.left),
            round_of_checks(case.right),
            "{}: different seeds must meet different wires",
            case.name
        );
    }
}

#[test]
fn every_answer_comes_back_though_the_wire_decides_the_order() {
    let log = round_of_checks(1);
    assert_eq!(log.len(), FOLLOWERS, "each follower answered once");

    let mut who: Vec<String> = log.iter().map(|entry| entry.message().to_owned()).collect();
    who.sort();
    who.dedup();
    assert_eq!(
        who.len(),
        FOLLOWERS,
        "the answers came from different nodes"
    );

    let instants: Vec<u64> = log.iter().map(|entry| entry.at().as_nanos()).collect();
    assert!(
        instants.windows(2).all(|pair| pair[0] <= pair[1]),
        "virtual time never goes backwards: {instants:?}"
    );
}

/// Asks for an acknowledgement, trying again whenever one does not come back in time.
///
/// The shape of every control loop that talks to something it cannot rely on: send, wait a bounded
/// while, and try again rather than wait forever.
async fn client<C, N>(
    clock: &C,
    node: &N,
    server: NodeId,
    patience: Duration,
    attempts: u32,
    history: &Recorder,
) where
    C: Clock,
    N: Network<Message = &'static str>,
{
    for attempt in 1..=attempts {
        node.send(server, "request");
        match clock.timeout(patience, node.recv()).await {
            Ok(_) => {
                history.record(clock, format!("acknowledged on attempt {attempt}"));
                return;
            }
            Err(_) => history.record(clock, format!("attempt {attempt} went unanswered")),
        }
    }
    history.record(clock, "gave up");
}

/// Answers everything that arrives, and stops once the wire has been quiet for `patience`.
async fn server<C, N>(clock: &C, node: &N, patience: Duration)
where
    C: Clock,
    N: Network<Message = &'static str>,
{
    while let Ok(asked) = clock.timeout(patience, node.recv()).await {
        node.send(asked.sender(), "ack");
    }
}

/// What a client recorded talking to a server over a link the test sets up through `wire`.
fn request_with_retries(
    seed: u64,
    loss: Odds,
    wire: impl FnOnce(&VirtualNetwork<&'static str, SeededRng>, NodeId, NodeId, &mut Executor),
) -> Vec<Entry> {
    const PATIENCE: Duration = Duration::from_secs(3);
    const ATTEMPTS: u32 = 8;

    let mut executor = Executor::new();
    let recorder = Recorder::new();
    let mut seeds = SeededRng::from_seed(seed);

    let network: VirtualNetwork<&'static str, SeededRng> =
        VirtualNetwork::new(executor.handle(), SeededRng::from_seed(seeds.next_u64()));
    let caller = network.add_node();
    let answerer = network.add_node();
    let (from, to) = (caller.id(), answerer.id());

    // Only the way out is unreliable. A link is one direction, so saying that is saying exactly
    // that — the answers come back over a link of their own, which this leaves alone.
    network.set_link(from, to, Link::default().with_loss(loss));
    wire(&network, from, to, &mut executor);

    let clock = executor.handle();
    let history = recorder.clone();
    executor.spawn(async move {
        client(&clock, &caller, to, PATIENCE, ATTEMPTS, &history).await;
    });

    let clock = executor.handle();
    executor.spawn(async move {
        server(&clock, &answerer, PATIENCE * ATTEMPTS).await;
    });

    executor
        .run()
        .unwrap_or_else(|e| panic!("seed {seed}: run did not finish: {e}"));
    recorder.finish().expect("every message is one line")
}

#[test]
fn a_request_the_wire_swallows_is_tried_again_until_it_lands() {
    // Seed 3 is a seed on which the link does lose the first request: a seed on which it happened
    // to lose none would pass this file without the retry ever running.
    let loss = Odds::new(1, 2).expect("one in two is ordinary odds");
    let log = request_with_retries(3, loss, |_, _, _, _| {});

    let said: Vec<&str> = log.iter().map(Entry::message).collect();
    assert!(
        said.len() > 1,
        "the wire swallowed something, so it took more than one attempt: {said:?}"
    );
    assert!(
        said[..said.len() - 1]
            .iter()
            .all(|line| line.ends_with("went unanswered")),
        "every attempt but the last went unanswered: {said:?}"
    );
    assert!(
        said.last()
            .is_some_and(|line| line.starts_with("acknowledged")),
        "the request landed in the end: {said:?}"
    );
}

#[test]
fn a_partition_stalls_an_exchange_and_a_heal_resumes_it() {
    const HEALS_AT: u64 = 7;

    // Nothing is lost at random here: the link carries perfectly, and the only thing stopping the
    // request is the partition. What the client does about it is retry until the wire comes back.
    let log = request_with_retries(1, Odds::never(), |network, from, to, executor| {
        network.partition(from, to);
        let clock = executor.handle();
        let healing = network.clone();
        executor.spawn(async move {
            clock.sleep(Duration::from_secs(HEALS_AT)).await;
            healing.heal(from, to);
        });
    });

    let landed = log
        .last()
        .expect("the client recorded something either way");
    assert!(
        landed.message().starts_with("acknowledged"),
        "the request got through once the link came back: {}",
        landed.message()
    );
    assert!(
        landed.at().as_nanos() > HEALS_AT * SECOND,
        "and not before then: {landed}"
    );
    assert!(
        log.len() > 1,
        "the attempts made during the outage are in the history too: {log:?}"
    );
}
