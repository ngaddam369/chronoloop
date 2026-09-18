//! The simulated network: what a message goes through between leaving one node and arriving at
//! another.
//!
//! A link takes time, loses things, occasionally says them twice, and can stop carrying anything at
//! all in one direction. Every one of those is drawn from the run's seed, so the wire a system met
//! on one run is the wire it meets on the next.
//!
//! Reordering is not among the settings, because it is not a decision: each copy of a message draws
//! its own delay, so one sent later can land first whenever the wire is uneven. That is also what
//! makes a duplicate interesting — the copy is not a shadow of the original but a second message
//! with its own time of arrival.
//!
//! Nothing here runs a delivery task. A send works out when its message lands and files it in the
//! destination's inbox; a wait takes the earliest message that has already landed, or arms a timer
//! for the earliest that has not. A run therefore costs what its messages cost, and a message
//! nobody ever waits for costs nothing at all.

use core::fmt;
use core::future::Future;
use core::ops::RangeInclusive;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use crate::clock::{Clock, VirtualTime};
use crate::event::EventId;
use crate::executor::Handle;
use crate::rng::Rng;

/// The shortest an ordinary link takes, when nothing says otherwise.
const DEFAULT_MIN_LATENCY: Duration = Duration::from_millis(1);

/// The longest an ordinary link takes, when nothing says otherwise.
const DEFAULT_MAX_LATENCY: Duration = Duration::from_millis(50);

/// A node's address on the simulated network.
///
/// There is no way to write one down: [`VirtualNetwork::add_node`] is the only source of one, so a
/// message addressed to a node that was never added cannot be expressed and sending needs no way to
/// fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(u64);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node {}", self.0)
    }
}

/// Returned when odds could not mean anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum OddsError {
    /// There were no trials to happen in.
    NoTrials,
    /// More occurrences were asked for than there are trials to hold them.
    TooLikely {
        /// The occurrences asked for.
        numerator: u32,
        /// The trials they were asked to fit in.
        denominator: u32,
    },
}

impl fmt::Display for OddsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoTrials => write!(f, "odds need at least one trial to happen in"),
            Self::TooLikely {
                numerator,
                denominator,
            } => write!(
                f,
                "odds of {numerator} in {denominator} ask for more than always"
            ),
        }
    }
}

impl std::error::Error for OddsError {}

/// How likely something on a link is, as a whole-number ratio.
///
/// The ratio is integers, so no floating-point rounding sits between a seed and a decision, and it
/// is checked once here rather than every time a draw is made against it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Odds {
    numerator: u32,
    denominator: u32,
}

impl Odds {
    /// Creates odds of `numerator` in `denominator`.
    ///
    /// # Errors
    ///
    /// Returns [`OddsError`] if there are no trials, or if more occurrences were asked for than
    /// there are trials to hold them.
    pub fn new(numerator: u32, denominator: u32) -> Result<Self, OddsError> {
        if denominator == 0 {
            return Err(OddsError::NoTrials);
        }
        if numerator > denominator {
            return Err(OddsError::TooLikely {
                numerator,
                denominator,
            });
        }
        Ok(Self {
            numerator,
            denominator,
        })
    }

    /// Odds of never.
    pub const fn never() -> Self {
        Self {
            numerator: 0,
            denominator: 1,
        }
    }

    /// Odds of always.
    pub const fn always() -> Self {
        Self {
            numerator: 1,
            denominator: 1,
        }
    }

    /// Draws against these odds.
    fn draw(self, rng: &mut impl Rng) -> bool {
        rng.chance(self.numerator, self.denominator)
    }
}

impl fmt::Display for Odds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} in {}", self.numerator, self.denominator)
    }
}

/// What one direction of a link between two nodes does to the messages it carries.
///
/// A link is directional: what a node hears from its neighbour is settled separately from what that
/// neighbour hears back, since a failure that cuts only one direction is both real and the kind a
/// system most easily assumes away.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    latency: RangeInclusive<Duration>,
    loss: Odds,
    duplication: Odds,
}

impl Default for Link {
    fn default() -> Self {
        Self::new(DEFAULT_MIN_LATENCY..=DEFAULT_MAX_LATENCY)
    }
}

impl Link {
    /// Creates a link that takes somewhere within `latency` and is otherwise perfect.
    pub fn new(latency: RangeInclusive<Duration>) -> Self {
        Self {
            latency,
            loss: Odds::never(),
            duplication: Odds::never(),
        }
    }

    /// Returns this link with the odds of a message being lost set to `loss`.
    #[must_use]
    pub fn with_loss(mut self, loss: Odds) -> Self {
        self.loss = loss;
        self
    }

    /// Returns this link with the odds of a message being said twice set to `duplication`.
    #[must_use]
    pub fn with_duplication(mut self, duplication: Odds) -> Self {
        self.duplication = duplication;
        self
    }
}

/// A message that arrived, and the node that sent it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery<M> {
    sender: NodeId,
    message: M,
}

impl<M> Delivery<M> {
    /// Returns the node the message came from.
    pub fn sender(&self) -> NodeId {
        self.sender
    }

    /// Returns what arrived.
    pub fn message(&self) -> &M {
        &self.message
    }

    /// Takes what arrived, leaving the envelope behind.
    pub fn into_message(self) -> M {
        self.message
    }
}

/// What a system may ask the simulation for when it needs to talk to another node.
///
/// A system under test names this capability rather than opening a socket, so everything its
/// messages go through is the simulation's to decide and the run's to record.
pub trait Network {
    /// What the network carries.
    type Message;

    /// The wait for the next message this node receives.
    type Recv: Future<Output = Delivery<Self::Message>>;

    /// Returns this node's address, which is what it gives others to reach it by.
    fn id(&self) -> NodeId;

    /// Sends `message` to `to`.
    ///
    /// Sending cannot fail and does not wait: what the link does with the message — carry it, lose
    /// it, say it twice — is settled here, and the message arrives later or not at all.
    fn send(&self, to: NodeId, message: Self::Message);

    /// Returns a future that completes once a message has arrived for this node.
    fn recv(&self) -> Self::Recv;
}

/// When a message lands, and which one lands first if two land together.
///
/// The sequence number is the network's, not the inbox's, so every message on the wire is ordered
/// against every other one — the same rule the event queue breaks its own ties by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Arrival {
    at: VirtualTime,
    seq: u64,
}

/// Identifies one wait on an inbox, so that wait can take its registration back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct WaitId(u64);

/// One node's incoming messages, and whoever is waiting for them.
#[derive(Debug)]
struct Inbox<M> {
    in_flight: BTreeMap<Arrival, Delivery<M>>,
    waiting: BTreeMap<WaitId, Waker>,
}

impl<M> Default for Inbox<M> {
    fn default() -> Self {
        Self {
            in_flight: BTreeMap::new(),
            waiting: BTreeMap::new(),
        }
    }
}

/// Every node's inbox, and the counters that keep both of its orderings deterministic.
#[derive(Debug)]
struct Inboxes<M> {
    nodes: BTreeMap<NodeId, Inbox<M>>,
    next_node: u64,
    next_arrival: u64,
    next_wait: u64,
}

impl<M> Default for Inboxes<M> {
    fn default() -> Self {
        Self {
            nodes: BTreeMap::new(),
            next_node: 0,
            next_arrival: 0,
            next_wait: 0,
        }
    }
}

impl<M> Inboxes<M> {
    /// Adds a node and returns its address.
    ///
    /// # Panics
    ///
    /// Panics after `u64::MAX` nodes, which cannot happen in practice.
    fn add(&mut self) -> NodeId {
        let id = NodeId(self.next_node);
        // Unreachable: a simulation cannot hold u64::MAX nodes.
        self.next_node = self
            .next_node
            .checked_add(1)
            .expect("node addresses exhausted after u64::MAX nodes");
        self.nodes.insert(id, Inbox::default());
        id
    }

    /// Puts `delivery` on the wire, to land at `at`.
    ///
    /// A node that was never added holds no inbox, so a message addressed to one goes nowhere.
    ///
    /// # Panics
    ///
    /// Panics after `u64::MAX` messages, which cannot happen in practice.
    fn file(&mut self, to: NodeId, at: VirtualTime, delivery: Delivery<M>) {
        let seq = self.next_arrival;
        // Unreachable: a simulation cannot send u64::MAX messages.
        self.next_arrival = seq
            .checked_add(1)
            .expect("arrival sequence number exhausted after u64::MAX messages");
        if let Some(inbox) = self.nodes.get_mut(&to) {
            inbox.in_flight.insert(Arrival { at, seq }, delivery);
        }
    }

    /// Takes the earliest message that has already landed at `node`.
    fn take_landed(&mut self, node: NodeId, now: VirtualTime) -> Option<Delivery<M>> {
        let inbox = self.nodes.get_mut(&node)?;
        let &earliest = inbox.in_flight.keys().next()?;
        if earliest.at > now {
            return None;
        }
        inbox.in_flight.remove(&earliest)
    }

    /// Returns when the earliest message still on its way to `node` lands.
    fn next_landing(&self, node: NodeId) -> Option<VirtualTime> {
        let inbox = self.nodes.get(&node)?;
        inbox.in_flight.keys().next().map(|arrival| arrival.at)
    }

    /// Hands out the identifier of a new wait.
    ///
    /// # Panics
    ///
    /// Panics after `u64::MAX` waits, which cannot happen in practice.
    fn next_wait_id(&mut self) -> WaitId {
        let id = WaitId(self.next_wait);
        // Unreachable: a simulation cannot wait u64::MAX times.
        self.next_wait = self
            .next_wait
            .checked_add(1)
            .expect("wait identifiers exhausted after u64::MAX waits");
        id
    }

    /// Records that the wait `id` on `node` is waiting, returning whatever it replaced.
    fn register(&mut self, node: NodeId, id: WaitId, waker: Waker) -> Option<Waker> {
        self.nodes
            .get_mut(&node)
            .and_then(|inbox| inbox.waiting.insert(id, waker))
    }

    /// Takes the registration of the wait `id` on `node` back out.
    fn unregister(&mut self, node: NodeId, id: WaitId) -> Option<Waker> {
        self.nodes
            .get_mut(&node)
            .and_then(|inbox| inbox.waiting.remove(&id))
    }

    /// Takes every wait on `node` out, so the caller can wake them.
    ///
    /// They come out in the order the waits were created, which is what keeps a delivery that
    /// several tasks could take going to the same one on every run.
    fn take_waiting(&mut self, node: NodeId) -> Vec<Waker> {
        match self.nodes.get_mut(&node) {
            Some(inbox) => core::mem::take(&mut inbox.waiting).into_values().collect(),
            None => Vec::new(),
        }
    }
}

/// What a send put on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sent {
    /// Nothing left: the link is partitioned, or the message was lost.
    Nothing,
    /// One copy, landing at this instant.
    Once(VirtualTime),
    /// Two copies, each with a delay of its own, in the order they were drawn.
    Twice(VirtualTime, VirtualTime),
}

/// The links between nodes, which directions are cut, and the run's share of its seed.
#[derive(Debug)]
struct Wire<R> {
    default_link: Link,
    links: BTreeMap<(NodeId, NodeId), Link>,
    partitioned: BTreeSet<(NodeId, NodeId)>,
    rng: R,
}

impl<R: Rng> Wire<R> {
    /// Works out what becomes of a message sent from `from` to `to` at `now`.
    ///
    /// **The order of the draws is part of the contract**: loss, then the delay, then duplication,
    /// then the duplicate's own delay. A change to it changes every history the engine has ever
    /// recorded, so it is a decision rather than an implementation detail.
    fn send(&mut self, from: NodeId, to: NodeId, now: VirtualTime) -> Sent {
        if self.partitioned.contains(&(from, to)) {
            return Sent::Nothing;
        }
        let link = self
            .links
            .get(&(from, to))
            .unwrap_or(&self.default_link)
            .clone();
        if link.loss.draw(&mut self.rng) {
            return Sent::Nothing;
        }
        let first = landing(now, &link, &mut self.rng);
        if link.duplication.draw(&mut self.rng) {
            return Sent::Twice(first, landing(now, &link, &mut self.rng));
        }
        Sent::Once(first)
    }
}

/// When a message drawing its delay from `link` lands, having left at `now`.
///
/// A delay reaching beyond the end of virtual time is capped there, matching what waiting on the
/// virtual clock can represent.
fn landing(now: VirtualTime, link: &Link, rng: &mut impl Rng) -> VirtualTime {
    let delay = rng.duration_in(link.latency.clone());
    now.checked_add(delay)
        .unwrap_or(VirtualTime::from_nanos(u64::MAX))
}

/// A network of nodes and the links between them, every decision drawn from one generator.
///
/// The generator is the network's own, seeded from the run's, so what a link does to a message does
/// not shift because a task happened to draw more often than it used to.
pub struct VirtualNetwork<M, R> {
    clock: Handle,
    inboxes: Rc<RefCell<Inboxes<M>>>,
    wire: Rc<RefCell<Wire<R>>>,
}

impl<M, R> Clone for VirtualNetwork<M, R> {
    fn clone(&self) -> Self {
        Self {
            clock: self.clock.clone(),
            inboxes: Rc::clone(&self.inboxes),
            wire: Rc::clone(&self.wire),
        }
    }
}

impl<M, R: fmt::Debug> fmt::Debug for VirtualNetwork<M, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VirtualNetwork")
            .field("nodes", &self.inboxes.borrow().nodes.len())
            .finish_non_exhaustive()
    }
}

impl<M, R> VirtualNetwork<M, R> {
    /// Creates a network with no nodes, drawing every decision from `rng`.
    pub fn new(clock: Handle, rng: R) -> Self {
        Self {
            clock,
            inboxes: Rc::new(RefCell::new(Inboxes::default())),
            wire: Rc::new(RefCell::new(Wire {
                default_link: Link::default(),
                links: BTreeMap::new(),
                partitioned: BTreeSet::new(),
                rng,
            })),
        }
    }

    /// Returns this network with `link` as what any pair of nodes gets unless told otherwise.
    #[must_use]
    pub fn with_default_link(self, link: Link) -> Self {
        self.wire.borrow_mut().default_link = link;
        self
    }

    /// Adds a node and returns its endpoint.
    pub fn add_node(&self) -> Endpoint<M, R> {
        let id = self.inboxes.borrow_mut().add();
        Endpoint {
            id,
            clock: self.clock.clone(),
            inboxes: Rc::clone(&self.inboxes),
            wire: Rc::clone(&self.wire),
        }
    }

    /// Gives the direction from `from` to `to` a link of its own.
    pub fn set_link(&self, from: NodeId, to: NodeId, link: Link) {
        self.wire.borrow_mut().links.insert((from, to), link);
    }

    /// Stops the direction from `from` to `to` carrying anything.
    ///
    /// A partition is what a link does to a send, not to what has already left: a message that got
    /// away before this still lands. Cutting a link both ways takes two calls, since a failure that
    /// cuts only one direction is real and worth being able to say.
    pub fn partition(&self, from: NodeId, to: NodeId) {
        self.wire.borrow_mut().partitioned.insert((from, to));
    }

    /// Lets the direction from `from` to `to` carry again.
    pub fn heal(&self, from: NodeId, to: NodeId) {
        self.wire.borrow_mut().partitioned.remove(&(from, to));
    }
}

/// One node's view of the network, handed out by [`VirtualNetwork::add_node`].
///
/// Cloning shares the node rather than making another: every clone sends as that node and takes
/// from its one inbox.
pub struct Endpoint<M, R> {
    id: NodeId,
    clock: Handle,
    inboxes: Rc<RefCell<Inboxes<M>>>,
    wire: Rc<RefCell<Wire<R>>>,
}

impl<M, R> Clone for Endpoint<M, R> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            clock: self.clock.clone(),
            inboxes: Rc::clone(&self.inboxes),
            wire: Rc::clone(&self.wire),
        }
    }
}

impl<M, R> fmt::Debug for Endpoint<M, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Endpoint")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl<M: Clone, R: Rng> Network for Endpoint<M, R> {
    type Message = M;
    type Recv = Recv<M>;

    fn id(&self) -> NodeId {
        self.id
    }

    fn send(&self, to: NodeId, message: M) {
        let now = self.clock.now();
        // The wire's borrow ends with the statement, and the inboxes' with the one after: the two
        // are never held at once, and neither is held while a waker is woken.
        let sent = self.wire.borrow_mut().send(self.id, to, now);
        let waiting = {
            let mut inboxes = self.inboxes.borrow_mut();
            match sent {
                Sent::Nothing => return,
                Sent::Once(at) => inboxes.file(
                    to,
                    at,
                    Delivery {
                        sender: self.id,
                        message,
                    },
                ),
                Sent::Twice(first, second) => {
                    inboxes.file(
                        to,
                        first,
                        Delivery {
                            sender: self.id,
                            message: message.clone(),
                        },
                    );
                    inboxes.file(
                        to,
                        second,
                        Delivery {
                            sender: self.id,
                            message,
                        },
                    );
                }
            }
            inboxes.take_waiting(to)
        };
        // Everyone waiting is woken rather than only the earliest, because what this send put on
        // the wire may land before whatever they were each waiting for. Waking marks a task ready
        // rather than polling it, so the sender runs on to whatever it does next first.
        for waker in waiting {
            waker.wake();
        }
    }

    fn recv(&self) -> Recv<M> {
        let id = self.inboxes.borrow_mut().next_wait_id();
        Recv {
            node: self.id,
            id,
            clock: self.clock.clone(),
            inboxes: Rc::clone(&self.inboxes),
            armed: None,
        }
    }
}

/// A wait for a message, created by [`Network::recv`].
///
/// Dropping the wait takes back both of the things it left behind it: the timer aimed at the next
/// arrival, and its registration in the inbox. An abandoned wait can therefore neither carry the
/// clock to a message nobody is waiting for, nor mark a task ready for one nobody wants.
pub struct Recv<M> {
    node: NodeId,
    id: WaitId,
    clock: Handle,
    inboxes: Rc<RefCell<Inboxes<M>>>,
    /// The arrival this wait is aimed at, the timer aimed there, and the waker it was armed with.
    armed: Option<(VirtualTime, EventId, Waker)>,
}

impl<M> Recv<M> {
    /// Takes this wait's timer back out of the queue, if it still holds one.
    fn disarm(&mut self) {
        if let Some((_, id, _)) = self.armed.take() {
            self.clock.cancel_wake(id);
        }
    }
}

impl<M> Drop for Recv<M> {
    fn drop(&mut self) {
        self.disarm();
        // The borrow ends with the statement, so the waker is dropped outside it.
        let registered = self.inboxes.borrow_mut().unregister(self.node, self.id);
        drop(registered);
    }
}

impl<M> fmt::Debug for Recv<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Recv")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

impl<M> Future for Recv<M> {
    type Output = Delivery<M>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Delivery<M>> {
        let this = self.get_mut();
        let now = this.clock.now();
        // One borrow, ending with the statement. What has landed and what is still coming are both
        // the inbox's to answer: a send may have filed something since this wait last looked, and a
        // wait that answered from its own copy would be describing a wire that has moved on.
        let (landed, next) = {
            let mut inboxes = this.inboxes.borrow_mut();
            (
                inboxes.take_landed(this.node, now),
                inboxes.next_landing(this.node),
            )
        };
        if let Some(delivery) = landed {
            this.disarm();
            return Poll::Ready(delivery);
        }

        // Re-aim only when the timer is no longer pointed at the earliest arrival — a send may have
        // put something on the wire that lands sooner — or when the waker it holds would not wake
        // whoever is polling now. Otherwise leave it where it is, rather than replacing a good
        // timer with an equivalent one.
        let aimed = this
            .armed
            .as_ref()
            .is_some_and(|(at, _, waker)| Some(*at) == next && waker.will_wake(cx.waker()));
        if !aimed {
            this.disarm();
            if let Some(at) = next {
                let waker = cx.waker().clone();
                let id = this.clock.schedule_wake(at, waker.clone());
                this.armed = Some((at, id, waker));
            }
        }

        // A timer alone is not enough, and when nothing is on the wire there is no timer at all:
        // what changes that is a send, and a send wakes through the inbox.
        // The borrow ends with the statement, so the waker it replaces is dropped outside it.
        let replaced = this
            .inboxes
            .borrow_mut()
            .register(this.node, this.id, cx.waker().clone());
        drop(replaced);
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use core::future::Future;
    use core::ops::RangeInclusive;
    use core::task::Poll;
    use core::time::Duration;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::rc::Rc;

    use super::*;
    use crate::executor::Executor;
    use crate::rng::SeededRng;

    const MILLIS: u64 = 1_000_000;

    /// A generator whose draws are written down in advance, so a link's behaviour is exact.
    ///
    /// A seed would do for asserting that the network is reproducible, but not for asserting what
    /// it does: hunting for a seed that happens to duplicate a message is how a test ends up
    /// pinning the generator's output rather than the network's behaviour.
    #[derive(Debug, Default)]
    struct Scripted {
        chances: VecDeque<bool>,
        delays: VecDeque<Duration>,
    }

    impl Scripted {
        /// Writes a script: what every chance comes out as, and what every delay is.
        ///
        /// A send that is not lost draws twice — loss, then duplication — whatever the link's odds
        /// are, so the chances here come in pairs.
        fn new(chances: &[bool], delays: &[u64]) -> Self {
            Self {
                chances: chances.iter().copied().collect(),
                delays: delays
                    .iter()
                    .map(|&nanos| Duration::from_nanos(nanos))
                    .collect(),
            }
        }
    }

    impl Rng for Scripted {
        fn next_u64(&mut self) -> u64 {
            unimplemented!("a link never draws a raw value")
        }

        fn range(&mut self, _bounds: RangeInclusive<u64>) -> u64 {
            unimplemented!("a link never draws a bare range")
        }

        fn chance(&mut self, _numerator: u32, _denominator: u32) -> bool {
            self.chances
                .pop_front()
                .expect("the script covers every chance the link draws")
        }

        fn duration_in(&mut self, _bounds: RangeInclusive<Duration>) -> Duration {
            self.delays
                .pop_front()
                .expect("the script covers every delay the link draws")
        }
    }

    /// What a collecting node took, in the order it took it.
    type Log = Rc<RefCell<Vec<(u64, NodeId, &'static str)>>>;

    /// A link that loses nothing, duplicates nothing, and takes between 10 and 100 milliseconds.
    fn ordinary() -> Link {
        Link::new(Duration::from_millis(10)..=Duration::from_millis(100))
    }

    /// Spawns a task that takes messages until `patience` goes by with none arriving.
    ///
    /// Written against the capabilities alone: it names a clock and a network, and has no way to
    /// reach the engine behind either.
    fn spawn_collector<R: Rng + 'static>(
        executor: &mut Executor,
        node: Endpoint<&'static str, R>,
        patience: Duration,
        log: &Log,
    ) {
        let clock = executor.handle();
        let log = Rc::clone(log);
        executor.spawn(async move {
            while let Ok(delivery) = clock.timeout(patience, node.recv()).await {
                log.borrow_mut().push((
                    clock.now().as_nanos(),
                    delivery.sender(),
                    *delivery.message(),
                ));
            }
        });
    }

    /// Runs `executor` to completion, failing the test rather than returning an error.
    fn finish(executor: &mut Executor) -> u64 {
        executor
            .run()
            .unwrap_or_else(|e| panic!("run did not finish: {e}"));
        executor.handle().now().as_nanos()
    }

    #[test]
    fn a_message_arrives_after_a_delay_the_link_drew() {
        // The one case a seed is the right instrument for: what matters is that the delay came from
        // the generator and landed inside the link's range, not that it took any exact value.
        let mut executor = Executor::new();
        let network = VirtualNetwork::new(executor.handle(), SeededRng::from_seed(7))
            .with_default_link(ordinary());
        let alice = network.add_node();
        let bob = network.add_node();
        let log = Log::default();

        spawn_collector(&mut executor, bob.clone(), Duration::from_secs(1), &log);
        alice.send(bob.id(), "hello");
        finish(&mut executor);

        let taken = log.borrow();
        assert_eq!(taken.len(), 1, "one message was sent, so one arrives");
        let (at, sender, message) = taken[0];
        assert_eq!((sender, message), (alice.id(), "hello"));
        assert!(
            (10 * MILLIS..=100 * MILLIS).contains(&at),
            "the delay came from inside the link's range: {at}"
        );
    }

    #[test]
    fn a_link_delivers_everything_or_nothing_as_its_odds_say() {
        struct Case {
            name: &'static str,
            loss: Odds,
            want: usize,
        }
        let cases = [
            Case {
                name: "a link that loses nothing",
                loss: Odds::never(),
                want: 3,
            },
            Case {
                name: "a link that loses everything",
                loss: Odds::always(),
                want: 0,
            },
        ];

        // The extremes need no script: whatever the generator says, odds of none and odds of all
        // can only come out one way.
        for case in cases {
            let mut executor = Executor::new();
            let network = VirtualNetwork::new(executor.handle(), SeededRng::from_seed(7))
                .with_default_link(ordinary().with_loss(case.loss));
            let alice = network.add_node();
            let bob = network.add_node();
            let log = Log::default();

            spawn_collector(&mut executor, bob.clone(), Duration::from_secs(1), &log);
            for message in ["one", "two", "three"] {
                alice.send(bob.id(), message);
            }
            finish(&mut executor);

            assert_eq!(log.borrow().len(), case.want, "{}", case.name);
        }
    }

    #[test]
    fn a_duplicated_message_arrives_twice_each_copy_with_its_own_delay() {
        // Not lost, eighty milliseconds on the wire, duplicated, and the copy takes twenty. The
        // copy therefore lands first — reordering is not a switch on the link, it is what drawing
        // each copy's delay separately produces.
        let mut executor = Executor::new();
        let script = Scripted::new(&[false, true], &[80 * MILLIS, 20 * MILLIS]);
        let network = VirtualNetwork::new(executor.handle(), script)
            .with_default_link(ordinary().with_duplication(Odds::always()));
        let alice = network.add_node();
        let bob = network.add_node();
        let log = Log::default();

        spawn_collector(&mut executor, bob.clone(), Duration::from_secs(1), &log);
        alice.send(bob.id(), "hello");
        finish(&mut executor);

        assert_eq!(
            *log.borrow(),
            [
                (20 * MILLIS, alice.id(), "hello"),
                (80 * MILLIS, alice.id(), "hello"),
            ]
        );
    }

    #[test]
    fn messages_sent_in_order_arrive_in_the_order_the_wire_gave_them() {
        // Neither is lost; the first takes eighty milliseconds and the second twenty.
        let mut executor = Executor::new();
        let script = Scripted::new(&[false, false, false, false], &[80 * MILLIS, 20 * MILLIS]);
        let network = VirtualNetwork::new(executor.handle(), script).with_default_link(ordinary());
        let alice = network.add_node();
        let bob = network.add_node();
        let log = Log::default();

        spawn_collector(&mut executor, bob.clone(), Duration::from_secs(1), &log);
        alice.send(bob.id(), "first");
        alice.send(bob.id(), "second");
        finish(&mut executor);

        assert_eq!(
            *log.borrow(),
            [
                (20 * MILLIS, alice.id(), "second"),
                (80 * MILLIS, alice.id(), "first"),
            ],
            "the one with the shorter delay overtakes the one sent before it"
        );
    }

    #[test]
    fn a_partitioned_link_carries_nothing_and_a_heal_resumes_it() {
        let mut executor = Executor::new();
        let network = VirtualNetwork::new(executor.handle(), SeededRng::from_seed(7))
            .with_default_link(ordinary());
        let alice = network.add_node();
        let bob = network.add_node();
        let log = Log::default();

        spawn_collector(&mut executor, bob.clone(), Duration::from_secs(5), &log);

        let clock = executor.handle();
        let healing = network.clone();
        let sender = alice.clone();
        let receiver = bob.id();
        // The collector outlasts the heal on purpose: a patience shorter than the outage would give
        // up before the link came back, and the empty log would look like a partition that stuck.
        executor.spawn(async move {
            healing.partition(sender.id(), receiver);
            sender.send(receiver, "while the link is down");
            clock.sleep(Duration::from_secs(2)).await;
            healing.heal(sender.id(), receiver);
            sender.send(receiver, "once it is back");
        });
        finish(&mut executor);

        let taken = log.borrow();
        assert_eq!(taken.len(), 1, "only the send after the heal got through");
        assert_eq!(taken[0].2, "once it is back");
    }

    #[test]
    fn a_message_already_on_the_wire_lands_though_the_link_partitions_behind_it() {
        // A partition is what a link does to a send, not to what has already left. The alternative
        // — swallowing whatever is in flight — makes a delivery depend on the link's state at two
        // different instants, and hides the message that arrives just too late to be wanted.
        let mut executor = Executor::new();
        let script = Scripted::new(&[false, false], &[80 * MILLIS]);
        let network = VirtualNetwork::new(executor.handle(), script).with_default_link(ordinary());
        let alice = network.add_node();
        let bob = network.add_node();
        let log = Log::default();

        spawn_collector(&mut executor, bob.clone(), Duration::from_secs(1), &log);
        alice.send(bob.id(), "already gone");

        let clock = executor.handle();
        let cutting = network.clone();
        let from = alice.id();
        let to = bob.id();
        executor.spawn(async move {
            clock.sleep(Duration::from_millis(10)).await;
            cutting.partition(from, to);
        });
        finish(&mut executor);

        assert_eq!(*log.borrow(), [(80 * MILLIS, alice.id(), "already gone")]);
    }

    #[test]
    fn a_partition_is_directional() {
        // Asymmetric failures are real, and a system that assumes silence is mutual is exactly the
        // kind this engine is meant to catch out.
        let mut executor = Executor::new();
        let network = VirtualNetwork::new(executor.handle(), SeededRng::from_seed(7))
            .with_default_link(ordinary());
        let alice = network.add_node();
        let bob = network.add_node();
        let heard_by_alice = Log::default();
        let heard_by_bob = Log::default();

        network.partition(alice.id(), bob.id());
        spawn_collector(
            &mut executor,
            alice.clone(),
            Duration::from_secs(1),
            &heard_by_alice,
        );
        spawn_collector(
            &mut executor,
            bob.clone(),
            Duration::from_secs(1),
            &heard_by_bob,
        );
        alice.send(bob.id(), "into the void");
        bob.send(alice.id(), "the other way still works");
        finish(&mut executor);

        assert!(
            heard_by_bob.borrow().is_empty(),
            "the cut direction is dead"
        );
        assert_eq!(heard_by_alice.borrow().len(), 1, "the other one is not");
        assert_eq!(heard_by_alice.borrow()[0].2, "the other way still works");
    }

    #[test]
    fn several_tasks_waiting_on_one_node_each_take_a_message() {
        // A mailbox holding one waker cannot do this: the wait registered last would be the only
        // one a delivery could reach, and the rest would sit there while the run stalled around
        // them.
        let mut executor = Executor::new();
        let script = Scripted::new(&[false, false, false, false], &[20 * MILLIS, 50 * MILLIS]);
        let network = VirtualNetwork::new(executor.handle(), script).with_default_link(ordinary());
        let alice = network.add_node();
        let bob = network.add_node();
        let log = Log::default();

        for _ in 0..2 {
            spawn_collector(&mut executor, bob.clone(), Duration::from_secs(1), &log);
        }
        alice.send(bob.id(), "one");
        alice.send(bob.id(), "two");
        finish(&mut executor);

        assert_eq!(
            *log.borrow(),
            [
                (20 * MILLIS, alice.id(), "one"),
                (50 * MILLIS, alice.id(), "two"),
            ]
        );
    }

    #[test]
    fn a_wait_abandoned_before_delivery_leaves_nothing_behind() {
        // The same defect the engine's timers were fixed for, in the network's terms: a wait that
        // walked away must leave neither a timer that can carry the clock to an arrival nobody is
        // waiting for, nor a waker that marks a task ready for a message nobody wants.
        let mut executor = Executor::new();
        let script = Scripted::new(&[false, false], &[80 * MILLIS]);
        let network = VirtualNetwork::new(executor.handle(), script).with_default_link(ordinary());
        let alice = network.add_node();
        let bob = network.add_node();

        alice.send(bob.id(), "nobody is waiting for this");

        let clock = executor.handle();
        executor.spawn(async move {
            let mut waiting = Box::pin(bob.recv());
            let armed = core::future::poll_fn(|cx| Poll::Ready(waiting.as_mut().poll(cx))).await;
            assert!(armed.is_pending(), "nothing has arrived yet");
            drop(waiting);
            clock.sleep(Duration::from_nanos(10)).await;
        });

        let ended_at = finish(&mut executor);
        assert_eq!(ended_at, 10, "the run ends where its work ended");
    }

    #[test]
    fn odds_reject_what_they_could_not_mean() {
        struct Case {
            name: &'static str,
            numerator: u32,
            denominator: u32,
            want: Result<(), OddsError>,
        }
        let cases = [
            Case {
                name: "never",
                numerator: 0,
                denominator: 1,
                want: Ok(()),
            },
            Case {
                name: "always",
                numerator: 1,
                denominator: 1,
                want: Ok(()),
            },
            Case {
                name: "three in four",
                numerator: 3,
                denominator: 4,
                want: Ok(()),
            },
            Case {
                name: "no trials at all",
                numerator: 0,
                denominator: 0,
                want: Err(OddsError::NoTrials),
            },
            Case {
                name: "more occurrences than trials",
                numerator: 2,
                denominator: 1,
                want: Err(OddsError::TooLikely {
                    numerator: 2,
                    denominator: 1,
                }),
            },
        ];

        for case in cases {
            assert_eq!(
                Odds::new(case.numerator, case.denominator).map(|_| ()),
                case.want,
                "{}",
                case.name
            );
        }
    }
}
