//! A replicated log: five replicas electing a leader and copying a client's commands to each other.
//!
//! The reconciler is a control loop, and everything else in this crate before it was a protocol
//! too small to go wrong in an interesting way. This is a system with nothing in common with the
//! reconciler — no regions, no listing, no timeline of what is wanted — run on the same engine, so
//! what the engine offers is not shaped around one workload.
//!
//! # The protocol
//!
//! It is Raft's shape. Each replica is a **follower**, a **candidate** or a **leader**, in a
//! numbered **term**. A follower that hears nothing from a leader for an election timeout becomes a
//! candidate in the next term, votes for itself and asks every other replica for its vote; a
//! replica votes at most once a term, and a candidate that collects a majority leads that term. A
//! leader appends a no-op to its log the moment it is elected, takes each command the client sends
//! it as the next entry of its log, and copies its log to every follower on a heartbeat. A follower
//! accepts entries only after the entry they follow, which it must already hold, and an entry that
//! conflicts with what the follower holds replaces it and everything after it. An entry is
//! **committed** once a majority holds it and it is from the leader's own term; a replica applies
//! committed entries in order, and applying is not something a replica can take back. Any message
//! from a later term makes its receiver a follower in that term.
//!
//! # What is missing, on purpose
//!
//! A candidate asks for a vote with its term and nothing else, and a replica grants the first
//! candidate to ask in a term whatever that candidate's log holds. Raft does not: a replica refuses
//! a candidate whose log is behind its own, which is what keeps every committed entry in every
//! later leader's log. Without it, a replica that spent a partition alone — timing out, standing,
//! failing, standing again, its term climbing all the while — comes back with a term every other
//! replica must defer to and a log missing whatever was committed while it was away. If it wins the
//! election that follows, the no-op it appends lands where a committed entry already is, and the
//! followers it copies its log to give that entry up. The draws decide whether it wins: whichever
//! replica's timeout runs out first after the heal stands first, and a stale replica standing first
//! is elected as readily as any other.
//!
//! # The run
//!
//! [`run`] starts the five replicas and a client over the simulated network. The client is node 0
//! and the replicas nodes 1 to 5, written `node-1` to `node-5` wherever the run writes them down,
//! so a fault on `node 3 -> node 1` is about the replicas the trace calls `node-3` and `node-1`.
//! The client sends [`COMMANDS`] commands one after another, each no earlier than its own place on
//! a fixed period, to whichever replica it believes leads; it follows a replica's hint to the
//! leader, and moves on to the next replica when it is given none or hears nothing back in time.
//! It is told a command is done only once the command is committed.
//!
//! The wire is **dependable**, for the reason `quorum` gives: it takes time and nothing else, so an
//! injected [`FaultSchedule`] is the only trouble a run meets. With none, the first leader keeps its
//! place for the whole run, since its heartbeat arrives well inside any follower's timeout. The seed
//! reaches a run through how long each message spends on the wire and through each replica's
//! election timeouts, drawn afresh every time a replica's timer is set, from a generator of the
//! replica's own. The network's generator is drawn from the run's seed first and the replicas'
//! after it, in node order, and that order is part of every history this module records.
//!
//! Every change a replica makes to what it holds is a step of the trace, and so are the client
//! sending a command and hearing it is done. Each loop stops at a fixed instant, since heartbeats
//! keep the wire busy for as long as a leader stands and a replica waiting for quiet would wait
//! forever.
//!
//! # The verdict
//!
//! A run is judged by three [`Invariant`]s over the world each step recorded, and the verdict is
//! the first breach [`invariant::check`] finds:
//!
//! - **"two leaders in one term"**, a safety promise: each replica carries a `led-<term>` flag for
//!   every term it led, a fact about the run rather than something the replica remembers, so two
//!   replicas carrying the same one is visible in a single world.
//! - **"a committed entry changed"**, a safety promise: every replica's `applied-<index>` agrees
//!   with every other's at that index, and with what that replica's own log holds there.
//! - **"did not commit every command"**, a liveness promise with a span too long for virtual time,
//!   so only the end of the run can break it: every command the client sent was answered, and every
//!   replica has applied it.

use core::cell::RefCell;
use core::fmt;
use core::ops::RangeInclusive;
use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use crate::clock::{Clock, VirtualTime};
use crate::executor::Executor;
use crate::fault::FaultSchedule;
use crate::history::Entry;
use crate::invariant::{self, Invariant};
use crate::net::{Link, Network, NodeId, VirtualNetwork};
use crate::outcome::{Outcome, Reason, ReasonError};
use crate::rng::{Rng, SeededRng};
use crate::store::StateStore;
use crate::systems::RunError;
use crate::trace::{Step, Trace};
use crate::world::{Name, NameError, Resource, Snapshot, Value, World};

/// How many replicas keep the log.
const REPLICAS: usize = 5;

/// How many replicas make a majority of [`REPLICAS`]: enough to elect a leader, and enough to
/// commit an entry.
const MAJORITY: usize = 3;

/// How many commands the client sends.
const COMMANDS: u64 = 5;

/// How far apart the client's commands are, at the least. Command `n` is sent no earlier than `n`
/// of these after the start of the run, and later only if the one before it is not done yet.
const PERIOD: Duration = Duration::from_secs(2);

/// How often a leader copies its log to its followers, whether or not it has anything new.
///
/// A heartbeat and the slowest message across take well under the shortest [`ELECTION`] timeout,
/// so a follower that can hear its leader never stands against it.
const HEARTBEAT: Duration = Duration::from_millis(500);

/// How long a follower or a candidate waits to hear from a leader before standing for election,
/// drawn afresh every time the wait starts.
///
/// Drawn rather than fixed so that two replicas whose timeouts started together do not stand
/// together again and split the vote forever; three times the [`HEARTBEAT`] at the least, so a
/// heartbeat missed by a whisker does not start an election.
const ELECTION: RangeInclusive<Duration> =
    Duration::from_millis(1_500)..=Duration::from_millis(3_000);

/// How long the client waits to hear that a command is done before asking another replica.
///
/// Longer than a request across, a round of copying and the answer back, each as slow as the wire
/// can be, so a leader that can reach a majority answers in time.
const PATIENCE: Duration = Duration::from_secs(1);

/// The instant every loop in the run stops at, counted from its start.
///
/// The last command is sent at ten seconds, so this leaves twenty for whatever trouble a run is
/// put through to clear and for every replica to hear the last commit.
const STOP: Duration = Duration::from_secs(30);

/// What the dependable wire takes to carry a message.
const LATENCY: RangeInclusive<Duration> = Duration::from_millis(10)..=Duration::from_millis(100);

/// What the client is called where the run writes down what it knows.
const CLIENT: &str = "client";

/// What each replica is called there, with its node number after it.
const NODE: &str = "node-";

/// The field holding a replica's term.
const TERM: &str = "term";

/// The field holding whether a replica follows, stands or leads.
const ROLE: &str = "role";

/// The field naming the replica a replica voted for in its term, absent if it voted for nobody.
const VOTED_FOR: &str = "voted-for";

/// The field holding how much of a replica's log it knows to be committed.
const COMMIT: &str = "commit";

/// The start of the fields describing one entry of a replica's log, with its index after it.
const ENTRY: &str = "entry-";

/// The end of the field holding the term an entry was taken in.
const ENTRY_TERM: &str = "-term";

/// The end of the field holding what an entry asks for.
const ENTRY_OP: &str = "-op";

/// The start of the field holding what a replica applied at an index, with the index after it.
const APPLIED: &str = "applied-";

/// The start of the flag a replica carries for each term it led, with the term after it.
const LED: &str = "led-";

/// The start of the fields describing one of the client's commands, with its number after it.
const COMMAND: &str = "command-";

/// The end of the field holding when the client first sent a command.
const SENT: &str = "-sent";

/// The end of the field holding when the client heard a command was done.
const DONE: &str = "-done";

/// Returns the instant command `command` may be sent from.
fn opens(command: u64) -> VirtualTime {
    VirtualTime::from_nanos(command.saturating_mul(as_nanos(PERIOD)))
}

/// Returns the instant every loop stops at.
fn stop() -> VirtualTime {
    VirtualTime::from_nanos(as_nanos(STOP))
}

/// Returns `duration` as a whole number of nanoseconds, capped at the end of virtual time.
fn as_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Returns how long there is left until `deadline`, which is nothing once it has passed.
fn until(now: VirtualTime, deadline: VirtualTime) -> Duration {
    Duration::from_nanos(deadline.as_nanos().saturating_sub(now.as_nanos()))
}

/// Returns the instant `duration` after `at`, capped at the end of virtual time.
fn after(at: VirtualTime, duration: Duration) -> VirtualTime {
    at.checked_add(duration)
        .unwrap_or(VirtualTime::from_nanos(u64::MAX))
}

/// Returns what the replica at `index` among the replicas is called.
fn node(index: usize) -> String {
    format!("{NODE}{}", index.saturating_add(1))
}

/// Returns "entry 4" or "entries 4 to 6", for the entries from `first` to `last`.
fn entries(first: u64, last: u64) -> String {
    if first == last {
        format!("entry {first}")
    } else {
        format!("entries {first} to {last}")
    }
}

/// Returns `count` as a log index, which a log too long for one cannot reach.
fn index(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

/// What an entry of the log asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    /// Nothing: the entry a leader appends the moment it is elected.
    Noop,
    /// One of the client's commands, by its number.
    Command(u64),
}

impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Noop => write!(f, "no-op"),
            Self::Command(command) => write!(f, "command {command}"),
        }
    }
}

/// One entry of the log: what it asks for, and the term of the leader that took it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LogEntry {
    term: u64,
    op: Op,
}

/// Whether a replica follows, stands or leads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    /// It copies the log of whichever leader its term has.
    Follower,
    /// It has stood for election in its term and is counting votes.
    Candidate,
    /// It won its term's election.
    Leader,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Follower => write!(f, "follower"),
            Self::Candidate => write!(f, "candidate"),
            Self::Leader => write!(f, "leader"),
        }
    }
}

/// A replica's answer to a candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ballot {
    /// The vote is the candidate's.
    Granted,
    /// It is not.
    Refused,
}

/// A replica's answer to the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Answer {
    /// The command is committed.
    Committed,
    /// This replica does not lead; the one it believes does, if it knows of one.
    NotLeader(Option<NodeId>),
}

/// What travels between the replicas, and between a replica and the client.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Message {
    /// A candidate asking for a vote in its term — with its term, and nothing about its log.
    RequestVote { term: u64 },
    /// A replica's answer to a candidate, with the replica's term.
    Vote { term: u64, ballot: Ballot },
    /// A leader copying its log from `prev_index` on, and saying how much of it is committed.
    Append {
        term: u64,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<LogEntry>,
        commit: u64,
    },
    /// A follower's answer to a leader: how far its log now matches the leader's, or nothing if the
    /// entry the copy followed was not one it holds.
    Appended { term: u64, matched: Option<u64> },
    /// The client asking for a command to be committed.
    Submit { command: u64 },
    /// A replica's answer to the client about a command.
    Submitted { command: u64, answer: Answer },
}

impl Message {
    /// The term a message between replicas was sent in; the client's messages are in none.
    fn term(&self) -> Option<u64> {
        match self {
            Self::RequestVote { term }
            | Self::Vote { term, .. }
            | Self::Append { term, .. }
            | Self::Appended { term, .. } => Some(*term),
            Self::Submit { .. } | Self::Submitted { .. } => None,
        }
    }
}

/// What a replica's timer should do once it has handled something.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rearm {
    /// Go on as it was.
    Keep,
    /// Fire again a heartbeat from now: the replica leads.
    Heartbeat,
    /// Fire again a freshly drawn election timeout from now.
    Election,
}

/// Everything about a replica a step writes down.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Recorded {
    term: u64,
    role: Role,
    voted_for: Option<usize>,
    commit: u64,
    log: Vec<LogEntry>,
    applied: Vec<Op>,
    led: BTreeSet<u64>,
}

/// What a replica's handling of one thing comes to: what to send, what changed, and its timer.
///
/// Each change is written down with the replica's state as it stood straight after that change,
/// so one message that moves a replica twice — a copy that appends and then commits — is two
/// steps, each showing its own state.
#[derive(Debug)]
struct Out {
    sends: Vec<(NodeId, Message)>,
    notes: Vec<(String, Recorded)>,
    rearm: Rearm,
}

impl Out {
    /// Nothing to send, nothing changed, and the timer left as it was.
    fn new() -> Self {
        Self {
            sends: Vec::new(),
            notes: Vec::new(),
            rearm: Rearm::Keep,
        }
    }
}

/// One replica's state, and how it answers whatever reaches it.
///
/// Nothing here waits, draws or reads a clock: a handler is handed one thing and says what comes of
/// it, and the task around it does the sending, the recording and the timing. That is what lets the
/// cases below hold the protocol to account one message at a time.
#[derive(Debug)]
struct Replica {
    me: usize,
    peers: Rc<[NodeId]>,
    recorded: Recorded,
    leader: Option<usize>,
    votes: BTreeSet<usize>,
    next: Vec<u64>,
    matched: Vec<u64>,
    pending: BTreeMap<u64, (NodeId, u64)>,
}

impl Replica {
    /// A follower in term 0 with nothing in its log, the replica at `me` among `peers`.
    fn new(me: usize, peers: Rc<[NodeId]>) -> Self {
        Self {
            me,
            peers,
            recorded: Recorded {
                term: 0,
                role: Role::Follower,
                voted_for: None,
                commit: 0,
                log: Vec::new(),
                applied: Vec::new(),
                led: BTreeSet::new(),
            },
            leader: None,
            votes: BTreeSet::new(),
            next: Vec::new(),
            matched: Vec::new(),
            pending: BTreeMap::new(),
        }
    }

    /// The index of the last entry of the log, which is 0 for an empty one.
    fn last_index(&self) -> u64 {
        index(self.recorded.log.len())
    }

    /// The term of the entry at `at`: 0 before the first entry, and nothing past the last.
    fn term_at(&self, at: u64) -> Option<u64> {
        let Some(offset) = at.checked_sub(1) else {
            return Some(0);
        };
        let offset = usize::try_from(offset).ok()?;
        self.recorded.log.get(offset).map(|entry| entry.term)
    }

    /// Where `node` sits among the replicas, if it is one of them.
    fn peer(&self, node: NodeId) -> Option<usize> {
        self.peers.iter().position(|peer| *peer == node)
    }

    /// Writes down that `message` happened, with the state as it stands now.
    fn note(&self, out: &mut Out, message: String) {
        out.notes.push((message, self.recorded.clone()));
    }

    /// The timer ran out: a leader copies its log, and anyone else stands for election.
    fn on_timer(&mut self) -> Out {
        let mut out = Out::new();
        if self.recorded.role == Role::Leader {
            self.copy_to_everyone(&mut out);
            out.rearm = Rearm::Heartbeat;
            return out;
        }
        self.recorded.term = self.recorded.term.saturating_add(1);
        self.recorded.role = Role::Candidate;
        self.recorded.voted_for = Some(self.me);
        self.leader = None;
        self.votes = BTreeSet::from([self.me]);
        let term = self.recorded.term;
        self.note(
            &mut out,
            format!("{} became candidate for term {term}", node(self.me)),
        );
        for (other, peer) in self.peers.iter().enumerate() {
            if other != self.me {
                out.sends.push((*peer, Message::RequestVote { term }));
            }
        }
        out.rearm = Rearm::Election;
        out
    }

    /// Answers `message`, which `from` sent.
    fn on_message(&mut self, from: NodeId, message: Message) -> Out {
        let mut out = Out::new();
        let before = (
            self.recorded.term,
            self.recorded.role,
            self.recorded.voted_for,
        );
        if let Some(term) = message.term()
            && term > self.recorded.term
        {
            self.adopt(term, &mut out);
        }
        match message {
            Message::RequestVote { term } => self.on_request_vote(from, term, &mut out),
            Message::Vote { term, ballot } => self.on_vote(from, term, ballot, &mut out),
            Message::Append {
                term,
                prev_index,
                prev_term,
                entries,
                commit,
            } => self.on_append(
                from,
                (term, prev_index, prev_term),
                &entries,
                commit,
                &mut out,
            ),
            Message::Appended { term, matched } => self.on_appended(from, term, matched, &mut out),
            Message::Submit { command } => self.on_submit(from, command, &mut out),
            Message::Submitted { .. } => {}
        }
        // A change of term or of role that nothing else wrote down is still a change.
        let now = (
            self.recorded.term,
            self.recorded.role,
            self.recorded.voted_for,
        );
        if out.notes.is_empty() && now != before {
            self.note(
                &mut out,
                format!(
                    "{} is a {} in term {}",
                    node(self.me),
                    self.recorded.role,
                    self.recorded.term
                ),
            );
        }
        out
    }

    /// Moves into the later term `term` as a follower with its vote unspent.
    ///
    /// A replica that was standing or leading drops whatever it was counting or owed: the client is
    /// told nothing, and finds out by hearing nothing in time.
    fn adopt(&mut self, term: u64, out: &mut Out) {
        self.recorded.term = term;
        self.recorded.voted_for = None;
        self.leader = None;
        if self.recorded.role != Role::Follower {
            self.recorded.role = Role::Follower;
            self.votes.clear();
            self.pending.clear();
            out.rearm = Rearm::Election;
        }
    }

    /// A candidate asked for this replica's vote in `term`.
    ///
    /// The vote goes to the first candidate to ask in the replica's term. Nothing about the
    /// candidate's log is asked, because nothing about it was sent — the gap the module's docs
    /// describe.
    fn on_request_vote(&mut self, from: NodeId, term: u64, out: &mut Out) {
        let Some(candidate) = self.peer(from) else {
            return;
        };
        let ballot = if term == self.recorded.term
            && self
                .recorded
                .voted_for
                .is_none_or(|voted| voted == candidate)
        {
            if self.recorded.voted_for.is_none() {
                self.recorded.voted_for = Some(candidate);
                self.note(
                    out,
                    format!(
                        "{} voted for {} in term {term}",
                        node(self.me),
                        node(candidate)
                    ),
                );
            }
            out.rearm = Rearm::Election;
            Ballot::Granted
        } else {
            Ballot::Refused
        };
        out.sends.push((
            from,
            Message::Vote {
                term: self.recorded.term,
                ballot,
            },
        ));
    }

    /// A replica answered this one's request for its vote.
    fn on_vote(&mut self, from: NodeId, term: u64, ballot: Ballot, out: &mut Out) {
        if self.recorded.role != Role::Candidate
            || term != self.recorded.term
            || ballot != Ballot::Granted
        {
            return;
        }
        let Some(voter) = self.peer(from) else {
            return;
        };
        self.votes.insert(voter);
        if self.votes.len() >= MAJORITY {
            self.lead(out);
        }
    }

    /// Takes the lead of the replica's term: appends a no-op, and copies the log out at once.
    ///
    /// The no-op is what lets a new leader commit anything at all: it counts only entries of its
    /// own term towards a commit, and the client may have nothing more to send.
    fn lead(&mut self, out: &mut Out) {
        let term = self.recorded.term;
        self.recorded.role = Role::Leader;
        self.recorded.led.insert(term);
        self.recorded.log.push(LogEntry { term, op: Op::Noop });
        self.leader = Some(self.me);
        self.votes.clear();
        let last = self.last_index();
        self.next = vec![last; REPLICAS];
        self.matched = vec![0; REPLICAS];
        if let Some(own) = self.matched.get_mut(self.me) {
            *own = last;
        }
        self.note(
            out,
            format!("{} became leader of term {term}", node(self.me)),
        );
        self.copy_to_everyone(out);
        out.rearm = Rearm::Heartbeat;
    }

    /// A leader copied its log to this replica, from the entry after `prev_index` on.
    fn on_append(
        &mut self,
        from: NodeId,
        (term, prev_index, prev_term): (u64, u64, u64),
        copied: &[LogEntry],
        commit: u64,
        out: &mut Out,
    ) {
        let refused = Message::Appended {
            term: self.recorded.term,
            matched: None,
        };
        let Some(leader) = self.peer(from) else {
            return;
        };
        // A leader hears from nobody else claiming its own term: there is only one of those.
        if term < self.recorded.term || self.recorded.role == Role::Leader {
            out.sends.push((from, refused));
            return;
        }
        if self.recorded.role == Role::Candidate {
            self.recorded.role = Role::Follower;
            self.votes.clear();
        }
        self.leader = Some(leader);
        out.rearm = Rearm::Election;
        if self.term_at(prev_index) != Some(prev_term) {
            out.sends.push((from, refused));
            return;
        }
        self.take(prev_index, copied, out);
        let last_new = prev_index.saturating_add(index(copied.len()));
        let reached = commit.min(last_new);
        if reached > self.recorded.commit {
            self.recorded.commit = reached;
            self.apply();
            self.note(
                out,
                format!("{} committed through {reached}", node(self.me)),
            );
        }
        out.sends.push((
            from,
            Message::Appended {
                term,
                matched: Some(last_new),
            },
        ));
    }

    /// Takes `copied`, which follows the entry at `prev_index`, into the log: an entry it holds is
    /// kept, and one that conflicts goes with everything after it.
    fn take(&mut self, prev_index: u64, copied: &[LogEntry], out: &mut Out) {
        let mut dropped = None;
        let mut appended = None;
        for (at, entry) in (prev_index.saturating_add(1)..).zip(copied) {
            match self.term_at(at) {
                Some(term) if term == entry.term => continue,
                Some(_) => {
                    let kept = at.saturating_sub(1);
                    self.recorded
                        .log
                        .truncate(usize::try_from(kept).unwrap_or(usize::MAX));
                    dropped.get_or_insert(kept);
                }
                None => {}
            }
            self.recorded.log.push(*entry);
            appended.get_or_insert(at);
        }
        if let Some(first) = appended {
            let range = entries(first, self.last_index());
            let message = match dropped {
                Some(kept) => format!(
                    "{} dropped entries after {kept} and appended {range}",
                    node(self.me)
                ),
                None => format!("{} appended {range}", node(self.me)),
            };
            self.note(out, message);
        }
    }

    /// Applies every committed entry not yet applied, in order.
    ///
    /// What is applied stays applied: a log that later gives an entry up cannot take back what was
    /// done with it, which is why a changed committed entry shows in a world at all.
    fn apply(&mut self) {
        let from = self.recorded.applied.len();
        let to = usize::try_from(self.recorded.commit).unwrap_or(usize::MAX);
        let ops: Vec<Op> = self
            .recorded
            .log
            .iter()
            .take(to)
            .skip(from)
            .map(|entry| entry.op)
            .collect();
        self.recorded.applied.extend(ops);
    }

    /// A follower answered this replica's copy.
    fn on_appended(&mut self, from: NodeId, term: u64, matched: Option<u64>, out: &mut Out) {
        if self.recorded.role != Role::Leader || term != self.recorded.term {
            return;
        }
        let Some(follower) = self.peer(from) else {
            return;
        };
        let (Some(next), Some(held)) = (self.next.get(follower), self.matched.get(follower)) else {
            return;
        };
        let (next, held) = match matched {
            Some(reached) => {
                let held = (*held).max(reached);
                (held.saturating_add(1), held)
            }
            None => (next.saturating_sub(1).max(1), *held),
        };
        if let Some(slot) = self.next.get_mut(follower) {
            *slot = next;
        }
        if let Some(slot) = self.matched.get_mut(follower) {
            *slot = held;
        }
        self.advance(out);
        if matched.is_none() || next <= self.last_index() {
            out.sends.push((from, self.copy_for(follower)));
        }
    }

    /// Commits the furthest entry of the leader's own term that a majority holds, if that is further
    /// than it has committed, and answers the client for what that commits.
    ///
    /// Only an entry of the leader's own term is counted. An entry from an earlier term held by a
    /// majority can still be replaced by a later leader that never saw it, so counting its copies
    /// would commit something that is not safe yet; it is committed along with the first entry of
    /// this term that a majority holds.
    fn advance(&mut self, out: &mut Out) {
        let term = self.recorded.term;
        let reached = (self.recorded.commit.saturating_add(1)..=self.last_index())
            .rev()
            .find(|&at| {
                self.term_at(at) == Some(term)
                    && self.matched.iter().filter(|&&held| held >= at).count() >= MAJORITY
            });
        let Some(reached) = reached else {
            return;
        };
        self.recorded.commit = reached;
        self.apply();
        self.note(
            out,
            format!("{} committed through {reached}", node(self.me)),
        );
        let owed = self.pending.split_off(&reached.saturating_add(1));
        let answered = core::mem::replace(&mut self.pending, owed);
        for (at, (client, command)) in answered {
            if self.op_at(at) == Some(Op::Command(command)) {
                out.sends.push((
                    client,
                    Message::Submitted {
                        command,
                        answer: Answer::Committed,
                    },
                ));
            }
        }
    }

    /// What the entry at `at` asks for, if the log reaches that far.
    fn op_at(&self, at: u64) -> Option<Op> {
        let offset = usize::try_from(at.checked_sub(1)?).ok()?;
        self.recorded.log.get(offset).map(|entry| entry.op)
    }

    /// The client asked for `command` to be committed.
    ///
    /// A leader takes it as the next entry, unless its log already holds it — a retry — in which
    /// case it answers at once if that entry is committed and once it is otherwise. Anyone else
    /// points the client at the leader it knows of.
    fn on_submit(&mut self, from: NodeId, command: u64, out: &mut Out) {
        if self.recorded.role != Role::Leader {
            let hint = self
                .leader
                .and_then(|leader| self.peers.get(leader).copied());
            out.sends.push((
                from,
                Message::Submitted {
                    command,
                    answer: Answer::NotLeader(hint),
                },
            ));
            return;
        }
        let held = (1..=self.last_index()).find(|&at| self.op_at(at) == Some(Op::Command(command)));
        match held {
            Some(at) if at <= self.recorded.commit => out.sends.push((
                from,
                Message::Submitted {
                    command,
                    answer: Answer::Committed,
                },
            )),
            Some(at) => {
                self.pending.insert(at, (from, command));
            }
            None => {
                let term = self.recorded.term;
                self.recorded.log.push(LogEntry {
                    term,
                    op: Op::Command(command),
                });
                let last = self.last_index();
                if let Some(own) = self.matched.get_mut(self.me) {
                    *own = last;
                }
                self.pending.insert(last, (from, command));
                self.note(
                    out,
                    format!("{} took command {command} as entry {last}", node(self.me)),
                );
                self.copy_to_everyone(out);
            }
        }
    }

    /// Copies the log to every other replica, each from where the leader believes it stands.
    fn copy_to_everyone(&self, out: &mut Out) {
        for (other, peer) in self.peers.iter().enumerate() {
            if other != self.me {
                out.sends.push((*peer, self.copy_for(other)));
            }
        }
    }

    /// The copy of the log the replica at `follower` is owed: everything from where the leader
    /// believes its log stops matching.
    fn copy_for(&self, follower: usize) -> Message {
        let next = self.next.get(follower).copied().unwrap_or(1).max(1);
        let prev_index = next.saturating_sub(1);
        let skip = usize::try_from(prev_index).unwrap_or(usize::MAX);
        Message::Append {
            term: self.recorded.term,
            prev_index,
            prev_term: self.term_at(prev_index).unwrap_or(0),
            entries: self.recorded.log.iter().skip(skip).copied().collect(),
            commit: self.recorded.commit,
        }
    }
}

/// Where the client stands with one of its commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Submission {
    sent: VirtualTime,
    done: Option<VirtualTime>,
}

/// Everything the run knows at one step: each replica that has started, and the client's commands.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct View {
    replicas: BTreeMap<usize, Recorded>,
    client: BTreeMap<u64, Submission>,
}

/// Something that happened, and what the run knew once it had.
struct Observation {
    at: VirtualTime,
    message: String,
    view: View,
}

/// Writes the run's view as a world: one resource for each replica that has started, and one for
/// the client once it has sent anything.
///
/// An index, a term and a command number are written four digits wide in a field's name, so the
/// fields of one resource list in order for the runs this module makes. Nothing reads the width
/// back: a wider number is written whole and is still the same field everywhere it appears.
fn world(view: &View) -> Result<World, NameError> {
    let mut world = World::new();
    for (&me, recorded) in &view.replicas {
        let mut replica = Resource::new()
            .with_field(Name::new(TERM)?, Value::Count(recorded.term))
            .with_field(Name::new(ROLE)?, Value::Text(recorded.role.to_string()))
            .with_field(Name::new(COMMIT)?, Value::Count(recorded.commit));
        if let Some(voted) = recorded.voted_for {
            replica.insert(Name::new(VOTED_FOR)?, Value::Text(node(voted)));
        }
        for (at, entry) in (1_u64..).zip(&recorded.log) {
            replica.insert(
                Name::new(format!("{ENTRY}{at:04}{ENTRY_TERM}"))?,
                Value::Count(entry.term),
            );
            replica.insert(
                Name::new(format!("{ENTRY}{at:04}{ENTRY_OP}"))?,
                Value::Text(entry.op.to_string()),
            );
        }
        for (at, op) in (1_u64..).zip(&recorded.applied) {
            replica.insert(
                Name::new(format!("{APPLIED}{at:04}"))?,
                Value::Text(op.to_string()),
            );
        }
        for term in &recorded.led {
            replica.insert(Name::new(format!("{LED}{term:04}"))?, Value::Flag(true));
        }
        world.insert(Name::new(node(me))?, replica);
    }
    if !view.client.is_empty() {
        let mut client = Resource::new();
        for (command, submission) in &view.client {
            client.insert(
                Name::new(format!("{COMMAND}{command:04}{SENT}"))?,
                Value::Instant(submission.sent),
            );
            if let Some(done) = submission.done {
                client.insert(
                    Name::new(format!("{COMMAND}{command:04}{DONE}"))?,
                    Value::Instant(done),
                );
            }
        }
        world.insert(Name::new(CLIENT)?, client);
    }
    Ok(world)
}

/// The resources of a recorded world that are replicas, which are the ones named for a node.
fn replicas(world: &World) -> impl Iterator<Item = &Resource> {
    world
        .resources()
        .filter(|(name, _)| name.as_str().starts_with(NODE))
        .map(|(_, replica)| replica)
}

/// The value of the field of `resource` called `field`, if it has one.
fn field<'a>(resource: &'a Resource, field: &str) -> Option<&'a Value> {
    resource
        .fields()
        .find(|(name, _)| name.as_str() == field)
        .map(|(_, value)| value)
}

/// Whether no two replicas led the same term.
///
/// Read off a recorded world: a replica carries a `led-<term>` flag for every term it led, so two
/// replicas carrying one flag are two leaders of that term.
fn one_leader_a_term(world: &World) -> bool {
    let mut led = BTreeSet::new();
    replicas(world).all(|replica| {
        replica
            .fields()
            .filter(|(name, _)| name.as_str().starts_with(LED))
            .all(|(name, _)| led.insert(name.as_str()))
    })
}

/// Whether every replica's applied entries agree with every other's and with its own log.
///
/// Read off a recorded world: what a replica applied at an index is its `applied-<index>`, and what
/// its log holds there is its `entry-<index>-op`. A log that no longer reaches an index it applied
/// has given that entry up, which is a change as much as a different entry there would be: a
/// replica's log is never cut back past what it has applied while every leader holds every
/// committed entry, so the first copy that does so is the step the entry is lost at.
fn committed_entries_hold(world: &World) -> bool {
    let mut applied: BTreeMap<&str, &Value> = BTreeMap::new();
    replicas(world).all(|replica| {
        replica.fields().all(|(name, value)| {
            let Some(at) = name.as_str().strip_prefix(APPLIED) else {
                return true;
            };
            let agreed = *applied.entry(at).or_insert(value) == value;
            let own = field(replica, &format!("{ENTRY}{at}{ENTRY_OP}"));
            agreed && own == Some(value)
        })
    })
}

/// Whether every command the client sent was answered and has been applied by every replica.
///
/// Read off a recorded world: a command was sent when the client has its `command-<n>-sent`,
/// answered when it has its `command-<n>-done`, and applied by a replica when some
/// `applied-<index>` of that replica names it.
fn every_command_committed(world: &World) -> bool {
    let Some(client) = world
        .resources()
        .find(|(name, _)| name.as_str() == CLIENT)
        .map(|(_, client)| client)
    else {
        return true;
    };
    client.fields().all(|(name, _)| {
        let Some(number) = name
            .as_str()
            .strip_prefix(COMMAND)
            .and_then(|rest| rest.strip_suffix(SENT))
        else {
            return true;
        };
        let Ok(command) = number.parse() else {
            return false;
        };
        let wanted = Value::Text(Op::Command(command).to_string());
        field(client, &format!("{COMMAND}{number}{DONE}")).is_some()
            && replicas(world).all(|replica| {
                replica
                    .fields()
                    .any(|(name, value)| name.as_str().starts_with(APPLIED) && *value == wanted)
            })
    })
}

/// What the replicated log promises, judged over the world each step of its run recorded.
///
/// The two safety promises stand first, so a run that breaks one at its last step is reported for
/// that rather than for falling short.
fn invariants() -> Result<[Invariant; 3], ReasonError> {
    Ok([
        Invariant::safety(Reason::new("two leaders in one term")?, one_leader_a_term),
        Invariant::safety(
            Reason::new("a committed entry changed")?,
            committed_entries_hold,
        ),
        Invariant::liveness(
            Reason::new("did not commit every command")?,
            Duration::MAX,
            every_command_committed,
        ),
    ])
}

/// Runs the five replicas and the client under `seed`, with `faults` to get in their way.
///
/// Returns what the run passed through, the states it passed through, and how it went. A run that
/// meets no faults elects one leader and commits every command on every replica:
///
/// ```
/// use chronoloop::fault::FaultSchedule;
/// use chronoloop::outcome::Outcome;
/// use chronoloop::systems::replog;
///
/// let (trace, store, outcome) = replog::run(20_261_006, &FaultSchedule::default())?;
///
/// assert_eq!(outcome, Outcome::Pass);
/// assert_eq!(trace.seed(), 20_261_006);
/// assert!(store.get(trace.steps()[0].state()).is_some());
/// # Ok::<(), chronoloop::systems::RunError>(())
/// ```
///
/// # Errors
///
/// Returns [`RunError`] if the simulation could not finish, or if the run wrote down something that
/// could not be read back.
pub fn run(seed: u64, faults: &FaultSchedule) -> Result<(Trace, StateStore, Outcome), RunError> {
    let (steps, store) = collect(observe(seed, faults)?)?;
    let trace = Trace::new(seed, steps);
    let outcome = verdict(&trace, &store)?;
    Ok((trace, store, outcome))
}

/// Runs the whole thing and returns what it wrote down.
fn observe(seed: u64, faults: &FaultSchedule) -> Result<Vec<Observation>, RunError> {
    let mut executor = Executor::new();
    let mut seeds = SeededRng::from_seed(seed);

    // The network's generator first, then each replica's in node order. That order is part of
    // every history this module records: change it and every run lands somewhere else.
    let network: VirtualNetwork<Message, SeededRng> =
        VirtualNetwork::new(executor.handle(), SeededRng::from_seed(seeds.next_u64()))
            .with_default_link(Link::new(LATENCY));

    // The client is added first, so it is node 0 and the replicas are the nodes after it.
    let client = network.add_node();
    let endpoints: Vec<_> = (0..REPLICAS).map(|_| network.add_node()).collect();
    let peers: Rc<[NodeId]> = endpoints.iter().map(Network::id).collect();
    network.set_faults(faults.clone());

    let log = Rc::new(RefCell::new(Vec::new()));
    for (me, endpoint) in endpoints.into_iter().enumerate() {
        let clock = executor.handle();
        let mut rng = SeededRng::from_seed(seeds.next_u64());
        let mut replica = Replica::new(me, Rc::clone(&peers));
        let observations = Rc::clone(&log);
        executor.spawn(async move {
            replicate(&clock, &mut rng, &endpoint, &mut replica, &observations).await;
        });
    }

    let clock = executor.handle();
    let observations = Rc::clone(&log);
    executor.spawn(async move {
        submit(&clock, &client, &peers, &observations).await;
    });

    executor.run()?;
    // Every task is finished and nothing is polling, so nothing else holds the log open.
    Ok(core::mem::take(&mut *log.borrow_mut()))
}

/// What one step changed in the run's view.
enum Change {
    /// A replica's state, as it stood straight after the change.
    Replica(usize, Recorded),
    /// The client sent a command for the first time.
    Sent(u64),
    /// The client heard a command was done.
    Done(u64),
}

/// Writes down what changed, on top of what the run knew before.
///
/// Nothing here waits, so the borrow is given up before whoever wrote goes on and is never held
/// across a poll.
fn write(
    observations: &RefCell<Vec<Observation>>,
    at: VirtualTime,
    message: String,
    change: Change,
) {
    let mut log = observations.borrow_mut();
    let mut view = log
        .last()
        .map_or_else(View::default, |last: &Observation| last.view.clone());
    match change {
        Change::Replica(me, recorded) => {
            view.replicas.insert(me, recorded);
        }
        Change::Sent(command) => {
            view.client.insert(
                command,
                Submission {
                    sent: at,
                    done: None,
                },
            );
        }
        Change::Done(command) => {
            if let Some(submission) = view.client.get_mut(&command) {
                submission.done = Some(at);
            }
        }
    }
    log.push(Observation { at, message, view });
}

/// A replica: answer whatever arrives, act when the timer runs out, and stop at the run's end.
async fn replicate<C, R, N>(
    clock: &C,
    rng: &mut R,
    endpoint: &N,
    replica: &mut Replica,
    observations: &RefCell<Vec<Observation>>,
) where
    C: Clock,
    R: Rng,
    N: Network<Message = Message>,
{
    let end = stop();
    write(
        observations,
        clock.now(),
        format!("{} started as a follower in term 0", node(replica.me)),
        Change::Replica(replica.me, replica.recorded.clone()),
    );
    let mut timer = after(clock.now(), rng.duration_in(ELECTION));
    while clock.now() < end {
        let wake = timer.min(end);
        let out = match clock
            .timeout(until(clock.now(), wake), endpoint.recv())
            .await
        {
            Ok(delivery) => {
                let from = delivery.sender();
                replica.on_message(from, delivery.into_message())
            }
            Err(_) if clock.now() >= end => break,
            Err(_) => replica.on_timer(),
        };
        let now = clock.now();
        for (to, message) in out.sends {
            endpoint.send(to, message);
        }
        for (message, recorded) in out.notes {
            write(
                observations,
                now,
                message,
                Change::Replica(replica.me, recorded),
            );
        }
        match out.rearm {
            Rearm::Keep => {}
            Rearm::Heartbeat => timer = after(now, HEARTBEAT),
            Rearm::Election => timer = after(now, rng.duration_in(ELECTION)),
        }
    }
}

/// The client: send each command in turn, and keep asking until it is done or the run ends.
async fn submit<C, N>(
    clock: &C,
    endpoint: &N,
    replicas: &[NodeId],
    observations: &RefCell<Vec<Observation>>,
) where
    C: Clock,
    N: Network<Message = Message>,
{
    let end = stop();
    let mut target = 0;
    for command in 1..=COMMANDS {
        clock.sleep_until(opens(command)).await;
        if clock.now() >= end {
            return;
        }
        write(
            observations,
            clock.now(),
            format!("client sent command {command}"),
            Change::Sent(command),
        );
        loop {
            if clock.now() >= end {
                return;
            }
            let Some(&to) = replicas.get(target) else {
                return;
            };
            endpoint.send(to, Message::Submit { command });
            match hear(
                clock,
                endpoint,
                command,
                after(clock.now(), PATIENCE).min(end),
            )
            .await
            {
                Some(Answer::Committed) => {
                    write(
                        observations,
                        clock.now(),
                        format!("client heard command {command} is done"),
                        Change::Done(command),
                    );
                    break;
                }
                Some(Answer::NotLeader(Some(hint))) => {
                    target = replicas
                        .iter()
                        .position(|replica| *replica == hint)
                        .unwrap_or((target + 1) % replicas.len());
                }
                Some(Answer::NotLeader(None)) | None => target = (target + 1) % replicas.len(),
            }
        }
    }
}

/// Waits until `deadline` for an answer about `command`, setting aside answers about commands gone
/// by, which a replica that heard a retry twice can still send.
async fn hear<C, N>(clock: &C, endpoint: &N, command: u64, deadline: VirtualTime) -> Option<Answer>
where
    C: Clock,
    N: Network<Message = Message>,
{
    loop {
        let delivery = clock
            .timeout(until(clock.now(), deadline), endpoint.recv())
            .await
            .ok()?;
        if let Message::Submitted {
            command: about,
            answer,
        } = delivery.into_message()
            && about == command
        {
            return Some(answer);
        }
    }
}

/// Reads the run's verdict off the world each step recorded: the first breach of its invariants.
fn verdict(trace: &Trace, store: &StateStore) -> Result<Outcome, RunError> {
    let broken = invariant::check(trace, store, &invariants()?)?;
    Ok(broken
        .into_iter()
        .next()
        .map_or(Outcome::Pass, Outcome::from))
}

/// Turns what the run observed into steps, keeping every state it passed through in a store.
fn collect(observed: Vec<Observation>) -> Result<(Vec<Step>, StateStore), RunError> {
    let mut store = StateStore::new();
    let mut steps = Vec::with_capacity(observed.len());
    for step in observed {
        let event = Entry::new(step.at, step.message)?;
        steps.push(Step::new(
            event,
            store.insert(&world(&step.view)?.snapshot()),
        ));
    }
    Ok((steps, store))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault::{Fault, Window};

    /// The seed the cases run, since none of them is about a particular one.
    const SEED: u64 = 20_261_006;

    /// The client's address, which is the first node the run adds.
    fn client() -> NodeId {
        NodeId::from_index(0)
    }

    /// The address of the replica at `index` among the replicas, counting from zero.
    fn peer(index: usize) -> NodeId {
        NodeId::from_index(index as u64 + 1)
    }

    /// Every replica's address, in the order the run adds them.
    fn peers() -> Rc<[NodeId]> {
        (0..REPLICAS).map(peer).collect()
    }

    /// The replica at `me`, fresh.
    fn replica(me: usize) -> Replica {
        Replica::new(me, peers())
    }

    /// An entry taking `command` in `term`.
    fn command(term: u64, command: u64) -> LogEntry {
        LogEntry {
            term,
            op: Op::Command(command),
        }
    }

    /// The replica at `me`, leading `term` with `log`, every follower assumed to hold nothing yet.
    fn leader(me: usize, term: u64, log: Vec<LogEntry>) -> Replica {
        let mut leader = replica(me);
        let last = index(log.len());
        leader.recorded.term = term;
        leader.recorded.role = Role::Leader;
        leader.recorded.voted_for = Some(me);
        leader.recorded.log = log;
        leader.recorded.led.insert(term);
        leader.leader = Some(me);
        leader.next = vec![last + 1; REPLICAS];
        leader.matched = vec![0; REPLICAS];
        leader.matched[me] = last;
        leader
    }

    /// A copy from a leader in `term`, following the entry at `prev_index` from `prev_term`.
    fn append(
        term: u64,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<LogEntry>,
        commit: u64,
    ) -> Message {
        Message::Append {
            term,
            prev_index,
            prev_term,
            entries,
            commit,
        }
    }

    /// The messages `out` sends, and to whom.
    fn sent(out: &Out) -> Vec<(NodeId, Message)> {
        out.sends.clone()
    }

    /// What `out` says changed, without the states.
    fn said(out: &Out) -> Vec<&str> {
        out.notes
            .iter()
            .map(|(message, _)| message.as_str())
            .collect()
    }

    /// A name, failing the test if it is not one.
    fn name(name: &str) -> Name {
        Name::new(name).unwrap_or_else(|e| panic!("{name:?} is a name: {e}"))
    }

    /// Runs the system, failing the test rather than returning an error no case expects.
    fn runs(seed: u64, faults: &FaultSchedule) -> (Trace, StateStore, Outcome) {
        run(seed, faults).unwrap_or_else(|e| panic!("seed {seed} did not finish: {e}"))
    }

    /// The world the last step of `trace` recorded, read back out of `store`.
    fn last_world(trace: &Trace, store: &StateStore) -> World {
        let last = trace
            .steps()
            .last()
            .unwrap_or_else(|| panic!("a run writes something down"));
        let node = store
            .get(last.state())
            .unwrap_or_else(|| panic!("the store keeps every state the trace names"));
        World::try_from(&node).unwrap_or_else(|e| panic!("a recorded state is a world: {e}"))
    }

    /// Each step's message and instant, for the steps whose message starts with `start`.
    fn steps_saying(trace: &Trace, start: &str) -> Vec<(VirtualTime, String)> {
        trace
            .steps()
            .iter()
            .filter(|step| step.event().message().starts_with(start))
            .map(|step| (step.event().at(), step.event().message().to_owned()))
            .collect()
    }

    /// Which replica led first, read off a run's steps.
    fn first_leader(trace: &Trace) -> usize {
        trace
            .steps()
            .iter()
            .find_map(|step| {
                let message = step.event().message();
                let (who, _) = message.split_once(" became leader")?;
                who.strip_prefix(NODE)?
                    .parse::<usize>()
                    .ok()?
                    .checked_sub(1)
            })
            .unwrap_or_else(|| panic!("a run with a leader names it"))
    }

    /// A schedule cutting `cut` off from every other node, both ways, for `during`.
    fn isolating(cut: &[usize], during: Window) -> FaultSchedule {
        let others =
            (0..=REPLICAS as u64).filter(|node| !cut.iter().any(|c| *c as u64 + 1 == *node));
        let mut faults = Vec::new();
        for other in others {
            for &replica in cut {
                let (one, two) = (peer(replica), NodeId::from_index(other));
                faults.push(Fault::Partition {
                    from: one,
                    to: two,
                    during,
                });
                faults.push(Fault::Partition {
                    from: two,
                    to: one,
                    during,
                });
            }
        }
        FaultSchedule::new(faults)
    }

    /// The window from `start` seconds to `end` seconds.
    fn seconds(start: u64, end: u64) -> Window {
        Window::new(
            VirtualTime::from_nanos(start * 1_000_000_000),
            VirtualTime::from_nanos(end * 1_000_000_000),
        )
        .unwrap_or_else(|e| panic!("a test window is a window: {e}"))
    }

    #[test]
    fn a_follower_whose_timer_runs_out_stands_and_asks_everyone_else() {
        let mut follower = replica(2);
        let out = follower.on_timer();

        assert_eq!(follower.recorded.term, 1);
        assert_eq!(follower.recorded.role, Role::Candidate);
        assert_eq!(follower.recorded.voted_for, Some(2), "it votes for itself");
        assert_eq!(said(&out), ["node-3 became candidate for term 1"]);
        assert_eq!(
            sent(&out),
            [0, 1, 3, 4]
                .map(|other| (peer(other), Message::RequestVote { term: 1 }))
                .to_vec(),
            "everyone but itself"
        );
        assert_eq!(
            out.rearm,
            Rearm::Election,
            "an election it cannot win times out too"
        );
    }

    #[test]
    fn a_replica_votes_at_most_once_a_term_and_never_backwards() {
        struct Case {
            name: &'static str,
            term: u64,
            voted_for: Option<usize>,
            asked_in: u64,
            ballot: Ballot,
            term_after: u64,
            voted_after: Option<usize>,
            notes: usize,
        }
        // The candidate is always the replica at 1, asking the replica at 0.
        let cases = [
            Case {
                name: "the first to ask in a term has the vote",
                term: 3,
                voted_for: None,
                asked_in: 3,
                ballot: Ballot::Granted,
                term_after: 3,
                voted_after: Some(1),
                notes: 1,
            },
            Case {
                name: "a second candidate in that term does not",
                term: 3,
                voted_for: Some(4),
                asked_in: 3,
                ballot: Ballot::Refused,
                term_after: 3,
                voted_after: Some(4),
                notes: 0,
            },
            Case {
                name: "the same candidate asking twice is answered twice and changes nothing",
                term: 3,
                voted_for: Some(1),
                asked_in: 3,
                ballot: Ballot::Granted,
                term_after: 3,
                voted_after: Some(1),
                notes: 0,
            },
            Case {
                name: "a candidate from a term gone by is refused",
                term: 3,
                voted_for: None,
                asked_in: 2,
                ballot: Ballot::Refused,
                term_after: 3,
                voted_after: None,
                notes: 0,
            },
            Case {
                name: "a later term frees a vote already spent",
                term: 3,
                voted_for: Some(4),
                asked_in: 5,
                ballot: Ballot::Granted,
                term_after: 5,
                voted_after: Some(1),
                notes: 1,
            },
        ];
        for case in cases {
            let mut voter = replica(0);
            voter.recorded.term = case.term;
            voter.recorded.voted_for = case.voted_for;
            let out = voter.on_message(
                peer(1),
                Message::RequestVote {
                    term: case.asked_in,
                },
            );

            assert_eq!(
                sent(&out),
                [(
                    peer(1),
                    Message::Vote {
                        term: case.term_after,
                        ballot: case.ballot
                    }
                )],
                "{}",
                case.name
            );
            assert_eq!(voter.recorded.term, case.term_after, "{}", case.name);
            assert_eq!(voter.recorded.voted_for, case.voted_after, "{}", case.name);
            assert_eq!(out.notes.len(), case.notes, "{}", case.name);
        }
    }

    #[test]
    fn a_vote_is_granted_without_asking_anything_of_the_candidates_log() {
        // The gap the module's docs describe, held where it lives: a replica holding three
        // committed entries grants its vote to a candidate that holds none, because a request for a
        // vote carries a term and nothing else. Raft's own rule would refuse it.
        let mut voter = replica(0);
        voter.recorded.term = 2;
        voter.recorded.log = vec![command(1, 1), command(1, 2), command(2, 3)];
        voter.recorded.commit = 3;
        let out = voter.on_message(peer(4), Message::RequestVote { term: 9 });

        assert_eq!(
            sent(&out),
            [(
                peer(4),
                Message::Vote {
                    term: 9,
                    ballot: Ballot::Granted
                }
            )]
        );
        assert_eq!(said(&out), ["node-1 voted for node-5 in term 9"]);
    }

    #[test]
    fn a_majority_of_votes_makes_a_leader_that_appends_a_no_op_and_copies_it_out() {
        let mut candidate = replica(0);
        candidate.on_timer();
        let granted = Message::Vote {
            term: 1,
            ballot: Ballot::Granted,
        };

        let one = candidate.on_message(peer(1), granted.clone());
        assert_eq!(
            candidate.recorded.role,
            Role::Candidate,
            "two of five is not a majority"
        );
        assert!(one.notes.is_empty());

        let refused = candidate.on_message(
            peer(2),
            Message::Vote {
                term: 1,
                ballot: Ballot::Refused,
            },
        );
        assert_eq!(
            candidate.recorded.role,
            Role::Candidate,
            "a refusal counts for nothing"
        );
        assert!(refused.notes.is_empty());

        let won = candidate.on_message(peer(3), granted);
        assert_eq!(candidate.recorded.role, Role::Leader);
        assert_eq!(said(&won), ["node-1 became leader of term 1"]);
        assert_eq!(
            candidate.recorded.log,
            [LogEntry {
                term: 1,
                op: Op::Noop
            }]
        );
        assert!(
            candidate.recorded.led.contains(&1),
            "the ledger has its term"
        );
        assert_eq!(won.rearm, Rearm::Heartbeat);
        assert_eq!(
            sent(&won),
            [1, 2, 3, 4]
                .map(|other| (
                    peer(other),
                    append(
                        1,
                        0,
                        0,
                        vec![LogEntry {
                            term: 1,
                            op: Op::Noop
                        }],
                        0
                    )
                ))
                .to_vec()
        );
    }

    #[test]
    fn a_leader_that_hears_of_a_later_term_steps_down_and_forgets_who_it_owed_answers() {
        let mut stale = leader(0, 2, vec![command(2, 1)]);
        stale.pending.insert(1, (client(), 1));
        let out = stale.on_message(
            peer(3),
            Message::Appended {
                term: 4,
                matched: None,
            },
        );

        assert_eq!(stale.recorded.role, Role::Follower);
        assert_eq!(stale.recorded.term, 4);
        assert_eq!(stale.recorded.voted_for, None);
        assert!(stale.pending.is_empty());
        assert!(
            sent(&out).is_empty(),
            "the client finds out by hearing nothing"
        );
        assert_eq!(said(&out), ["node-1 is a follower in term 4"]);
        assert_eq!(out.rearm, Rearm::Election);
    }

    /// A follower handed one copy, and what it should make of it.
    struct CopyCase {
        name: &'static str,
        log: Vec<LogEntry>,
        commit: u64,
        copy: Message,
        log_after: Vec<LogEntry>,
        commit_after: u64,
        matched: Option<u64>,
        said: Vec<&'static str>,
    }

    /// The cases of `a_follower_takes_a_copy_only_after_an_entry_it_holds`.
    fn copy_cases() -> Vec<CopyCase> {
        vec![
            CopyCase {
                name: "a copy after an entry it does not have is refused",
                log: vec![command(1, 1)],
                commit: 0,
                copy: append(2, 2, 1, vec![command(2, 3)], 0),
                log_after: vec![command(1, 1)],
                commit_after: 0,
                matched: None,
                said: vec!["node-2 is a follower in term 2"],
            },
            CopyCase {
                name: "a copy after an entry from another term is refused",
                log: vec![command(1, 1)],
                commit: 0,
                copy: append(1, 1, 2, vec![command(2, 3)], 0),
                log_after: vec![command(1, 1)],
                commit_after: 0,
                matched: None,
                said: vec![],
            },
            CopyCase {
                name: "a conflicting entry is replaced, with everything after it",
                log: vec![command(1, 1), command(1, 2), command(1, 3)],
                commit: 1,
                copy: append(2, 1, 1, vec![command(2, 4)], 1),
                log_after: vec![command(1, 1), command(2, 4)],
                commit_after: 1,
                matched: Some(2),
                said: vec!["node-2 dropped entries after 1 and appended entry 2"],
            },
            CopyCase {
                name: "an entry it already holds keeps what follows it",
                log: vec![command(1, 1), command(1, 2)],
                commit: 0,
                copy: append(1, 0, 0, vec![command(1, 1)], 0),
                log_after: vec![command(1, 1), command(1, 2)],
                commit_after: 0,
                matched: Some(1),
                said: vec![],
            },
            CopyCase {
                name: "the leader's commit is taken only as far as the copy reaches",
                log: vec![],
                commit: 0,
                copy: append(1, 0, 0, vec![command(1, 1), command(1, 2)], 5),
                log_after: vec![command(1, 1), command(1, 2)],
                commit_after: 2,
                matched: Some(2),
                said: vec![
                    "node-2 appended entries 1 to 2",
                    "node-2 committed through 2",
                ],
            },
            CopyCase {
                name: "a commit is never taken back",
                log: vec![command(1, 1), command(1, 2), command(1, 3)],
                commit: 3,
                copy: append(1, 3, 1, vec![], 1),
                log_after: vec![command(1, 1), command(1, 2), command(1, 3)],
                commit_after: 3,
                matched: Some(3),
                said: vec![],
            },
        ]
    }

    #[test]
    fn a_follower_takes_a_copy_only_after_an_entry_it_holds() {
        for case in copy_cases() {
            let mut follower = replica(1);
            follower.recorded.term = 1;
            follower.recorded.log = case.log;
            follower.recorded.commit = case.commit;
            follower.recorded.applied = follower
                .recorded
                .log
                .iter()
                .take(usize::try_from(case.commit).unwrap_or(0))
                .map(|entry| entry.op)
                .collect();
            let term = case.copy.term().unwrap_or(0);
            let out = follower.on_message(peer(0), case.copy);

            assert_eq!(follower.recorded.log, case.log_after, "{}", case.name);
            assert_eq!(follower.recorded.commit, case.commit_after, "{}", case.name);
            assert_eq!(
                sent(&out),
                [(
                    peer(0),
                    Message::Appended {
                        term,
                        matched: case.matched
                    }
                )],
                "{}",
                case.name
            );
            assert_eq!(said(&out), case.said, "{}", case.name);
            assert_eq!(follower.leader, Some(0), "{}", case.name);
            assert_eq!(
                out.rearm,
                Rearm::Election,
                "{}: a leader was heard",
                case.name
            );
        }
    }

    #[test]
    fn a_follower_applies_what_is_committed_in_order_and_keeps_it() {
        let mut follower = replica(1);
        follower.on_message(
            peer(0),
            append(1, 0, 0, vec![command(1, 7), command(1, 8)], 1),
        );
        assert_eq!(follower.recorded.applied, [Op::Command(7)]);

        // A copy that replaces an applied entry cannot take the applying back: what was applied
        // stays applied, which is what lets a world show the change at all.
        follower.on_message(peer(2), append(2, 0, 0, vec![command(2, 9)], 0));
        assert_eq!(follower.recorded.log, [command(2, 9)]);
        assert_eq!(follower.recorded.applied, [Op::Command(7)]);
    }

    #[test]
    fn a_copy_from_a_term_gone_by_is_refused_with_the_later_term() {
        let mut follower = replica(1);
        follower.recorded.term = 4;
        let out = follower.on_message(peer(0), append(3, 0, 0, vec![command(3, 1)], 0));

        assert!(follower.recorded.log.is_empty());
        assert_eq!(
            sent(&out),
            [(
                peer(0),
                Message::Appended {
                    term: 4,
                    matched: None
                }
            )]
        );
        assert_eq!(
            out.rearm,
            Rearm::Keep,
            "a stale leader is not a leader heard"
        );
    }

    #[test]
    fn a_leader_commits_only_entries_from_its_own_term_by_counting() {
        // An entry from an earlier term held by a majority can still be replaced by a later leader
        // that never saw it, so counting copies of it commits nothing. It is committed only along
        // with an entry of the leader's own term that a majority holds.
        let mut leading = leader(0, 3, vec![command(1, 1), command(2, 2)]);
        for follower in [1, 2] {
            leading.on_message(
                peer(follower),
                Message::Appended {
                    term: 3,
                    matched: Some(2),
                },
            );
        }
        assert_eq!(
            leading.recorded.commit, 0,
            "a majority of an old term's entry is not enough"
        );

        leading.recorded.log.push(command(3, 3));
        leading.matched[0] = 3;
        leading.on_message(
            peer(1),
            Message::Appended {
                term: 3,
                matched: Some(3),
            },
        );
        assert_eq!(leading.recorded.commit, 0, "two of five is not a majority");
        let out = leading.on_message(
            peer(2),
            Message::Appended {
                term: 3,
                matched: Some(3),
            },
        );
        assert_eq!(leading.recorded.commit, 3);
        assert_eq!(said(&out), ["node-1 committed through 3"]);
        assert_eq!(
            leading.recorded.applied,
            [Op::Command(1), Op::Command(2), Op::Command(3)]
        );
    }

    #[test]
    fn a_leader_answers_the_client_once_its_command_is_committed() {
        let mut leading = leader(
            0,
            1,
            vec![LogEntry {
                term: 1,
                op: Op::Noop,
            }],
        );
        let took = leading.on_message(client(), Message::Submit { command: 4 });
        assert_eq!(said(&took), ["node-1 took command 4 as entry 2"]);
        assert_eq!(
            took.sends.len(),
            REPLICAS - 1,
            "the new entry is copied out at once"
        );
        assert!(
            took.sends.iter().all(|(to, _)| *to != client()),
            "nothing is said to the client until it is committed"
        );

        let again = leading.on_message(client(), Message::Submit { command: 4 });
        assert_eq!(leading.recorded.log.len(), 2, "a retry is not taken twice");
        assert!(again.notes.is_empty());

        for follower in [1, 2] {
            let out = leading.on_message(
                peer(follower),
                Message::Appended {
                    term: 1,
                    matched: Some(2),
                },
            );
            if follower == 2 {
                assert!(
                    out.sends.contains(&(
                        client(),
                        Message::Submitted {
                            command: 4,
                            answer: Answer::Committed
                        }
                    )),
                    "the third copy commits it"
                );
            }
        }

        let late = leading.on_message(client(), Message::Submit { command: 4 });
        assert_eq!(
            sent(&late),
            [(
                client(),
                Message::Submitted {
                    command: 4,
                    answer: Answer::Committed
                }
            )],
            "a retry of a committed command is told so straight away"
        );
    }

    #[test]
    fn a_replica_that_does_not_lead_points_the_client_at_the_one_that_does() {
        struct Case {
            name: &'static str,
            leader: Option<usize>,
            answer: Answer,
        }
        let cases = [
            Case {
                name: "a follower that has heard from a leader",
                leader: Some(3),
                answer: Answer::NotLeader(Some(peer(3))),
            },
            Case {
                name: "a follower that has not",
                leader: None,
                answer: Answer::NotLeader(None),
            },
        ];
        for case in cases {
            let mut follower = replica(1);
            follower.leader = case.leader;
            let out = follower.on_message(client(), Message::Submit { command: 2 });

            assert_eq!(
                sent(&out),
                [(
                    client(),
                    Message::Submitted {
                        command: 2,
                        answer: case.answer
                    }
                )],
                "{}",
                case.name
            );
            assert!(follower.recorded.log.is_empty(), "{}", case.name);
        }
    }

    #[test]
    fn a_leader_whose_copy_is_refused_backs_up_one_entry_and_tries_again() {
        let mut leading = leader(0, 2, vec![command(1, 1), command(2, 2)]);
        let out = leading.on_message(
            peer(4),
            Message::Appended {
                term: 2,
                matched: None,
            },
        );
        assert_eq!(
            sent(&out),
            [(peer(4), append(2, 1, 1, vec![command(2, 2)], 0))]
        );
    }

    #[test]
    fn a_leader_copies_its_log_on_every_heartbeat() {
        let mut leading = leader(2, 1, vec![command(1, 1)]);
        let out = leading.on_timer();

        assert_eq!(out.rearm, Rearm::Heartbeat);
        assert!(
            out.notes.is_empty(),
            "a heartbeat changes nothing about the leader"
        );
        assert_eq!(
            sent(&out),
            [0, 1, 3, 4]
                .map(|other| (peer(other), append(1, 1, 1, vec![], 0)))
                .to_vec()
        );
    }

    /// A replica's recorded state, built by hand, for the invariant cases.
    fn recorded(log: Vec<LogEntry>, applied: Vec<Op>, led: &[u64]) -> Recorded {
        Recorded {
            term: 1,
            role: Role::Follower,
            voted_for: None,
            commit: index(applied.len()),
            log,
            applied,
            led: led.iter().copied().collect(),
        }
    }

    /// The world a view of these replicas and these commands is written as.
    fn written(replicas: Vec<Recorded>, client: &[(u64, bool)]) -> World {
        let view = View {
            replicas: replicas.into_iter().enumerate().collect(),
            client: client
                .iter()
                .map(|&(command, done)| {
                    (
                        command,
                        Submission {
                            sent: VirtualTime::from_nanos(command),
                            done: done.then_some(VirtualTime::from_nanos(command + 1)),
                        },
                    )
                })
                .collect(),
        };
        world(&view).unwrap_or_else(|e| panic!("a view is written with names: {e}"))
    }

    #[test]
    fn a_view_is_written_with_the_fields_the_invariants_read() {
        let mut leading = recorded(vec![command(2, 7)], vec![Op::Command(7)], &[2]);
        leading.role = Role::Leader;
        leading.voted_for = Some(0);
        let world = written(vec![leading], &[(7, true)]);

        let replica = world
            .get(&name("node-1"))
            .unwrap_or_else(|| panic!("the replica is written down"));
        for (field, value) in [
            ("term", Value::Count(1)),
            ("role", Value::Text("leader".to_owned())),
            ("voted-for", Value::Text("node-1".to_owned())),
            ("commit", Value::Count(1)),
            ("entry-0001-term", Value::Count(2)),
            ("entry-0001-op", Value::Text("command 7".to_owned())),
            ("applied-0001", Value::Text("command 7".to_owned())),
            ("led-0002", Value::Flag(true)),
        ] {
            assert_eq!(replica.get(&name(field)), Some(&value), "{field}");
        }
        let client = world
            .get(&name("client"))
            .unwrap_or_else(|| panic!("the client is written down"));
        assert_eq!(
            client.get(&name("command-0007-sent")),
            Some(&Value::Instant(VirtualTime::from_nanos(7)))
        );
        assert_eq!(
            client.get(&name("command-0007-done")),
            Some(&Value::Instant(VirtualTime::from_nanos(8)))
        );
    }

    /// A recorded world, and what each invariant should say of it.
    struct WorldCase {
        name: &'static str,
        world: World,
        one_leader: bool,
        committed: bool,
        finished: bool,
    }

    /// The cases of `each_invariant_breaks_on_the_world_it_is_about_and_holds_on_the_others`.
    fn world_cases() -> Vec<WorldCase> {
        let applied = |ops: &[u64]| -> Vec<Op> { ops.iter().map(|&op| Op::Command(op)).collect() };
        let log = |ops: &[u64]| -> Vec<LogEntry> { ops.iter().map(|&op| command(1, op)).collect() };
        vec![
            WorldCase {
                name: "agreement everywhere",
                world: written(
                    vec![
                        recorded(log(&[1, 2]), applied(&[1, 2]), &[1]),
                        recorded(log(&[1, 2]), applied(&[1]), &[]),
                    ],
                    &[(1, true)],
                ),
                one_leader: true,
                committed: true,
                finished: true,
            },
            WorldCase {
                name: "two replicas each leading a term of its own",
                world: written(
                    vec![
                        recorded(vec![], vec![], &[1, 3]),
                        recorded(vec![], vec![], &[2]),
                    ],
                    &[],
                ),
                one_leader: true,
                committed: true,
                finished: true,
            },
            WorldCase {
                name: "two replicas that led one term",
                world: written(
                    vec![
                        recorded(vec![], vec![], &[2]),
                        recorded(vec![], vec![], &[2]),
                    ],
                    &[],
                ),
                one_leader: false,
                committed: true,
                finished: true,
            },
            WorldCase {
                name: "two replicas that applied different things at one index",
                world: written(
                    vec![
                        recorded(log(&[1]), applied(&[1]), &[]),
                        recorded(log(&[2]), applied(&[2]), &[]),
                    ],
                    &[],
                ),
                one_leader: true,
                committed: false,
                finished: true,
            },
            WorldCase {
                name: "a replica whose log no longer holds what it applied",
                world: written(vec![recorded(log(&[2]), applied(&[1]), &[])], &[]),
                one_leader: true,
                committed: false,
                finished: true,
            },
            WorldCase {
                name: "a replica whose log is shorter than what it applied",
                world: written(vec![recorded(log(&[]), applied(&[1]), &[])], &[]),
                one_leader: true,
                committed: false,
                finished: true,
            },
            WorldCase {
                name: "a command sent and never answered",
                world: written(vec![recorded(log(&[1]), applied(&[1]), &[])], &[(1, false)]),
                one_leader: true,
                committed: true,
                finished: false,
            },
            WorldCase {
                name: "a command answered that one replica has not applied",
                world: written(
                    vec![
                        recorded(log(&[1]), applied(&[1]), &[]),
                        recorded(log(&[1]), applied(&[]), &[]),
                    ],
                    &[(1, true)],
                ),
                one_leader: true,
                committed: true,
                finished: false,
            },
        ]
    }

    #[test]
    fn each_invariant_breaks_on_the_world_it_is_about_and_holds_on_the_others() {
        for case in world_cases() {
            assert_eq!(
                one_leader_a_term(&case.world),
                case.one_leader,
                "{}",
                case.name
            );
            assert_eq!(
                committed_entries_hold(&case.world),
                case.committed,
                "{}",
                case.name
            );
            assert_eq!(
                every_command_committed(&case.world),
                case.finished,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn a_run_that_meets_no_faults_commits_every_command_on_every_replica() {
        let (trace, store, outcome) = runs(SEED, &FaultSchedule::default());

        assert_eq!(outcome, Outcome::Pass);
        assert_eq!(
            steps_saying(&trace, "")
                .iter()
                .filter(|(_, m)| m.contains(" became leader of term "))
                .count(),
            1,
            "the first leader's heartbeat keeps every follower from standing against it"
        );
        // Read off the last recorded world rather than asked of the run, so this is not the
        // liveness invariant checked against itself.
        let world = last_world(&trace, &store);
        for me in 0..REPLICAS {
            let replica = world
                .get(&name(&node(me)))
                .unwrap_or_else(|| panic!("{} is written down", node(me)));
            let applied: Vec<&Value> = replica
                .fields()
                .filter(|(field, _)| field.as_str().starts_with(APPLIED))
                .map(|(_, value)| value)
                .collect();
            for command in 1..=COMMANDS {
                assert!(
                    applied.contains(&&Value::Text(format!("command {command}"))),
                    "{} applied command {command}",
                    node(me)
                );
            }
        }
    }

    #[test]
    fn a_client_no_replica_hears_leaves_every_command_uncommitted() {
        let cut: Vec<Fault> = (0..REPLICAS)
            .map(|replica| Fault::Partition {
                from: client(),
                to: peer(replica),
                during: Window::forever_from(VirtualTime::ZERO),
            })
            .collect();
        let (trace, _, outcome) = runs(SEED, &FaultSchedule::new(cut));

        assert_eq!(
            outcome,
            Outcome::Fail {
                reason: Reason::new("did not commit every command")
                    .unwrap_or_else(|e| panic!("a reason is a reason: {e}")),
                step: trace.steps().len() - 1,
            },
            "a promise about the end of the run is broken at its last step"
        );
    }

    #[test]
    fn an_isolated_leader_is_replaced_and_the_commands_still_commit() {
        // Which replica leads first is the seed's doing, so it is read off the run with nothing in
        // its way rather than assumed. A fault draws exactly what the link would have, so the run
        // under the cut is that same run up to the instant the cut opens.
        let (calm, _, _) = runs(SEED, &FaultSchedule::default());
        let first = first_leader(&calm);
        let (trace, _, outcome) = runs(SEED, &isolating(&[first], seconds(5, 12)));

        let later: Vec<_> = steps_saying(&trace, "")
            .into_iter()
            .filter(|(_, message)| message.contains(" became leader of term "))
            .collect();
        assert!(later.len() >= 2, "a second leader stood: {later:?}");
        assert!(
            later
                .iter()
                .skip(1)
                .all(|(_, message)| !message.starts_with(&format!("{} ", node(first)))),
            "and it was not the one cut off: {later:?}"
        );
        assert_eq!(outcome, Outcome::Pass);
    }

    #[test]
    fn a_leader_cut_off_with_a_minority_commits_nothing_while_it_lasts() {
        let (calm, _, _) = runs(SEED, &FaultSchedule::default());
        let first = first_leader(&calm);
        let with = (first + 1) % REPLICAS;
        let (trace, _, _) = runs(SEED, &isolating(&[first, with], seconds(5, 15)));

        for cut in [first, with] {
            let committed: Vec<_> = steps_saying(&trace, &format!("{} committed", node(cut)))
                .into_iter()
                .filter(|(at, _)| {
                    // Anything already on its way in at five seconds may still land.
                    *at > after(
                        VirtualTime::from_nanos(5_000_000_000),
                        Duration::from_millis(100),
                    ) && *at < VirtualTime::from_nanos(15_000_000_000)
                })
                .collect();
            assert!(
                committed.is_empty(),
                "{} committed {committed:?}",
                node(cut)
            );
        }
    }

    #[test]
    fn different_seeds_run_different_logs() {
        // Compared on the steps rather than the written form, which names its seed. This says only
        // that there was a difference; the recorded trace in `tests/replog.rs` says what the steps
        // are.
        assert_ne!(
            runs(0, &FaultSchedule::default()).0.steps(),
            runs(1, &FaultSchedule::default()).0.steps()
        );
    }
}
