//! A replicated log: five replicas electing a leader and copying three clients' commands to each
//! other.
//!
//! The reconciler is a control loop, and everything else in this crate before it was a protocol
//! too small to go wrong in an interesting way. This is a system with nothing in common with the
//! reconciler — no regions, no listing, no timeline of what is wanted — run on the same engine, so
//! what the engine offers is not shaped around one workload.
//!
//! # The protocol
//!
//! It is Raft's shape. Each replica is a **follower**, a **candidate** or a **leader**, in a
//! numbered **term**. A follower that hears nothing from a leader for an election timeout first asks
//! every other replica whether it could stand in the next term — a **pre-vote**, which moves no term
//! and spends no vote — and only once a majority says it could does it become a candidate in that
//! term, vote for itself and ask every other replica for its vote; a replica votes at most once a
//! term, and a candidate that collects a majority leads that term. A
//! leader appends a no-op to its log the moment it is elected, takes each command a client sends
//! it as the next entry of its log, and copies its log to every follower on a heartbeat. A follower
//! accepts entries only after the entry they follow, which it must already hold, and an entry that
//! conflicts with what the follower holds replaces it and everything after it. An entry is
//! **committed** once a majority holds it and it is from the leader's own term; a replica applies
//! committed entries in order, and applying is not something a replica can take back. Any message
//! from a later term makes its receiver a follower in that term.
//!
//! # Why a stale replica cannot lead
//!
//! A candidate asks for a vote with its term and with where its log ends: the index of its last
//! entry and the term that entry is from. A replica refuses a candidate whose log is behind its
//! own — last entry from an earlier term, or from the same term and shorter — and a refusal does
//! not spend its vote for the term. A committed entry is held by a majority, and a leader needs a
//! majority's votes, so at least one of its voters holds the entry. Together with a leader counting
//! only its own term's entries towards a commit, that is Raft's argument that every later leader's
//! log holds every committed entry — an argument this module relies on and does not prove.
//! Without the rule, a replica that spent a partition alone — timing out, standing, failing,
//! standing again, its term climbing all the while — would come back with a term every other
//! replica must defer to and a log missing whatever was committed while it was away, and win as
//! readily as any other. With it alone, it stands, takes the others to its term, and is turned away
//! by each of them — deposing whichever leader they had on the way.
//!
//! # Why a replica that cannot hear cannot depose
//!
//! Every term a message carries is one its receiver must take, so a replica standing in a term
//! nobody else has reached makes every replica it asks a follower in that term, the leader among
//! them. A replica that can send and cannot hear — one direction of its links cut — never hears a
//! leader, stands every time its timer runs out, and would depose every leader for as long as it
//! stayed deaf. The pre-vote is what stops it: the term a replica asks about is one nobody has
//! begun, so it is not taken; the answer is yes only from a replica whose own log is no further on
//! and which is following no leader; and the asker stands only on a majority's yes, which a replica
//! that cannot hear never receives. The same keeps a replica back from a partition alone from
//! deposing the leader it returns to, since its term never climbed while it was away. The log rule
//! is asked twice, of the pre-vote and of the vote itself: a pre-vote promises nothing, and the
//! voters' logs may have moved on between the two.
//!
//! What is still missing is the other half of the same failure, **check-quorum**: a leader that can
//! send and cannot hear keeps every follower — its heartbeats arrive — and commits nothing for as
//! long as it stays deaf, since nobody's answers reach it. Nothing in this module notices.
//!
//! # The run
//!
//! [`run`] starts three clients and the five replicas over the simulated network. The clients are
//! nodes 0 to 2 and the replicas nodes 3 to 7, and each is written with its own node number
//! wherever the run writes it down — `client-0` to `client-2`, `node-3` to `node-7` — so a fault on
//! `node 7 -> node 3` is about the replicas the trace calls `node-7` and `node-3`.
//!
//! The clients send [`ROUNDS`] rounds of commands, one command each a round, numbered across all
//! three: client `c`'s command in round `k` is `3(k - 1) + c + 1`. Every client's command in a round
//! is sent no earlier than the round's own place on a fixed period, and later only if that client's
//! command before it is not done yet. A client is sequential on its own and the three overlap with
//! each other, so each round puts three commands in flight together and the log is free to order
//! them however it likes. A client sends to whichever replica it believes leads; it follows a
//! replica's hint to the leader, and moves on to the next replica when it is given none or hears
//! nothing back in time. It is told a command is done only once the command is committed.
//!
//! The wire is **dependable**, for the reason `quorum` gives: it takes time and nothing else, so an
//! injected [`FaultSchedule`] is the only trouble a run meets. With none, the first leader keeps its
//! place for the whole run, since its heartbeat arrives well inside any follower's timeout. The seed
//! reaches a run through how long each message spends on the wire and through each replica's
//! election timeouts, drawn afresh every time a replica's timer is set, from a generator of the
//! replica's own. The network's generator is drawn from the run's seed first and the replicas'
//! after it, in node order, and that order is part of every history this module records. The
//! clients draw nothing.
//!
//! Every change a replica makes to what it holds is a step of the trace, and so are a client
//! sending a command and hearing it is done. Each loop stops at a fixed instant, since heartbeats
//! keep the wire busy for as long as a leader stands and a replica waiting for quiet would wait
//! forever.
//!
//! # The verdict
//!
//! A run is judged by four [`Invariant`]s over the world each step recorded, and the verdict is
//! the first breach [`invariant::check`] finds:
//!
//! - **"two leaders in one term"**, a safety promise: each replica carries a `led-<term>` flag for
//!   every term it led, a fact about the run rather than something the replica remembers, so two
//!   replicas carrying the same one is visible in a single world.
//! - **"a committed entry changed"**, a safety promise: every replica's `applied-<index>` agrees
//!   with every other's at that index, and with what that replica's own log holds there.
//! - **"not linearizable"**, a safety promise judged from the clients' side rather than the
//!   replicas': every replica applied each command at most once, applied only commands a client
//!   sent, and never applied a command ahead of one that was done before it was sent.
//! - **"did not commit every command"**, a liveness promise with a span too long for virtual time,
//!   so only the end of the run can break it: every command a client sent was answered, and every
//!   replica has applied it.
//!
//! # Linearizability, for a log
//!
//! A history is linearizable when its operations can be put in one order, each taking effect at a
//! single moment between being sent and being answered, that a log on its own would have produced.
//! A command here answers with nothing but "done", so the only order there is to choose is the
//! order the entries were applied in — and the replicas wrote that down. Checking it against what
//! the clients saw is therefore the whole of the check, not a shortcut through a search: a command
//! answered before another was sent must be applied before it, commands in flight together may go
//! either way, and a command not yet answered may have taken effect or not.
//!
//! What it does not reach: each client is sequential on its own, so the only commands in flight
//! together are different clients' commands in one round, and a reply carries no value a later
//! read could contradict.
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

/// How many clients send commands. They are the first nodes the run adds, ahead of the replicas.
const CLIENTS: usize = 3;

/// How many commands each client sends, one a round.
const ROUNDS: u64 = 5;

/// How far apart the rounds are, at the least. A client's command in round `k` is sent no earlier
/// than `k` of these after the start of the run, and later only if its command before it is not
/// done yet.
const PERIOD: Duration = Duration::from_secs(2);

/// How often a leader copies its log to its followers, whether or not it has anything new.
///
/// A heartbeat and the slowest message across take well under the shortest [`ELECTION`] timeout,
/// so a follower that can hear its leader never stands against it.
const HEARTBEAT: Duration = Duration::from_millis(500);

/// How long a follower or a candidate waits to hear from a leader before asking whether it could
/// stand, drawn afresh every time the wait starts.
///
/// Drawn rather than fixed so that two replicas whose timeouts started together do not stand
/// together again and split the vote forever; three times the [`HEARTBEAT`] at the least, so a
/// heartbeat missed by a whisker does not start an election.
const ELECTION: RangeInclusive<Duration> =
    Duration::from_millis(1_500)..=Duration::from_millis(3_000);

/// How long a client waits to hear that a command is done before asking another replica.
///
/// Longer than a request across, a round of copying and the answer back, each as slow as the wire
/// can be, so a leader that can reach a majority answers in time.
const PATIENCE: Duration = Duration::from_secs(1);

/// The instant every loop in the run stops at, counted from its start.
///
/// The last round is sent at ten seconds, so this leaves twenty for whatever trouble a run is
/// put through to clear and for every replica to hear the last commit.
const STOP: Duration = Duration::from_secs(30);

/// What the dependable wire takes to carry a message.
const LATENCY: RangeInclusive<Duration> = Duration::from_millis(10)..=Duration::from_millis(100);

/// What each client is called where the run writes down what it knows, with its node number after
/// it.
const CLIENT: &str = "client-";

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

/// The start of the fields describing one of a client's commands, with its number after it.
const COMMAND: &str = "command-";

/// The end of the field holding when a client first sent a command.
const SENT: &str = "-sent";

/// The end of the field holding when a client heard a command was done.
const DONE: &str = "-done";

/// Returns the instant the commands of round `round` may be sent from.
fn opens(round: u64) -> VirtualTime {
    VirtualTime::from_nanos(round.saturating_mul(as_nanos(PERIOD)))
}

/// Returns the number of the command the client at `client` sends in round `round`, counting
/// rounds from one: the clients' commands in one round are numbered one after another.
fn numbered(round: u64, client: usize) -> u64 {
    let client = index(client);
    round
        .saturating_sub(1)
        .saturating_mul(index(CLIENTS))
        .saturating_add(client)
        .saturating_add(1)
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

/// Returns what the replica at `index` among the replicas is called: its node number, which comes
/// after every client's.
fn node(index: usize) -> String {
    format!("{NODE}{}", index.saturating_add(CLIENTS))
}

/// Returns what the client at `index` among the clients is called, which is its node number.
fn client(index: usize) -> String {
    format!("{CLIENT}{index}")
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
    /// One of the clients' commands, by its number.
    Command(u64),
}

impl Op {
    /// Reads back what [`Op`]'s `Display` wrote, or nothing if `text` is not something it writes.
    fn read(text: &str) -> Option<Self> {
        if text == "no-op" {
            return Some(Self::Noop);
        }
        text.strip_prefix("command ")?
            .parse()
            .ok()
            .map(Self::Command)
    }
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

/// A replica's answer to a client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Answer {
    /// The command is committed.
    Committed,
    /// This replica does not lead; the one it believes does, if it knows of one.
    NotLeader(Option<NodeId>),
}

/// Where a log ends: the term its last entry is from, then that entry's index, both 0 for an empty
/// log.
///
/// The fields are declared in that order so that the derived ordering *is* the rule for which of
/// two logs is the more up to date — the later last term, and between two equal ones the longer
/// log — and nothing that compares two of them can put the halves the wrong way round.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct LogEnd {
    term: u64,
    index: u64,
}

/// What travels between the replicas, and between a replica and a client.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Message {
    /// A replica whose timer ran out asking whether it could stand in `proposed`, the term after
    /// its own, saying where its log ends.
    ///
    /// `proposed` is a term nobody has begun, so unlike every other term a message carries it is
    /// not one its receiver takes.
    PreVote { proposed: u64, end: LogEnd },
    /// A replica's answer to a question about `proposed`, with the replica's own term.
    PreVoted {
        term: u64,
        proposed: u64,
        ballot: Ballot,
    },
    /// A candidate asking for a vote in its term, saying where its log ends.
    RequestVote { term: u64, end: LogEnd },
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
    /// A client asking for a command to be committed.
    Submit { command: u64 },
    /// A replica's answer to a client about a command.
    Submitted { command: u64, answer: Answer },
}

impl Message {
    /// The term a message between replicas was sent in; the clients' messages are in none.
    fn term(&self) -> Option<u64> {
        match self {
            Self::PreVoted { term, .. }
            | Self::RequestVote { term, .. }
            | Self::Vote { term, .. }
            | Self::Append { term, .. }
            | Self::Appended { term, .. } => Some(*term),
            Self::PreVote { .. } | Self::Submit { .. } | Self::Submitted { .. } => None,
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
    prevotes: BTreeSet<usize>,
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
            prevotes: BTreeSet::new(),
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

    /// Where the log ends.
    fn last(&self) -> LogEnd {
        let index = self.last_index();
        LogEnd {
            term: self.term_at(index).unwrap_or(0),
            index,
        }
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

    /// The timer ran out: a leader copies its log, and anyone else asks whether it could stand.
    ///
    /// Asking spends nothing. The replica's term, its vote and its role stay as they were, and it
    /// stands only once a majority has said it could — so a replica that cannot hear its peers'
    /// answers never moves anyone into a term it began.
    fn on_timer(&mut self) -> Out {
        let mut out = Out::new();
        if self.recorded.role == Role::Leader {
            self.copy_to_everyone(&mut out);
            out.rearm = Rearm::Heartbeat;
            return out;
        }
        let proposed = self.recorded.term.saturating_add(1);
        let end = self.last();
        self.leader = None;
        self.prevotes = BTreeSet::from([self.me]);
        self.note(
            &mut out,
            format!(
                "{} asked whether it could stand for term {proposed}",
                node(self.me)
            ),
        );
        for (other, peer) in self.peers.iter().enumerate() {
            if other != self.me {
                out.sends.push((*peer, Message::PreVote { proposed, end }));
            }
        }
        out.rearm = Rearm::Election;
        out
    }

    /// A majority said this replica could stand: it begins the next term as a candidate, votes for
    /// itself and asks every other replica for its vote.
    fn stand(&mut self, out: &mut Out) {
        self.recorded.term = self.recorded.term.saturating_add(1);
        self.recorded.role = Role::Candidate;
        self.recorded.voted_for = Some(self.me);
        self.leader = None;
        self.votes = BTreeSet::from([self.me]);
        let term = self.recorded.term;
        let end = self.last();
        self.note(
            out,
            format!("{} became candidate for term {term}", node(self.me)),
        );
        for (other, peer) in self.peers.iter().enumerate() {
            if other != self.me {
                out.sends.push((*peer, Message::RequestVote { term, end }));
            }
        }
        out.rearm = Rearm::Election;
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
            Message::PreVote { proposed, end } => self.on_pre_vote(from, proposed, end, &mut out),
            Message::PreVoted {
                proposed, ballot, ..
            } => self.on_pre_voted(from, proposed, ballot, &mut out),
            Message::RequestVote { term, end } => {
                self.on_request_vote(from, term, end, &mut out);
            }
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
    /// A replica that was standing or leading drops whatever it was counting or owed: a client is
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

    /// A replica asked whether it could stand in `proposed`, its log ending at `end`.
    ///
    /// The answer is yes when `proposed` is ahead of this replica's term, the asker's log is at
    /// least as up to date as its own, and it is following no leader: a replica that still hears
    /// one has no reason to want an election, and saying yes would let a replica that alone cannot
    /// hear the leader depose it. Answering moves nothing — no term, no vote, no timer.
    fn on_pre_vote(&self, from: NodeId, proposed: u64, end: LogEnd, out: &mut Out) {
        let ballot = if proposed > self.recorded.term && end >= self.last() && self.leader.is_none()
        {
            Ballot::Granted
        } else {
            Ballot::Refused
        };
        out.sends.push((
            from,
            Message::PreVoted {
                term: self.recorded.term,
                proposed,
                ballot,
            },
        ));
    }

    /// A replica answered this one's question about standing in `proposed`.
    ///
    /// Only an answer to the question it is asking now counts: one about a term it has since moved
    /// past, or arriving after it stopped asking, is about an election it no longer means to stand
    /// in.
    fn on_pre_voted(&mut self, from: NodeId, proposed: u64, ballot: Ballot, out: &mut Out) {
        if !self.prevotes.contains(&self.me)
            || proposed != self.recorded.term.saturating_add(1)
            || ballot != Ballot::Granted
        {
            return;
        }
        let Some(voter) = self.peer(from) else {
            return;
        };
        self.prevotes.insert(voter);
        if self.prevotes.len() >= MAJORITY {
            self.prevotes.clear();
            self.stand(out);
        }
    }

    /// A candidate asked for this replica's vote in `term`, its log ending at `end`.
    ///
    /// The vote goes to the first candidate to ask in the replica's term whose log is at least as
    /// up to date as this replica's own: its last entry from a later term, or from the same term
    /// and no shorter. A candidate refused for its log has not had the vote, so the term's one vote
    /// is still there for a candidate that holds everything. A refusal does not touch the timer —
    /// though a request from a later term has already made a leader or a candidate a follower by
    /// the time it is weighed, and that drew it a fresh election timeout.
    fn on_request_vote(&mut self, from: NodeId, term: u64, end: LogEnd, out: &mut Out) {
        let Some(candidate) = self.peer(from) else {
            return;
        };
        let ballot = if term == self.recorded.term
            && end >= self.last()
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
    /// own term towards a commit, and the clients may have nothing more to send.
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
        self.prevotes.clear();
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
    /// than it has committed, and answers each client for what that commits.
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

    /// A client asked for `command` to be committed.
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

/// Where a client stands with one of its commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Submission {
    sent: VirtualTime,
    done: Option<VirtualTime>,
}

/// Everything the run knows at one step: each replica that has started, and each client's commands.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct View {
    replicas: BTreeMap<usize, Recorded>,
    clients: BTreeMap<usize, BTreeMap<u64, Submission>>,
}

/// Something that happened, and what the run knew once it had.
struct Observation {
    at: VirtualTime,
    message: String,
    view: View,
}

/// Writes the run's view as a world: one resource for each replica that has started, and one for
/// each client once it has sent anything.
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
    for (&me, commands) in &view.clients {
        let mut asking = Resource::new();
        for (command, submission) in commands {
            asking.insert(
                Name::new(format!("{COMMAND}{command:04}{SENT}"))?,
                Value::Instant(submission.sent),
            );
            if let Some(done) = submission.done {
                asking.insert(
                    Name::new(format!("{COMMAND}{command:04}{DONE}"))?,
                    Value::Instant(done),
                );
            }
        }
        world.insert(Name::new(client(me))?, asking);
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

/// The resources of a recorded world that are clients, which are the ones named for a client.
fn clients(world: &World) -> impl Iterator<Item = &Resource> {
    world
        .resources()
        .filter(|(name, _)| name.as_str().starts_with(CLIENT))
        .map(|(_, client)| client)
}

/// Every command the clients sent, by its number: when it was sent, and when it was answered if it
/// has been.
///
/// Read off a recorded world: a command was sent when some client has its `command-<n>-sent`, and
/// answered when that client has its `command-<n>-done`.
fn asked(world: &World) -> BTreeMap<u64, (VirtualTime, Option<VirtualTime>)> {
    let mut asked = BTreeMap::new();
    for asking in clients(world) {
        for (name, value) in asking.fields() {
            let Some(number) = name
                .as_str()
                .strip_prefix(COMMAND)
                .and_then(|rest| rest.strip_suffix(SENT))
            else {
                continue;
            };
            let (Ok(command), Value::Instant(sent)) = (number.parse(), value) else {
                continue;
            };
            let done = match field(asking, &format!("{COMMAND}{number}{DONE}")) {
                Some(Value::Instant(done)) => Some(*done),
                _ => None,
            };
            asked.insert(command, (*sent, done));
        }
    }
    asked
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

/// Whether every command a client sent was answered and has been applied by every replica.
///
/// Read off a recorded world: a command was sent and answered as [`asked`] reads them, and applied
/// by a replica when some `applied-<index>` of that replica names it.
fn every_command_committed(world: &World) -> bool {
    asked(world).into_iter().all(|(command, (_, done))| {
        let wanted = Value::Text(Op::Command(command).to_string());
        done.is_some()
            && replicas(world).all(|replica| {
                replica
                    .fields()
                    .any(|(name, value)| name.as_str().starts_with(APPLIED) && *value == wanted)
            })
    })
}

/// The commands `replica` applied, in the order it applied them, with the no-ops left out; or
/// nothing if something it applied is neither.
///
/// Read off a recorded world: what a replica applied at an index is its `applied-<index>`. The
/// indices are read as numbers rather than taken in the order the fields list in, which the width
/// they are written at keeps right only for the logs this module makes.
fn applied_commands(replica: &Resource) -> Option<Vec<u64>> {
    let mut applied = BTreeMap::new();
    for (name, value) in replica.fields() {
        let Some(at) = name.as_str().strip_prefix(APPLIED) else {
            continue;
        };
        let (Ok(at), Value::Text(text)) = (at.parse::<u64>(), value) else {
            return None;
        };
        applied.insert(at, Op::read(text)?);
    }
    Some(
        applied
            .into_values()
            .filter_map(|op| match op {
                Op::Noop => None,
                Op::Command(command) => Some(command),
            })
            .collect(),
    )
}

/// Whether every replica applied the clients' commands in an order each client could have seen.
///
/// The order the entries were applied in is the only order a log whose answers carry nothing but
/// "done" has to offer, so it is the order judged. Every replica applied each command at most
/// once, applied only commands some client sent, and applied a command only after every command
/// that was done by the instant it was sent. Commands in flight together may be applied either
/// way round, and a command not yet answered may have been applied or not.
///
/// "Done by the instant it was sent" counts an answer at the very instant as before. That is the
/// client's own order: a client whose command ran late sends its next one the instant it hears,
/// and the next one cannot have taken effect first. Nor can counting it so order two commands the
/// log was free to put either way round, since a command is answered only a crossing of the wire
/// after it is committed, and so after it is applied by the leader that committed it.
///
/// Read off a recorded world: what was sent and answered is what [`asked`] reads, and what each
/// replica applied is what [`applied_commands`] reads.
fn linearizable(world: &World) -> bool {
    let asked = asked(world);
    replicas(world).all(|replica| {
        let Some(order) = applied_commands(replica) else {
            return false;
        };
        let mut place = BTreeMap::new();
        if !order
            .iter()
            .enumerate()
            .all(|(at, command)| place.insert(*command, at).is_none())
        {
            return false;
        }
        order.iter().enumerate().all(|(at, command)| {
            let Some(&(sent, _)) = asked.get(command) else {
                return false;
            };
            asked.iter().all(|(earlier, &(_, done))| {
                done.is_none_or(|done| done > sent)
                    || place.get(earlier).is_some_and(|&before| before < at)
            })
        })
    })
}

/// What the replicated log promises, judged over the world each step of its run recorded.
///
/// The safety promises stand first, so a run that breaks one at its last step is reported for that
/// rather than for falling short; and among them the replicas' own stand before the clients', so a
/// step that breaks both is reported for the cause rather than for what the clients saw of it.
fn invariants() -> Result<[Invariant; 4], ReasonError> {
    Ok([
        Invariant::safety(Reason::new("two leaders in one term")?, one_leader_a_term),
        Invariant::safety(
            Reason::new("a committed entry changed")?,
            committed_entries_hold,
        ),
        Invariant::safety(Reason::new("not linearizable")?, linearizable),
        Invariant::liveness(
            Reason::new("did not commit every command")?,
            Duration::MAX,
            every_command_committed,
        ),
    ])
}

/// Runs the five replicas and the three clients under `seed`, with `faults` to get in their way.
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

    // The clients are added first, so they are nodes 0 to 2 and the replicas the nodes after them.
    let askers: Vec<_> = (0..CLIENTS).map(|_| network.add_node()).collect();
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

    for (me, endpoint) in askers.into_iter().enumerate() {
        let clock = executor.handle();
        let replicas = Rc::clone(&peers);
        let observations = Rc::clone(&log);
        executor.spawn(async move {
            submit(&clock, me, &endpoint, &replicas, &observations).await;
        });
    }

    executor.run()?;
    // Every task is finished and nothing is polling, so nothing else holds the log open.
    Ok(core::mem::take(&mut *log.borrow_mut()))
}

/// What one step changed in the run's view.
enum Change {
    /// A replica's state, as it stood straight after the change.
    Replica(usize, Recorded),
    /// The client at the first number sent the command at the second for the first time.
    Sent(usize, u64),
    /// The client at the first number heard the command at the second was done.
    Done(usize, u64),
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
        Change::Sent(me, command) => {
            view.clients.entry(me).or_default().insert(
                command,
                Submission {
                    sent: at,
                    done: None,
                },
            );
        }
        Change::Done(me, command) => {
            if let Some(submission) = view
                .clients
                .get_mut(&me)
                .and_then(|commands| commands.get_mut(&command))
            {
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

/// The client at `me` among the clients: send its command of each round in turn, and keep asking
/// until it is done or the run ends.
async fn submit<C, N>(
    clock: &C,
    me: usize,
    endpoint: &N,
    replicas: &[NodeId],
    observations: &RefCell<Vec<Observation>>,
) where
    C: Clock,
    N: Network<Message = Message>,
{
    let end = stop();
    let mut target = 0;
    for round in 1..=ROUNDS {
        clock.sleep_until(opens(round)).await;
        if clock.now() >= end {
            return;
        }
        let command = numbered(round, me);
        write(
            observations,
            clock.now(),
            format!("{} sent command {command}", client(me)),
            Change::Sent(me, command),
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
                        format!("{} heard command {command} is done", client(me)),
                        Change::Done(me, command),
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

    /// The address of the client at `index` among the clients, which are the first nodes the run
    /// adds.
    fn asker(index: usize) -> NodeId {
        NodeId::from_index(index as u64)
    }

    /// The address of the replica at `index` among the replicas, counting from zero.
    fn peer(index: usize) -> NodeId {
        NodeId::from_index((index + CLIENTS) as u64)
    }

    /// Every replica's address, in the order the run adds them.
    fn peers() -> Rc<[NodeId]> {
        (0..REPLICAS).map(peer).collect()
    }

    /// The replica at `me`, fresh.
    fn replica(me: usize) -> Replica {
        Replica::new(me, peers())
    }

    /// The end of a log whose last entry is entry `index`, from `term`.
    fn end(term: u64, index: u64) -> LogEnd {
        LogEnd { term, index }
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

    /// A replica in `term` answering a question about `proposed`.
    fn pre_voted(term: u64, proposed: u64, ballot: Ballot) -> Message {
        Message::PreVoted {
            term,
            proposed,
            ballot,
        }
    }

    /// Makes `replica` a candidate in its next term the way a run does: its timer runs out, and two
    /// others say it could stand.
    fn stand(replica: &mut Replica) {
        let (me, term) = (replica.me, replica.recorded.term);
        replica.on_timer();
        for other in (0..REPLICAS).filter(|other| *other != me).take(2) {
            replica.on_message(
                peer(other),
                pre_voted(term, term.saturating_add(1), Ballot::Granted),
            );
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
                    .checked_sub(CLIENTS)
            })
            .unwrap_or_else(|| panic!("a run with a leader names it"))
    }

    /// A schedule cutting `cut` off from every other node, both ways, for `during`.
    fn isolating(cut: &[usize], during: Window) -> FaultSchedule {
        let others = (0..(CLIENTS + REPLICAS) as u64)
            .filter(|node| !cut.iter().any(|c| peer(*c) == NodeId::from_index(*node)));
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
    fn a_follower_whose_timer_runs_out_asks_whether_it_could_stand_and_changes_nothing_else() {
        // With a log whose last entry is from an earlier term than the replica's own, so the question
        // is seen to carry the last entry's term and not the replica's — and whose length is not that
        // term, so it is seen to carry each of the two in its own place.
        let mut follower = replica(2);
        follower.recorded.term = 4;
        follower.recorded.log = vec![command(1, 1), command(1, 2), command(1, 3)];
        follower.leader = Some(0);
        let before = follower.recorded.clone();
        let out = follower.on_timer();

        assert_eq!(
            follower.recorded, before,
            "no term is spent, no vote is cast, on a question"
        );
        assert_eq!(follower.leader, None, "the leader it followed is gone");
        assert_eq!(
            said(&out),
            ["node-5 asked whether it could stand for term 5"]
        );
        assert_eq!(
            sent(&out),
            [0, 1, 3, 4]
                .map(|other| (
                    peer(other),
                    Message::PreVote {
                        proposed: 5,
                        end: end(1, 3)
                    }
                ))
                .to_vec(),
            "everyone but itself, with where its log ends"
        );
        assert_eq!(
            out.rearm,
            Rearm::Election,
            "a question nobody answers times out too"
        );
    }

    #[test]
    fn a_majority_of_pre_votes_makes_a_candidate_that_asks_for_real_ones() {
        let mut asking = replica(2);
        asking.recorded.term = 4;
        asking.recorded.log = vec![command(1, 1), command(1, 2), command(1, 3)];
        asking.on_timer();

        let one = asking.on_message(peer(0), pre_voted(4, 5, Ballot::Granted));
        assert_eq!(
            asking.recorded.role,
            Role::Follower,
            "two of five is not a majority"
        );
        assert_eq!(asking.recorded.term, 4);
        assert!(sent(&one).is_empty());
        let refused = asking.on_message(peer(3), pre_voted(4, 5, Ballot::Refused));
        assert_eq!(
            asking.recorded.role,
            Role::Follower,
            "a refusal counts for nothing"
        );
        assert!(sent(&refused).is_empty());

        let out = asking.on_message(peer(1), pre_voted(4, 5, Ballot::Granted));
        assert_eq!(asking.recorded.term, 5);
        assert_eq!(asking.recorded.role, Role::Candidate);
        assert_eq!(asking.recorded.voted_for, Some(2), "it votes for itself");
        assert_eq!(said(&out), ["node-5 became candidate for term 5"]);
        assert_eq!(
            sent(&out),
            [0, 1, 3, 4]
                .map(|other| (
                    peer(other),
                    Message::RequestVote {
                        term: 5,
                        end: end(1, 3)
                    }
                ))
                .to_vec(),
            "everyone but itself, with where its log ends"
        );
        assert_eq!(
            out.rearm,
            Rearm::Election,
            "an election it cannot win times out too"
        );

        let late = asking.on_message(peer(4), pre_voted(4, 5, Ballot::Granted));
        assert!(
            sent(&late).is_empty() && said(&late).is_empty(),
            "a pre-vote that comes after it stood is spent"
        );
        assert_eq!(asking.recorded.term, 5);
    }

    #[test]
    fn a_pre_vote_answering_an_earlier_question_counts_for_nothing() {
        // It asked about term 5, was told term 6 had begun, and asked about term 7. Grants for the
        // first question arriving now are about an election it no longer means to stand in.
        let mut asking = replica(2);
        asking.recorded.term = 4;
        asking.on_timer();
        asking.on_message(peer(0), pre_voted(6, 5, Ballot::Refused));
        assert_eq!(asking.recorded.term, 6, "the later term is taken");
        asking.on_timer();

        for other in [1, 3] {
            let out = asking.on_message(peer(other), pre_voted(4, 5, Ballot::Granted));
            assert!(sent(&out).is_empty(), "{other}'s grant is about term 5");
        }
        assert_eq!(asking.recorded.role, Role::Follower);
        assert_eq!(asking.recorded.term, 6);
    }

    #[test]
    fn a_replica_that_hears_a_leader_while_asking_stops_asking() {
        let mut asking = replica(2);
        asking.recorded.term = 4;
        asking.on_timer();
        asking.on_message(peer(0), pre_voted(4, 5, Ballot::Granted));
        asking.on_message(peer(3), append(4, 0, 0, Vec::new(), 0));
        assert_eq!(asking.leader, Some(3));

        let out = asking.on_message(peer(1), pre_voted(4, 5, Ballot::Granted));
        assert!(
            sent(&out).is_empty(),
            "a replica following a leader does not stand against it"
        );
        assert_eq!(asking.recorded.role, Role::Follower);
        assert_eq!(asking.recorded.term, 4);
    }

    #[test]
    fn a_pre_vote_goes_only_to_an_up_to_date_replica_while_no_leader_is_heard() {
        struct Case {
            name: &'static str,
            voter: fn() -> Replica,
            proposed: u64,
            end: LogEnd,
            ballot: Ballot,
        }
        // Every voter is in term 2 holding three entries, the last of them from term 2, and none
        // has voted. Whatever it answers, it answers from where it stands: a question moves no
        // term, spends no vote and puts no timer back.
        fn unled() -> Replica {
            let mut voter = replica(0);
            voter.recorded.term = 2;
            voter.recorded.log = vec![command(1, 1), command(1, 2), command(2, 3)];
            voter
        }
        fn following() -> Replica {
            let mut voter = unled();
            voter.leader = Some(1);
            voter
        }
        fn leading() -> Replica {
            let mut voter = leader(0, 2, vec![command(1, 1), command(1, 2), command(2, 3)]);
            voter.recorded.voted_for = None;
            voter
        }
        let cases = [
            Case {
                name: "an up-to-date asker with no leader in its way has it",
                voter: unled,
                proposed: 9,
                end: end(2, 3),
                ballot: Ballot::Granted,
            },
            Case {
                name: "an asker whose longer log ends in an earlier term is refused",
                voter: unled,
                proposed: 9,
                end: end(1, 7),
                ballot: Ballot::Refused,
            },
            Case {
                name: "an asker whose shorter log ends in the same term is refused",
                voter: unled,
                proposed: 9,
                end: end(2, 2),
                ballot: Ballot::Refused,
            },
            Case {
                name: "a replica following a leader refuses",
                voter: following,
                proposed: 9,
                end: end(2, 3),
                ballot: Ballot::Refused,
            },
            Case {
                name: "a leader refuses",
                voter: leading,
                proposed: 9,
                end: end(2, 3),
                ballot: Ballot::Refused,
            },
            Case {
                name: "a term not ahead of the voter's is refused",
                voter: unled,
                proposed: 2,
                end: end(2, 3),
                ballot: Ballot::Refused,
            },
        ];
        for case in cases {
            let mut voter = (case.voter)();
            let before = voter.recorded.clone();
            let out = voter.on_message(
                peer(4),
                Message::PreVote {
                    proposed: case.proposed,
                    end: case.end,
                },
            );

            assert_eq!(
                sent(&out),
                [(peer(4), pre_voted(2, case.proposed, case.ballot))],
                "{}",
                case.name
            );
            assert_eq!(voter.recorded, before, "{}", case.name);
            assert!(said(&out).is_empty(), "{}", case.name);
            assert_eq!(out.rearm, Rearm::Keep, "{}", case.name);
        }
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
        // The candidate is always the replica at 1, asking the replica at 0. Both logs are empty, so
        // the candidate's is as up to date as the voter's and only the terms and the ballot decide.
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
                    end: end(0, 0),
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
    fn a_vote_goes_only_to_a_candidate_whose_log_is_at_least_as_up_to_date() {
        struct Case {
            name: &'static str,
            end: LogEnd,
            ballot: Ballot,
        }
        // The voter holds three entries, the last of them from term 2, and has not voted in term 9.
        // Whichever log's last entry is from the later term is the more up to date, and between two
        // whose last entries share a term the longer one is.
        let cases = [
            Case {
                name: "a candidate holding nothing is refused",
                end: end(0, 0),
                ballot: Ballot::Refused,
            },
            Case {
                name: "a longer log ending in an earlier term is refused",
                end: end(1, 7),
                ballot: Ballot::Refused,
            },
            Case {
                name: "a shorter log ending in the same term is refused",
                end: end(2, 2),
                ballot: Ballot::Refused,
            },
            Case {
                name: "a log ending where the voter's does has the vote",
                end: end(2, 3),
                ballot: Ballot::Granted,
            },
            Case {
                name: "a longer log ending in the same term has the vote",
                end: end(2, 4),
                ballot: Ballot::Granted,
            },
            Case {
                name: "a shorter log ending in a later term has the vote",
                end: end(3, 2),
                ballot: Ballot::Granted,
            },
        ];
        for case in cases {
            let mut voter = replica(0);
            voter.recorded.term = 2;
            voter.recorded.log = vec![command(1, 1), command(1, 2), command(2, 3)];
            voter.recorded.commit = 3;
            let out = voter.on_message(
                peer(4),
                Message::RequestVote {
                    term: 9,
                    end: case.end,
                },
            );

            assert_eq!(
                sent(&out),
                [(
                    peer(4),
                    Message::Vote {
                        term: 9,
                        ballot: case.ballot
                    }
                )],
                "{}",
                case.name
            );
            assert_eq!(voter.recorded.term, 9, "the later term is taken either way");
            let granted = case.ballot == Ballot::Granted;
            assert_eq!(
                voter.recorded.voted_for,
                granted.then_some(4),
                "{}",
                case.name
            );
            let expected: &[&str] = if granted {
                &["node-3 voted for node-7 in term 9"]
            } else {
                &["node-3 is a follower in term 9"]
            };
            assert_eq!(said(&out), expected, "{}", case.name);
        }
    }

    #[test]
    fn a_refused_candidate_leaves_the_vote_for_one_that_is_up_to_date_and_the_timer_running() {
        // A refusal is not a vote: the stale candidate is turned away without spending the term's
        // one vote, so a candidate that does hold everything can still have it — and the voter's
        // timer is not put back by a candidate it would never follow.
        let mut voter = replica(0);
        voter.recorded.term = 9;
        voter.recorded.log = vec![command(1, 1), command(2, 2)];
        let stale = voter.on_message(
            peer(4),
            Message::RequestVote {
                term: 9,
                end: end(1, 1),
            },
        );
        assert_eq!(
            sent(&stale),
            [(
                peer(4),
                Message::Vote {
                    term: 9,
                    ballot: Ballot::Refused
                }
            )]
        );
        assert_eq!(voter.recorded.voted_for, None);
        assert_eq!(stale.rearm, Rearm::Keep);

        let current = voter.on_message(
            peer(3),
            Message::RequestVote {
                term: 9,
                end: end(2, 2),
            },
        );
        assert_eq!(
            sent(&current),
            [(
                peer(3),
                Message::Vote {
                    term: 9,
                    ballot: Ballot::Granted
                }
            )]
        );
        assert_eq!(voter.recorded.voted_for, Some(3));
        assert_eq!(current.rearm, Rearm::Election);
    }

    #[test]
    fn a_leader_asked_by_a_stale_candidate_from_a_later_term_steps_down_and_still_refuses_it() {
        // The request's term is a fact the leader has to take, whatever the log behind it: the
        // leader becomes a follower in that term and redraws its timer, as any later term makes it.
        // The refusal that follows leaves that timer as the step down set it, and the vote unspent.
        let mut leading = leader(0, 3, vec![command(1, 1), command(3, 2)]);
        leading.pending.insert(2, (asker(0), 2));
        let out = leading.on_message(
            peer(4),
            Message::RequestVote {
                term: 4,
                end: end(1, 1),
            },
        );

        assert_eq!(
            sent(&out),
            [(
                peer(4),
                Message::Vote {
                    term: 4,
                    ballot: Ballot::Refused
                }
            )]
        );
        assert_eq!(leading.recorded.role, Role::Follower);
        assert_eq!(leading.recorded.term, 4);
        assert_eq!(leading.recorded.voted_for, None, "a refusal is not a vote");
        assert!(
            leading.pending.is_empty(),
            "what it owed went with the lead"
        );
        assert_eq!(said(&out), ["node-3 is a follower in term 4"]);
        assert_eq!(
            out.rearm,
            Rearm::Election,
            "it waits for a leader now, not to send a heartbeat"
        );
    }

    #[test]
    fn a_majority_of_votes_makes_a_leader_that_appends_a_no_op_and_copies_it_out() {
        let mut candidate = replica(0);
        stand(&mut candidate);
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
        assert_eq!(said(&won), ["node-3 became leader of term 1"]);
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
        stale.pending.insert(1, (asker(0), 1));
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
        assert_eq!(said(&out), ["node-3 is a follower in term 4"]);
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
                said: vec!["node-4 is a follower in term 2"],
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
                said: vec!["node-4 dropped entries after 1 and appended entry 2"],
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
                    "node-4 appended entries 1 to 2",
                    "node-4 committed through 2",
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
        assert_eq!(said(&out), ["node-3 committed through 3"]);
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
        let took = leading.on_message(asker(0), Message::Submit { command: 4 });
        assert_eq!(said(&took), ["node-3 took command 4 as entry 2"]);
        assert_eq!(
            took.sends.len(),
            REPLICAS - 1,
            "the new entry is copied out at once"
        );
        assert!(
            took.sends.iter().all(|(to, _)| *to != asker(0)),
            "nothing is said to the client until it is committed"
        );

        let again = leading.on_message(asker(0), Message::Submit { command: 4 });
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
                        asker(0),
                        Message::Submitted {
                            command: 4,
                            answer: Answer::Committed
                        }
                    )),
                    "the third copy commits it"
                );
            }
        }

        let late = leading.on_message(asker(0), Message::Submit { command: 4 });
        assert_eq!(
            sent(&late),
            [(
                asker(0),
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
            let out = follower.on_message(asker(0), Message::Submit { command: 2 });

            assert_eq!(
                sent(&out),
                [(
                    asker(0),
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

    /// One command a case's clients sent: the client, the command, the instant it was sent, and the
    /// instant it was answered if it was.
    type Asked = (usize, u64, u64, Option<u64>);

    /// The world a view of these replicas and these commands is written as.
    fn written(replicas: Vec<Recorded>, asked: &[Asked]) -> World {
        let mut clients: BTreeMap<usize, BTreeMap<u64, Submission>> = BTreeMap::new();
        for &(me, command, sent, done) in asked {
            clients.entry(me).or_default().insert(
                command,
                Submission {
                    sent: VirtualTime::from_nanos(sent),
                    done: done.map(VirtualTime::from_nanos),
                },
            );
        }
        let view = View {
            replicas: replicas.into_iter().enumerate().collect(),
            clients,
        };
        world(&view).unwrap_or_else(|e| panic!("a view is written with names: {e}"))
    }

    #[test]
    fn a_view_is_written_with_the_fields_the_invariants_read() {
        let mut leading = recorded(vec![command(2, 7)], vec![Op::Command(7)], &[2]);
        leading.role = Role::Leader;
        leading.voted_for = Some(0);
        let world = written(vec![leading], &[(1, 7, 7, Some(8))]);

        let replica = world
            .get(&name("node-3"))
            .unwrap_or_else(|| panic!("the replica is written down"));
        for (field, value) in [
            ("term", Value::Count(1)),
            ("role", Value::Text("leader".to_owned())),
            ("voted-for", Value::Text("node-3".to_owned())),
            ("commit", Value::Count(1)),
            ("entry-0001-term", Value::Count(2)),
            ("entry-0001-op", Value::Text("command 7".to_owned())),
            ("applied-0001", Value::Text("command 7".to_owned())),
            ("led-0002", Value::Flag(true)),
        ] {
            assert_eq!(replica.get(&name(field)), Some(&value), "{field}");
        }
        let client = world
            .get(&name("client-1"))
            .unwrap_or_else(|| panic!("the client is written down under its own name"));
        assert_eq!(
            client.get(&name("command-0007-sent")),
            Some(&Value::Instant(VirtualTime::from_nanos(7)))
        );
        assert_eq!(
            client.get(&name("command-0007-done")),
            Some(&Value::Instant(VirtualTime::from_nanos(8)))
        );
        assert_eq!(
            world.get(&name("client-0")),
            None,
            "a client that has sent nothing is not written down"
        );
    }

    /// One of the promises the run is judged by, named for the predicate that keeps it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Promise {
        OneLeader,
        Committed,
        Linearizable,
        Finished,
    }

    impl Promise {
        /// Whether the promise holds of `world`.
        fn holds(self, world: &World) -> bool {
            match self {
                Self::OneLeader => one_leader_a_term(world),
                Self::Committed => committed_entries_hold(world),
                Self::Linearizable => linearizable(world),
                Self::Finished => every_command_committed(world),
            }
        }
    }

    /// A recorded world, and which promises it breaks.
    struct WorldCase {
        name: &'static str,
        world: World,
        broken: &'static [Promise],
    }

    /// The cases of `each_invariant_breaks_on_the_world_it_is_about_and_holds_on_the_others` about
    /// the replicas: who led, what they applied, and whether they caught up.
    fn world_cases() -> Vec<WorldCase> {
        let applied = |ops: &[u64]| -> Vec<Op> { ops.iter().map(|&op| Op::Command(op)).collect() };
        let log = |ops: &[u64]| -> Vec<LogEntry> { ops.iter().map(|&op| command(1, op)).collect() };
        vec![
            WorldCase {
                name: "agreement everywhere",
                world: written(
                    vec![
                        recorded(log(&[1, 2]), applied(&[1, 2]), &[1]),
                        recorded(log(&[1, 2]), applied(&[1, 2]), &[]),
                    ],
                    &[(0, 1, 1, Some(2)), (0, 2, 3, Some(4))],
                ),
                broken: &[],
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
                broken: &[],
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
                broken: &[Promise::OneLeader],
            },
            WorldCase {
                name: "two replicas that applied different things at one index",
                world: written(
                    vec![
                        recorded(log(&[1, 2]), applied(&[1, 2]), &[]),
                        recorded(log(&[2, 1]), applied(&[2, 1]), &[]),
                    ],
                    &[(0, 1, 1, Some(4)), (1, 2, 1, Some(4))],
                ),
                broken: &[Promise::Committed],
            },
            WorldCase {
                name: "a replica whose log no longer holds what it applied",
                world: written(
                    vec![recorded(log(&[2]), applied(&[1]), &[])],
                    &[(0, 1, 1, Some(2))],
                ),
                broken: &[Promise::Committed],
            },
            WorldCase {
                name: "a replica whose log is shorter than what it applied",
                world: written(
                    vec![recorded(log(&[]), applied(&[1]), &[])],
                    &[(0, 1, 1, Some(2))],
                ),
                broken: &[Promise::Committed],
            },
            WorldCase {
                name: "a command sent and never answered",
                world: written(
                    vec![recorded(log(&[1]), applied(&[1]), &[])],
                    &[(0, 1, 1, None)],
                ),
                broken: &[Promise::Finished],
            },
            WorldCase {
                name: "a command answered that one replica has not applied",
                world: written(
                    vec![
                        recorded(log(&[1]), applied(&[1]), &[]),
                        recorded(log(&[1]), applied(&[]), &[]),
                    ],
                    &[(0, 1, 1, Some(2))],
                ),
                broken: &[Promise::Finished],
            },
        ]
    }

    /// The cases of `each_invariant_breaks_on_the_world_it_is_about_and_holds_on_the_others` about
    /// the order the clients' commands were applied in.
    fn order_cases() -> Vec<WorldCase> {
        let applied = |ops: &[u64]| -> Vec<Op> { ops.iter().map(|&op| Op::Command(op)).collect() };
        let log = |ops: &[u64]| -> Vec<LogEntry> { ops.iter().map(|&op| command(1, op)).collect() };
        // One replica that applied `ops` in that order, its log holding them.
        let applying = |ops: &[u64]| vec![recorded(log(ops), applied(ops), &[])];
        vec![
            WorldCase {
                name: "two commands in flight together, applied in the order they were sent",
                world: written(applying(&[1, 2]), &[(0, 1, 1, Some(4)), (1, 2, 2, Some(5))]),
                broken: &[],
            },
            WorldCase {
                name: "two commands in flight together, applied the other way round",
                world: written(applying(&[2, 1]), &[(0, 1, 1, Some(4)), (1, 2, 2, Some(5))]),
                broken: &[],
            },
            WorldCase {
                name: "no-ops between the commands, which are no command's place",
                world: written(
                    vec![recorded(
                        vec![
                            LogEntry {
                                term: 1,
                                op: Op::Noop,
                            },
                            command(1, 1),
                            LogEntry {
                                term: 2,
                                op: Op::Noop,
                            },
                            command(2, 2),
                        ],
                        vec![Op::Noop, Op::Command(1), Op::Noop, Op::Command(2)],
                        &[],
                    )],
                    &[(0, 1, 1, Some(2)), (1, 2, 3, Some(4))],
                ),
                broken: &[],
            },
            WorldCase {
                name: "a command applied ahead of one that was done before it was sent",
                world: written(applying(&[2, 1]), &[(0, 1, 1, Some(2)), (1, 2, 3, Some(4))]),
                broken: &[Promise::Linearizable],
            },
            WorldCase {
                name: "a client's next command, sent the instant its last was done, applied ahead of it",
                world: written(applying(&[2, 1]), &[(0, 1, 1, Some(3)), (0, 2, 3, Some(4))]),
                broken: &[Promise::Linearizable],
            },
            WorldCase {
                name: "a command applied twice",
                world: written(applying(&[1, 1]), &[(0, 1, 1, Some(2))]),
                broken: &[Promise::Linearizable],
            },
            WorldCase {
                name: "a command applied that no client sent",
                world: written(applying(&[1]), &[]),
                broken: &[Promise::Linearizable],
            },
            WorldCase {
                // Falling short at the end of the run as well, since command 1 was answered and
                // never applied; but a replica applying command 2 without it is wrong at once.
                name: "a command applied while one done before it was sent is missing",
                world: written(applying(&[2]), &[(0, 1, 1, Some(2)), (1, 2, 3, Some(4))]),
                broken: &[Promise::Linearizable, Promise::Finished],
            },
            WorldCase {
                name: "a command not yet answered that no replica has applied",
                world: written(applying(&[1]), &[(0, 1, 1, Some(2)), (1, 2, 3, None)]),
                broken: &[Promise::Finished],
            },
        ]
    }

    #[test]
    fn each_invariant_breaks_on_the_world_it_is_about_and_holds_on_the_others() {
        let promises = [
            Promise::OneLeader,
            Promise::Committed,
            Promise::Linearizable,
            Promise::Finished,
        ];
        for case in world_cases().into_iter().chain(order_cases()) {
            for promise in promises {
                assert_eq!(
                    promise.holds(&case.world),
                    !case.broken.contains(&promise),
                    "{}: {promise:?}",
                    case.name
                );
            }
        }
    }

    #[test]
    fn the_clients_have_commands_in_flight_together_in_every_round() {
        // Without this the order the log applies commands in is settled by one client's own order,
        // and the linearizability check has nothing to judge that a single client would not. Read off
        // the last recorded world rather than from the module's numbering, so a run whose clients
        // went one after another fails here whatever the numbers say.
        let (trace, store, outcome) = runs(SEED, &FaultSchedule::default());
        assert_eq!(outcome, Outcome::Pass);
        let asked = asked(&last_world(&trace, &store));
        assert_eq!(index(asked.len()), index(CLIENTS) * ROUNDS);

        for round in 1..=ROUNDS {
            let commands: Vec<_> = (0..CLIENTS)
                .map(|me| {
                    let (sent, done) = asked[&numbered(round, me)];
                    (
                        sent,
                        done.unwrap_or_else(|| panic!("round {round} is done")),
                    )
                })
                .collect();
            let together = commands.iter().enumerate().any(|(one, &(sent, done))| {
                commands
                    .iter()
                    .skip(one + 1)
                    .any(|&(other_sent, other_done)| sent < other_done && other_sent < done)
            });
            assert!(together, "round {round}: {commands:?}");
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
            for command in 1..=numbered(ROUNDS, CLIENTS - 1) {
                assert!(
                    applied.contains(&&Value::Text(format!("command {command}"))),
                    "{} applied command {command}",
                    node(me)
                );
            }
        }
    }

    #[test]
    fn clients_no_replica_hears_leave_every_command_uncommitted() {
        let cut: Vec<Fault> = (0..CLIENTS)
            .flat_map(|me| {
                (0..REPLICAS).map(move |replica| Fault::Partition {
                    from: asker(me),
                    to: peer(replica),
                    during: Window::forever_from(VirtualTime::ZERO),
                })
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
