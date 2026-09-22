//! Deterministic simulation engine for infrastructure control loops.
//!
//! Control loops — Kubernetes operators, reconcilers, provisioning state machines — fail in ways
//! that are almost impossible to reproduce. The bug needs a specific interleaving of a slow API
//! call, a retry, a teardown, and a replica that has not caught up yet. You see it once in
//! production and never again in a test environment.
//!
//! chronoloop runs a control loop in **virtual time** on a single-threaded deterministic executor.
//! Time, randomness, network, and I/O are traits, so the same loop logic runs against a simulated
//! world where every delay, drop, and partition is drawn from one seed and nothing else. A run is
//! therefore a pure function of its seed: the same seed replays the same history, byte for byte,
//! forever.
//!
//! On top of that determinism sit the two capabilities the engine exists for:
//!
//! - **Time travel** — every step's world state is content-addressed, so a run can be inspected at
//!   step 37, diffed against step 36, and forked into a new timeline from there.
//! - **Shrinking** — given a seed that fails, the engine reduces the injected-fault schedule to the
//!   shortest sequence that still reproduces the failure.
//!
//! # Prior art
//!
//! Deterministic simulation testing is not new. [`madsim`], [`turmoil`], [`shuttle`], [`loom`],
//! FoundationDB's simulator, and Antithesis all attack this family of problems, and chronoloop
//! borrows liberally from their ideas. Where it differs: those tools are built to answer *"does my
//! network protocol survive partitions?"* chronoloop is built for **control loops**, and its
//! headline capability is what happens *after* a failing run — inspecting, diffing, and forking the
//! recorded history, and shrinking the failure to a minimal repro.
//!
//! chronoloop samples seeds. It does **not** prove the absence of bugs; that is model checking's
//! job.
//!
//! [`madsim`]: https://github.com/madsim-rs/madsim
//! [`turmoil`]: https://github.com/tokio-rs/turmoil
//! [`shuttle`]: https://github.com/awslabs/shuttle
//! [`loom`]: https://github.com/tokio-rs/loom
//!
//! # Status
//!
//! What works today is the deterministic core and one system running on it. A run is driven by a
//! [`VirtualClock`] over an [`EventQueue`], polled by a single-threaded [`Executor`], with every
//! random choice drawn from a [`SeededRng`]; [`pingpong`] is a two-task exchange over that clock,
//! and its [`Recording`] can be written to a file and replayed against a later run of the engine.
//!
//! Time, randomness and the wire are already capabilities a system asks for rather than reaches
//! for: written against [`Clock`], [`Rng`] and [`Network`], it can wait, arm a deadline, draw a
//! delay and talk to another node, and it has no way to get at the machine's clock, its entropy or
//! a socket. What its messages go through is a [`VirtualNetwork`], where latency, loss, duplication
//! and partitions all come from the same seed as everything else. A running task can start another
//! and wait for what it produces, with identifiers handed out in spawn order rather than taken from
//! an address. The trouble a run is put through is a [`FaultSchedule`] — a value written to a file
//! and read back, rather than a method called on a network — which is what the shrinking will
//! reduce. And the state a run leaves behind is now content-addressed: a [`World`] of named
//! resources hands back a tree of [`Snapshot`] nodes whose branches hash over their children's
//! hashes, so two states are the same state exactly when they answer to one [`StateHash`], and
//! a part of the world that did not change keeps the name it had. A [`StateStore`] takes that up:
//! it keeps each node once under the name it answers to, with a branch naming its children rather
//! than holding them, so a run costs what it changed rather than a copy of the world per step. What
//! puts those states in order is a [`Trace`]: for every step, the event and the name of the state it
//! left the run in, written to a file and read back strictly enough that a file with a step missing
//! from the middle is refused. A trace is addressed by step, and two of its states can be told
//! apart: [`diff`] walks both at once and stops wherever two subtrees answer to one name, so what
//! comes back is the path down to each thing that moved and what stands there on either side, at a
//! cost that is the size of the change rather than the size of the world. And a recorded step is
//! somewhere a run can be sent off from: [`fork`] names the instant a step happened at, and a run
//! given that instant and another seed draws from its own seed up to it and from the other one
//! after — so the steps up to the fork are not an approximation of where the run had got to, they
//! are the run, and the steps after it are another. [`ring`] is the system that shows it, a ring of
//! nodes writing down what they hear.
//!
//! All of that is reachable from a terminal. `trace --seed <n>` writes a run's trace to a file, and
//! `inspect --step`, `diff` and `fork --at` read one back: the first shows a step and, through
//! [`list`], the world it left the run in; the second says what moved between two of them; the
//! third sends the run off from one of them under another seed. A trace names states without
//! holding them, so each of the three runs again what the file's header and fork line name and
//! refuses to show anything at all if what comes out is not what is written down.
//!
//! What none of that can say is whether a run was **all right**. An [`Outcome`] is that verdict: a
//! run either held up or it broke, and a failure carries what broke and the step of the trace it
//! broke at — so the thing to read next is the step the verdict names. Two failures are the same
//! failure when their reasons agree, and not when the steps they surfaced at do, because cutting a
//! schedule down moves a step and leaves the failure standing; that is the question a reduction has
//! to ask of every candidate it tries. [`quorum`] is the system that shows it: a coordinator asking
//! five replicas to acknowledge it, round after round, over a wire that takes time and does nothing
//! else — so the only thing that can cost a round its quorum is the [`FaultSchedule`] the run was
//! given.
//!
//! And a verdict is what the reduction tests against. [`shrink`] takes a failing run's schedule and
//! makes it smaller: it drops entries by delta debugging over the list, narrows each window by
//! bisection towards the span the failure needs, and lowers each odds to the fewest occurrences it
//! needs, running the system again after every candidate and keeping only those whose run still fails
//! for the same reason. What comes back is a [`Reduction`] — the faults the failure could not do
//! without, and the failure the run under them produces — which is what turns a schedule a sweep
//! threw at a run into a repro a person can read.
//!
//! A reduction dies with the process that made it, and a [`Repro`] is what outlives one: the seed, the
//! faults the failure could not do without, and the failure to expect out the other end, written to a
//! file of a few hundred bytes and read back. It invents no form of its own — the failure is an
//! [`Outcome`] as an outcome writes itself, and everything under it is a [`FaultSchedule`] as a
//! schedule writes itself, header and all, so the tail of a repro file is a schedule file. A failing
//! run of a five-node system comes to under a third of a kilobyte, which is small enough to commit as
//! a fixture and hand to a run months later.
//!
//! All of that starts from a seed that is known to break, and [`sweep`] is what finds one: it runs a
//! whole range of seeds under one schedule of faults and hands back the ones that could not take it,
//! each of which is something a reduction can then cut down. Seeds are independent — a run of one
//! draws nothing from a run of another — so this is the one place in the engine that uses real
//! threads, and the fourth determinism rule is untouched because every run is still a
//! single-threaded simulation on an executor of its own. What the rule does demand is that nothing
//! about the answer come from the split, so a sweep runs every seed even after one has broken and
//! merges what the workers found **by seed**: `--jobs` buys wall-clock time and moves nothing else.
//! The scan in `tests/audit.rs` names that one file as the one place a thread is allowed, for that
//! one rule, in a table that says why.
//!
//! [`fork`]: fork::fork
//! [`ring`]: systems::ring
//! [`quorum`]: systems::quorum
//!
//! [`diff`]: diff::diff
//! [`list`]: diff::list
//! [`VirtualClock`]: clock::VirtualClock
//! [`EventQueue`]: event::EventQueue
//! [`Executor`]: executor::Executor
//! [`SeededRng`]: rng::SeededRng
//! [`Clock`]: clock::Clock
//! [`Rng`]: rng::Rng
//! [`Network`]: net::Network
//! [`VirtualNetwork`]: net::VirtualNetwork
//! [`FaultSchedule`]: fault::FaultSchedule
//! [`Outcome`]: outcome::Outcome
//! [`shrink`]: shrink::shrink
//! [`sweep`]: sweep::sweep
//! [`Reduction`]: shrink::Reduction
//! [`Repro`]: repro::Repro
//! [`pingpong`]: systems::pingpong
//! [`Recording`]: history::Recording
//! [`StateStore`]: store::StateStore
//! [`Trace`]: trace::Trace
//! [`World`]: world::World
//! [`Snapshot`]: world::Snapshot
//! [`StateHash`]: world::StateHash

pub mod clock;
pub mod diff;
pub mod event;
pub mod executor;
pub mod fault;
pub mod fork;
pub mod history;
pub mod net;
pub mod outcome;
pub mod repro;
pub mod rng;
pub mod shrink;
pub mod store;
pub mod sweep;
pub mod systems;
pub mod trace;
pub mod world;
