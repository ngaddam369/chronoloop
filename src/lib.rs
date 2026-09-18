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
//! an address. The content-addressed state history, the fault schedules, and the shrinking built on
//! top of them are not here yet.
//!
//! [`VirtualClock`]: clock::VirtualClock
//! [`EventQueue`]: event::EventQueue
//! [`Executor`]: executor::Executor
//! [`SeededRng`]: rng::SeededRng
//! [`Clock`]: clock::Clock
//! [`Rng`]: rng::Rng
//! [`Network`]: net::Network
//! [`VirtualNetwork`]: net::VirtualNetwork
//! [`pingpong`]: systems::pingpong
//! [`Recording`]: history::Recording

pub mod clock;
pub mod event;
pub mod executor;
pub mod history;
pub mod net;
pub mod rng;
pub mod systems;
