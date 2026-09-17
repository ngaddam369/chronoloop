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
//! Scaffold only. The deterministic core is built in phases from here.

pub mod clock;
pub mod event;
pub mod executor;
pub mod rng;
